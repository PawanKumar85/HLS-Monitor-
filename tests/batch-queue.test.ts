import { test, mock } from 'node:test';
import assert from 'node:assert/strict';
import { UnsortedBatchQueue, ServerBatchQueueManager, type BatchQueueOptions } from '../src/services/batch-queue.service.js';

const record = (id: string) => ({ messageId: id, streamId: id, streamUrl: 'https://example.test/a.m3u8', serverIp: 'server', createdAt: '2026-01-01', receivedAt: '2026-01-01', checkedAt: '2026-01-01', delay: 0 });
const bytes = Buffer.byteLength(JSON.stringify(record('1')));
function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((r) => { resolve = r; });
  return { promise, resolve };
}

// All persistence is stubbed; no Redis or MongoDB connection is opened.
test('queue concurrency and lifecycle', async (t) => {
  let write: () => Promise<unknown> = async () => ({});
  const writer = { writeBatch: mock.fn(async (_records: any[], _dataset: string) => { await write(); }) };
  const ack = mock.fn(async (_key: string, _group: string, ids: readonly string[]) => ids.length);
  const deps = { store: writer, acknowledger: { acknowledge: ack } };
  const makeQueue = (options: Partial<BatchQueueOptions> = {}) => new UnsortedBatchQueue({ ...deps, ...options });
  t.after(() => mock.restoreAll());

  await t.test('capacity includes in-flight batch and blocked pushes resume', async () => {
    const gate = deferred();
    write = () => gate.promise;
    const q = makeQueue({ maxSizeBytes: bytes * 2, thresholdRatio: 1 });
    await q.push(record('1'));
    const flushing = q.flush();
    await q.push(record('2'));
    let admitted = false;
    const waiting = q.push(record('3')).then(() => { admitted = true; });
    await new Promise(setImmediate);
    assert.equal(admitted, false);
    assert.equal(q.getStats().totalSizeBytes, bytes * 2);
    gate.resolve();
    await Promise.all([flushing, waiting]);
    await q.stop();
    assert.equal(q.getStats().totalSizeBytes, 0);
  });

  await t.test('flushAll drains records arriving during active write', async () => {
    const gate = deferred();
    write = () => gate.promise;
    const manager = new ServerBatchQueueManager(deps);
    await manager.push(record('1'));
    const active = manager.getQueue('server').flush();
    await manager.push(record('2'));
    const drain = manager.flushAll();
    gate.resolve();
    await Promise.all([active, drain]);
    assert.equal(manager.getQueue('server').getStats().totalSizeBytes, 0);
    await manager.stopAll();
    await assert.rejects(manager.push(record('3')), /stopping/);
    assert.throws(() => manager.getQueue('new-server'), /stopping/);
  });

  await t.test('stop rejects waiting and new pushes and is shared', async () => {
    const gate = deferred();
    write = () => gate.promise;
    const q = makeQueue({ maxSizeBytes: bytes, thresholdRatio: 1 });
    const active = q.push(record('1'));
    const waiting = assert.rejects(q.push(record('2')), /STOPPING|STOPPED/);
    const stopping = q.stop();
    assert.equal(q.stop(), stopping);
    await assert.rejects(q.push(record('3')), /STOPPING/);
    gate.resolve();
    await Promise.all([active, waiting, stopping]);
    assert.equal(q.getStats().state, 'STOPPED');
  });

  await t.test('failed write retains bounded records, does not ACK, and stop can retry', async () => {
    const gate = deferred();
    write = async () => { await gate.promise; throw new Error('write failed'); };
    const q = makeQueue({ maxSizeBytes: bytes * 2, thresholdRatio: 1 });
    await q.push(record('1'));
    const failed = assert.rejects(q.flush(), /write failed/);
    await q.push(record('2'));
    const ackCount = ack.mock.callCount();
    const stopping = assert.rejects(q.stop(), /write failed/);
    gate.resolve();
    await Promise.all([failed, stopping]);
    assert.equal(q.getStats().totalSizeBytes, bytes * 2);
    assert.equal(ack.mock.callCount(), ackCount);
    write = async () => ({});
    await q.stop();
    assert.equal(q.getStats().totalSizeBytes, 0);
  });

  await t.test('oversized records and invalid configuration are rejected', async () => {
    const q = makeQueue({ maxSizeBytes: bytes - 1 });
    await assert.rejects(q.push(record('1')), /exceeds/);
    assert.equal(q.getStats().totalSizeBytes, 0);
    await q.stop();
    assert.throws(() => makeQueue({ maxSizeBytes: 0 }), RangeError);
    assert.throws(() => makeQueue({ thresholdRatio: NaN }), RangeError);
  });

  await t.test('custom groups ACK after persistence and repeated IDs are acknowledged once', async () => {
    write = async () => ({});
    const manager = new ServerBatchQueueManager(deps);
    await manager.push(record('1'), 'custom-workers');
    await manager.push(record('1'), 'custom-workers');
    const before = ack.mock.callCount();
    await manager.flushAll();
    assert.equal(ack.mock.callCount(), before + 1);
    assert.deepEqual(ack.mock.calls.at(-1)?.arguments, ['hls:streams:server', 'custom-workers', ['1']]);
    await manager.stopAll();
  });

  await t.test('persist probe time as a Date and collapse repeated delivery within a batch', async () => {
    const q = makeQueue();
    const item = { ...record('probe'), checkedAt: '2026-11-08T10:00:00.000Z', health: { status: 0 as const, latency: 3000 } };
    await q.push(item);
    await q.push(item);
    const before = writer.writeBatch.mock.callCount();
    await q.stop();
    const operations = writer.writeBatch.mock.calls[before]?.arguments[0];
    assert.equal(operations.length, 1);
    const doc = operations[0];
    assert.equal(doc.checkedAt.toISOString(), item.checkedAt);
    assert.deepEqual(doc.health, { status: 0, segmentLatencyMs: 3000 });
    assert.ok(doc.ingestedAt instanceof Date);
  });

});

test('caller mutation cannot grow a buffered record or change its health during an outage', async () => {
  let failing = true;
  const writes: import('../src/types/hls-metric.types.js').HlsStreamMetric[][] = [];
  let acknowledgments = 0;
  const q = new UnsortedBatchQueue({ serverIp: 'server', maxSizeBytes: 2000, thresholdRatio: 1,
    store: { async writeBatch(records) { writes.push([...records]); if (failing) throw new Error('test outage'); } },
    acknowledger: { async acknowledge(_stream, _group, ids) { acknowledgments += ids.length; return ids.length; } },
  });
  const item = { ...record('1'), health: { status: 1 as 0 | 1, latency: 7 } };
  const context = { originalStreamKey: q.streamKey, originalMessageId: '1', serverIp: 'server', groupName: q.groupName, consumerName: 'worker', rawFields: ['x', 'small'] };
  try {
    await q.push(item, context);
    const acceptedBytes = q.getStats().totalSizeBytes;
    item.health.status = 0; item.health.latency = 999;
    context.rawFields[1] = 'x'.repeat(5000);
    await assert.rejects(q.flush(), /test outage/);
    assert.equal(q.getStats().totalSizeBytes, acceptedBytes);
    assert.ok(q.getStats().totalSizeBytes <= q.maxSizeBytes);
    assert.equal(acknowledgments, 0);
    failing = false; await q.stop();
    assert.equal(acknowledgments, 1);
    for (const batch of writes) assert.deepEqual(batch[0]?.health, { status: 1, segmentLatencyMs: 7 });
  } finally { failing = false; await q.stop(); }
});

test('partial retries isolate original context from both caller and failure handler mutations', async () => {
  const { PartialMetricsWriteError } = await import('../src/storage/persistence-errors.js');
  let failing = true;
  const received: import('../src/dlq/dead-letter.types.js').MessageContext[] = [];
  const q = new UnsortedBatchQueue({ serverIp: 'server', maxSizeBytes: 2000, thresholdRatio: 1,
    store: { async writeBatch() { if (failing) throw new PartialMetricsWriteError([{ index: 0, failure: { code: 'TEMPORARY', message: 'retry', retryable: true } }]); } },
    acknowledger: { async acknowledge(_stream, _group, ids) { return ids.length; } },
    failures: { async prepare() { return true; }, async failed(context) {
      received.push({ ...context, rawFields: [...context.rawFields] });
      context.rawFields.push('extra', 'x'.repeat(8000)); context.groupName = 'changed-by-handler'; return false;
    } },
  });
  const context = { originalStreamKey: q.streamKey, originalMessageId: '1', serverIp: 'server', groupName: q.groupName, consumerName: 'worker', rawFields: ['x', 'original'] };
  const expected = { ...context, rawFields: [...context.rawFields] };
  try {
    await q.push(record('1'), context);
    const acceptedBytes = q.getStats().totalSizeBytes;
    context.rawFields[1] = 'x'.repeat(5000); context.groupName = 'changed-by-caller'; context.originalMessageId = 'other';
    for (let attempt = 0; attempt < 2; attempt++) {
      await assert.rejects(q.flush(), /Unsettled persistence failures/);
      assert.equal(q.getStats().totalSizeBytes, acceptedBytes);
    }
    assert.deepEqual(received, [expected, expected]);
  } finally { failing = false; await q.stop(); }
});

test('push snapshots nested values before waiting for in-flight capacity', async () => {
  const first = { ...record('1'), health: { status: 1 as 0 | 1, latency: 7 } };
  const contextFor = (id: string) => ({ originalStreamKey: 'hls:streams:server', originalMessageId: id, serverIp: 'server', groupName: 'hls-workers', consumerName: 'worker', rawFields: ['x', 'small'] });
  const budget = Buffer.byteLength(JSON.stringify({ ...first, processingContext: contextFor('1') })) + 1;
  const gate = deferred();
  const writes: import('../src/types/hls-metric.types.js').HlsStreamMetric[][] = [];
  const q = new UnsortedBatchQueue({ serverIp: 'server', maxSizeBytes: budget, thresholdRatio: 1,
    store: { async writeBatch(records) { writes.push([...records]); await gate.promise; } },
    acknowledger: { async acknowledge(_stream, _group, ids) { return ids.length; } },
  });
  try {
    await q.push(first, contextFor('1'));
    const flushing = q.flush();
    const item = { ...record('2'), health: { status: 1 as 0 | 1, latency: 7 } };
    const context = contextFor('2');
    let admitted = false;
    const waiting = q.push(item, context).then(() => { admitted = true; });
    await new Promise(setImmediate); assert.equal(admitted, false);
    item.health.status = 0; context.rawFields[1] = 'x'.repeat(5000);
    gate.resolve(); await Promise.all([flushing, waiting]);
    assert.ok(q.getStats().totalSizeBytes <= budget);
    await q.stop();
    assert.deepEqual(writes[1]?.[0]?.health, { status: 1, segmentLatencyMs: 7 });
  } finally { gate.resolve(); await q.stop(); }
});
