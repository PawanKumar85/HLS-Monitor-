import { test } from 'node:test';
import assert from 'node:assert/strict';
import { HlsMessageProcessor } from '../src/services/hls-message-processor.service.js';
import { HlsConsumer } from '../src/consumers/hls.consumer.js';
import type { RawStreamMessage, StreamSource } from '../src/ports/messaging.js';
import type { HlsConsumedStream } from '../src/types/stream.types.js';

const entry: RawStreamMessage = ['1-0', ['streamId', 'channel', 'streamUrl', 'https://example.test/a.m3u8', 'serverIp', 'untrusted', 'createdAt', '2026-11-08T00:00:00Z']];

test('message processor handles offline results through only checker, publisher and sink ports', async () => {
  const calls: string[] = [];
  let queued: HlsConsumedStream | undefined;
  const processor = new HlsMessageProcessor(
    { async check(url) { assert.equal(url, 'https://example.test/a.m3u8'); calls.push('check'); return { status: 0, latency: 3000 }; } },
    { publish(event) { calls.push('publish'); assert.equal(event.data.serverIp, 'server'); } },
    { async push(message, group) { calls.push('queue'); queued = message; assert.equal(group, 'custom'); } },
  );
  await processor.process('server', entry, 'custom');
  assert.deepEqual(calls, ['check', 'publish', 'queue']);
  assert.equal(queued?.health?.status, 0);
  assert.equal(queued?.serverIp, 'server');
  assert.ok(Number.isFinite(Date.parse(queued!.checkedAt!)));
});

test('unexpected checker errors propagate without publishing or queueing', async () => {
  let touched = false;
  const processor = new HlsMessageProcessor(
    { async check() { throw new Error('checker failed'); } },
    { publish() { touched = true; } },
    { async push() { touched = true; } },
  );
  await assert.rejects(processor.process('server', entry, 'custom'), /checker failed/);
  assert.equal(touched, false);
});

test('custom handler replaces queue processing and owns persistence/ACK contract', async () => {
  let handled = false;
  const processor = new HlsMessageProcessor(
    { async check() { return { status: 1, latency: 1 }; } },
    { publish() {} },
    { async push() { throw new Error('queue must not run'); } },
  );
  await processor.process('server', entry, 'custom', async (message, health) => {
    handled = true;
    assert.equal(message.health, health);
  });
  assert.equal(handled, true);
});

test('consumer runs with a completely driver-free source and processor', async () => {
  const calls: string[] = [];
  let consumer!: HlsConsumer;
  let shutdown!: Promise<void>;
  const source: StreamSource = {
    async ensureGroup(key, group) { calls.push('group'); assert.equal(key, 'hls:streams:server'); assert.equal(group, 'test'); },
    createReader() { return {
      async connect() { calls.push('connect'); },
      async claim() { calls.push('claim'); return { cursor: '0-0', messages: [], deletedIds: [] }; },
      async read() { calls.push('read'); return [entry]; },
      close() { calls.push('close'); },
    }; },
  };
  consumer = new HlsConsumer({ source, processor: { async process(server, message, group) {
    calls.push('process');
    assert.equal(server, 'server'); assert.equal(message, entry); assert.equal(group, 'test');
    shutdown = consumer.stop();
  } } });
  await consumer.consume('server', undefined, { groupName: 'test' });
  await shutdown;
  assert.deepEqual(calls.slice(0, 5), ['group', 'connect', 'claim', 'read', 'process']);
  assert.equal(calls.at(-1), 'close');
});

for (const mode of ['throw', 'reject'] as const) {
  test(`publication ${mode} logs only redacted text while queue processing continues`, async t => {
    const logs: unknown[][] = [];
    t.mock.method(console, 'error', (...args: unknown[]) => { logs.push(args); });
    const error = new Error('Failed https://url-user:url-pass@example.test/live?token=query-value#private password=password-value Bearer bearer-value', { cause: new Error('private cause details') });
    let queued = 0;
    const processor = new HlsMessageProcessor(
      { async check() { return { status: 1, latency: 1 }; } },
      { publish() { if (mode === 'throw') throw error; return Promise.reject(error); } },
      { async push() { queued++; } },
    );
    await processor.process('server', entry, 'custom');
    await new Promise(resolve => setImmediate(resolve));
    assert.equal(queued, 1); assert.equal(logs.length, 1);
    assert.ok(logs[0]!.every(value => typeof value === 'string'));
    const text = logs[0]!.join(' ');
    for (const secret of ['url-user', 'url-pass', 'query-value', 'private', 'password-value', 'bearer-value']) assert.ok(!text.includes(secret), secret);
    assert.match(text, /example\.test\/live/); assert.match(text, /\[redacted\]/);
  });
}

for (const source of ['sink', 'handler'] as const) {
  test(`unrelated ${source} RangeError remains a bounded retryable worker failure`, async () => {
    const { ProcessingFailure } = await import('../src/dlq/dead-letter.types.js');
    const fail = async () => { throw new RangeError('Unrelated range failure'); };
    const processor = new HlsMessageProcessor(
      { async check() { return { status: 1, latency: 1 }; } },
      { publish() {} },
      { push: fail },
    );
    await assert.rejects(processor.process('server', entry, 'custom', source === 'handler' ? fail : undefined), error => {
      assert.ok(error instanceof ProcessingFailure);
      assert.equal(error.stage, 'QUEUE'); assert.equal(error.code, 'WORKER_EXCEPTION');
      assert.equal(error.retryable, true); assert.equal(error.bounded, true); return true;
    });
  });
}

test('real queue oversized record yields a permanent capacity failure without buffering, writing or ACKing', async () => {
  const { UnsortedBatchQueue } = await import('../src/services/batch-queue.service.js');
  const { ProcessingFailure } = await import('../src/dlq/dead-letter.types.js');
  let writes = 0; let acknowledgments = 0;
  const queue = new UnsortedBatchQueue({ serverIp: 'server', groupName: 'custom', maxSizeBytes: 64,
    store: { async writeBatch() { writes++; } },
    acknowledger: { async acknowledge() { acknowledgments++; return 1; } },
  });
  const processor = new HlsMessageProcessor(
    { async check() { return { status: 1, latency: 1 }; } },
    { publish() {} },
    { async push(message, _group, context) { await queue.push(message, context); } },
  );
  try {
    await assert.rejects(processor.process('server', entry, 'custom'), error => {
      assert.ok(error instanceof ProcessingFailure);
      assert.equal(error.stage, 'QUEUE'); assert.equal(error.code, 'QUEUE_RECORD_TOO_LARGE');
      assert.equal(error.retryable, false); return true;
    });
    assert.equal(queue.getStats().itemCount, 0);
    assert.equal(writes, 0); assert.equal(acknowledgments, 0);
  } finally { await queue.stop(); }
});

test('a slow probe holds only its own slot: later entries finish while it is still running', async () => {
  const done: string[] = [];
  let consumer!: HlsConsumer;
  let releaseSlow!: () => void;
  const slow = new Promise<void>(resolve => { releaseSlow = resolve; });
  const inbox: RawStreamMessage[] = (['1-0', '2-0', '3-0', '4-0'] as const).map(id => [id, ['streamId', id]]);
  const counts: number[] = [];
  const source: StreamSource = {
    async ensureGroup() {},
    createReader() { return {
      async connect() {},
      async claim() { return { cursor: '0-0', messages: [], deletedIds: [] }; },
      // XREADGROUP '>' semantics: each entry once, never more than requested.
      async read(options) { counts.push(options.count); if (!inbox.length) await new Promise(r => setTimeout(r, 5)); return inbox.splice(0, options.count); },
      close() {},
    }; },
  };
  let active = 0; let peak = 0;
  consumer = new HlsConsumer({ source, processor: { async process(_server, [id]) {
    peak = Math.max(peak, ++active);
    if (id === '1-0') await slow; // a dead stream timing out
    active--; done.push(id);
    if (done.length === 3) { assert.deepEqual(done, ['2-0', '3-0', '4-0']); releaseSlow(); }
    if (done.length === 4) void consumer.stop();
  } } });
  await consumer.consume('server', undefined, { healthCheckConcurrency: 2 });
  assert.deepEqual(done, ['2-0', '3-0', '4-0', '1-0']); // with a batch barrier, 3-0 and 4-0 would wait for 1-0
  assert.equal(peak, 2);
  assert.ok(counts.every(count => count >= 1 && count <= 2), `read counts ${counts}`);
});
