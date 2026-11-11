import { test, mock } from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { fork } from 'node:child_process';
import { once } from 'node:events';
import { randomUUID } from 'node:crypto';
import { Collection } from 'mongodb';
import { MongoMetricsReader } from '../../src/storage/adapters/mongo-metrics.reader.js';
import { HLS_METRICS_RETENTION_SECONDS } from '../../src/types/hls-metric.types.js';
import { mongoConfig } from '../../src/config/mongo.config.js';
import { redisConfig, redisClient } from '../../src/config/redis.config.js';
import { createApplicationRuntime } from '../../src/application/create-runtime.js';
import { HlsConsumer } from '../../src/consumers/hls.consumer.js';
import { UnsortedBatchQueue } from '../../src/services/batch-queue.service.js';
import { parseHlsStreamMessage } from '../../src/utils/index.js';

const enabled = process.env['RUN_INTEGRATION'] === '1';
async function until(check: () => Promise<boolean> | boolean, timeoutMs = 10000) {
  const deadline = Date.now() + timeoutMs;
  while (!await check()) {
    if (Date.now() > deadline) throw new Error('Timed out waiting for pipeline');
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}
function gate() {
  let resolve!: () => void;
  const promise = new Promise<void>((r) => { resolve = r; });
  return { promise, resolve };
}

test('real Redis → HTTP HLS → MongoDB → XACK reliability', { skip: !enabled, timeout: 120000 }, async (t) => {
  // Never drop or reuse an application database; Redis cleanup uses exact unique keys.
  const database = process.env['MONGO_DATABASE'];
  assert.match(database ?? '', /^gate_test_[a-zA-Z0-9_]+$/);
  assert.ok(process.env['MONGO_URI'], 'Explicit test MongoDB URI required');
  process.env['STATUS_FEED_NAMESPACE'] = `pipeline-${randomUUID()}`;
  process.env['HLS_PROBE_NAMESPACE'] = `pipeline-${randomUUID()}`;
  const runtime = createApplicationRuntime();
  const { store: metricsStore, batchQueueManager } = runtime;
  const prefix = `integration-${randomUUID()}`;
  let mongoConnected = false;
  const keys: string[] = [...runtime.statusEvents.keys, ...runtime.probeScheduleStore.keys, runtime.playlistFreshness.key];
  const consumers: HlsConsumer[] = [];
  const tasks: Promise<void>[] = [];
  const http = createServer((request, response) => {
    if (request.url === '/live.m3u8') response.end('#EXTM3U\n#EXTINF:1,\nsegment.ts\n');
    else if (request.url === '/segment.ts') response.end(Buffer.from([1, 2, 3]));
    else { response.statusCode = 503; response.end('offline'); }
  });
  t.after(async () => {
    mock.restoreAll();
    await Promise.allSettled(consumers.map((c) => c.stop()));
    await Promise.allSettled(tasks);
    await batchQueueManager.stopAll();
    if (keys.length && redisClient.status === 'ready') await redisClient.del(...keys);
    if (mongoConnected) await mongoConfig.getDb().dropDatabase();
    await Promise.allSettled([mongoConfig.disconnect(), redisConfig.disconnect()]);
    http.closeAllConnections();
    await new Promise<void>((resolve) => http.close(() => resolve()));
  });
  await mongoConfig.connect();
  mongoConnected = true;
  // Seed the previous 14-day TTL to verify startup shortens it to the current window in place.
  await mongoConfig.getDb().collection('hls_stream_metrics').createIndex({ ingestedAt: 1 }, { expireAfterSeconds: 1209600 });
  await Promise.all([metricsStore.connect(), redisConfig.connect()]);
  http.listen(0, '127.0.0.1');
  await once(http, 'listening');
  const address = http.address();
  assert.ok(address && typeof address !== 'string');
  const base = `http://127.0.0.1:${address.port}`;
  const db = mongoConfig.getDb();
  const collection = db.collection('hls_stream_metrics');
  const server = (suffix: string) => {
    const ip = `${prefix}-${suffix}`;
    keys.push(`hls:streams:${ip}`);
    return ip;
  };
  const fields = (ip: string, path = '/live.m3u8') => ['streamId', 'channel', 'streamUrl', base + path, 'serverIp', ip, 'createdAt', new Date().toISOString()];
  const publish = (ip: string, path?: string) => redisClient.xadd(`hls:streams:${ip}`, '*', ...fields(ip, path));
  const pending = async (ip: string, group: string) => Number((await redisClient.xpending(`hls:streams:${ip}`, group) as unknown[])[0]);
  const start = (ip: string, group: string) => {
    const consumer = new HlsConsumer({ source: runtime.messaging, processor: runtime.processor });
    consumers.push(consumer);
    const task = consumer.consume(ip, undefined, { groupName: group, blockTimeoutMs: 20, recoveryIntervalMs: 20, pendingMinIdleMs: 50 });
    tasks.push(task);
    // Observe rejection promptly while preserving it for the test.
    void task.catch(() => {});
    return consumer;
  };

  await t.test('history hides records past the retention cutoff even before the TTL monitor deletes them', async () => {
    // This collection has no TTL index; the reader enforces the same cutoff the TTL deletes at.
    const name = 'retention_visibility';
    const raw = db.collection(name);
    const now = new Date();
    const cutoff = new Date(now.getTime() - HLS_METRICS_RETENTION_SECONDS * 1000);
    const observation = { serverIp: 'server', streamId: 'channel', streamUrl: base + '/live.m3u8',
      createdAt: now, receivedAt: now, checkedAt: now, pipelineDelayMs: 0, redisStreamKey: 'fixture' };
    await raw.insertMany([
      { ...observation, redisMessageId: 'expired', ingestedAt: new Date(cutoff.getTime() - 1) },
      { ...observation, redisMessageId: 'boundary', ingestedAt: cutoff },
      { ...observation, redisMessageId: 'after', ingestedAt: new Date(cutoff.getTime() + 1) },
      { ...observation, redisMessageId: 'fresh', ingestedAt: now, checkedAt: new Date(now.getTime() - 1) },
      { ...observation, redisMessageId: 'other-server', serverIp: 'other', ingestedAt: now },
      { ...observation, redisMessageId: 'other-stream', streamId: 'other', ingestedAt: now },
      { ...observation, redisMessageId: 'missing-ingestion-date' },
    ]);
    const reader = new MongoMetricsReader(db, () => now);
    const records = await reader.readHistory({ serverIp: 'server', streamId: 'channel', from: new Date('2000-01-01') }, name);
    assert.deepEqual(records.map((record) => record.redisMessageId), ['after', 'fresh']);
    const exact = await reader.readHistory({ serverIp: 'server', streamId: 'channel', from: now, to: now }, name);
    assert.deepEqual(exact.map((record) => record.redisMessageId), ['after']);
    assert.equal(await raw.countDocuments(), 7, 'expired rows are filtered, not deleted by the reader');
  });

  await t.test('startup preserves analytics indexes and sets the ingestedAt TTL to the retention window', async () => {
    const indexes = await collection.listIndexes().toArray();
    assert.deepEqual(indexes.filter((index) => index.expireAfterSeconds !== undefined).map((index) => [index.name, index.expireAfterSeconds]),
      [['ingestedAt_1', HLS_METRICS_RETENTION_SECONDS]]);
    assert.deepEqual(indexes.find((index) => index.key['checkedAt'] === -1)?.key,
      { serverIp: 1, streamId: 1, checkedAt: -1 });
  });

  await t.test('online/offline results persist and two custom groups ACK independently', async () => {
    const ip = server('health');
    await publish(ip);
    await publish(ip, '/offline.m3u8');
    const a = start(ip, 'group-a');
    const b = start(ip, 'group-b');
    await until(() => batchQueueManager.getQueue(ip, 'group-a').getStats().itemCount >= 2 && batchQueueManager.getQueue(ip, 'group-b').getStats().itemCount >= 2);
    assert.equal(await pending(ip, 'group-a'), 2);
    assert.equal(await pending(ip, 'group-b'), 2);
    await Promise.all([a.stop(), b.stop()]);
    await batchQueueManager.flushAll();
    assert.equal(await pending(ip, 'group-a'), 0);
    assert.equal(await pending(ip, 'group-b'), 0);
    const docs = await collection.find({ redisStreamKey: `hls:streams:${ip}` }).toArray();
    assert.equal(docs.length, 2);
    assert.ok(docs.every((doc) => doc['checkedAt'] instanceof Date && doc['ingestedAt'] instanceof Date));
    assert.deepEqual(docs.map((d) => d['health'].status).sort(), [0, 1]);
  });

  await t.test('MongoDB rejects a write: pending survives, retry persists once and ACKs', async () => {
    const ip = server('write-failure');
    const group = 'writer';
    const name = 'rejected_metrics';
    await db.createCollection(name, { validator: { streamId: { $type: 'int' } } });
    const consumer = new HlsConsumer({ source: runtime.messaging, processor: runtime.processor });
    await consumer.createGroup(ip, group);
    const id = await publish(ip);
    const raw = await redisClient.xreadgroup('GROUP', group, 'reader', 'STREAMS', `hls:streams:${ip}`, '>') as [string, [string, string[]][]][];
    const q = new UnsortedBatchQueue({ store: runtime.store, acknowledger: runtime.messaging, serverIp: ip, groupName: group, collectionName: name });
    t.after(() => q.stop());
    await q.push(parseHlsStreamMessage(raw[0]![1][0]![0], raw[0]![1][0]![1]));
    await assert.rejects(q.flush());
    assert.equal(await pending(ip, group), 1);
    assert.equal(await db.collection(name).countDocuments(), 0);
    await db.command({ collMod: name, validator: {} });
    await q.drain();
    assert.equal(await pending(ip, group), 0);
    assert.equal(await db.collection(name).countDocuments({ redisMessageId: id }), 1);
    await q.stop();
  });

  await t.test('ACK failure before or after Redis applies it safely retries persisted records', async () => {
    for (const ambiguous of [false, true]) {
      const ip = server(`ack-${ambiguous}`);
      const group = 'acker';
      const consumer = new HlsConsumer({ source: runtime.messaging, processor: runtime.processor });
      await consumer.createGroup(ip, group);
      await publish(ip);
      const raw = await redisClient.xreadgroup('GROUP', group, 'reader', 'STREAMS', `hls:streams:${ip}`, '>') as [string, [string, string[]][]][];
      const q = new UnsortedBatchQueue({ store: runtime.store, acknowledger: runtime.messaging, serverIp: ip, groupName: group });
      const item = parseHlsStreamMessage(raw[0]![1][0]![0], raw[0]![1][0]![1]);
      await q.push(item);
      const original = redisClient.xack.bind(redisClient);
      const fault = mock.method(redisClient, 'xack', async (...args: Parameters<typeof original>) => {
        if (ambiguous) await original(...args);
        throw new Error('Injected lost ACK response');
      });
      try { await assert.rejects(q.flush(), /lost ACK/); }
      finally { fault.mock.restore(); }
      assert.equal(await collection.countDocuments({ redisStreamKey: q.streamKey }), 1);
      assert.equal(await pending(ip, group), ambiguous ? 0 : 1);
      // Repeat delivery is intentionally included in the retried batch.
      await q.push(item);
      await q.stop();
      assert.equal(await collection.countDocuments({ redisStreamKey: q.streamKey }), 1);
      assert.equal(await pending(ip, group), 0);
    }
  });

  await t.test('SIGKILL after buffering loses RAM; replacement reclaims pending and persists', async () => {
    const ip = server('crash');
    const group = 'recover';
    await publish(ip);
    const child = fork(new URL('./crash-worker.ts', import.meta.url), [ip, group], { execArgv: ['--import', 'tsx'], stdio: ['ignore', 'ignore', 'pipe', 'ipc'] });
    t.after(() => { if (child.exitCode === null) child.kill('SIGKILL'); });
    let errors = '';
    child.stderr?.on('data', (data) => { errors += data; });
    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`Worker did not buffer: ${errors}`)), 10000);
      child.once('message', () => { clearTimeout(timer); resolve(); });
      child.once('error', (error) => { clearTimeout(timer); reject(error); });
      child.once('exit', () => { clearTimeout(timer); reject(new Error(`Worker exited early: ${errors}`)); });
    });
    const exited = once(child, 'exit');
    child.kill('SIGKILL');
    await exited;
    assert.equal(await pending(ip, group), 1);
    assert.equal(await collection.countDocuments({ redisStreamKey: `hls:streams:${ip}` }), 0);
    const replacement = start(ip, group);
    await until(() => batchQueueManager.getQueue(ip, group).getStats().itemCount > 0);
    await replacement.stop();
    await batchQueueManager.flushAll();
    assert.equal(await pending(ip, group), 0);
    assert.equal(await collection.countDocuments({ redisStreamKey: `hls:streams:${ip}` }), 1);
  });

  await t.test('shutdown waits for active Mongo write before draining and ACKing', async () => {
    const ip = server('shutdown');
    const group = 'shutdown';
    await publish(ip);
    const consumer = start(ip, group);
    await until(() => batchQueueManager.getQueue(ip, group).getStats().itemCount > 0);
    const entered = gate();
    const release = gate();
    const original = Collection.prototype.bulkWrite;
    const fault = mock.method(Collection.prototype, 'bulkWrite', async function (this: Collection, ...args: Parameters<typeof original>) {
      entered.resolve();
      await release.promise;
      return original.apply(this, args);
    });
    try {
      const flush = batchQueueManager.flushAll();
      await entered.promise;
      let stopped = false;
      const shutdown = (async () => { await consumer.stop(); await batchQueueManager.stopAll(); stopped = true; })();
      await new Promise((resolve) => setTimeout(resolve, 20));
      assert.equal(stopped, false);
      assert.equal(await pending(ip, group), 1);
      release.resolve();
      await Promise.all([flush, shutdown]);
      assert.equal(await pending(ip, group), 0);
      assert.equal(await collection.countDocuments({ redisStreamKey: `hls:streams:${ip}` }), 1);
    } finally { release.resolve(); fault.mock.restore(); }
  });
});
