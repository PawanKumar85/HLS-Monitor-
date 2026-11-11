import { test } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Redis } from 'ioredis';
import { MongoClient } from 'mongodb';
import { JOB_RETENTION_MS, RedisProbeScheduleStore } from '../../src/monitoring/adapters/redis-probe-schedule.store.js';
import { RedisPlaylistFreshnessStore } from '../../src/monitoring/adapters/redis-playlist-freshness.store.js';
import { normalizeProbeConfig } from '../../src/monitoring/probe-contracts.js';
const enabled = process.env['RUN_INTEGRATION'] === '1';
const client = () => new Redis({ host: process.env['REDIS_HOST'] ?? '127.0.0.1', port: Number(process.env['REDIS_PORT'] ?? 6379), password: process.env['REDIS_PASSWORD'] ?? 'admin' });
async function until(check: () => Promise<boolean> | boolean, timeoutMs = 10000) {
  const deadline = Date.now() + timeoutMs;
  while (!await check()) { if (Date.now() > deadline) throw new Error('Monitoring integration timeout'); await new Promise(resolve => setTimeout(resolve, 20)); }
}

test('atomic Redis scheduling suppresses duplicate queued/pending jobs across instances and expired leases', { skip: !enabled, timeout: 10000 }, async t => {
  const redis = client(); const namespace = `test-${randomUUID()}`;
  const first = new RedisProbeScheduleStore(redis, namespace); const second = new RedisProbeScheduleStore(redis, namespace);
  const config = normalizeProbeConfig({ serverIp: namespace, streamId: 'one', streamUrl: 'https://example.test/live', intervalMs: 1000 });
  const stream = `hls:streams:${config.serverIp}`; const id = first.identity(config.serverIp, config.streamId);
  t.after(async () => { t.mock.restoreAll(); await redis.del(...first.keys, stream); await redis.quit(); });
  await first.upsert(config); assert.equal((await first.due(100)).length, 1);
  const results = await Promise.all(Array.from({ length: 20 }, (_, index) => (index % 2 ? first : second).enqueueDue(config, 0)));
  assert.equal(results.filter(value => value === 'queued').length, 1); assert.equal(await redis.xlen(stream), 1);
  const lease = JSON.parse((await redis.hget(first.keys[2]!, id))!); lease.expiresAt = 0;
  await redis.hset(first.keys[2]!, id, JSON.stringify(lease)); await redis.zadd(first.keys[1]!, 0, id);
  assert.equal(await second.enqueueDue(config, 0), 'busy'); // Still unread, even after expiry.
  const received = await redis.xreadgroup('GROUP', first.groupName, 'crashed-worker', 'COUNT', 1, 'STREAMS', stream, '>') as [string, [string, string[]][]][];
  const jobId = received[0]![1][0]![0];
  await redis.zadd(first.keys[1]!, 0, id); assert.equal(await first.enqueueDue(config, 0), 'busy'); // Pending.
  const recovered = await redis.xautoclaim(stream, first.groupName, 'replacement', 0, '0-0', 'COUNT', 1) as [string, [string, string[]][]];
  assert.equal(recovered[1][0]?.[0], jobId);
  await redis.xack(stream, first.groupName, jobId); await redis.zadd(first.keys[1]!, 0, id);
  assert.equal(await second.enqueueDue(config, 0), 'queued'); assert.equal(await redis.xlen(stream), 2);
  const changed = { ...config, enabled: false }; await first.upsert(changed);
  assert.equal((await first.due(100)).length, 0); assert.equal(await second.enqueueDue(config, 0), 'changed');
  assert.equal((await first.get(config.serverIp, config.streamId))?.enabled, false);
});

test('enqueue trims job messages older than the retention window, keeping recent ones', { skip: !enabled, timeout: 10000 }, async t => {
  const redis = client(); const namespace = `test-${randomUUID()}`;
  const store = new RedisProbeScheduleStore(redis, namespace);
  const config = normalizeProbeConfig({ serverIp: namespace, streamId: 'one', streamUrl: 'https://example.test/live', intervalMs: 1000 });
  const stream = `hls:streams:${config.serverIp}`;
  t.after(async () => { await redis.del(...store.keys, stream); await redis.quit(); });
  const now = Date.now();
  await redis.xadd(stream, `${now - JOB_RETENTION_MS - 60_000}-0`, 'old', '1');   // past retention: trimmed
  await redis.xadd(stream, `${now - JOB_RETENTION_MS + 60_000}-0`, 'recent', '1'); // inside retention: kept
  await store.upsert(config); await redis.zadd(store.keys[1]!, 0, store.identity(config.serverIp, config.streamId));
  assert.equal(await store.enqueueDue(config, 0), 'queued');
  const ids = (await redis.xrange(stream, '-', '+')).map(([id]) => id);
  assert.equal(ids.length, 2); assert.equal(ids[0], `${now - JOB_RETENTION_MS + 60_000}-0`);
});

test('dispatch failure after XADD retains a reservation so retry cannot enqueue another copy', { skip: !enabled, timeout: 10000 }, async t => {
  const redis = client(); const namespace = `test-${randomUUID()}`;
  const store = new RedisProbeScheduleStore(redis, namespace);
  const config = normalizeProbeConfig({ serverIp: namespace, streamId: 'one', streamUrl: 'https://example.test/live' });
  const stream = `hls:streams:${config.serverIp}`;
  t.after(async () => { t.mock.restoreAll(); await redis.del(...store.keys, stream); await redis.quit(); });
  await store.upsert(config);
  const original = redis.eval.bind(redis);
  const fault = t.mock.method(redis, 'eval', async (...args: Parameters<typeof original>) => {
    const script = String(args[0]);
    const finish = "redis.call('ZADD', KEYS[2], now + config.intervalMs + tonumber(ARGV[5]), ARGV[1])";
    if (script.includes(finish)) args[0] = script.replace(finish, "error('injected failure after enqueue')");
    return original(...args);
  });
  try { await assert.rejects(store.enqueueDue(config, 0), /injected failure/); } finally { fault.mock.restore(); }
  assert.equal(await redis.xlen(stream), 1);
  assert.equal(await store.enqueueDue(config, 0), 'busy'); assert.equal(await redis.xlen(stream), 1);
});

test('Redis freshness survives adapter restart and distinguishes frozen, resumed and slow-target playlists', { skip: !enabled, timeout: 10000 }, async t => {
  const redis = client(); const namespace = `test-${randomUUID()}`;
  const store = new RedisPlaylistFreshnessStore(redis, namespace);
  t.after(async () => { await redis.del(store.key); await redis.quit(); });
  const observe = (fingerprint: string, targetDurationMs = 2000) => store.observe('s', 'one', 'https://example.test/live', { fingerprint, targetDurationMs }, 1000);
  assert.equal(await observe('one'), true);
  const age = async () => {
    const values = await redis.hgetall(store.key); const id = Object.keys(values)[0]!;
    const state = JSON.parse(values[id]!); state.changedAt = Date.now() - 60_000; await redis.hset(store.key, id, JSON.stringify(state));
  };
  await age(); assert.equal(await observe('one'), false);
  const restarted = new RedisPlaylistFreshnessStore(redis, namespace);
  assert.equal(await restarted.observe('s', 'one', 'https://example.test/live', { fingerprint: 'one', targetDurationMs: 2000 }, 1000), false);
  assert.equal(await observe('two'), true);
  await age(); assert.equal(await observe('two', 100_000), true); // threshold >= 3 target durations.
});

test('Redis failure streak: one blip stays online, two in a row go offline, a success resets it', { skip: !enabled, timeout: 10000 }, async t => {
  const redis = client(); const namespace = `test-${randomUUID()}`;
  const store = new RedisPlaylistFreshnessStore(redis, namespace);
  t.after(async () => { await redis.del(store.key); await redis.quit(); });
  const url = 'https://example.test/live';
  assert.equal(await store.fail('s', 'new', url), false); // never seen online: offline at once
  const ok = () => store.observe('s', 'one', url, { fingerprint: 'a', targetDurationMs: 2000 }, 30_000);
  assert.equal(await ok(), true);
  assert.equal(await store.fail('s', 'one', url), true);  // blip 1: still online
  assert.equal(await store.fail('s', 'one', url), false); // blip 2 in a row: offline
  assert.equal(await store.fail('s', 'one', url), false); // stays offline
  assert.equal(await ok(), true);                          // one success: online again
  assert.equal(await store.fail('s', 'one', url), true);  // streak was reset
  assert.equal(await store.fail('s', 'one', 'https://example.test/other'), false); // URL changed: no carried-over online
});

test('JSON-enabled runtime repeatedly probes, persists offline/frozen/recovered history and publishes SSE snapshots', { skip: !enabled, timeout: 20000 }, async t => {
  assert.match(process.env['MONGO_DATABASE'] ?? '', /^gate_test_[a-zA-Z0-9_]+$/); assert.ok(process.env['MONGO_URI']);
  const namespace = `monitor-${randomUUID()}`;
  const database = `gate_test_monitor_${randomUUID().replaceAll('-', '')}`;
  const directory = await mkdtemp(join(tmpdir(), 'gate-monitor-runtime-')); const file = join(directory, 'streams.json');
  let sequence = 1;
  const http = createServer((request, response) => {
    if (request.url === '/live.m3u8') response.end(`#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:${sequence}\n#EXTINF:1,\nsegment.ts\n`);
    else if (request.url === '/segment.ts') response.end(Buffer.from([1, 2, 3]));
    else { response.statusCode = 404; response.end(); }
  });
  http.listen(0, '127.0.0.1'); await once(http, 'listening');
  const address = http.address(); assert.ok(address && typeof address !== 'string');
  await writeFile(file, JSON.stringify([{ serverIp: namespace, streamId: 'one', streamUrl: `http://127.0.0.1:${address.port}/live.m3u8`, intervalMs: 1000, staleAfterMs: 1000 }]));
  process.env['MONGO_DATABASE'] = database; process.env['HLS_PROBE_NAMESPACE'] = namespace;
  process.env['STATUS_FEED_NAMESPACE'] = namespace; process.env['HLS_MONITORING_ENABLED'] = 'true'; process.env['HLS_STREAMS_FILE'] = file;
  const { createApplicationRuntime } = await import('../../src/application/create-runtime.js');
  const { buildApp } = await import('../../src/app.js');
  const runtime = createApplicationRuntime();
  const control = client(); const mongo = new MongoClient(process.env['MONGO_URI']!); await mongo.connect();
  const records = mongo.db(database).collection('hls_stream_metrics');
  // Other integration files own the shared DLQ registry; these probes produce no
  // processing failures, so leave the independently-tested archive projector idle.
  const { failureProjector: _projector, ...services } = runtime;
  const app = await buildApp({ ...services, auth: 'disabled' });
  t.after(async () => {
    try { await app.close(); } finally {
      await control.del(...runtime.probeScheduleStore.keys, runtime.playlistFreshness.key, ...runtime.statusEvents.keys, `hls:streams:${namespace}`);
      await mongo.db(database).dropDatabase(); await mongo.close(); await control.quit();
      http.closeAllConnections(); await new Promise<void>(resolve => http.close(() => resolve()));
      await rm(directory, { recursive: true });
    }
  });
  await runtime.dbManager.connectAll(); await app.ready();
  await until(() => runtime.batchQueueManager.getQueue(namespace).getStats().itemCount > 0);
  await new Promise(resolve => setTimeout(resolve, 1200));
  assert.equal(await control.xlen(`hls:streams:${namespace}`), 1, 'Queued/pending probe must not duplicate before persistence');
  await runtime.batchQueueManager.flushAll(); assert.equal(await records.countDocuments(), 1);
  const states = await control.hgetall(runtime.playlistFreshness.key); const stateId = Object.keys(states)[0]!;
  const state = JSON.parse(states[stateId]!); state.changedAt = Date.now() - 60_000;
  await control.hset(runtime.playlistFreshness.key, stateId, JSON.stringify(state));
  await until(async () => (await runtime.statusEvents.snapshot()).streams[0]?.status === 0);
  sequence = 2;
  await until(() => runtime.batchQueueManager.getQueue(namespace).getStats().itemCount > 0);
  await runtime.batchQueueManager.flushAll();
  assert.equal(await records.countDocuments({ 'health.status': 0 }), 1);
  await until(async () => (await runtime.statusEvents.snapshot()).streams[0]?.status === 1);
  await until(() => runtime.batchQueueManager.getQueue(namespace).getStats().itemCount > 0);
  await runtime.batchQueueManager.flushAll();
  assert.equal(await records.countDocuments(), 3);
  const snapshot = await app.inject('/api/hls/status/snapshot'); assert.equal(snapshot.json().streams[0].status, 1);
  const pending = await control.xpending(`hls:streams:${namespace}`, 'hls-workers') as unknown[];
  assert.equal(Number(pending[0]), 0);
});
