import type { LightMyRequestResponse } from 'fastify';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { Redis } from 'ioredis';
import { MongoClient } from 'mongodb';
import { MongoStreamInventoryRepository } from '../../src/inventory/adapters/mongo-stream-inventory.repository.js';
import { StreamService } from '../../src/inventory/stream-service.js';
import { InventorySyncWorker } from '../../src/inventory/inventory-sync-worker.js';
import { RedisProbeScheduleStore } from '../../src/monitoring/adapters/redis-probe-schedule.store.js';
import type { StreamScheduleProjection } from '../../src/inventory/stream-inventory.js';
import { buildApp } from '../../src/app.js';
const enabled = process.env['RUN_INTEGRATION'] === '1';
const redisClient = () => new Redis({ host: process.env['REDIS_HOST'] ?? '127.0.0.1', port: Number(process.env['REDIS_PORT'] ?? 6379), password: process.env['REDIS_PASSWORD'] ?? 'admin' });
const config = { serverIp: 'server', streamId: 'one', streamUrl: 'https://user:secret@example.test/live?token=private', intervalMs: 1000, staleAfterMs: 1000, enabled: true };
const token = 'integration-operator-token-01234567890123456789';
const headers = { authorization: `Bearer ${token}` };
async function until(check: () => Promise<boolean> | boolean, timeoutMs = 15000) {
  const end = Date.now() + timeoutMs;
  while (!await check()) { if (Date.now() > end) throw new Error('Inventory integration timeout'); await new Promise(resolve => setTimeout(resolve, 20)); }
}
test('Mongo CRUD and pending marker stay atomic; Redis outage, stale sync and Redis reset recover safely', { skip: !enabled, timeout: 20000 }, async t => {
  assert.match(process.env['MONGO_DATABASE'] ?? '', /^gate_test_[a-zA-Z0-9_]+$/); assert.ok(process.env['MONGO_URI']);
  const mongo = new MongoClient(process.env['MONGO_URI']!); await mongo.connect();
  const db = mongo.db(`gate_test_inventory_${randomUUID().replaceAll('-', '')}`);
  const namespace = `api-${randomUUID()}`; const redis = redisClient();
  const repository = new MongoStreamInventoryRepository({ getDb: () => db }, namespace); const service = new StreamService(repository);
  const projection = new RedisProbeScheduleStore(redis, namespace);
  const unavailableClient = redisClient(); await unavailableClient.ping(); unavailableClient.disconnect();
  let target: StreamScheduleProjection = new RedisProbeScheduleStore(unavailableClient, namespace);
  let now = Date.now() + 100;
  const worker = new InventorySyncWorker(repository, { apply: record => target.apply(record) }, { pollMs: 60_000, now: () => new Date(now) });
  const app = await buildApp({ auth: 'disabled', streams: { service }, statusTransport: { async register() {} },
    dbManager: { async healthCheck() { return { redis: true, storage: true, storageProvider: 'mongodb' }; }, async disconnectAll() {} },
    hlsConsumer: { async stop() {} }, batchQueueManager: { async stopAll() {} },
  });
  t.after(async () => { await worker.stop(); await app.close(); try { await db.dropDatabase(); } finally { await mongo.close(); await redis.del(...projection.keys); await redis.quit(); } });
  const response = await app.inject({ method: 'POST', url: '/api/streams', headers, payload: config });
  assert.equal(response.statusCode, 201); const id = response.json().id;
  assert.equal(response.json().pendingSync, true); assert.ok(!response.body.includes('secret') && !response.body.includes('private'));
  const original = (await repository.get(id))!;
  assert.equal(original.version, 1); assert.equal(original.pendingSync, true);
  await worker.start(); assert.equal((await repository.get(id))!.pendingSync, true);
  target = projection; now += 2000; await worker.tick();
  assert.equal((await repository.get(id))!.pendingSync, false);
  assert.equal((await projection.get(config.serverIp, config.streamId))?.streamUrl, config.streamUrl);
  const due = await redis.zscore(projection.keys[1]!, projection.identity(config.serverIp, config.streamId));
  assert.equal(await projection.apply(original), 'applied');
  assert.equal(await redis.zscore(projection.keys[1]!, projection.identity(config.serverIp, config.streamId)), due, 'idempotent sync must not reset due time');
  const updates = await Promise.allSettled([service.patch(id, 1, { intervalMs: 2000 }), service.patch(id, 1, { intervalMs: 3000 })]);
  assert.equal(updates.filter(result => result.status === 'fulfilled').length, 1);
  assert.equal((await repository.get(id))?.version, 2);
  assert.equal(await repository.markSynced(id, 1, new Date()), false);
  const updated = (await repository.get(id))!; await projection.apply(updated);
  assert.equal(await projection.apply(original), 'stale');
  const deleted = await service.remove(id, 2); await projection.apply(deleted);
  assert.equal(await projection.get(config.serverIp, config.streamId), null);
  assert.equal(await projection.apply(updated), 'stale');
  assert.equal((await service.list()).items.length, 0);
  const recreated = await service.create(config); assert.equal(recreated.id, id); assert.equal(recreated.version, 4);
  await projection.apply(recreated); await repository.markSynced(id, 4, new Date());
  // Reconciliation must rebuild even records whose pending marker was already clear.
  await redis.del(...projection.keys); now += 2000;
  await worker.tick(); await worker.tick(); await worker.tick();
  assert.ok(await projection.get(config.serverIp, config.streamId));
  // Equal-version retry repairs partial projection damage instead of silently skipping it.
  await redis.del(projection.keys[0]!); await projection.apply(recreated);
  assert.ok(await projection.get(config.serverIp, config.streamId));
  await assert.rejects(projection.upsert(config), /Managed inventory/);
  assert.deepEqual(await projection.legacy(), []); // Managed entries are never re-migrated on boot.
  // Versions remain decimal strings in Lua beyond cjson's normal numeric precision.
  await projection.apply({ ...recreated, version: Number.MAX_SAFE_INTEGER });
  assert.equal(await projection.apply(recreated), 'stale');
});

test('API-created DB registrations enter the loop gradually with global probe limits; delete stops future dispatch', { skip: !enabled, timeout: 25000 }, async t => {
  assert.match(process.env['MONGO_DATABASE'] ?? '', /^gate_test_[a-zA-Z0-9_]+$/); assert.ok(process.env['MONGO_URI']);
  const namespace = `loop-${randomUUID()}`; const database = `gate_test_api_loop_${randomUUID().replaceAll('-', '')}`;
  let active = 0; let peak = 0; let sequence = 0;
  const http = createServer((request, response) => {
    active++; peak = Math.max(peak, active);
    response.once('finish', () => { active--; });
    setTimeout(() => {
      if (request.url?.startsWith('/live.m3u8')) response.end(`#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:${sequence++}\n#EXTINF:1,\nsegment.ts\n`);
      else response.end(Buffer.from([1, 2, 3]));
    }, 50);
  });
  http.listen(0, '127.0.0.1'); await once(http, 'listening'); const address = http.address(); assert.ok(address && typeof address !== 'string');
  process.env['MONGO_DATABASE'] = database; process.env['HLS_PROBE_NAMESPACE'] = namespace; process.env['STATUS_FEED_NAMESPACE'] = namespace;
  process.env['HLS_MONITORING_ENABLED'] = 'true'; delete process.env['HLS_STREAMS_FILE'];
  process.env['HLS_MAX_CONCURRENT_PROBES'] = '2'; process.env['HLS_SYNC_BATCH_SIZE'] = '4'; process.env['HLS_SYNC_POLL_MS'] = '100'; process.env['HLS_SCHEDULER_BATCH_SIZE'] = '2';
  const { createApplicationRuntime } = await import('../../src/application/create-runtime.js');
  const runtime = createApplicationRuntime(); const { failureProjector: _projector, ...services } = runtime;
  const app = await buildApp({ ...services, auth: 'disabled' }); const control = redisClient(); const mongo = new MongoClient(process.env['MONGO_URI']!); await mongo.connect();
  const servers = Array.from({ length: 4 }, (_, index) => `${namespace}-${index}`);
  t.after(async () => {
    try { await app.close(); } finally {
      const retryKeys: string[] = [];
      for (const server of servers) {
        const stream = `hls:streams:${server}`;
        for (const [id] of await control.xrange(stream, '-', '+')) retryKeys.push(runtime.deadLetters.metadataKey(stream, 'hls-workers', id));
      }
      await control.del(...runtime.probeScheduleStore.keys, runtime.playlistFreshness.key, ...runtime.statusEvents.keys, ...servers.map(server => `hls:streams:${server}`), ...retryKeys);
      await mongo.db(database).dropDatabase(); await mongo.close(); await control.quit();
      http.closeAllConnections(); await new Promise<void>(resolve => http.close(() => resolve()));
    }
  });
  await runtime.dbManager.connectAll(); await app.ready();
  const ids: string[] = [];
  for (const serverIp of servers) {
    const result: LightMyRequestResponse = await app.inject({ method: 'POST', url: '/api/streams', headers,
      payload: { ...config, serverIp, streamUrl: `http://127.0.0.1:${address.port}/live.m3u8` } });
    assert.equal(result.statusCode, 201); ids.push(result.json().id);
  }
  const records = mongo.db(database).collection('hls_stream_metrics');
  assert.equal(await records.countDocuments(), 0);
  await until(() => Object.values(runtime.batchQueueManager.getAllStats()).reduce((sum, stat) => sum + stat.itemCount, 0) >= 4);
  assert.ok(peak <= 2 && peak > 0); await runtime.batchQueueManager.flushAll();
  assert.ok(await records.countDocuments() >= 4);
  const removed = await app.inject({ method: 'DELETE', url: `/api/streams/${ids[0]}`, headers: { ...headers, 'if-match': '1' } });
  assert.equal(removed.statusCode, 202);
  await until(async () => (await runtime.streamInventory.get(ids[0]!))?.pendingSync === false);
  assert.equal(await runtime.probeScheduleStore.get(servers[0]!, config.streamId), null);
  const length = await control.xlen(`hls:streams:${servers[0]}`);
  await new Promise(resolve => setTimeout(resolve, 1300));
  assert.equal(await control.xlen(`hls:streams:${servers[0]}`), length);
  assert.ok(await records.countDocuments({ serverIp: servers[0] }) >= 1, 'deleting inventory must preserve metric history');
});
