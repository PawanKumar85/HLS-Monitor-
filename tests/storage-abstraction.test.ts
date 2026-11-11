import { test, mock } from 'node:test';
import assert from 'node:assert/strict';
import { UnsortedBatchQueue, ServerBatchQueueManager } from '../src/services/batch-queue.service.js';
import { DatabaseManager } from '../src/config/index.js';
import type { ManagedConnection } from '../src/ports/connection.js';
import type { MetricsStore, MetricsWriter } from '../src/storage/metrics-store.js';
import type { HlsStreamMetric } from '../src/types/hls-metric.types.js';
import { createMetricsStore } from '../src/storage/create-metrics-store.js';

const item = { messageId: '1-0', streamId: 'channel', serverIp: 'server', streamUrl: 'https://example.test/live.m3u8', createdAt: '2026-11-08', receivedAt: '2026-11-08', checkedAt: '2026-11-08', delay: 0 };

test('queue uses an injected writer and ACKs only after it resolves; failure retries', async () => {
  const events: string[] = [];
  let fail = true;
  let persisted: readonly HlsStreamMetric[] = [];
  const store: MetricsWriter = {
    async writeBatch(records, dataset) {
      events.push('write');
      assert.equal(dataset, 'observations');
      if (fail) throw new Error('SQL transaction failed');
      persisted = records;
      events.push('committed');
    },
  };
  const acknowledger = { async acknowledge() { events.push('ack'); return 1; } };
  const q = new UnsortedBatchQueue({ store, acknowledger, dataset: 'observations', serverIp: 'server' });
  try {
    await q.push(item);
    await q.push(item);
    await assert.rejects(q.flush(), /transaction failed/);
    assert.deepEqual(events, ['write']);
    assert.equal(q.getStats().itemCount, 2);
    fail = false;
    await q.stop();
    assert.deepEqual(events, ['write', 'write', 'committed', 'ack']);
    assert.equal(persisted.length, 1);
    assert.equal(persisted[0]?.redisStreamKey, 'hls:streams:server');
    assert.ok(persisted[0]?.checkedAt instanceof Date);
  } finally { mock.restoreAll(); }
});

test('server manager propagates the adapter and dataset to every queue', async () => {
  const calls: string[] = [];
  const store: MetricsWriter = { async writeBatch(records, dataset) { calls.push(`${dataset}/${records[0]?.serverIp}`); } };
  const acknowledger = { async acknowledge() { return 1; } };
  try {
    const manager = new ServerBatchQueueManager({ store, acknowledger, dataset: 'sql_history' });
    await manager.push(item);
    await manager.push({ ...item, serverIp: 'other' });
    await manager.stopAll();
    assert.deepEqual(calls.sort(), ['sql_history/other', 'sql_history/server']);
  } finally { mock.restoreAll(); }
});

test('connection manager uses injected storage for lifecycle and provider-neutral health', async () => {
  const events: string[] = [];
  let healthy = true;
  const storage: MetricsStore = {
    name: 'replacement-sql',
    async connect() { events.push('storage connect'); },
    async disconnect() { events.push('storage disconnect'); },
    async healthCheck() { if (!healthy) throw new Error('offline'); return true; },
    async writeBatch() {},
  };
  const redis = {
    async connect() { events.push('redis connect'); },
    async disconnect() { events.push('redis disconnect'); },
    healthCheck: async () => true,
  } satisfies ManagedConnection;
  const manager = new DatabaseManager(storage, redis);
  await manager.connectAll();
  assert.deepEqual(await manager.healthCheck(), { redis: true, storage: true, storageProvider: 'replacement-sql' });
  healthy = false;
  assert.equal((await manager.healthCheck()).storage, false);
  await manager.disconnectAll();
  assert.deepEqual(events, ['redis connect', 'storage connect', 'redis disconnect', 'storage disconnect']);
});

test('provider selection rejects unimplemented databases instead of falling back silently', () => {
  assert.throws(() => createMetricsStore('postgresql'), /Unsupported METRICS_STORE/);
});

test('connection manager reports close failures while attempting every connection', async () => {
  const calls: string[] = [];
  const manager = new DatabaseManager({
    name: 'test', async connect() {}, async healthCheck() { return true; },
    async disconnect() { calls.push('storage'); throw new Error('storage close failed'); },
  }, {
    async connect() {}, async healthCheck() { return true; },
    async disconnect() { calls.push('redis'); throw new Error('redis close failed'); },
  });
  await assert.rejects(manager.disconnectAll(), (error) => error instanceof AggregateError && error.errors.length === 2);
  assert.deepEqual(calls, ['redis', 'storage']);
});
