import { test } from 'node:test';
import assert from 'node:assert/strict';
import type { Db, Filter } from 'mongodb';
import { MongoMetricsReader } from '../src/storage/adapters/mongo-metrics.reader.js';
import type { HlsStreamMetric } from '../src/types/hls-metric.types.js';

const now = new Date('2026-11-09T12:00:00Z');
const cutoff = new Date('2026-11-02T12:00:00Z'); // now − 7 days

test('history always scopes by stream and ingestion cutoff, with bounded newest-first reads', async () => {
  let filter!: Filter<HlsStreamMetric>;
  let projection: unknown;
  let sorting: unknown;
  let size: number | undefined;
  let dataset: string | undefined;
  const cursor = {
    sort(order: unknown) { sorting = order; return this; },
    limit(value: number) { size = value; return this; },
    async toArray() { return []; },
  };
  const db = { collection(name: string) { dataset = name; return {
    find(query: Filter<HlsStreamMetric>, options: unknown) { filter = query; projection = options; return cursor; },
  }; } } as unknown as Db;
  let time = now;
  const store = new MongoMetricsReader(db, () => time);
  const query = { serverIp: 'server', streamId: 'channel', from: new Date('2026-10-01'), to: now, limit: 50 };
  await store.readHistory(query);
  assert.deepEqual(filter, { serverIp: 'server', streamId: 'channel', ingestedAt: { $gt: cutoff }, checkedAt: { $gte: query.from, $lte: query.to } });
  assert.equal(dataset, 'hls_stream_metrics');
  assert.deepEqual(projection, { projection: { _id: 0 } });
  assert.deepEqual(sorting, { checkedAt: -1 });
  assert.equal(size, 50);
  await store.readHistory({ serverIp: 'server', streamId: 'channel', from: now, to: now });
  assert.deepEqual(filter.checkedAt, { $gte: now, $lte: now });
  time = new Date(now.getTime() + 1000);
  await store.readHistory({ serverIp: 'server', streamId: 'channel' }, 'custom_history');
  assert.deepEqual(filter.ingestedAt, { $gt: new Date(cutoff.getTime() + 1000) });
  assert.equal(size, 100);
  assert.equal(dataset, 'custom_history');
});

test('invalid or unbounded history queries are rejected before reaching the database', async () => {
  const store = new MongoMetricsReader({ collection() { throw new Error('unexpected query'); } } as unknown as Db);
  for (const limit of [0, -1, 501, Infinity, 1.5]) {
    await assert.rejects(store.readHistory({ serverIp: 'server', streamId: 'channel', limit }), RangeError);
  }
  await assert.rejects(store.readHistory({ serverIp: '', streamId: 'channel' }), RangeError);
  await assert.rejects(store.readHistory({ serverIp: 'server', streamId: 'channel', from: new Date(NaN) }), RangeError);
  await assert.rejects(store.readHistory({ serverIp: 'server', streamId: 'channel', from: new Date(now.getTime() + 1), to: now }), RangeError);
});
