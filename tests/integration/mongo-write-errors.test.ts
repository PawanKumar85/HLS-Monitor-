import { test } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { MongoClient, Collection, MongoBulkWriteError } from 'mongodb';
import { MongoMetricsStore } from '../../src/storage/adapters/mongo-metrics.store.js';
import { PartialMetricsWriteError } from '../../src/storage/persistence-errors.js';
import type { HlsStreamMetric } from '../../src/types/hls-metric.types.js';

test('real Mongo duplicate conflicts reconcile only the same stored producer observation', { skip: process.env['RUN_INTEGRATION'] !== '1', timeout: 15000 }, async t => {
  assert.match(process.env['MONGO_DATABASE'] ?? '', /^gate_test_[a-zA-Z0-9_]+$/);
  assert.ok(process.env['MONGO_URI']);
  const mongo = new MongoClient(process.env['MONGO_URI']!);
  const db = mongo.db(`${process.env['MONGO_DATABASE']!.slice(0, 20)}_writes_${randomUUID().replaceAll('-', '')}`);
  const store = new MongoMetricsStore({ connect: async () => { await mongo.connect(); return db; }, getDb: () => db, disconnect: async () => {} });
  const metric: HlsStreamMetric = { serverIp: 'server', streamId: 'channel', streamUrl: 'https://example.test/live', redisStreamKey: 'hls:streams:server', redisMessageId: '1-0',
    createdAt: new Date(1700000000000), checkedAt: new Date(), receivedAt: new Date(), ingestedAt: new Date(), pipelineDelayMs: 0 };
  t.after(async () => { t.mock.restoreAll(); try { await db.dropDatabase(); } finally { await mongo.close(); } });
  await store.connect();
  const collection = db.collection<HlsStreamMetric>('observations');
  await store.writeBatch([metric], 'observations');
  let duplicate!: MongoBulkWriteError;
  try {
    await collection.bulkWrite([{ insertOne: { document: { ...metric } } }]);
    assert.fail('Expected real duplicate key failure');
  } catch (error) { assert.ok(error instanceof MongoBulkWriteError); duplicate = error; }
  assert.equal(duplicate.writeErrors instanceof Array && duplicate.writeErrors[0]?.code, 11000);
  // Simulate the upsert race: another writer has already inserted this identity.
  const original = Collection.prototype.bulkWrite;
  const fault = t.mock.method(Collection.prototype, 'bulkWrite', async function (this: Collection, ...args: Parameters<typeof original>) {
    if (this.collectionName === 'observations') {
      assert.deepEqual(args[1]?.writeConcern, { w: 'majority' });
      throw duplicate;
    }
    return original.apply(this, args);
  });
  await store.writeBatch([{ ...metric, checkedAt: new Date(), health: { status: 0, segmentLatencyMs: 9 } }], 'observations');
  await assert.rejects(store.writeBatch([{ ...metric, streamUrl: 'https://other.test/live' }], 'observations'), error => {
    assert.ok(error instanceof PartialMetricsWriteError);
    assert.equal(error.failures[0]?.failure.code, 'DUPLICATE_KEY_UNCONFIRMED');
    assert.equal(error.failures[0]?.failure.retryable, true); return true;
  });
  fault.mock.restore();
  assert.equal(await collection.countDocuments(), 1);
  await collection.createIndex({ streamId: 1 }, { unique: true });
  await assert.rejects(store.writeBatch([{ ...metric, redisMessageId: '2-0' }], 'observations'), error => {
    assert.ok(error instanceof PartialMetricsWriteError);
    assert.equal(error.failures[0]?.failure.code, 'DUPLICATE_KEY_UNCONFIRMED'); return true;
  });
  assert.equal(await collection.countDocuments(), 1);
});
