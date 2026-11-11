import { test } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { MongoClient } from 'mongodb';
import { Redis } from 'ioredis';
import { MonitoringResetService, RESET_COLLECTIONS } from '../../src/application/monitoring-reset.js';
const enabled = process.env['RUN_INTEGRATION'] === '1';

test('confirmed reset deletes monitoring records and Redis keys while preserving identity and settings', { skip: !enabled, timeout: 20000 }, async t => {
  assert.match(process.env['MONGO_DATABASE'] ?? '', /^gate_test_[a-zA-Z0-9_]+$/);
  const id = randomUUID().replaceAll('-', '');
  const mongo = new MongoClient(process.env['MONGO_URI']!, { serverSelectionTimeoutMS: 5000 });
  const db = mongo.db(`gate_test_reset_${id}`);
  const redis = new Redis({ host: process.env['REDIS_HOST'] ?? '127.0.0.1', port: Number(process.env['REDIS_PORT'] ?? 6379), password: process.env['REDIS_PASSWORD'], maxRetriesPerRequest: 1 });
  const prefix = `gate:test-reset:${id}:`;
  t.after(async () => {
    const keys = await redis.keys(`${prefix}*`); if (keys.length) await redis.del(...keys);
    await redis.quit(); await db.dropDatabase(); await mongo.close();
  });
  for (const name of RESET_COLLECTIONS) await db.collection(name).insertOne({ sentinel: true });
  await db.collection('users').insertOne({ username: 'preserved' });
  await db.collection('push_subscriptions').insertOne({ sentinel: true });
  await db.collection('app_settings').insertOne({ _id: 'vapid', publicKey: 'preserved' } as never);
  await redis.mset(`${prefix}hls:streams:test`, 'job', `${prefix}hls:status:{test}:latest`, 'status', `${prefix}gate:session:test`, 'preserved');
  // Prefix adapter isolates every Redis command from the real application's keys.
  const isolatedRedis = {
    async scan(cursor: string) {
      const [next, keys] = await redis.scan(cursor, 'MATCH', `${prefix}hls:*`, 'COUNT', 500);
      return [next, keys.map(key => key.slice(prefix.length))];
    },
    async unlink(...keys: string[]) { return redis.unlink(...keys.map(key => prefix + key)); },
  };
  const reset = new MonitoringResetService({ db: () => db, redis: isolatedRedis as never, quiesce: async () => {}, resume: async () => {}, clearMemory: () => {} });
  await reset.run();
  for (const name of RESET_COLLECTIONS) assert.equal(await db.collection(name).countDocuments(), 0, name);
  assert.equal(await db.collection('users').countDocuments(), 1);
  assert.equal(await db.collection('push_subscriptions').countDocuments(), 1);
  assert.equal(await db.collection('app_settings').countDocuments(), 2);
  assert.equal(await redis.get(`${prefix}gate:session:test`), 'preserved');
  assert.equal(await redis.get(`${prefix}hls:streams:test`), null);
  assert.equal(await redis.get(`${prefix}hls:status:{test}:latest`), null);
});
