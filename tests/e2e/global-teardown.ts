import 'dotenv/config';
import { mongoConfig } from '../../src/config/mongo.config.js';
import { redisConfig } from '../../src/config/redis.config.js';

/** Removes only what the e2e run created: its database and its "e2e"-namespaced Redis keys. */
export default async function globalTeardown() {
  const db = await mongoConfig.connect();
  if (db.databaseName !== 'gate_test_e2e') throw new Error('refusing to drop a non-e2e database');
  await db.dropDatabase(); await mongoConfig.disconnect();
  await redisConfig.connect();
  const redis = redisConfig.getClient(); const keys: string[] = []; let cursor = '0';
  do { const [next, found] = await redis.scan(cursor, 'MATCH', '*e2e*', 'COUNT', 1000); cursor = next; keys.push(...found); } while (cursor !== '0');
  if (keys.length) await redis.del(...keys);
  await redisConfig.disconnect();
}
