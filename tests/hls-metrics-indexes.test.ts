import { test } from 'node:test';
import assert from 'node:assert/strict';
import type { Db, IndexDescription } from 'mongodb';
import { ensureHlsMetricsIndexes, HLS_METRICS_RETENTION_SECONDS } from '../src/database/hls-metrics.js';

const SEVEN_DAYS = 604800;
const ttlSpec = { key: { ingestedAt: 1 }, name: 'ingestedAt_1', expireAfterSeconds: SEVEN_DAYS };

/** Fake Db recording index builds and collMod commands; `existing` is what listIndexes returns. */
function fakeDb(existing: unknown[] = []) {
  const builds: { collection: string; indexes: IndexDescription[] }[] = [];
  const commands: unknown[] = [];
  let fail = false;
  const db = {
    collection: (collection: string) => ({
      listIndexes: () => ({ toArray: async () => existing }),
      createIndexes: async (indexes: IndexDescription[]) => {
        builds.push({ collection, indexes });
        if (fail) throw new Error('index failure');
        return [];
      },
    }),
    command: async (command: unknown) => { commands.push(command); return { ok: 1 }; },
  } as unknown as Db;
  return { db, builds, commands, setFail: (value: boolean) => { fail = value; } };
}

test('history indexes and the 7-day TTL install once for concurrent queues; failed builds can retry', async () => {
  const { db, builds, setFail } = fakeDb();
  await Promise.all([ensureHlsMetricsIndexes(db), ensureHlsMetricsIndexes(db)]);
  assert.deepEqual(builds.map(b => b.indexes), [
    [{ key: { redisStreamKey: 1, redisMessageId: 1 }, unique: true }, { key: { serverIp: 1, streamId: 1, checkedAt: -1 } }],
    [ttlSpec],
  ]);
  assert.equal(HLS_METRICS_RETENTION_SECONDS, SEVEN_DAYS);
  setFail(true);
  await assert.rejects(ensureHlsMetricsIndexes(db, 'other'), /index failure/);
  setFail(false);
  await ensureHlsMetricsIndexes(db, 'other');
  assert.equal(builds.filter(b => b.collection === 'other').length, 3); // failed attempt, then base + TTL
});

test('an existing 14-day TTL is shortened to 7 days in place, never dropped', async () => {
  const { db, builds, commands } = fakeDb([
    { name: 'ingestedAt_1', key: { ingestedAt: 1 }, expireAfterSeconds: 1209600 },
    { name: 'other_ttl', key: { otherDate: 1 }, expireAfterSeconds: 10 },
  ]);
  await ensureHlsMetricsIndexes(db);
  assert.deepEqual(commands, [{ collMod: 'hls_stream_metrics', index: { name: 'ingestedAt_1', expireAfterSeconds: SEVEN_DAYS } }]);
  assert.equal(builds.length, 1); // only the base indexes; no second TTL build
});

test('a TTL already at 7 days is left alone', async () => {
  const { db, builds, commands } = fakeDb([{ name: 'ingestedAt_1', key: { ingestedAt: 1 }, expireAfterSeconds: SEVEN_DAYS }]);
  await ensureHlsMetricsIndexes(db);
  assert.deepEqual(commands, []);
  assert.equal(builds.length, 1);
});
