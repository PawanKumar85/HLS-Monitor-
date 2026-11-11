import { test } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { MongoClient } from 'mongodb';
import { MongoFailureHistoryStore } from '../../src/dlq/adapters/mongo-failure-history.store.js';
import type { HlsDeadLetter } from '../../src/dlq/dead-letter.types.js';
const enabled = process.env['RUN_INTEGRATION'] === '1';

test('Mongo failure feed returns newest first-page, then only changed records after the cursor', { skip: !enabled, timeout: 10000 }, async t => {
  assert.match(process.env['MONGO_DATABASE'] ?? '', /^gate_test_[a-zA-Z0-9_]+$/);
  const mongo = new MongoClient(process.env['MONGO_URI']!);
  const db = mongo.db(`gate_test_failure_feed_${randomUUID().replaceAll('-', '')}`);
  t.after(async () => { await db.dropDatabase(); await mongo.close(); });
  let clock = Date.now();
  const store = new MongoFailureHistoryStore({ getDb: () => db }, () => clock);
  const record = (id: string, revision: number): HlsDeadLetter => ({
    originalStreamKey: 'hls:streams:192.168.1.10', originalMessageId: id, groupName: 'hls-workers', serverIp: '192.168.1.10', consumerName: 'c',
    rawFields: ['streamUrl', '[redacted]'], failureId: `f-${id}`, failureStage: 'HEALTH_CHECK', errorCode: 'TIMEOUT', errorMessage: 'timed out',
    attempts: 5, firstFailedAt: 'x', lastFailedAt: 'x', updatedAt: 'x', retryable: true, bounded: true, generation: 1, revision, resolution: 'OPEN',
  });
  await store.write(record('1-0', 1)); await store.write(record('2-0', 1)); await store.write(record('3-0', 1));
  assert.deepEqual(await store.readAfter(undefined, 10), []); // younger than the settle window
  clock += 10_000;
  const page = await store.readAfter(undefined, 2);
  assert.deepEqual(page.map(e => e.failure.failureId), ['f-2-0', 'f-3-0']);
  assert.ok(!('rawFields' in page[0]!.failure) && !('consumerName' in page[0]!.failure));
  assert.deepEqual(await store.readAfter(page[1]!.cursor, 10), []);

  await store.write(record('1-0', 1)); // same revision: no change, must not reappear
  await store.write(record('2-0', 2)); // replay/resolution: newer revision reappears
  clock += 10_000;
  assert.deepEqual((await store.readAfter(page[1]!.cursor, 10)).map(e => [e.failure.failureId, e.failure.revision]), [['f-2-0', 2]]);
});

test('failure history: bulk write, 30-day TTL only while resolved, summary, search and replay lookup', { skip: !enabled, timeout: 15000 }, async t => {
  assert.match(process.env['MONGO_DATABASE'] ?? '', /^gate_test_[a-zA-Z0-9_]+$/);
  const mongo = new MongoClient(process.env['MONGO_URI']!);
  const db = mongo.db(`gate_test_failure_history_${randomUUID().replaceAll('-', '')}`);
  t.after(async () => { await db.dropDatabase(); await mongo.close(); });
  const store = new MongoFailureHistoryStore({ getDb: () => db });
  const record = (id: string, revision: number, over: Partial<HlsDeadLetter> = {}): HlsDeadLetter => ({
    originalStreamKey: 'hls:streams:10.0.0.1', originalMessageId: id, groupName: 'hls-workers', serverIp: '10.0.0.1', consumerName: 'c',
    rawFields: [], failureId: `f-${id}`, failureStage: 'PERSISTENCE', errorCode: 'MONGO_TIMEOUT', errorMessage: 'write timed out (a.b*)',
    attempts: 5, firstFailedAt: 'x', lastFailedAt: 'x', updatedAt: 'x', retryable: true, bounded: true, generation: 1, revision, resolution: 'OPEN', streamId: `s${id}`, ...over,
  });
  await store.writeMany([record('1-0', 1), record('2-0', 1), record('3-0', 1, { failureStage: 'PARSE', errorCode: 'INVALID_MESSAGE', errorMessage: 'bad json' })]);
  const col = db.collection('hls_processing_failures');
  assert.equal(await col.countDocuments(), 3);

  await store.write(record('1-0', 2, { resolution: 'RESOLVED' }));
  assert.ok((await col.findOne({ failureId: 'f-1-0' }))!['resolvedAt'] instanceof Date, 'resolved: TTL clock starts');
  await store.write(record('1-0', 3, { resolution: 'OPEN' }));
  assert.equal((await col.findOne({ failureId: 'f-1-0' }))!['resolvedAt'], undefined, 'reopened: no longer expires');
  await store.write(record('2-0', 2, { resolution: 'RESOLVED' }));
  const ttl = (await col.listIndexes().toArray()).find(i => i.name === 'resolved_ttl');
  assert.equal(ttl?.expireAfterSeconds, 30 * 24 * 60 * 60);

  assert.deepEqual(await store.summary(), { total: 3, byResolution: { OPEN: 2, RESOLVED: 1 }, openByStage: { PERSISTENCE: 1, PARSE: 1 } });

  const open = await store.search({ resolution: 'OPEN' });
  assert.deepEqual(open.items.map(f => f.failureId).sort(), ['f-1-0', 'f-3-0']);
  assert.deepEqual((await store.search({ q: 'A.B*' })).items.map(f => f.failureId).sort(), ['f-1-0', 'f-2-0'], 'literal, case-insensitive text match');
  assert.deepEqual((await store.search({ stage: 'PARSE' })).items.map(f => f.failureId), ['f-3-0']);
  const first = await store.search({ limit: 2 });
  assert.equal(first.items.length, 2); assert.ok(first.next);
  const second = await store.search({ limit: 2, before: first.next });
  assert.equal(second.items.length, 1); assert.equal(second.next, undefined);
  assert.equal(new Set([...first.items, ...second.items].map(f => f.failureId)).size, 3, 'pages do not overlap');
  assert.ok(!('rawFields' in first.items[0]!), 'search returns the operator view, not raw message fields');

  assert.deepEqual(await store.replayTarget('f-3-0'), { originalStreamKey: 'hls:streams:10.0.0.1', originalMessageId: '3-0', groupName: 'hls-workers', serverIp: '10.0.0.1' });
  assert.equal(await store.replayTarget('nope'), null);
});
