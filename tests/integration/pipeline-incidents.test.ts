import { test } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { MongoClient } from 'mongodb';
import { MongoPipelineIncidentStore } from '../../src/monitoring/adapters/mongo-pipeline-incident.store.js';
const enabled = process.env['RUN_INTEGRATION'] === '1';

test('Mongo incident store: one open incident per key across instances, resolved once, listed with history', { skip: !enabled, timeout: 15000 }, async t => {
  assert.match(process.env['MONGO_DATABASE'] ?? '', /^gate_test_[a-zA-Z0-9_]+$/);
  const mongo = new MongoClient(process.env['MONGO_URI']!);
  const db = mongo.db(`gate_test_pipeline_${randomUUID().replaceAll('-', '')}`);
  t.after(async () => { await db.dropDatabase(); await mongo.close(); });
  const first = new MongoPipelineIncidentStore({ getDb: () => db }); const second = new MongoPipelineIncidentStore({ getDb: () => db });
  const incident = { key: 'node:xcode3.test', kind: 'CONVICT' as const, title: 'Xcode3 is down', channels: ['A'] };
  const t0 = new Date('2026-11-10T10:00:00Z');
  // Two instances race to open the same incident: exactly one wins.
  const opened = await Promise.all([first.open(incident, t0), second.open({ ...incident, channels: ['A', 'B'] }, t0)]);
  assert.equal(opened.filter(Boolean).length, 1);
  assert.equal(await first.open({ ...incident, channels: ['A', 'B', 'C'] }, t0), false, 're-opening refreshes, does not re-announce');
  assert.deepEqual(await first.openKeys(), ['node:xcode3.test']);
  const resolved = await second.resolve('node:xcode3.test', new Date('2026-11-10T10:05:00Z'));
  assert.deepEqual(resolved?.channels, ['A', 'B', 'C']); assert.equal(resolved?.openedAt.toISOString(), t0.toISOString());
  assert.equal(await first.resolve('node:xcode3.test', new Date()), null, 'resolved once');
  assert.equal(await first.open(incident, new Date('2026-11-10T11:00:00Z')), true, 'a new outage of the same node is a new incident');
  const list = await first.list(new Date('2026-11-10T00:00:00Z'));
  assert.equal(list.length, 2); assert.equal('open' in list[0]!, false);
  const indexes = await db.collection('pipeline_incidents').listIndexes().toArray();
  assert.equal(indexes.find(i => i.name === 'resolved_ttl')?.expireAfterSeconds, 604800);
});
