import { test } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { MongoClient } from 'mongodb';
import { MongoProbeTargetStore } from '../../src/monitoring/adapters/mongo-probe-target.store.js';
const enabled = process.env['RUN_INTEGRATION'] === '1';

test('Mongo probe target store: save replaces per hop, lists all, removes once', { skip: !enabled, timeout: 15000 }, async t => {
  assert.match(process.env['MONGO_DATABASE'] ?? '', /^gate_test_[a-zA-Z0-9_]+$/);
  const mongo = new MongoClient(process.env['MONGO_URI']!);
  const db = mongo.db(`gate_test_probes_${randomUUID().replaceAll('-', '')}`);
  t.after(async () => { await db.dropDatabase(); await mongo.close(); });
  const store = new MongoProbeTargetStore({ getDb: () => db });
  await store.set('abn', 'cdn', { kind: 'hls', url: 'https://cdn.test/a.m3u8' }, 'op');
  await store.set('abn', 'cdn', { kind: 'tcp', host: 'cdn.test', port: 443 }, 'op');
  await store.set('abn', 'ingest', { kind: 'tcp', host: 'ingest.test', port: 1935 }, null);
  assert.deepEqual([...await store.all()], [['abn/cdn', { kind: 'tcp', host: 'cdn.test', port: 443 }], ['abn/ingest', { kind: 'tcp', host: 'ingest.test', port: 1935 }]]);
  assert.equal(await store.remove('abn', 'cdn'), true);
  assert.equal(await store.remove('abn', 'cdn'), false);
  assert.deepEqual([...(await store.all()).keys()], ['abn/ingest']);
});
