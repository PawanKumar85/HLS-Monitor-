import { test } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { MongoClient } from 'mongodb';
import { MongoChannelStore } from '../../src/monitoring/adapters/mongo-channel.store.js';
const enabled = process.env['RUN_INTEGRATION'] === '1';

test('Mongo channel store: create once, list by name, replace, remove', { skip: !enabled, timeout: 15000 }, async t => {
  assert.match(process.env['MONGO_DATABASE'] ?? '', /^gate_test_[a-zA-Z0-9_]+$/);
  const mongo = new MongoClient(process.env['MONGO_URI']!);
  const db = mongo.db(`gate_test_channels_${randomUUID().replaceAll('-', '')}`);
  t.after(async () => { await db.dropDatabase(); await mongo.close(); });
  const store = new MongoChannelStore({ getDb: () => db });
  const channel = (triggerId: string, channelName: string) => ({ triggerId, channelName, nodes: [{ id: 'cdn', name: 'CDN', type: 'EGRESS_CDN' as const }], edges: [] });
  assert.equal(await store.create(channel('zee', 'Zee')), true);
  assert.equal(await store.create(channel('zee', 'Zee again')), false, 'same ID refused');
  assert.equal(await store.create(channel('abn', 'ABN')), true);
  assert.deepEqual((await store.list()).map(c => c.channelName), ['ABN', 'Zee']);
  assert.equal(await store.replace(channel('zee', 'Zee News')), true);
  assert.equal((await store.get('zee'))?.channelName, 'Zee News');
  assert.equal(await store.replace(channel('missing', 'X')), false);
  assert.equal(await store.remove('zee'), true);
  assert.equal(await store.remove('zee'), false);
  assert.deepEqual(await store.get('zee'), null);
});
