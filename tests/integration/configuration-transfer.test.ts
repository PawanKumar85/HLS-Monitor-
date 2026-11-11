import { test } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { MongoClient } from 'mongodb';
import { ConfigurationTransfer } from '../../src/application/configuration-transfer.js';
import { MongoChannelStore } from '../../src/monitoring/adapters/mongo-channel.store.js';
import { MongoProbeTargetStore } from '../../src/monitoring/adapters/mongo-probe-target.store.js';
import { MongoStreamInventoryRepository } from '../../src/inventory/adapters/mongo-stream-inventory.repository.js';
import { StreamService } from '../../src/inventory/stream-service.js';
const enabled = process.env['RUN_INTEGRATION'] === '1';

test('configuration export/import round-trip uses durable inventory and preserves disabled HLS settings', { skip: !enabled, timeout: 15000 }, async t => {
  assert.match(process.env['MONGO_DATABASE'] ?? '', /^gate_test_[a-zA-Z0-9_]+$/);
  const mongo = new MongoClient(process.env['MONGO_URI']!, { serverSelectionTimeoutMS: 5000 });
  const id = randomUUID().replaceAll('-', '');
  const dbs = [mongo.db(`gate_test_export_${id}`), mongo.db(`gate_test_import_${id}`)];
  t.after(async () => { for (const db of dbs) await db.dropDatabase(); await mongo.close(); });
  const transfers = dbs.map(db => {
    const connection = { getDb: () => db };
    return new ConfigurationTransfer(new MongoChannelStore(connection), new MongoProbeTargetStore(connection), new StreamService(new MongoStreamInventoryRepository(connection)));
  });
  const bundle = { format: 'hls-monitor-configuration', version: 1,
    channels: [{ triggerId: 'news', channelName: 'News', nodes: [{ id: 'final', name: 'Final', type: 'EGRESS_CDN' }], edges: [] }],
    probes: [{ triggerId: 'news', nodeId: 'final', target: { kind: 'hls', url: 'https://cdn.test/live.m3u8?token=private' } }],
    streams: [{ serverIp: 'cdn.test', streamId: 'pipeline:news/final', streamUrl: 'https://cdn.test/live.m3u8?token=private', enabled: false, intervalMs: 20000, staleAfterMs: 90000 }],
  };
  await transfers[0]!.import(bundle, 'operator');
  const exported = await transfers[0]!.export();
  await transfers[1]!.import(JSON.parse(JSON.stringify(exported)), 'operator');
  const restored = await transfers[1]!.export();
  assert.deepEqual(restored.channels, exported.channels);
  assert.deepEqual(restored.streams, exported.streams);
  assert.deepEqual(restored.probes, exported.probes);
  assert.equal(restored.streams[0]!.enabled, false);
  assert.equal(await dbs[1]!.collection('hls_stream_inventory').countDocuments({ pendingSync: true }), 1);
  await assert.rejects(transfers[1]!.import(exported, 'operator'), /conflict/);
  assert.equal(await dbs[1]!.collection('pipeline_channels').countDocuments(), 1);
});
