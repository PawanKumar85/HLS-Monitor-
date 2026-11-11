import { test } from 'node:test';
import assert from 'node:assert/strict';
import { ConfigurationTransfer, parseConfiguration, TransferConflictError, TransferValidationError } from '../src/application/configuration-transfer.js';
import { StreamService } from '../src/inventory/stream-service.js';
import { MemoryInventory } from './helpers/stream-inventory.js';
import type { ChannelTopology } from '../src/types/pipeline.types.js';
import type { ProbeTarget } from '../src/config/pipeline-probes.js';

const bundle = () => ({ format: 'hls-monitor-configuration', version: 1,
  channels: [{ triggerId: 'news', channelName: 'News', nodes: [{ id: 'main', name: 'Main', type: 'INGEST' }, { id: 'final', name: 'Final', type: 'EGRESS_CDN' }], edges: [{ fromNodeId: 'main', toNodeId: 'final', type: 'PRIMARY' }] }],
  probes: [{ triggerId: 'news', nodeId: 'main', target: { kind: 'tcp', host: 'ingest.test', port: 1935 } }, { triggerId: 'news', nodeId: 'final', target: { kind: 'hls', url: 'https://cdn.test/live.m3u8?token=private' } }],
  streams: [{ serverIp: 'origin.test', streamId: 'standalone', streamUrl: 'https://origin.test/live.m3u8', intervalMs: 20000, staleAfterMs: 60000, enabled: false }],
});
function fixture() {
  const channels = new Map<string, ChannelTopology>(); const probes = new Map<string, ProbeTarget>(); let fail = false;
  const service = new ConfigurationTransfer({ list: async () => [...channels.values()], get: async id => channels.get(id) ?? null,
    create: async channel => { if (channels.has(channel.triggerId)) return false; channels.set(channel.triggerId, channel); return true; },
    replace: async () => false, remove: async id => channels.delete(id) },
    { all: async () => new Map(probes), set: async (channel, node, value) => { if (fail) throw new Error('target storage failed'); probes.set(`${channel}/${node}`, value); }, remove: async (channel, node) => probes.delete(`${channel}/${node}`) },
    new StreamService(new MemoryInventory()));
  return { service, channels, probes, fail: () => { fail = true; } };
}
test('preview is read-only; export/import preserves topology, TCP/HLS targets, URL tokens and paused stream settings', async () => {
  const source = fixture(); const input = bundle();
  const preview = await source.service.preview(input);
  assert.deepEqual(preview.summary, { channels: 1, probes: 2, streams: 2 });
  assert.equal(source.channels.size, 0); assert.equal(source.probes.size, 0);
  await source.service.import(input, 'operator');
  const exported = await source.service.export();
  assert.equal(exported.streams.find(s => s.streamId === 'standalone')!.enabled, false);
  assert.equal(exported.streams.find(s => s.streamId === 'standalone')!.intervalMs, 20000);
  assert.equal(exported.probes[1]!.target.kind, 'hls');
  const target = fixture(); await target.service.import(JSON.parse(JSON.stringify(exported)), 'operator');
  const imported = await target.service.export();
  assert.deepEqual(imported.channels, exported.channels);
  assert.deepEqual(imported.probes, exported.probes);
  assert.deepEqual(imported.streams.sort((a, b) => a.streamId.localeCompare(b.streamId)), exported.streams.sort((a, b) => a.streamId.localeCompare(b.streamId)));
  assert.ok(!('users' in exported));
});
test('invalid files and all conflicts are rejected before importing any rows', async () => {
  const f = fixture();
  for (const bad of [null, {}, { ...bundle(), version: 2 }, { ...bundle(), channels: [...bundle().channels, ...bundle().channels] }, { ...bundle(), probes: [{ triggerId: 'missing', nodeId: 'final', target: { kind: 'hls', url: 'http://cdn.test/a' } }] }, { ...bundle(), streams: [{ ...bundle().streams[0], intervalMs: 1 }] }]) {
    assert.throws(() => parseConfiguration(bad), TransferValidationError);
  }
  assert.equal(f.channels.size, 0);
  await f.service.import(bundle(), 'operator');
  const before = await f.service.export();
  await assert.rejects(f.service.import(bundle(), 'operator'), TransferConflictError);
  const after = await f.service.export();
  assert.deepEqual(after.channels, before.channels); assert.deepEqual(after.streams, before.streams);
});
test('failed probe storage rolls back new streams and does not expose partial channel topology', async () => {
  const f = fixture(); f.fail();
  await assert.rejects(f.service.import(bundle(), 'operator'), /target storage failed/);
  const state = await f.service.export();
  assert.equal(state.channels.length, 0); assert.equal(state.probes.length, 0); assert.equal(state.streams.length, 0);
});
