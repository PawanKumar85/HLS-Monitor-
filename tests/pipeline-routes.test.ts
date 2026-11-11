import { test } from 'node:test';
import assert from 'node:assert/strict';
import Fastify from 'fastify';
import { registerPipelineRoutes } from '../src/http/pipeline-routes.js';
import { channelId, parseChannel, type ChannelStore } from '../src/config/pipeline-channels.js';
import { targetKey, type ProbeTarget, type ProbeTargetStore } from '../src/config/pipeline-probes.js';
import type { ChannelTopology } from '../src/types/pipeline.types.js';

class MemoryChannels implements ChannelStore {
  readonly rows = new Map<string, ChannelTopology>();
  async list() { return [...this.rows.values()]; }
  async get(id: string) { return this.rows.get(id) ?? null; }
  async create(c: ChannelTopology) { if (this.rows.has(c.triggerId)) return false; this.rows.set(c.triggerId, c); return true; }
  async replace(c: ChannelTopology) { if (!this.rows.has(c.triggerId)) return false; this.rows.set(c.triggerId, c); return true; }
  async remove(id: string) { return this.rows.delete(id); }
}
class MemoryProbes implements ProbeTargetStore {
  readonly map = new Map<string, ProbeTarget>();
  async all() { return new Map(this.map); }
  async set(t: string, n: string, target: ProbeTarget) { this.map.set(targetKey(t, n), target); }
  async remove(t: string, n: string) { return this.map.delete(targetKey(t, n)); }
}

// What the dashboard form sends: main hops in a PRIMARY chain, a backup ingest failing over, a VOD fallback.
const channel = {
  channelName: 'ABN Andhrajyoty',
  nodes: [
    { id: 'ingest', name: 'Ingest', type: 'INGEST', host: 'live1.test' },
    { id: 'xcode', name: 'Transcoder', type: 'TRANSCODER', host: 'xcode.test' },
    { id: 'cdn', name: 'CDN', type: 'EGRESS_CDN', host: 'cdn.test' },
    { id: 'ingest-b', name: 'Backup ingest', type: 'INGEST', host: 'live2.test', port: 1935, isBackup: true },
    { id: 'vod', name: 'VOD slate', type: 'FALLBACK_SOURCE', isBackup: true },
  ],
  edges: [
    { fromNodeId: 'ingest', toNodeId: 'xcode', type: 'PRIMARY' },
    { fromNodeId: 'xcode', toNodeId: 'cdn', type: 'PRIMARY' },
    { fromNodeId: 'ingest-b', toNodeId: 'xcode', type: 'FAILOVER' },
    { fromNodeId: 'vod', toNodeId: 'xcode', type: 'FALLBACK' },
  ],
};

test('parseChannel: accepts a form channel; refuses loops, unknown hops, duplicate IDs and a missing or second CDN egress', () => {
  assert.equal(channelId('ABN Andhrajyoty'), 'abn_andhrajyoty');
  assert.equal(channelId('  !!  '), 'channel');
  const parsed = parseChannel(channel, 'abn');
  assert.equal(parsed.nodes.length, 5); assert.equal(parsed.nodes[3]!.port, 1935);
  const bad: [string, unknown][] = [
    ['no hops', { ...channel, nodes: [] }],
    ['no name', { ...channel, channelName: ' ' }],
    ['loop', { ...channel, edges: [...channel.edges, { fromNodeId: 'cdn', toNodeId: 'ingest', type: 'PRIMARY' }] }],
    ['unknown hop', { ...channel, edges: [{ fromNodeId: 'ingest', toNodeId: 'nope', type: 'PRIMARY' }] }],
    ['self edge', { ...channel, edges: [{ fromNodeId: 'xcode', toNodeId: 'xcode', type: 'PRIMARY' }] }],
    ['duplicate edge', { ...channel, edges: [...channel.edges, { fromNodeId: 'ingest', toNodeId: 'xcode', type: 'BYPASS' }] }],
    ['duplicate id', { ...channel, nodes: [...channel.nodes, { id: 'cdn', name: 'Again', type: 'TRANSCODER' }] }],
    ['no egress', { ...channel, nodes: channel.nodes.map(n => n.id === 'cdn' ? { ...n, type: 'TRANSCODER' } : n) }],
    ['two egress', { ...channel, nodes: channel.nodes.map(n => n.id === 'xcode' ? { ...n, type: 'EGRESS_CDN' } : n) }],
    ['bad type', { ...channel, nodes: channel.nodes.map(n => n.id === 'xcode' ? { ...n, type: 'ROUTER' } : n) }],
    ['bad port', { ...channel, nodes: channel.nodes.map(n => n.id === 'cdn' ? { ...n, port: 70000 } : n) }],
    ['bad host', { ...channel, nodes: channel.nodes.map(n => n.id === 'cdn' ? { ...n, host: 'a b' } : n) }],
  ];
  for (const [why, value] of bad) assert.throws(() => parseChannel(value, 'x'), TypeError, why);
});

test('channel API: create, list, rename, edit and delete; removed hops lose their probe targets', async () => {
  const channels = new MemoryChannels(); const probes = new MemoryProbes();
  const app = Fastify();
  registerPipelineRoutes(app, { channels, probes, monitor: { status: () => ({ evaluatedAt: null, intervalMs: 10_000, channels: [] }) } });
  try {
    const created = await app.inject({ method: 'POST', url: '/api/pipeline/channels', payload: channel });
    assert.equal(created.statusCode, 201); assert.equal(created.json().triggerId, 'abn_andhrajyoty');
    assert.equal((await app.inject({ method: 'POST', url: '/api/pipeline/channels', payload: { ...channel, channelName: 'abn  andhrajyoty' } })).statusCode, 409, 'same ID');
    const invalid = await app.inject({ method: 'POST', url: '/api/pipeline/channels', payload: { ...channel, nodes: channel.nodes.filter(n => n.id !== 'cdn'), edges: [] } });
    assert.equal(invalid.statusCode, 400); assert.match(invalid.json().error, /CDN egress/);

    const list = (await app.inject('/api/pipeline/channels')).json();
    assert.equal(list.total, 1); assert.ok(list.channels[0].nodes.every((n: { probe: string }) => n.probe === 'none'));

    for (const nodeId of ['xcode', 'cdn']) assert.equal((await app.inject({ method: 'PUT', url: `/api/pipeline/probes/abn_andhrajyoty/${nodeId}`, payload: { hls: `https://${nodeId}.test/a.m3u8` } })).statusCode, 200);
    assert.equal((await app.inject({ method: 'PUT', url: '/api/pipeline/probes/abn_andhrajyoty/nope', payload: { tcp: 'h:1' } })).statusCode, 404);

    // Rename and drop the transcoder: the ID stays, the transcoder's probe target goes.
    const edited = { channelName: 'ABN', nodes: channel.nodes.filter(n => n.id === 'ingest' || n.id === 'cdn'), edges: [{ fromNodeId: 'ingest', toNodeId: 'cdn', type: 'PRIMARY' }] };
    const put = await app.inject({ method: 'PUT', url: '/api/pipeline/channels/abn_andhrajyoty', payload: edited });
    assert.equal(put.statusCode, 200); assert.equal(put.json().channelName, 'ABN');
    assert.deepEqual([...probes.map.keys()], ['abn_andhrajyoty/cdn']);
    assert.equal((await app.inject({ method: 'PUT', url: '/api/pipeline/channels/missing', payload: edited })).statusCode, 404);

    assert.equal((await app.inject({ method: 'DELETE', url: '/api/pipeline/channels/abn_andhrajyoty' })).statusCode, 204);
    assert.equal(probes.map.size, 0, 'deleting a channel removes its probe targets');
    assert.equal((await app.inject({ method: 'DELETE', url: '/api/pipeline/channels/abn_andhrajyoty' })).statusCode, 404);
    assert.equal((await app.inject('/api/pipeline/channels')).json().total, 0);
  } finally { await app.close(); }
});
