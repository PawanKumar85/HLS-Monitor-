import { test } from 'node:test';
import assert from 'node:assert/strict';
import Fastify from 'fastify';
import { PipelineMonitor, incidentsFrom, type IncidentStore, type PipelineIncident } from '../src/monitoring/pipeline-monitor.js';
import { PipelineRcaEngine } from '../src/services/pipeline-rca.service.js';
import { parseProbeTarget, pipelineStream, targetKey, type ProbeTarget, type ProbeTargetStore } from '../src/config/pipeline-probes.js';
import { PipelineProbeService } from '../src/monitoring/pipeline-probe.service.js';
import type { StreamRegistration } from '../src/inventory/stream-inventory.js';
import { registerPipelineRoutes } from '../src/http/pipeline-routes.js';
import type { ChannelTopology } from '../src/types/pipeline.types.js';
import type { ChannelStore } from '../src/config/pipeline-channels.js';

// Two channels sharing one transcoder host, as xcode3 is shared in the real topology.
const channel = (triggerId: string): ChannelTopology => ({
  triggerId, channelName: triggerId.toUpperCase(),
  nodes: [
    { id: 'ingest', name: 'Ingest', type: 'INGEST', host: 'ingest.test', port: 1935 },
    { id: 'xcode', name: 'Xcode3', type: 'TRANSCODER', host: 'xcode3.test' },
    { id: 'cdn', name: 'CDN', type: 'EGRESS_CDN', host: 'cdn.test' },
  ],
  edges: [{ fromNodeId: 'ingest', toNodeId: 'xcode', type: 'PRIMARY' }, { fromNodeId: 'xcode', toNodeId: 'cdn', type: 'PRIMARY' }],
});
const channels = [channel('a'), channel('b')];
const targets = new Map<string, ProbeTarget>(channels.flatMap(c => [
  [targetKey(c.triggerId, 'ingest'), parseProbeTarget({ tcp: 'ingest.test:1935' })],
  [targetKey(c.triggerId, 'xcode'), parseProbeTarget({ hls: `https://xcode3.test/${c.triggerId}.m3u8?token=secret` })],
  [targetKey(c.triggerId, 'cdn'), parseProbeTarget({ hls: `https://cdn.test/${c.triggerId}.m3u8` })],
]));

class MemoryChannels implements ChannelStore {
  constructor(readonly rows = new Map<string, ChannelTopology>()) {}
  async list() { return [...this.rows.values()]; }
  async get(id: string) { return this.rows.get(id) ?? null; }
  async create(c: ChannelTopology) { if (this.rows.has(c.triggerId)) return false; this.rows.set(c.triggerId, c); return true; }
  async replace(c: ChannelTopology) { if (!this.rows.has(c.triggerId)) return false; this.rows.set(c.triggerId, c); return true; }
  async remove(id: string) { return this.rows.delete(id); }
}
const channelStore = () => new MemoryChannels(new Map(channels.map(c => [c.triggerId, c])));

class MemoryProbes implements ProbeTargetStore {
  constructor(readonly map = new Map<string, ProbeTarget>()) {}
  async all() { return new Map(this.map); }
  async set(triggerId: string, nodeId: string, target: ProbeTarget) { this.map.set(targetKey(triggerId, nodeId), target); }
  async remove(triggerId: string, nodeId: string) { return this.map.delete(targetKey(triggerId, nodeId)); }
}

class MemoryIncidents implements IncidentStore {
  readonly rows: PipelineIncident[] = [];
  async open(i: Omit<PipelineIncident, 'openedAt' | 'resolvedAt'>, at: Date) {
    const existing = this.rows.find(r => r.key === i.key && !r.resolvedAt);
    if (existing) { existing.channels = i.channels; return false; }
    this.rows.push({ ...i, openedAt: at, resolvedAt: null }); return true;
  }
  async resolve(key: string, at: Date) { const r = this.rows.find(x => x.key === key && !x.resolvedAt); if (!r) return null; r.resolvedAt = at; return r; }
  async openKeys() { return this.rows.filter(r => !r.resolvedAt).map(r => r.key); }
  async list() { return this.rows; }
}

/** The live status snapshot the stream flow would produce: one entry per HLS hop, matched by host. 'missing' = not checked yet. */
function snapshot(hls: Record<string, boolean | 'missing'>, at: number) {
  return [...targets].flatMap(([key, t]) => {
    if (t.kind !== 'hls') return [];
    const host = Object.keys(hls).find(k => t.url.includes(k));
    const v = host === undefined ? true : hls[host];
    if (v === 'missing') return [];
    const [triggerId, nodeId] = key.split('/') as [string, string];
    return [{ ...pipelineStream(triggerId, nodeId, t.url), status: (v ? 1 : 0) as 0 | 1, latencyMs: 5, checkedAt: new Date(at).toISOString() }];
  });
}

function fixture(hls: Record<string, boolean | 'missing'>, tcp = true) {
  let now = 1_000_000;
  const events: string[] = [];
  const incidents = new MemoryIncidents();
  const monitor = new PipelineMonitor({
    channels: async () => channels, targets: async () => targets, rca: new PipelineRcaEngine(), incidents, intervalMs: 10_000, holdDownMs: 30_000, now: () => now,
    liveStatus: async () => snapshot(hls, now),
    checkTcp: async () => tcp,
    notify: async e => { events.push(`${e.type}:${e.incident.key}:${e.incident.channels.join(',')}`); },
  });
  return { monitor, incidents, events, tick: (ms = 10_000) => { now += ms; } };
}
const states = (m: PipelineMonitor) => m.status().channels.map(c => `${c.triggerId}:${c.overallState}`);

test('a shared transcoder down for two channels is one incident naming both; it opens once and resolves once', async () => {
  const hls = { 'xcode3.test': false, 'cdn.test': false } as Record<string, boolean | 'missing'>;
  const f = fixture(hls);
  await f.monitor.round();
  assert.deepEqual(states(f.monitor), ['a:CRITICAL_CONVICT', 'b:CRITICAL_CONVICT']);
  assert.deepEqual(f.events, ['opened:node:xcode3.test:A,B']);
  f.tick(); await f.monitor.round();
  assert.equal(f.events.length, 1, 'no repeat alert while it stays down');
  hls['xcode3.test'] = true; hls['cdn.test'] = true; f.tick(); await f.monitor.round();
  assert.deepEqual(states(f.monitor), ['a:OPTIMAL', 'b:OPTIMAL']);
  assert.deepEqual(f.events, ['opened:node:xcode3.test:A,B', 'resolved:node:xcode3.test:A,B']);
});

test('TCP hops follow the 2-failure rule; an HLS hop with no recent stream status is UNPROBED, never down', async () => {
  // tcp now fails once: still live (one blip), then down on the second failure.
  const flaky = fixture({});
  await flaky.monitor.round();
  const opts = (flaky.monitor as unknown as { options: { checkTcp: () => Promise<boolean> } }).options;
  opts.checkTcp = async () => false;
  flaky.tick(); await flaky.monitor.round();
  assert.equal(flaky.monitor.status().channels[0]!.hopEvaluations[0]!.status, 'HEALTHY');
  flaky.tick(); await flaky.monitor.round();
  assert.equal(flaky.monitor.status().channels[0]!.hopEvaluations[0]!.status, 'CONVICT');

  const missing = fixture({ 'xcode3.test': 'missing' });
  await missing.monitor.round();
  assert.equal(missing.monitor.status().channels[0]!.hopEvaluations[1]!.status, 'UNPROBED');
  assert.equal(missing.monitor.status().channels[0]!.overallState, 'OPTIMAL');

  // Checks stopped arriving: a status older than 60 s is unknown, not live.
  const old = snapshot({}, 0);
  const stale = new PipelineMonitor({ channels: async () => channels, targets: async () => targets, rca: new PipelineRcaEngine(), checkTcp: async () => true, liveStatus: async () => old, now: () => 61_000 });
  await stale.round();
  assert.equal(stale.status().channels[0]!.overallState, 'UNVERIFIED');

  // Status store down: HLS hops unknown for the round, TCP hops still checked, the round still completes.
  const down = new PipelineMonitor({ channels: async () => channels, targets: async () => targets, rca: new PipelineRcaEngine(), checkTcp: async () => true, liveStatus: async () => { throw new Error('redis down'); }, log: () => {} });
  await down.round();
  assert.deepEqual(down.status().channels[0]!.hopEvaluations.map(h => h.status), ['HEALTHY', 'UNPROBED', 'UNPROBED']);
});

test('hold-down: right after the transcoder recovers, a still-down CDN is cascaded, not convicted', async () => {
  const hls = { 'xcode3.test': false, 'cdn.test': false } as Record<string, boolean | 'missing'>;
  const f = fixture(hls);
  await f.monitor.round();
  hls['xcode3.test'] = true; f.tick(); await f.monitor.round();
  assert.equal(f.monitor.status().channels[0]!.hopEvaluations[2]!.status, 'CASCADED');
  f.tick(40_000); await f.monitor.round();
  assert.equal(f.monitor.status().channels[0]!.hopEvaluations[2]!.status, 'CONVICT');
});

test("the pipeline monitor leaves other owners' incidents open (e.g. the check watchdog's)", async () => {
  const f = fixture({});
  await f.incidents.open({ key: 'monitor:checks-stalled', kind: 'OFFLINE', title: 'Monitoring stopped', channels: [] }, new Date());
  await f.monitor.round();
  assert.deepEqual(await f.incidents.openKeys(), ['monitor:checks-stalled']);
});

test('incidentsFrom: offline and fallback channels get their own incident keys', () => {
  const rca = new PipelineRcaEngine();
  // Egress down while its upstream recovered moments ago: no convict yet, so a channel-level OFFLINE incident.
  const offline = rca.evaluate(channels[0]!, new Map([['ingest', true], ['xcode', true], ['cdn', false]]), new Set(['xcode']));
  assert.deepEqual(incidentsFrom([offline], channels).map(i => i.key), ['channel:a:offline']);
  // A convict hop without a host still gets a stable per-channel key.
  const noHost = { ...channels[0]!, nodes: channels[0]!.nodes.map(n => n.id === 'xcode' ? { id: n.id, name: n.name, type: n.type } : n) };
  const convict = rca.evaluate(noHost, new Map([['ingest', true], ['xcode', false], ['cdn', false]]));
  assert.deepEqual(incidentsFrom([convict], [noHost]).map(i => i.key), ['node:a/xcode']);
});

test('probe target form value: hls URL or tcp host:port, nothing else', () => {
  assert.deepEqual(parseProbeTarget({ tcp: ' ingest.test:1935 ' }), { kind: 'tcp', host: 'ingest.test', port: 1935 });
  assert.deepEqual(parseProbeTarget({ tcp: '[::1]:80' }), { kind: 'tcp', host: '::1', port: 80 });
  assert.equal(parseProbeTarget({ hls: 'https://x.test/a.m3u8' }).kind, 'hls');
  for (const bad of [{}, null, [], { hls: 'ftp://x/y' }, { hls: 'not a url' }, { tcp: 'host' }, { tcp: 'h:0' }, { tcp: 'h:70000' }, { tcp: 'a b:1' }, { hls: 'https://x', tcp: 'h:1' }, { udp: 'h:1' }]) {
    assert.throws(() => parseProbeTarget(bad), TypeError, JSON.stringify(bad));
  }
});

test('a target saved from the dashboard is used from the next round', async () => {
  const probes = new MemoryProbes();
  const down = (key: string, t: ProbeTarget) => t.kind === 'hls' ? [{ ...pipelineStream(...key.split('/') as [string, string], t.url), status: 0 as const, latencyMs: 5, checkedAt: new Date().toISOString() }] : [];
  const monitor = new PipelineMonitor({ channels: async () => channels, targets: () => probes.all(), rca: new PipelineRcaEngine(), liveStatus: async () => [...probes.map].flatMap(([k, t]) => down(k, t)), intervalMs: 10_000 });
  await monitor.round();
  assert.equal(monitor.status().channels[0]!.overallState, 'UNVERIFIED');
  await probes.set('a', 'cdn', parseProbeTarget({ hls: 'https://cdn.test/a.m3u8' }));
  await monitor.round();
  assert.equal(monitor.status().channels[0]!.overallState, 'CRITICAL_CONVICT');
});

test('probe target API: save, list and remove; unknown hops and bad values refused', async () => {
  const probes = new MemoryProbes();
  const app = Fastify();
  registerPipelineRoutes(app, { channels: channelStore(), probes, monitor: { status: () => ({ evaluatedAt: null, intervalMs: 10_000, channels: [] }) } });
  try {
    const put = await app.inject({ method: 'PUT', url: '/api/pipeline/probes/a/xcode', payload: { hls: 'https://xcode3.test/a.m3u8?token=secret' } });
    assert.equal(put.statusCode, 200); assert.equal(put.json().kind, 'hls');
    assert.equal((await app.inject({ method: 'PUT', url: '/api/pipeline/probes/a/ingest', payload: { tcp: 'ingest.test:1935' } })).json().tcp, 'ingest.test:1935');
    assert.equal((await app.inject({ method: 'PUT', url: '/api/pipeline/probes/a/nope', payload: { tcp: 'h:1' } })).statusCode, 404);
    assert.equal((await app.inject({ method: 'PUT', url: '/api/pipeline/probes/zzz/cdn', payload: { tcp: 'h:1' } })).statusCode, 404);
    assert.equal((await app.inject({ method: 'PUT', url: '/api/pipeline/probes/a/cdn', payload: { tcp: 'nope' } })).statusCode, 400);
    assert.equal((await app.inject('/api/pipeline/probes')).json().probes.length, 2);
    assert.equal((await app.inject('/api/pipeline/channels')).json().channels[0].nodes[1].probe, 'hls');
    assert.ok(!(await app.inject('/api/pipeline/channels')).body.includes('secret'), 'the topology list never carries probe URLs');
    assert.equal((await app.inject({ method: 'DELETE', url: '/api/pipeline/probes/a/xcode' })).statusCode, 204);
    assert.deepEqual([...probes.map.keys()], ['a/ingest']);
  } finally { await app.close(); }
});

test('API reads the last round only, never probes, and never returns probe URLs', async () => {
  let calls = 0;
  const monitor = { status: () => { calls++; return { evaluatedAt: null, intervalMs: 10_000, channels: [] }; } };
  const app = Fastify(); registerPipelineRoutes(app, { channels: channelStore(), probes: new MemoryProbes(new Map(targets)), monitor, incidents: new MemoryIncidents() });
  try {
    const status = await app.inject('/api/pipeline/status');
    assert.equal(status.json().pending, true); assert.equal(calls, 1);
    const list = await app.inject('/api/pipeline/channels');
    assert.equal(list.json().channels[0].nodes[1].probe, 'hls');
    assert.ok(!list.body.includes('secret') && !list.body.includes('https://'), 'probe URLs must not leak');
    assert.equal((await app.inject('/api/pipeline/status/nope')).statusCode, 404);
    assert.equal((await app.inject('/api/pipeline/incidents?hours=24')).statusCode, 200);
    assert.equal((await app.inject('/api/pipeline/incidents?hours=999')).statusCode, 400);
  } finally { await app.close(); }
});

test('saving an HLS target registers it as a stream; changing host or kind moves or removes that stream', async () => {
  const rows = new Map<string, StreamRegistration>();
  let n = 0;
  const id = (ip: string, sid: string) => JSON.stringify([ip, sid]);
  const streams = {
    findIdentity: async (ip: string, sid: string) => rows.get(id(ip, sid)) ?? null,
    create: async (v: { serverIp?: string; streamId: string; streamUrl: string }) => {
      const r = { id: String(++n), config: { serverIp: v.serverIp!, streamId: v.streamId, streamUrl: v.streamUrl, intervalMs: 10_000, staleAfterMs: 30_000, enabled: true }, version: 1, deleted: false } as StreamRegistration;
      rows.set(id(v.serverIp!, v.streamId), r); return r;
    },
    patch: async (rid: string, _v: number, p: { streamUrl?: string }) => { const r = [...rows.values()].find(x => x.id === rid)!; r.config.streamUrl = p.streamUrl ?? r.config.streamUrl; r.version++; return r; },
    remove: async (rid: string) => { const r = [...rows.values()].find(x => x.id === rid)!; r.deleted = true; r.version++; return r; },
  };
  const service = new PipelineProbeService(new MemoryProbes(), streams);
  const live = () => [...rows.values()].filter(r => !r.deleted).map(r => `${r.config.serverIp} ${r.config.streamId} ${r.config.streamUrl}`);

  await service.set('a', 'cdn', parseProbeTarget({ hls: 'https://cdn.test/a.m3u8' }), 'op');
  assert.deepEqual(live(), ['cdn.test pipeline:a/cdn https://cdn.test/a.m3u8']);
  await service.set('a', 'cdn', parseProbeTarget({ hls: 'https://cdn.test/b.m3u8' }), 'op');
  assert.deepEqual(live(), ['cdn.test pipeline:a/cdn https://cdn.test/b.m3u8'], 'same host: the stream URL is updated');
  await service.set('a', 'cdn', parseProbeTarget({ hls: 'https://edge.test/b.m3u8' }), 'op');
  assert.deepEqual(live(), ['edge.test pipeline:a/cdn https://edge.test/b.m3u8'], 'new host: old stream removed, new one added');
  await service.set('a', 'cdn', parseProbeTarget({ tcp: 'edge.test:443' }), 'op');
  assert.deepEqual(live(), [], 'tcp targets are not streams');
  await service.set('a', 'xcode', parseProbeTarget({ hls: 'https://x.test/a.m3u8' }), 'op');
  rows.get(id('x.test', 'pipeline:a/xcode'))!.deleted = true; // deleted from the Streams page
  await service.relinkAll();
  assert.deepEqual(live(), ['x.test pipeline:a/xcode https://x.test/a.m3u8']);
  assert.equal(await service.remove('a', 'xcode'), true);
  assert.deepEqual(live(), []);
});
