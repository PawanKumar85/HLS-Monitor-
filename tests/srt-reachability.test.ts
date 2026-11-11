import { test } from 'node:test';
import assert from 'node:assert/strict';
import { SrtReachabilityMonitor, pingSrtHost } from '../src/monitoring/srt-reachability.js';
import { parseChannel } from '../src/config/pipeline-channels.js';
import { srtPingLabel } from '../frontend/src/lib/srt-ping.js';
import type { ChannelTopology } from '../src/types/pipeline.types.js';
const channel: ChannelTopology = { triggerId: 'a', channelName: 'a', nodes: [{ id: 'srt', name: 'SCTE', type: 'SCTE_RELAY', host: 'server.test', transport: 'srt' }, { id: 'hls', name: 'CDN', type: 'EGRESS_CDN', host: 'hls.test' }], edges: [{ fromNodeId: 'srt', toNodeId: 'hls', type: 'PRIMARY' }] };
test('only explicit SRT servers are pinged, shared hosts deduplicated, results are cached', async () => {
  let channels = [channel, { ...channel, triggerId: 'b' }];
  const calls: string[] = [];
  const monitor = new SrtReachabilityMonitor(async () => channels, async host => { calls.push(host); return { host, state: 'REACHABLE', latencyMs: 80, checkedAt: new Date().toISOString() }; });
  const first = monitor.round(); assert.equal(monitor.round(), first);
  await first;
  assert.deepEqual(calls, ['server.test']);
  assert.equal(monitor.status().servers[0]?.state, 'REACHABLE');
  assert.equal(monitor.status().servers[0]?.latencyMs, 80);
  monitor.status(); assert.equal(calls.length, 1, 'status reads never ping');
  channels = []; await monitor.round(); assert.deepEqual(monitor.status().servers, []);
});
test('ping failures remain unavailable and do not throw into stream monitoring', async () => {
  const monitor = new SrtReachabilityMonitor(async () => [channel], async () => { throw Error('ping not installed'); });
  await monitor.round(); assert.equal(monitor.status().servers[0]?.state, 'UNAVAILABLE');
  assert.equal((await pingSrtHost('-c1000')).state, 'UNAVAILABLE');
  assert.equal((await pingSrtHost('host;echo secret')).state, 'UNAVAILABLE');
});
test('SRT transport persists in channel topology while invalid transport is rejected', () => {
  assert.equal(parseChannel(channel, 'a').nodes[0]?.transport, 'srt');
  assert.throws(() => parseChannel({ ...channel, nodes: channel.nodes.map(n => ({ ...n, transport: 'bad' })) }, 'a'));
});
test('ping labels distinguish reachability from missing replies, unavailable and stale results', () => {
  const now = Date.now(); const ping = { host: 'server.test', state: 'REACHABLE' as const, latencyMs: 80.123, checkedAt: new Date(now).toISOString() };
  assert.equal(srtPingLabel(ping, now), 'Reachable · 80.1 ms');
  assert.equal(srtPingLabel({ ...ping, state: 'NO_REPLY' }, now), 'No ICMP reply');
  assert.equal(srtPingLabel({ ...ping, state: 'UNAVAILABLE' }, now), 'Ping unavailable');
  assert.equal(srtPingLabel(ping, now + 100_000), 'Ping result stale');
});
