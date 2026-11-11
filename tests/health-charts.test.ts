import { test } from 'node:test';
import assert from 'node:assert/strict';
import { healthCharts, hopHealth } from '../frontend/src/lib/health-charts.js';
import type { Topology, ChannelPipelineVerdict, HopEvaluation } from '../frontend/src/Pipeline.js';
const makeChannel = (id: string): Topology => ({ triggerId: id, channelName: id, nodes: [{ id: 'main', name: 'Main', type: 'INGEST', host: 'shared.test' }, { id: 'backup', name: 'Backup', type: 'INGEST', isBackup: true, host: 'backup.test' }, { id: 'xcode', name: 'Transcoding', type: 'TRANSCODER', host: 'xcode.test' }], edges: [] });
const hop = (nodeId: string, status: HopEvaluation['status'], isLive: boolean | null): HopEvaluation => ({ nodeId, nodeName: nodeId, nodeType: 'INGEST', status, isLive });
const verdict: ChannelPipelineVerdict = { triggerId: 'a', channelName: 'a', overallState: 'DEGRADED_FAILOVER', rootCauseSummary: null, convictNodeIds: [], hopEvaluations: [hop('main', 'CONVICT', false), hop('backup', 'HEALTHY', true), hop('xcode', 'FAILOVER_ACTIVE', true)], alertSuppression: { suppressedCount: 0, alertRecommended: false } };

test('charts merge server hosts across channels while preserving hop and role counts', () => {
  const charts = healthCharts([makeChannel('a'), makeChannel('b')], [], [verdict], true);
  const server = charts.servers.find(r => r.id === 'shared.test')!;
  assert.equal(server.total, 2); assert.equal(server.offline, 1); assert.equal(server.unknown, 1);
  assert.equal(charts.channels.find(r => r.id === 'a')!.degraded, 1);
  assert.equal(charts.roles.find(r => r.id === 'Main')!.offline, 1);
  assert.equal(charts.roles.find(r => r.id === 'Backup')!.healthy, 1);
  assert.equal(charts.roles.find(r => r.id === 'Transcoding')!.degraded, 1);
  assert.equal(charts.roles.find(r => r.id === 'SCTE')!.total, 0);
  for (const row of [...charts.servers, ...charts.channels, ...charts.roles]) assert.equal(row.total, row.healthy + row.degraded + row.offline + row.unknown);
});
test('stale observations and newly configured nodes are unknown, never healthy', () => {
  const chart = healthCharts([makeChannel('a')], [], [verdict], false);
  assert.equal(chart.channels[0]!.unknown, 3);
  assert.equal(chart.channels[0]!.healthy, 0);
  assert.equal(hopHealth(undefined), 'unknown');
  assert.equal(hopHealth(hop('scte', 'UNPROBED', null)), 'unknown');
});
test('server identity uses probe targets when explicit host is missing', () => {
  const channel = makeChannel('a'); channel.nodes = [{ id: 'main', name: 'Main', type: 'INGEST' }];
  const charts = healthCharts([channel], [{ triggerId: 'a', nodeId: 'main', kind: 'hls', hls: 'https://live.test/one/index.m3u8' }], [], true);
  assert.equal(charts.servers[0]!.label, 'live.test');
  assert.equal(charts.servers[0]!.unknown, 1);
});
test('fallback and standby are reported separately from healthy hops', () => {
  assert.equal(hopHealth(hop('fallback', 'FALLBACK_ACTIVE', true)), 'degraded');
  assert.equal(hopHealth(hop('backup', 'STANDBY', false)), 'offline');
  assert.equal(hopHealth(hop('downstream', 'CASCADED', false)), 'offline');
  assert.equal(healthCharts([], [], [], true).channels.length, 0);
});
