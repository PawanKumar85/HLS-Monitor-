import { test } from 'node:test';
import assert from 'node:assert/strict';
import { dashboardSummary, incidentServer } from '../frontend/src/lib/dashboard-summary.js';
import type { ChannelPipelineVerdict, Topology } from '../frontend/src/Pipeline.js';
const verdict = (triggerId: string, overallState: ChannelPipelineVerdict['overallState']): ChannelPipelineVerdict => ({ triggerId, channelName: triggerId, overallState, rootCauseSummary: null, convictNodeIds: [], hopEvaluations: [], alertSuppression: { suppressedCount: 0, alertRecommended: overallState === 'OFFLINE' } });
const configured = (triggerId: string): Topology => ({ triggerId, channelName: triggerId, nodes: [], edges: [] });

test('dashboard prioritizes critical, fallback and backup channels ahead of healthy channels', () => {
  const summary = dashboardSummary([verdict('healthy', 'OPTIMAL'), verdict('backup', 'DEGRADED_FAILOVER'), verdict('fallback', 'ON_FALLBACK'), verdict('offline', 'OFFLINE'), verdict('unknown', 'UNVERIFIED')], null);
  assert.equal(summary.healthy, 1); assert.equal(summary.critical, 2); assert.equal(summary.backup, 1); assert.equal(summary.unknown, 1);
  assert.deepEqual(summary.channels.map(c => c.triggerId), ['offline', 'fallback', 'backup', 'unknown', 'healthy']);
});
test('new channels await verification and deleted channels do not inflate the report', () => {
  const summary = dashboardSummary([verdict('current', 'OPTIMAL'), verdict('deleted', 'OFFLINE')], [configured('current'), configured('new')]);
  assert.equal(summary.total, 2); assert.equal(summary.healthy, 1); assert.equal(summary.critical, 0); assert.equal(summary.unknown, 1);
  assert.equal(summary.pending[0]?.triggerId, 'new');
});
test('pending probe rounds do not mark configured channels healthy', () => {
  const summary = dashboardSummary([], [configured('one'), configured('two')]);
  assert.equal(summary.healthy, 0); assert.equal(summary.unknown, 2); assert.equal(summary.total, 2);
  assert.equal(dashboardSummary([], []).total, 0);
});

test('incidentServer: the host of a node incident, port dropped; channel-level and host-less incidents have none', () => {
  assert.equal(incidentServer('node:xcode3.ottlive.co.in'), 'xcode3.ottlive.co.in');
  assert.equal(incidentServer('node:148.113.16.234:63004'), '148.113.16.234');
  assert.equal(incidentServer('node:2001:db8::1'), '2001:db8::1', 'bare IPv6 stays whole');
  assert.equal(incidentServer('node:abn/xcode'), null);
  assert.equal(incidentServer('channel:abn:offline'), null);
  assert.equal(incidentServer('monitor:checks-stalled'), null);
});
