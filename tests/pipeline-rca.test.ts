import { test } from 'node:test';
import assert from 'node:assert/strict';
import { PipelineRcaEngine } from '../src/services/pipeline-rca.service.js';
import type { ChannelTopology, HopObservation } from '../src/types/pipeline.types.js';

const rca = new PipelineRcaEngine();
// SCTE bypass: ingest-main → scte-relay → xcode → cdn-egress, plus a bypass ingest-main → xcode.
const abn: ChannelTopology = {
  triggerId: 'abn', channelName: 'ABN',
  nodes: [
    { id: 'ingest-main', name: 'Live1 Ingest', type: 'INGEST', host: 'live1.test' },
    { id: 'scte-relay', name: 'SCTE Relay', type: 'SCTE_RELAY', host: '10.0.0.1', port: 63004 },
    { id: 'xcode', name: 'Xcode2 Transcoder', type: 'TRANSCODER', host: 'xcode2.test' },
    { id: 'cdn-egress', name: 'Stream CDN Egress', type: 'EGRESS_CDN', host: 'stream.test' },
  ],
  edges: [
    { fromNodeId: 'ingest-main', toNodeId: 'scte-relay', type: 'PRIMARY' },
    { fromNodeId: 'scte-relay', toNodeId: 'xcode', type: 'PRIMARY' },
    { fromNodeId: 'ingest-main', toNodeId: 'xcode', type: 'BYPASS' },
    { fromNodeId: 'xcode', toNodeId: 'cdn-egress', type: 'PRIMARY' },
  ],
};
// Dual ingest: a backup ingest fails over into the SCTE relay.
const sakshi: ChannelTopology = {
  triggerId: 'sakshi', channelName: 'Sakshi',
  nodes: [
    { id: 'ingest-main', name: 'Ingest3 Main', type: 'INGEST', host: 'ingest3.test' },
    { id: 'ingest-backup', name: 'Ingest Backup', type: 'INGEST', host: 'ingest.test', isBackup: true },
    { id: 'scte-relay', name: 'Multi SCTE Relay', type: 'SCTE_RELAY', host: 'multi.test', port: 63042 },
    { id: 'xcode', name: 'Xcode3 Transcoder', type: 'TRANSCODER', host: 'xcode3.test' },
    { id: 'cdn-egress', name: 'Stream CDN Egress', type: 'EGRESS_CDN', host: 'stream.test' },
  ],
  edges: [
    { fromNodeId: 'ingest-main', toNodeId: 'scte-relay', type: 'PRIMARY' },
    { fromNodeId: 'ingest-backup', toNodeId: 'scte-relay', type: 'FAILOVER' },
    { fromNodeId: 'ingest-main', toNodeId: 'xcode', type: 'BYPASS' },
    { fromNodeId: 'scte-relay', toNodeId: 'xcode', type: 'PRIMARY' },
    { fromNodeId: 'xcode', toNodeId: 'cdn-egress', type: 'PRIMARY' },
  ],
};
const linear: ChannelTopology = {
  triggerId: 'linear', channelName: 'Linear',
  nodes: [
    { id: 'ingest', name: 'Ingest', type: 'INGEST', host: 'ingest.test' },
    { id: 'scte', name: 'SCTE', type: 'SCTE_RELAY', host: '10.0.0.2', port: 63004 },
    { id: 'xcode', name: 'Xcode', type: 'TRANSCODER', host: 'xcode.test' },
    { id: 'vod', name: 'VOD', type: 'FALLBACK_SOURCE', isBackup: true },
    { id: 'cdn', name: 'CDN', type: 'EGRESS_CDN', host: 'cdn.test' },
  ],
  edges: [
    { fromNodeId: 'ingest', toNodeId: 'scte', type: 'PRIMARY' },
    { fromNodeId: 'scte', toNodeId: 'xcode', type: 'PRIMARY' },
    { fromNodeId: 'vod', toNodeId: 'xcode', type: 'FALLBACK' },
    { fromNodeId: 'xcode', toNodeId: 'cdn', type: 'PRIMARY' },
  ],
};
const obs = (o: Record<string, HopObservation>) => new Map(Object.entries(o));
const status = (v: ReturnType<typeof rca.evaluate>) => Object.fromEntries(v.hopEvaluations.map(h => [h.nodeId, h.status]));

test('all hops live: OPTIMAL, no alert', () => {
  const v = rca.evaluate(linear, obs({ ingest: true, scte: true, xcode: true, vod: true, cdn: true }));
  assert.equal(v.overallState, 'OPTIMAL'); assert.equal(v.alertSuppression.alertRecommended, false);
});

test('SCTE down: SCTE is the convict, transcoder and CDN are cascaded and point at it, one alert', () => {
  const v = rca.evaluate(linear, obs({ ingest: true, scte: false, xcode: false, vod: true, cdn: false }));
  assert.deepEqual(status(v), { ingest: 'HEALTHY', scte: 'CONVICT', xcode: 'CASCADED', vod: 'HEALTHY', cdn: 'CASCADED' });
  assert.equal(v.overallState, 'CRITICAL_CONVICT'); assert.deepEqual(v.convictNodeIds, ['scte']);
  assert.equal(v.hopEvaluations.find(h => h.nodeId === 'cdn')!.convictNodeId, 'scte');
  assert.equal(v.alertSuppression.suppressedCount, 2); assert.equal(v.alertSuppression.alertRecommended, true);
  assert.match(v.rootCauseSummary!, /SCTE \(10\.0\.0\.2:63004\)/);
});

test('unprobed hops are never convicted or assumed healthy; the first probed down hop is convicted as unverified', () => {
  // Ingest and SCTE are UDP/SRT: no probe. Transcoder down.
  const v = rca.evaluate(linear, obs({ ingest: null, scte: null, xcode: false, vod: null, cdn: false }));
  assert.deepEqual(status(v), { ingest: 'UNPROBED', scte: 'UNPROBED', xcode: 'CONVICT', vod: 'UNPROBED', cdn: 'CASCADED' });
  assert.match(v.hopEvaluations.find(h => h.nodeId === 'xcode')!.details!, /not probed/);
  // Only the egress is probed and it is down: it is the first probed down hop, convicted as unverified.
  const onlyEgress = rca.evaluate(linear, obs({ cdn: false }));
  assert.equal(onlyEgress.overallState, 'CRITICAL_CONVICT'); assert.match(onlyEgress.hopEvaluations.at(-1)!.details!, /not probed/);
  // Egress down with no convict (upstream recovered moments ago): OFFLINE, still alerted.
  const offline = rca.evaluate(linear, obs({ ingest: true, scte: true, xcode: true, vod: true, cdn: false }), new Set(['xcode']));
  assert.equal(offline.overallState, 'OFFLINE'); assert.equal(offline.alertSuppression.alertRecommended, true);
  assert.equal(rca.evaluate(linear, obs({ ingest: true })).overallState, 'UNVERIFIED');
});

test('only the VOD fallback can feed the transcoder: ON_FALLBACK, alerted (viewers do not see the live channel)', () => {
  const v = rca.evaluate(linear, obs({ ingest: true, scte: false, xcode: true, vod: true, cdn: true }));
  assert.equal(status(v)['xcode'], 'FALLBACK_ACTIVE');
  assert.equal(v.overallState, 'ON_FALLBACK'); assert.equal(v.alertSuppression.alertRecommended, true);
});

test('SCTE down but the bypass from ingest is live: FAILOVER_ACTIVE, degraded, no alert', () => {
  const v = rca.evaluate(abn, obs({ 'ingest-main': true, 'scte-relay': false, xcode: true, 'cdn-egress': true }));
  assert.equal(status(v)['xcode'], 'FAILOVER_ACTIVE'); assert.equal(status(v)['scte-relay'], 'CONVICT');
  assert.equal(v.overallState, 'DEGRADED_FAILOVER'); assert.equal(v.alertSuppression.alertRecommended, false);
});

test('primary ingest down, backup ingest live (Sakshi): failover, no critical alert; a down backup is STANDBY', () => {
  const v = rca.evaluate(sakshi, obs({ 'ingest-main': false, 'ingest-backup': true, 'scte-relay': true, xcode: true, 'cdn-egress': true }));
  assert.equal(status(v)['scte-relay'], 'FAILOVER_ACTIVE'); assert.equal(v.overallState, 'DEGRADED_FAILOVER');
  const backupDown = rca.evaluate(sakshi, obs({ 'ingest-main': true, 'ingest-backup': false, 'scte-relay': true, xcode: true, 'cdn-egress': true }));
  assert.equal(status(backupDown)['ingest-backup'], 'STANDBY'); assert.equal(backupDown.overallState, 'OPTIMAL');
});

test('state transitions across rounds: hold-down after upstream recovery, then the still-down hop becomes the convict', () => {
  // Round 1: SCTE down, transcoder cascaded.
  assert.equal(status(rca.evaluate(linear, obs({ ingest: true, scte: false, xcode: false, vod: true, cdn: false })))['xcode'], 'CASCADED');
  // Round 2: SCTE back moments ago, transcoder not yet: still CASCADED (hold-down), no new convict.
  const holding = rca.evaluate(linear, obs({ ingest: true, scte: true, xcode: false, vod: true, cdn: false }), new Set(['scte']));
  assert.equal(status(holding)['xcode'], 'CASCADED'); assert.deepEqual(holding.convictNodeIds, []);
  // Round 3: hold-down over, transcoder still down: it is the real root cause now (CASCADED → CONVICT).
  const after = rca.evaluate(linear, obs({ ingest: true, scte: true, xcode: false, vod: true, cdn: false }));
  assert.equal(status(after)['xcode'], 'CONVICT'); assert.equal(after.overallState, 'CRITICAL_CONVICT');
  // Backup dies during failover (FAILOVER_ACTIVE → CASCADED/CONVICT).
  const both = rca.evaluate(sakshi, obs({ 'ingest-main': false, 'ingest-backup': false, 'scte-relay': false, xcode: false, 'cdn-egress': false }));
  assert.equal(both.overallState, 'CRITICAL_CONVICT'); assert.deepEqual(both.convictNodeIds, ['ingest-main']);
});

test('every channel shape evaluates without throwing for all-live, all-down and all-unprobed rounds', () => {
  for (const channel of [abn, sakshi, linear]) {
    for (const value of [true, false, null]) {
      const v = rca.evaluate(channel, new Map(channel.nodes.map(n => [n.id, value])));
      assert.equal(v.hopEvaluations.length, channel.nodes.length);
    }
  }
});
