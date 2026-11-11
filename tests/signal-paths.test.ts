import { test } from 'node:test';
import assert from 'node:assert/strict';
import { signalPaths } from '../frontend/src/lib/signal-paths.js';
import type { Topology, ChannelPipelineVerdict } from '../frontend/src/Pipeline.js';
const channel: Topology = { triggerId: 'sakshi', channelName: 'Sakshi TV', nodes: [{ id: 'main', name: 'Main', type: 'INGEST' }, { id: 'backup', name: 'Backup', type: 'INGEST', isBackup: true }, { id: 'scte', name: 'SCTE', type: 'SCTE_RELAY' }, { id: 'xcode', name: 'Transcoding', type: 'TRANSCODER' }, { id: 'cdn', name: 'Final', type: 'EGRESS_CDN' }, { id: 'vod', name: 'VOD', type: 'FALLBACK_SOURCE', isBackup: true }], edges: [{ fromNodeId: 'backup', toNodeId: 'xcode', type: 'FAILOVER' }, { fromNodeId: 'vod', toNodeId: 'xcode', type: 'FALLBACK' }] };
const verdict: ChannelPipelineVerdict = { triggerId: 'sakshi', channelName: 'Sakshi TV', overallState: 'OPTIMAL', rootCauseSummary: null, convictNodeIds: [], alertSuppression: { suppressedCount: 0, alertRecommended: false }, hopEvaluations: [{ nodeId: 'main', nodeName: 'Main', nodeType: 'INGEST', isLive: true, status: 'HEALTHY' }] };
test('Signal paths includes every configured hop even when only primary results have arrived', () => {
 const result=signalPaths([verdict],[channel])[0]!;
 assert.equal(result.hopEvaluations.length,6);
 assert.equal(result.hopEvaluations.find(h=>h.nodeId==='main')?.status,'HEALTHY');
 const backup=result.hopEvaluations.find(h=>h.nodeId==='backup')!;
 assert.equal(backup.backup,true);assert.equal(backup.status,'UNPROBED');
 assert.deepEqual(backup.feeds,[{name:'Transcoding',type:'FAILOVER'}]);
 assert.equal(result.hopEvaluations.find(h=>h.nodeId==='vod')?.fallback,true);
});
test('unprobed or new channels retain all nodes before the first verdict', () => {
 const result=signalPaths([],[channel])[0]!;
 assert.equal(result.overallState,'UNVERIFIED');
 assert.equal(result.hopEvaluations.length,6);
 assert.ok(result.hopEvaluations.every(h=>h.isLive===null&&h.status==='UNPROBED'));
});
test('deleted nodes are excluded and status-only fallback still renders without topology', () => {
 assert.equal(signalPaths([verdict],[{...channel,nodes:channel.nodes.filter(n=>n.id!=='main')}])[0]!.hopEvaluations.some(h=>h.nodeId==='main'),false);
 assert.equal(signalPaths([verdict],null)[0]!.hopEvaluations.length,1);
});
