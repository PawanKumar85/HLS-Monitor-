import { test } from 'node:test';
import assert from 'node:assert/strict';
import { serverDomain, workspaceGraph } from '../frontend/src/lib/workspace-graph.js';
import type { Topology, SavedProbe } from '../frontend/src/Pipeline.js';

const channel = (id: string): Topology => ({ triggerId: id, channelName: id.toUpperCase(), nodes: [{ id: 'main', name: 'Main', type: 'INGEST' }, { id: 'cdn', name: 'Final', type: 'EGRESS_CDN' }], edges: [{ fromNodeId: 'main', toNodeId: 'cdn', type: 'PRIMARY' }] });
const probe = (triggerId: string, nodeId: string, hls: string): SavedProbe => ({ triggerId, nodeId, kind: 'hls', hls });

test('workspace merges domains across channels while retaining every channel link and hop', () => {
  const graph = workspaceGraph([channel('abn'), channel('sakshi')], [
    probe('abn', 'main', 'https://INGEST.test:443/abn/index.m3u8?token=secret'),
    probe('sakshi', 'main', 'https://ingest.test:8443/sakshi/index.m3u8'),
    probe('abn', 'cdn', 'https://stream.test/abn/index.m3u8'),
    probe('sakshi', 'cdn', 'https://stream.test/sakshi/index.m3u8'),
  ]);
  assert.equal(graph.nodes.length, 2);
  assert.equal(graph.shared, 2);
  assert.equal(graph.hops, 4);
  assert.equal(graph.edges.length, 2);
  assert.equal(new Set(graph.edges.map(e => e.id)).size, 2);
  assert.equal(graph.nodes[0]!.data.label, 'ingest.test');
  assert.deepEqual(graph.nodes[0]!.data.members.map(m => m.channelName), ['ABN', 'SAKSHI']);
  assert.notEqual(graph.edges[0]!.style?.stroke, graph.edges[1]!.style?.stroke);
  assert.ok(!JSON.stringify(graph).includes('secret'), 'do not copy signed URLs into graph labels or metadata');
});

test('unknown servers remain separate even when channel-local hop IDs match', () => {
  const graph = workspaceGraph([channel('a'), channel('b')], []);
  assert.equal(graph.nodes.length, 4);
  assert.equal(graph.shared, 0);
  assert.equal(new Set(graph.nodes.map(n => n.id)).size, 4);
});

test('hostname grouping supports explicit host, TCP IPv6 and normalized HLS domains', () => {
  const node = channel('a').nodes[0]!;
  assert.equal(serverDomain({ ...node, host: 'Origin.TEST.' }, probe('a', 'main', 'https://cdn.test/a')), 'origin.test');
  assert.equal(serverDomain(node, { triggerId: 'a', nodeId: 'main', kind: 'tcp', tcp: '[2001:db8::1]:8080' }), '2001:db8::1');
  assert.equal(serverDomain(node, probe('a', 'main', 'invalid')), null);
  assert.equal(serverDomain(node, probe('a', 'main', 'https://host.test./a')), 'host.test');
});

test('same-server internal hops are preserved in details without misleading self-links', () => {
  const graph = workspaceGraph([channel('a')], [probe('a', 'main', 'https://same.test/main'), probe('a', 'cdn', 'https://same.test/final')]);
  assert.equal(graph.nodes.length, 1);
  assert.equal(graph.nodes[0]!.data.members.length, 2);
  assert.equal(graph.edges.length, 0);
  assert.equal(graph.links.length, 1);
  assert.equal(graph.links[0]!.internal, true);
});

test('different channels may traverse shared servers in opposite directions', () => {
  const graph = workspaceGraph([channel('a'), channel('b')], [probe('a', 'main', 'https://one.test/a'), probe('a', 'cdn', 'https://two.test/a'), probe('b', 'main', 'https://two.test/b'), probe('b', 'cdn', 'https://one.test/b')]);
  assert.equal(graph.nodes.length, 2);
  assert.equal(graph.edges.length, 2);
  assert.ok(graph.nodes.every(n => Number.isFinite(n.position.x)));
  assert.notDeepEqual(graph.nodes[0]!.position, graph.nodes[1]!.position);
  assert.deepEqual(workspaceGraph([], []).nodes, []);
});
