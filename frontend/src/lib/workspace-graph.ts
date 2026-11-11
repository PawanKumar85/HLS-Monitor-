import type { Node, Edge, MarkerType } from '@xyflow/react';
import type { Topology, TopologyNode, SavedProbe, EdgeType } from '../Pipeline';
import { depths } from './canvas-editing';

export interface WorkspaceMember { channelId: string; channelName: string; nodeId: string; name: string; type: string; probe: string }
export type ServerNode = Node<{ label: string; domain: string | null; members: WorkspaceMember[] }>;
export interface WorkspaceLink { channelName: string; from: string; to: string; kind: EdgeType; internal: boolean; color: string }
const COLORS = ['#7db8ed', '#c7a1ed', '#80c9a4', '#e8b97b', '#e397aa', '#84cfd0'];

/** Domain is the shared identity; ports and playlist paths do not create extra servers. */
export function serverDomain(node: TopologyNode, probe?: SavedProbe): string | null {
  let host = node.host?.trim();
  if (!host && probe?.kind === 'hls' && probe.hls) {
    try { host = new URL(probe.hls).hostname; } catch { return null; }
  }
  if (!host && probe?.kind === 'tcp' && probe.tcp) {
    try { host = new URL(`http://${probe.tcp}`).hostname; } catch { return null; }
  }
  return host ? host.toLowerCase().replace(/^\[|\]$/g, '').replace(/\.$/, '') : null;
}

export function workspaceGraph(channels: Topology[], probes: SavedProbe[]) {
  const targets = new Map(probes.map(p => [JSON.stringify([p.triggerId, p.nodeId]), p]));
  const servers = new Map<string, ServerNode>();
  const assignments = new Map<string, string>();
  const edges: Edge[] = [];
  const links: WorkspaceLink[] = [];
  const key = (channel: string, hop: string) => JSON.stringify([channel, hop]);
  const legend = channels.map((channel, i) => ({ id: channel.triggerId, name: channel.channelName, color: COLORS[i % COLORS.length]! }));
  for (const channel of channels) {
    for (const hop of channel.nodes) {
      const probe = targets.get(key(channel.triggerId, hop.id));
      const domain = serverDomain(hop, probe);
      const id = domain ? `server:${domain}` : `hop:${key(channel.triggerId, hop.id)}`;
      assignments.set(key(channel.triggerId, hop.id), id);
      let server = servers.get(id);
      if (!server) {
        server = { id, type: 'server', position: { x: 0, y: 0 }, data: { label: domain ?? hop.name, domain, members: [] } };
        servers.set(id, server);
      }
      server.data.members.push({ channelId: channel.triggerId, channelName: channel.channelName, nodeId: hop.id, name: hop.name, type: hop.type, probe: probe?.kind ?? 'none' });
    }
  }
  channels.forEach((channel, index) => {
    const color = legend[index]!.color;
    channel.edges.forEach(edge => {
      const source = assignments.get(key(channel.triggerId, edge.fromNodeId));
      const target = assignments.get(key(channel.triggerId, edge.toNodeId));
      if (!source || !target) return;
      links.push({ channelName: channel.channelName, from: channel.nodes.find(n => n.id === edge.fromNodeId)!.name, to: channel.nodes.find(n => n.id === edge.toNodeId)!.name, kind: edge.type, internal: source === target, color });
      if (source === target) return; // Hops hosted together appear inside the single server card.
      edges.push({ id: key(channel.triggerId, JSON.stringify([edge.fromNodeId, edge.toNodeId, edge.type])), source, target, sourceHandle: 'out', targetHandle: 'in', type: 'default', label: `${channel.channelName} · ${edge.type}`, style: { stroke: color, strokeWidth: 2, ...(edge.type !== 'PRIMARY' ? { strokeDasharray: '7 4' } : {}) }, labelStyle: { fill: color }, markerEnd: { type: 'arrowclosed' as MarkerType, color }, pathOptions: { curvature: 0.25 + index * 0.12 } } as Edge);
    });
  });
  // Layer by hop depth within each channel. Do not topologically sort merged servers:
  // different channels can legitimately use the same servers in opposite directions.
  const ranks = new Map<string, number>();
  for (const channel of channels) {
    const depth = depths(channel.nodes.map(n => n.id), channel.edges.map(e => ({ source: e.fromNodeId, target: e.toNodeId })));
    channel.nodes.forEach(n => { const id = assignments.get(key(channel.triggerId, n.id))!; ranks.set(id, Math.max(ranks.get(id) ?? 0, depth.get(n.id) ?? 0)); });
  }
  const rows = new Map<number, number>();
  const nodes = [...servers.values()].map(node => {
    const column = ranks.get(node.id) ?? 0; const row = rows.get(column) ?? 0;
    rows.set(column, row + 1);
    return { ...node, position: { x: column * 400, y: row * 260 } };
  });
  return { nodes, edges, links, legend, shared: nodes.filter(n => new Set(n.data.members.map(m => m.channelId)).size > 1).length, hops: channels.reduce((sum, c) => sum + c.nodes.length, 0) };
}
