import type { ChannelTopology, PipelineEdge, PipelineEdgeType, PipelineNode, PipelineNodeType } from '../types/pipeline.types.js';

export interface ChannelStore {
  list(): Promise<ChannelTopology[]>;
  get(triggerId: string): Promise<ChannelTopology | null>;
  /** False when a channel with this ID already exists. */
  create(channel: ChannelTopology): Promise<boolean>;
  /** False when the channel does not exist. */
  replace(channel: ChannelTopology): Promise<boolean>;
  remove(triggerId: string): Promise<boolean>;
}

const NODE_TYPES: readonly PipelineNodeType[] = ['INGEST', 'SCTE_RELAY', 'TRANSCODER', 'EGRESS_CDN', 'FALLBACK_SOURCE'];
const EDGE_TYPES: readonly PipelineEdgeType[] = ['PRIMARY', 'FAILOVER', 'BYPASS', 'FALLBACK'];
const MAX_HOPS = 30;

/** "ABN Andhrajyoty" → "abn_andhrajyoty". Fixed at creation, so renaming a channel keeps its probe targets. */
export const channelId = (name: string) => name.toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_+|_+$/g, '').slice(0, 60) || 'channel';

const record = (value: unknown, what: string): Record<string, unknown> => {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new TypeError(`${what} must be an object`);
  return value as Record<string, unknown>;
};
const text = (value: unknown, what: string) => {
  if (typeof value !== 'string' || !value.trim() || value.length > 100) throw new TypeError(`${what} must be 1 to 100 characters`);
  return value.trim();
};

/**
 * Validates a channel sent by the dashboard: {channelName, nodes, edges}. Throws TypeError with a message fit for the user.
 * Guarantees what the RCA engine relies on: unique hop IDs, edges between known hops, no loops, and exactly one
 * main CDN egress hop (the hop viewers watch, which decides the channel state).
 */
export function parseChannel(value: unknown, triggerId: string): ChannelTopology {
  const body = record(value, 'Channel');
  const channelName = text(body['channelName'], 'Channel name');
  const rawNodes = body['nodes'];
  if (!Array.isArray(rawNodes) || !rawNodes.length || rawNodes.length > MAX_HOPS) throw new TypeError(`A channel needs 1 to ${MAX_HOPS} hops`);

  const nodes: PipelineNode[] = rawNodes.map((raw, i) => {
    const n = record(raw, `Hop ${i + 1}`);
    const name = text(n['name'], `Hop ${i + 1} name`);
    if (typeof n['id'] !== 'string' || !/^[a-z0-9-]{1,40}$/.test(n['id'])) throw new TypeError(`${name}: hop ID must be 1 to 40 lowercase letters, digits or dashes`);
    if (!NODE_TYPES.includes(n['type'] as PipelineNodeType)) throw new TypeError(`${name}: type must be one of ${NODE_TYPES.join(', ')}`);
    const node: PipelineNode = { id: n['id'], name, type: n['type'] as PipelineNodeType };
    if (n['host'] !== undefined && n['host'] !== null && n['host'] !== '') {
      if (typeof n['host'] !== 'string' || !/^[A-Za-z0-9.:[\]-]{1,253}$/.test(n['host'].trim())) throw new TypeError(`${name}: host must be a hostname or IP address`);
      node.host = n['host'].trim();
    }
    if (n['port'] !== undefined && n['port'] !== null && n['port'] !== '') {
      if (!Number.isInteger(n['port']) || (n['port'] as number) < 1 || (n['port'] as number) > 65535) throw new TypeError(`${name}: port must be 1 to 65535`);
      node.port = n['port'] as number;
    }
    if (n['transport'] !== undefined && n['transport'] !== 'srt') throw new TypeError(`${name}: transport must be srt when specified`);
    if (n['transport'] === 'srt') node.transport = 'srt';
    if (n['isBackup'] === true) node.isBackup = true;
    else if (n['isBackup'] !== undefined && n['isBackup'] !== false) throw new TypeError(`${name}: isBackup must be true or false`);
    return node;
  });
  const ids = new Set(nodes.map(n => n.id));
  if (ids.size !== nodes.length) throw new TypeError('Two hops have the same ID');

  const rawEdges = body['edges'] ?? [];
  if (!Array.isArray(rawEdges) || rawEdges.length > MAX_HOPS * 4) throw new TypeError('Edges must be a list');
  const seen = new Set<string>();
  const edges: PipelineEdge[] = rawEdges.map(raw => {
    const e = record(raw, 'Edge');
    const fromNodeId = e['fromNodeId'], toNodeId = e['toNodeId'], type = e['type'];
    if (typeof fromNodeId !== 'string' || typeof toNodeId !== 'string' || !ids.has(fromNodeId) || !ids.has(toNodeId)) throw new TypeError('An edge points at a hop that does not exist');
    if (fromNodeId === toNodeId) throw new TypeError('A hop cannot feed itself');
    if (!EDGE_TYPES.includes(type as PipelineEdgeType)) throw new TypeError(`Edge type must be one of ${EDGE_TYPES.join(', ')}`);
    if (seen.has(`${fromNodeId}>${toNodeId}`)) throw new TypeError('Two edges join the same two hops');
    seen.add(`${fromNodeId}>${toNodeId}`);
    return { fromNodeId, toNodeId, type: type as PipelineEdgeType };
  });

  if (nodes.filter(n => n.type === 'EGRESS_CDN' && !n.isBackup).length !== 1) throw new TypeError('Mark exactly one main hop as CDN egress: the hop viewers watch');

  // Kahn's algorithm: every hop must be reachable in upstream-first order, or the hops form a loop.
  const inDegree = new Map(nodes.map(n => [n.id, 0]));
  for (const e of edges) inDegree.set(e.toNodeId, inDegree.get(e.toNodeId)! + 1);
  const queue = nodes.filter(n => inDegree.get(n.id) === 0).map(n => n.id);
  let ordered = 0;
  while (queue.length) {
    const id = queue.shift()!; ordered++;
    for (const e of edges) if (e.fromNodeId === id && inDegree.set(e.toNodeId, inDegree.get(e.toNodeId)! - 1).get(e.toNodeId) === 0) queue.push(e.toNodeId);
  }
  if (ordered !== nodes.length) throw new TypeError('The hops form a loop; a signal must flow one way from ingest to CDN');

  return { triggerId, channelName, nodes, edges };
}
