import type { Edge } from '@xyflow/react';
import { flowEdge } from './pipeline-canvas-edge';
export const CHANNEL_NODE_ID = '__channel__';
export const isChannelLink = (edge: Pick<Edge, 'source' | 'target'>) => edge.source === CHANNEL_NODE_ID || edge.target === CHANNEL_NODE_ID;
export function readChannelLinks(raw: string | null, hopIds: string[]): Edge[] {
  try {
    const items: unknown = JSON.parse(raw ?? '[]');
    if (!Array.isArray(items)) return [];
    const ids = new Set([CHANNEL_NODE_ID, ...hopIds]);
    const seen = new Set<string>();
    return items.flatMap(item => {
      if (!item || typeof item.source !== 'string' || typeof item.target !== 'string' || item.source === item.target || !ids.has(item.source) || !ids.has(item.target) || !isChannelLink(item)) return [];
      const key = JSON.stringify([item.source, item.target]);
      if (seen.has(key)) return [];
      seen.add(key);
      return [{ ...flowEdge(item.source, item.target, 'PRIMARY', item.sourceHandle, item.targetHandle), label: 'CHANNEL' }];
    });
  } catch { return []; }
}
