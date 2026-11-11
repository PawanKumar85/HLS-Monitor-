import type { Edge, MarkerType } from '@xyflow/react';
import type { EdgeType } from '../Pipeline';

const sides = new Set(['top', 'right', 'bottom', 'left']);
const side = (value: string | null | undefined, fallback: string) => value && sides.has(value) ? value : fallback;

/** Keep the mouse-selected connection sides when creating or editing links. */
export function flowEdge(from: string, to: string, kind: EdgeType, sourceSide?: string | null, targetSide?: string | null): Edge & { pathOptions: { curvature: number } } {
  return {
    id: `${from}-${to}`, source: from, target: to,
    sourceHandle: side(sourceSide, kind === 'BYPASS' ? 'bottom' : 'right'), targetHandle: side(targetSide, 'left'),
    type: 'default', label: kind, data: { kind },
    pathOptions: { curvature: kind === 'BYPASS' ? 0.7 : 0.25 },
    style: kind === 'BYPASS' ? { strokeDasharray: '7 4' } : {},
    markerEnd: { type: 'arrowclosed' as MarkerType },
  };
}
