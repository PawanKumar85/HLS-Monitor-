import type { Connection, Edge } from '@xyflow/react';
export const ARROW_SIDES = ['top', 'right', 'bottom', 'left'] as const;
export type ArrowSide = typeof ARROW_SIDES[number];
export interface ArrowLayout { source: string; target: string; sourceSide: ArrowSide; targetSide: ArrowSide; curvature: number }
export type ArrowLayouts = Record<string, ArrowLayout>;
const isSide = (value: unknown): value is ArrowSide => ARROW_SIDES.includes(value as ArrowSide);
export function readArrowLayouts(value: string | null): ArrowLayouts {
  try {
    const parsed: unknown = JSON.parse(value ?? '{}');
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return {};
    return Object.fromEntries(Object.entries(parsed).filter(([, item]) => {
      const x = item as Partial<ArrowLayout> | null;
      return x && typeof x.source === 'string' && typeof x.target === 'string' && isSide(x.sourceSide) && isSide(x.targetSide) && typeof x.curvature === 'number' && Number.isFinite(x.curvature) && x.curvature >= 0 && x.curvature <= 1.5;
    }));
  } catch { return {}; }
}
export function arrowLayout(edge: Edge, saved?: ArrowLayout): ArrowLayout {
  return saved?.source === edge.source && saved.target === edge.target ? saved : { source: edge.source, target: edge.target, sourceSide: 'right', targetSide: 'left', curvature: 0.25 };
}
/** Presentation-only reconnection: never allow a different pair of servers. */
export function reconnectArrow(edge: Edge, connection: Connection, saved?: ArrowLayout): ArrowLayout | null {
  if (connection.source !== edge.source || connection.target !== edge.target || !isSide(connection.sourceHandle) || !isSide(connection.targetHandle)) return null;
  return { ...arrowLayout(edge, saved), sourceSide: connection.sourceHandle, targetSide: connection.targetHandle };
}
