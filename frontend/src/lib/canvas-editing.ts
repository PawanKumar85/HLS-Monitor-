/** Each hop's column: the longest path from a hop with no incoming link (Kahn's algorithm). Hops in a loop stay at 0. */
export function depths(ids: readonly string[], edges: readonly { source: string; target: string }[]): Map<string, number> {
  const known = new Set(ids);
  const links = edges.filter(e => known.has(e.source) && known.has(e.target));
  const incoming = new Map(ids.map(id => [id, links.filter(e => e.target === id).length]));
  const depth = new Map(ids.map(id => [id, 0]));
  const queue = ids.filter(id => incoming.get(id) === 0);
  for (let i = 0; i < queue.length; i++) {
    const id = queue[i]!;
    for (const edge of links.filter(e => e.source === id)) {
      depth.set(edge.target, Math.max(depth.get(edge.target)!, depth.get(id)! + 1));
      incoming.set(edge.target, incoming.get(edge.target)! - 1);
      if (incoming.get(edge.target) === 0) queue.push(edge.target);
    }
  }
  return depth;
}

/** Arrange a directed graph into columns; keep branches on separate rows. */
export function arrangeNodes<T extends { id: string; position: { x: number; y: number } }>(nodes: T[], edges: { source: string; target: string }[]): T[] {
  const depth = depths(nodes.map(n => n.id), edges);
  const rows = new Map<number, number>();
  return nodes.map(node => {
    const column = depth.get(node.id)!;
    const row = rows.get(column) ?? 0;
    rows.set(column, row + 1);
    return { ...node, position: { x: column * 300, y: row * 160 } };
  });
}

/** Ignore selection and rendering measurements when checking for unsaved edits. */
export function canvasSignature(name: string, nodes: { id: string; position: unknown; data: unknown }[], edges: { id: string; source: string; target: string; sourceHandle?: string | null; targetHandle?: string | null; data?: unknown }[]): string {
  return JSON.stringify({ name, nodes: nodes.map(({ id, position, data }) => ({ id, position, data })), edges: edges.map(({ id, source, target, sourceHandle, targetHandle, data }) => ({ id, source, target, sourceHandle, targetHandle, data })) });
}

/** True when a signal can travel from `from` to `to` along the links (following their direction). */
export function pathExists(links: readonly { source: string; target: string }[], from: string, to: string, seen = new Set<string>()): boolean {
  if (from === to) return true;
  if (seen.has(from)) return false;
  seen.add(from);
  return links.some(e => e.source === from && pathExists(links, e.target, to, seen));
}
