import type { Report } from '../api';
/** Sum observed stream time; do not average per-server percentages or imply wall-clock downtime. */
export function reportSummary(servers: Report['servers']) {
  const observedMs = servers.reduce((sum, s) => sum + s.observedMs, 0);
  const downtimeMs = servers.reduce((sum, s) => sum + s.downtimeMs, 0);
  return { servers: servers.length, affected: servers.filter(s => s.downtimeMs > 0).length,
    outages: servers.reduce((sum, s) => sum + s.outages, 0), downtimeMs,
    availability: observedMs > 0 ? Math.max(0, Math.min(100, Math.floor(10000 * (1 - downtimeMs / observedMs)) / 100)) : null };
}
