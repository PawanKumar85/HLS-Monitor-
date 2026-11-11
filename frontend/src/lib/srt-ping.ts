export interface SrtPing { host: string; state: 'REACHABLE' | 'NO_REPLY' | 'UNAVAILABLE'; latencyMs: number | null; checkedAt: string }
export function srtPingLabel(ping: SrtPing | undefined, now = Date.now()): string {
  if (!ping) return 'Ping pending';
  const age = now - Date.parse(ping.checkedAt);
  if (!Number.isFinite(age) || age > 90_000) return 'Ping result stale';
  if (ping.state === 'REACHABLE') return `Reachable${ping.latencyMs !== null ? ` · ${ping.latencyMs.toFixed(1)} ms` : ''}`;
  return ping.state === 'NO_REPLY' ? 'No ICMP reply' : 'Ping unavailable';
}
