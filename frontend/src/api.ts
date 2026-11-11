/** Same-origin API calls with the session cookie. A 401 means the session ended: send the user to sign in. */
export class AuthError extends Error {}
export class ApiError extends Error { constructor(message: string, readonly status: number) { super(message); } }

export function toLogin(): void {
  location.assign(`/login?error=expired&next=${encodeURIComponent(location.pathname + location.search + location.hash)}`);
}

export async function api<T>(path: string, init: RequestInit = {}): Promise<T> {
  const headers = { ...(init.body ? { 'Content-Type': 'application/json' } : {}), ...(init.headers as Record<string, string> | undefined) };
  const response = await fetch(path, { ...init, headers, cache: 'no-store' });
  if (response.status === 401) { toLogin(); throw new AuthError('Signed out'); }
  let body: unknown = null; try { body = await response.json(); } catch { /* empty body */ }
  if (!response.ok) throw new ApiError((body as { error?: string } | null)?.error || `HTTP ${response.status}`, response.status);
  return body as T;
}

// ---- Shapes returned by the backend
export interface User { id: string; username: string }
export interface WorstStream {
  streamId: string; checks: number; observedMs: number; downtimeMs: number; availabilityPercent: number; outages: number; mttrMs: number | null;
  avgLatencyMs: number | null; p95LatencyMs: number | null; p99LatencyMs: number | null; lastOfflineAt: string | null;
}
export interface ServerSummary extends Omit<WorstStream, 'streamId'> { serverIp: string; streams: number; streamsWithOffline: number; failureReasons: { reason: string; checks: number }[]; worstStreams: WorstStream[] }
export interface Report { from: string; to: string; hours: number; servers: ServerSummary[] }

// ---- Formatting
export const clock = new Intl.DateTimeFormat(undefined, { hour: '2-digit', minute: '2-digit', second: '2-digit' });
export const stamp = new Intl.DateTimeFormat(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
export function ago(ms: number, now = Date.now()): string {
  const s = Math.max(0, Math.round((now - ms) / 1000));
  if (s < 60) return `${s} s ago`;
  if (s < 3600) return `${Math.floor(s / 60)} min ago`;
  return `${Math.floor(s / 3600)} h ago`;
}

// ---- Filters and the report window live in the query string, so a link or reload keeps them.
export const param = (name: string) => new URLSearchParams(location.search).get(name) ?? '';
export function setParam(name: string, value: string): void {
  const params = new URLSearchParams(location.search);
  if (value) params.set(name, value); else params.delete(name);
  // Keep history.state: the router stores its entry index there, which useBlocker needs for Back/Forward.
  history.replaceState(history.state, '', `${location.pathname}${params.size ? `?${params}` : ''}${location.hash}`);
}
