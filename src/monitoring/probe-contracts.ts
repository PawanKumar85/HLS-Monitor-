export interface StreamProbeConfig {
  serverIp: string;
  streamId: string;
  streamUrl: string;
  intervalMs: number;
  staleAfterMs: number;
  enabled: boolean;
}
export interface StreamProbeRegistration {
  serverIp: string;
  streamId: string;
  streamUrl: string;
  intervalMs?: number;
  staleAfterMs?: number;
  enabled?: boolean;
}
export interface DueProbe { config: StreamProbeConfig; nextCheckAt: number }
export interface StreamInventory {
  upsert(config: StreamProbeRegistration, initialJitterMs?: number): Promise<void>;
  list(): Promise<StreamProbeConfig[]>;
  get(serverIp: string, streamId: string): Promise<StreamProbeConfig | null>;
  due(limit: number): Promise<DueProbe[]>;
}
export interface ProbeJobProducer {
  enqueueDue(config: StreamProbeConfig, jitterMs: number): Promise<'queued' | 'busy' | 'not-due' | 'changed'>;
}
export interface PlaylistProgress { fingerprint: string; targetDurationMs: number }
export interface PlaylistFreshnessStore {
  observe(serverIp: string, streamId: string, streamUrl: string, progress: PlaylistProgress, staleAfterMs: number): Promise<boolean>;
  /** Records a failed fetch. True while the stream should still show online (a single blip). */
  fail(serverIp: string, streamId: string, streamUrl: string): Promise<boolean>;
}
export function normalizeProbeConfig(value: StreamProbeRegistration): StreamProbeConfig {
  if (!value || typeof value !== 'object') throw new TypeError('Invalid probe registration');
  const { serverIp, streamId, streamUrl } = value;
  if (typeof serverIp !== 'string' || !serverIp.trim() || serverIp.length > 255 ||
    typeof streamId !== 'string' || !streamId.trim() || streamId.length > 1024 ||
    typeof streamUrl !== 'string' || streamUrl.length > 8192) throw new TypeError('Invalid probe identity or URL');
  const url = new URL(streamUrl);
  if (!['http:', 'https:'].includes(url.protocol)) throw new TypeError('Probe URL must use HTTP or HTTPS');
  const intervalMs = value.intervalMs ?? 10_000;
  const staleAfterMs = value.staleAfterMs ?? 30_000;
  if (![intervalMs, staleAfterMs].every(n => Number.isSafeInteger(n) && n >= 1000 && n <= 2_147_483_647)) throw new RangeError('Probe intervals must be between 1000 and 2147483647ms');
  if (value.enabled !== undefined && typeof value.enabled !== 'boolean') throw new TypeError('enabled must be boolean');
  return { serverIp, streamId, streamUrl: url.href, intervalMs, staleAfterMs, enabled: value.enabled ?? true };
}
