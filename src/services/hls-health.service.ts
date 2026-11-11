import { createHash } from 'node:crypto';
import type { HlsFailureReason, HlsHealthResult } from '../types/hls-health.types.js';
import type { PlaylistProgress } from '../monitoring/probe-contracts.js';
export type { HlsHealthResult } from '../types/hls-health.types.js';
export interface HlsProbeFailure {
  stage: 'playlist-headers' | 'playlist-body' | 'playlist-parse' | 'segment-headers' | 'segment-body';
  host: string; elapsedMs: number; reason: string; code?: string; httpStatus?: number;
}
export interface HlsInspection { health: HlsHealthResult; progress?: PlaylistProgress; failure?: HlsProbeFailure }

/** Reads until the first non-empty chunk, then cancels. Servers that ignore Range still don't stream the whole segment to us. */
async function hasMediaBytes(response: Response): Promise<boolean> {
  if (!response.body) throw new Error('Missing HLS response body');
  const reader = response.body.getReader();
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) return false;
      if (value.byteLength > 0) return true;
    }
  } finally { await reader.cancel().catch(() => {}); reader.releaseLock(); }
}
async function readBody(response: Response, maxBytes: number): Promise<Uint8Array[]> {
  if (!response.body) throw new Error('Missing HLS response body');
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let size = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) return chunks;
      size += value.byteLength;
      if (size > maxBytes) throw new Error('HLS response exceeds probe limit');
      chunks.push(value);
    }
  } finally { await reader.cancel().catch(() => {}); reader.releaseLock(); }
}

/** One short code per failure, for the report: network vs origin vs encoder problems. */
export function failureReason(f: Pick<HlsProbeFailure, 'stage' | 'reason' | 'code' | 'httpStatus'>): HlsFailureReason {
  if (f.code === 'ENOTFOUND' || f.code === 'EAI_AGAIN') return 'DNS';
  if (f.code === 'ECONNREFUSED') return 'REFUSED';
  if (f.reason === 'TimeoutError' || f.reason === 'AbortError') return 'TIMEOUT';
  if (f.httpStatus === 404) return 'HTTP_404';
  if (f.httpStatus !== undefined && f.httpStatus >= 500) return 'HTTP_5XX';
  if (f.httpStatus !== undefined && f.httpStatus >= 400) return 'HTTP_4XX';
  if (f.stage === 'playlist-parse') return 'BAD_PLAYLIST';
  if (f.stage === 'segment-body') return 'EMPTY_SEGMENT';
  return 'NETWORK';
}

/** Probe one media playlist (following up to two master-playlist variants). */
export async function inspectHls(streamUrl: string, fetcher: typeof fetch = fetch): Promise<HlsInspection> {
  const start = performance.now();
  const overall = AbortSignal.timeout(6000);
  const result = (status: 0 | 1): HlsHealthResult => ({ status, latency: Math.round(performance.now() - start) });
  let stage: HlsProbeFailure['stage'] = 'playlist-headers';
  let host = '';
  let httpStatus: number | undefined;
  try {
    let playlistUrl = streamUrl;
    for (let depth = 0; depth < 3; depth++) {
      stage = 'playlist-headers'; host = new URL(playlistUrl).hostname; httpStatus = undefined;
      const response = await fetcher(playlistUrl, { signal: AbortSignal.any([overall, AbortSignal.timeout(3000)]), headers: { 'Cache-Control': 'no-cache' } });
      httpStatus = response.status;
      if (!response.ok) { await response.body?.cancel(); throw new Error('Playlist unavailable'); }
      stage = 'playlist-body';
      const playlist = Buffer.concat(await readBody(response, 1024 * 1024)).toString('utf8').replace(/^\uFEFF/, '').trim();
      stage = 'playlist-parse';
      if (!playlist.startsWith('#EXTM3U')) throw new Error('Invalid HLS playlist');
      const lines = playlist.split(/\r?\n/).map(line => line.trim()).filter(Boolean);
      const uris = lines.filter(line => !line.startsWith('#'));
      if (!uris.length) throw new Error('No media segments');
      const base = response.url || playlistUrl;
      if (lines.some(line => line.startsWith('#EXT-X-STREAM-INF:'))) {
        playlistUrl = new URL(uris[0]!, base).href;
        continue;
      }
      // An ended/VOD playlist is accessible, but no longer a live broadcast.
      if (lines.includes('#EXT-X-ENDLIST') || !lines.some(line => line.startsWith('#EXTINF:'))) return { health: { ...result(0), reason: 'ENDED' } };
      const segmentUrl = new URL(uris.at(-1)!, base);
      if (!['http:', 'https:'].includes(segmentUrl.protocol)) throw new Error('Invalid segment protocol');
      // Only proof that media bytes are served is needed; downloading whole segments cost ~2 MB per check.
      stage = 'segment-headers'; host = segmentUrl.hostname; httpStatus = undefined;
      const segment = await fetcher(segmentUrl.href, { signal: AbortSignal.any([overall, AbortSignal.timeout(3000)]), headers: { 'Cache-Control': 'no-cache', Range: 'bytes=0-1023' } });
      httpStatus = segment.status;
      if (!segment.ok) { await segment.body?.cancel(); throw new Error('Segment unavailable'); }
      stage = 'segment-body';
      if (!await hasMediaBytes(segment)) throw new Error('Empty media segment');
      stage = 'playlist-parse';
      const sequence = lines.find(line => line.startsWith('#EXT-X-MEDIA-SEQUENCE:'))?.split(':')[1];
      if (sequence !== undefined && !/^\d+$/.test(sequence)) throw new Error('Invalid media sequence');
      const lastSequence = sequence === undefined ? null : (BigInt(sequence) + BigInt(uris.length - 1)).toString();
      const targetSeconds = Number(lines.find(line => line.startsWith('#EXT-X-TARGETDURATION:'))?.split(':')[1] ?? 0);
      if (!Number.isFinite(targetSeconds) || targetSeconds < 0 || targetSeconds * 3000 > 2_147_483_647) throw new Error('Invalid target duration');
      // With a media sequence, rotating auth query strings must not fake progress.
      const mediaIdentity = lastSequence === null ? segmentUrl.href : segmentUrl.origin + segmentUrl.pathname;
      const range = lines.filter(line => line.startsWith('#EXT-X-BYTERANGE:')).at(-1) ?? '';
      const fingerprint = createHash('sha256').update(JSON.stringify([lastSequence, mediaIdentity, range])).digest('hex');
      return { health: result(1), progress: { fingerprint, targetDurationMs: targetSeconds * 1000 } };
    }
    throw new Error('HLS master nesting limit exceeded');
  } catch (error) {
    const cause = error instanceof Error ? error.cause : undefined;
    const rawCode = cause && typeof cause === 'object' && 'code' in cause ? cause.code : undefined;
    // Do not log full URLs, query strings, credentials, or arbitrary remote error messages.
    const code = typeof rawCode === 'string' && /^[A-Z0-9_]{1,64}$/.test(rawCode) ? rawCode : undefined;
    const reason = error instanceof Error && ['TimeoutError', 'AbortError', 'TypeError'].includes(error.name) ? error.name : 'ProbeError';
    const failure: HlsProbeFailure = { stage, host, elapsedMs: result(0).latency, reason, ...(code ? { code } : {}), ...(httpStatus !== undefined ? { httpStatus } : {}) };
    return { health: { ...result(0), reason: failureReason(failure) }, failure };
  }
}

/** Stateless compatibility API; the runtime adds shared freshness observation. */
export async function checkHls(streamUrl: string): Promise<HlsHealthResult> {
  return (await inspectHls(streamUrl)).health;
}
