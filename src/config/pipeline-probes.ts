/**
 * How one hop is probed. Users set this from the dashboard (stored in MongoDB). Hops with no
 * entry are UNPROBED: UDP and SRT feeds cannot be checked with an HTTP or TCP request, and an
 * unknown URL must never be guessed.
 *   hls: playlist + first media bytes, with the 2-failure rule and frozen-playlist detection
 *   tcp: "host:port" accepts a connection (RTMP ingest, any TCP listener)
 */
export type ProbeTarget = { kind: 'hls'; url: string } | { kind: 'tcp'; host: string; port: number };
export type ProbeTargets = ReadonlyMap<string, ProbeTarget>;
export const targetKey = (triggerId: string, nodeId: string) => `${triggerId}/${nodeId}`;

export interface ProbeTargetStore {
  /** Every saved target, keyed by targetKey(triggerId, nodeId). */
  all(): Promise<Map<string, ProbeTarget>>;
  set(triggerId: string, nodeId: string, target: ProbeTarget, by: string | null): Promise<void>;
  remove(triggerId: string, nodeId: string): Promise<boolean>;
}

/** Parses the form value {"hls": "https://..."} or {"tcp": "host:port"}. Throws TypeError with a message fit for the user. */
export function parseProbeTarget(spec: unknown): ProbeTarget {
  const entries = spec && typeof spec === 'object' && !Array.isArray(spec) ? Object.entries(spec) : [];
  const [kind, raw] = entries.length === 1 ? entries[0]! : [undefined, undefined];
  if (kind === 'hls' && typeof raw === 'string') {
    let url: URL; try { url = new URL(raw.trim()); } catch { throw new TypeError('Playlist URL must be a full http(s) URL'); }
    if (!['http:', 'https:'].includes(url.protocol)) throw new TypeError('Playlist URL must use http or https');
    if (url.href.length > 8192) throw new TypeError('Playlist URL is too long');
    return { kind: 'hls', url: url.href };
  }
  if (kind === 'tcp' && typeof raw === 'string') {
    const match = /^([A-Za-z0-9.-]{1,253}|\[[0-9A-Fa-f:.]+\]):(\d{1,5})$/.exec(raw.trim());
    const port = Number(match?.[2]);
    if (!match || port < 1 || port > 65535) throw new TypeError('TCP address must be "host:port", for example ingest.example.com:1935');
    return { kind: 'tcp', host: match[1]!.replace(/^\[|\]$/g, ''), port };
  }
  throw new TypeError('Give exactly one of {"hls": "<playlist URL>"} or {"tcp": "<host:port>"}');
}

/**
 * An HLS hop is checked by the normal stream flow (Stream API → MongoDB → Redis schedule → consumers),
 * registered as a stream with this identity: its origin host as server, and a reserved stream ID.
 */
export const pipelineStream = (triggerId: string, nodeId: string, url: string) =>
  ({ serverIp: new URL(url).hostname, streamId: `pipeline:${triggerId}/${nodeId}` });
