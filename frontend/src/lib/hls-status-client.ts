export interface HlsStatus {
  streamId: string;
  serverIp: string;
  status: 0 | 1;
  latencyMs: number;
  checkedAt: string;
}
export type HlsDisplayState = "ONLINE" | "OFFLINE" | "STALE" | "UNKNOWN";

function parseEvent(raw: unknown): HlsStatus | null {
  if (typeof raw !== "string") return null;
  try {
    const event = JSON.parse(raw);
    if (event?.type !== "hls:status") return null;
    const data = event.data;
    if (!data || typeof data.streamId !== "string" || typeof data.serverIp !== "string" ||
        (data.status !== 0 && data.status !== 1) ||
        typeof data.latencyMs !== "number" || !Number.isFinite(data.latencyMs) || data.latencyMs < 0 ||
        typeof data.checkedAt !== "string" || !Number.isFinite(Date.parse(data.checkedAt))) return null;
    return data;
  } catch { return null; }
}

export interface StatusEventSource {
  readonly readyState: number;
  onerror: ((event: Event) => void) | null;
  addEventListener(type: string, listener: (event: MessageEvent<string>) => void): void;
  close(): void;
}
export interface StatusClientDependencies {
  open(url: string): StatusEventSource;
  fetchSnapshot(url: string, signal: AbortSignal): Promise<unknown>;
  now(): number;
}
interface Options {
  url: string;
  snapshotUrl: string;
  baseUrl: string;
  onSnapshot(streams: Record<string, HlsStatus>): void;
  onStatus(stream: HlsStatus): void;
  onConnected(connected: boolean): void;
  /** Called on each failed snapshot (it retries every 3 s); `status` is the HTTP status when there was one, e.g. 401. */
  onSnapshotError?(status: number | undefined): void;
}
/** Framework-independent lifecycle used by the React hook. */
export function startHlsStatusClient(options: Options, dependencies: StatusClientDependencies = {
  open: url => new EventSource(url),
  fetchSnapshot: async (url, signal) => {
    const response = await fetch(url, { signal, cache: 'no-store' });
    if (!response.ok) throw Object.assign(new Error('Snapshot unavailable'), { status: response.status });
    return response.json();
  },
  now: () => Date.now(),
}): () => void {
  const { url, snapshotUrl } = options;
  let disposed = false;
  let source: StatusEventSource | undefined;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let controller: AbortController | undefined;
  let epoch = "", sequence = 0, cursor = "";
  let lastSignal = dependencies.now();
  let unreadySince: number | null = dependencies.now();
  let reconnectErrors = 0;
  const validCursor = (value: unknown): value is { epoch: string; sequence: number; cursor: string } => {
    const v = value as { epoch?: string; sequence?: number; cursor?: string } | null;
    return !!v && typeof v.epoch === "string" && /^[a-f0-9-]{36}$/.test(v.epoch) && Number.isSafeInteger(v.sequence) && v.sequence! >= 0 && v.cursor === `${v.epoch}:${v.sequence}`;
  };
  const connect = () => {
    if (disposed) return;
    source?.close(); options.onConnected(false);
    unreadySince = dependencies.now();
    const endpoint = new URL(url, options.baseUrl);
    endpoint.searchParams.set("after", cursor);
    const current = dependencies.open(endpoint.href);
    source = current;
    current.addEventListener("hls:status", (event: MessageEvent<string>) => {
      if (disposed || source !== current) return;
      lastSignal = dependencies.now();
      try {
        const value: unknown = JSON.parse(event.data);
        const stream = parseEvent(event.data);
        if (!validCursor(value) || !stream || event.lastEventId !== value.cursor) { void snapshot(); return; }
        if (value.epoch !== epoch) { void snapshot(); return; }
        if (value.sequence <= sequence) return;
        if (value.sequence !== sequence + 1) { connect(); return; }
        sequence = value.sequence; cursor = value.cursor;
        options.onStatus(stream);
      } catch { void snapshot(); }
    });
    current.addEventListener("hls:ready", (event: MessageEvent<string>) => {
      if (disposed || source !== current) return;
      try {
        if (JSON.parse(event.data).cursor !== cursor) { connect(); return; }
        lastSignal = dependencies.now(); reconnectErrors = 0; unreadySince = null; options.onConnected(true);
      } catch { void snapshot(); }
    });
    current.addEventListener("hls:heartbeat", () => { if (source === current) lastSignal = dependencies.now(); });
    current.addEventListener("hls:resync", () => { if (source === current) void snapshot(); });
    current.onerror = () => {
      if (disposed || source !== current) return;
      options.onConnected(false);
      if (unreadySince === null) unreadySince = dependencies.now();
      reconnectErrors++;
      // A missing resync frame must not cause an endless reconnect loop.
      if (current.readyState === 2 || reconnectErrors >= 3) void snapshot();
    };
  };
  const snapshot = async () => {
    if (disposed) return;
    source?.close(); source = undefined; options.onConnected(false);
    reconnectErrors = 0;
    controller?.abort(); controller = new AbortController();
    const attempt = controller;
    if (timer) clearTimeout(timer);
    const timeout = setTimeout(() => attempt.abort(), 10_000);
    try {
      const value = await dependencies.fetchSnapshot(new URL(snapshotUrl, new URL(url, options.baseUrl)).href, attempt.signal) as Record<string, unknown>;
      const snapshotStreams: unknown = value?.streams;
      if (!validCursor(value) || !Array.isArray(snapshotStreams)) throw new Error("Invalid snapshot");
      const next: Record<string, HlsStatus> = {};
      for (const data of snapshotStreams) {
        const stream = parseEvent(JSON.stringify({ type: "hls:status", data }));
        if (!stream) throw new Error("Invalid snapshot stream");
        next[JSON.stringify([stream.serverIp, stream.streamId])] = stream;
      }
      if (disposed || controller !== attempt) return;
      epoch = value.epoch; sequence = value.sequence; cursor = value.cursor;
      options.onSnapshot(next); lastSignal = dependencies.now(); connect();
    } catch (error) {
      if (!disposed && controller === attempt) {
        options.onSnapshotError?.((error as { status?: number }).status);
        timer = setTimeout(() => { void snapshot(); }, 3000);
      }
    } finally { clearTimeout(timeout); }
  };
  void snapshot();
  const watchdog = setInterval(() => {
    if (source && (dependencies.now() - lastSignal > 45_000 || (unreadySince !== null && dependencies.now() - unreadySince > 30_000))) void snapshot();
  }, 5000);
  return () => { disposed = true; source?.close(); controller?.abort(); if (timer) clearTimeout(timer); clearInterval(watchdog); };

}
