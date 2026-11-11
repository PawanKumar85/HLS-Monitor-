export interface HlsFailure {
  failureId: string;
  serverIp: string;
  streamId?: string;
  streamUrl?: string;
  originalMessageId: string;
  failureStage: "PARSE" | "HEALTH_CHECK" | "QUEUE" | "PERSISTENCE";
  errorCode: string;
  errorMessage: string;
  attempts: number;
  firstFailedAt: string;
  lastFailedAt: string;
  updatedAt: string;
  retryable: boolean;
  revision: number;
  resolution: "OPEN" | "REPLAY_REQUESTED" | "RESOLVED";
}
interface Options {
  url: string;
  onFailure(failure: HlsFailure): void;
  onConnected(connected: boolean): void;
  onUnauthorized?(): void;
}

/** Splits an SSE body into { id, event, data } frames. */
export function parseSseFrame(frame: string): { id: string; event: string; data: string } {
  let id = "", event = "message", data = "";
  for (const line of frame.split("\n")) {
    const colon = line.indexOf(":");
    if (colon <= 0) continue; // comments (": connected") and blank lines
    const field = line.slice(0, colon), value = line.slice(colon + 1).replace(/^ /, "");
    if (field === "id") id = value; else if (field === "event") event = value; else if (field === "data") data += (data ? "\n" : "") + value;
  }
  return { id, event, data };
}

/**
 * Failure feed for a signed-in user (the session cookie goes with same-origin requests). Read with fetch()
 * so a 401 can be told apart from a network drop; resumes from the last event id after any disconnect.
 */
export function startHlsFailureClient(options: Options, fetchImpl: typeof fetch = (input, init) => fetch(input, init), retryMs = 3000): () => void {
  let disposed = false;
  let cursor = "";
  let controller: AbortController | undefined;
  let timer: ReturnType<typeof setTimeout> | undefined;
  const connect = async () => {
    if (disposed) return;
    const attempt = controller = new AbortController();
    let lastSignal = Date.now();
    const watchdog = setInterval(() => { if (Date.now() - lastSignal > 45_000) attempt.abort(); }, 5000); // server heartbeats every 15 s
    let retry = true;
    try {
      const response = await fetchImpl(options.url, {
        headers: { Accept: "text/event-stream", ...(cursor ? { "Last-Event-ID": cursor } : {}) },
        cache: "no-store", credentials: "same-origin", signal: attempt.signal,
      });
      if (response.status === 401) { retry = false; options.onUnauthorized?.(); return; }
      if (response.status === 400) { cursor = ""; return; } // cursor rejected: start again from the newest failures
      if (!response.ok || !response.body) return;
      options.onConnected(true);
      const reader = response.body.pipeThrough(new TextDecoderStream()).getReader();
      let buffer = "";
      for (;;) {
        const { value, done } = await reader.read();
        if (done) return;
        lastSignal = Date.now();
        buffer += value;
        for (let end = buffer.indexOf("\n\n"); end >= 0; end = buffer.indexOf("\n\n")) {
          const frame = parseSseFrame(buffer.slice(0, end));
          buffer = buffer.slice(end + 2);
          if (frame.event !== "hls:failure") continue;
          const failure = JSON.parse(frame.data) as HlsFailure;
          if (typeof failure?.failureId !== "string" || !Number.isFinite(failure.revision)) continue;
          if (frame.id) cursor = frame.id;
          options.onFailure(failure);
        }
      }
    } catch { /* network error or abort: reconnect below */ }
    finally {
      clearInterval(watchdog);
      options.onConnected(false);
      if (!disposed && retry) timer = setTimeout(() => { void connect(); }, retryMs);
    }
  };
  void connect();
  return () => { disposed = true; controller?.abort(); if (timer) clearTimeout(timer); };
}
