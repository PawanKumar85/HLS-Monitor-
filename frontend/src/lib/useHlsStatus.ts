"use client";
import { useEffect, useRef, useState } from "react";
import { startHlsStatusClient, type HlsStatus, type HlsDisplayState } from "./hls-status-client.js";
export type { HlsStatus, HlsDisplayState } from "./hls-status-client.js";

/**
 * Snapshot first, then replay from the same cursor.
 * Default 25 s: two checks can be 10.5 s (interval + tick) + 6 s (probe timeout) apart, so 15 s flagged healthy streams STALE.
 */
export function useHlsStatus(url: string, staleAfterMs = 25_000, snapshotUrl = "/api/hls/status/snapshot", onSnapshotError?: (status: number | undefined) => void) {
  if (!Number.isFinite(staleAfterMs) || staleAfterMs <= 0) throw new RangeError("staleAfterMs must be positive");
  const [streams, setStreams] = useState<Record<string, HlsStatus>>({});
  const [connected, setConnected] = useState(false);
  // loaded: a snapshot has arrived at least once. failed: the latest snapshot attempt failed (it keeps retrying).
  const [loaded, setLoaded] = useState(false);
  const [failed, setFailed] = useState(false);
  const [now, setNow] = useState(() => Date.now());
  const onError = useRef(onSnapshotError); onError.current = onSnapshotError;
  useEffect(() => {
    setStreams({}); setConnected(false); setLoaded(false); setFailed(false);
    return startHlsStatusClient({
      url, snapshotUrl, baseUrl: window.location.href,
      onConnected: setConnected,
      onSnapshotError: status => { setFailed(true); onError.current?.(status); },
      onSnapshot: value => { setStreams(value); setLoaded(true); setFailed(false); setNow(Date.now()); },
      onStatus: stream => {
        const key = JSON.stringify([stream.serverIp, stream.streamId]);
        setStreams(previous => previous[key] && Date.parse(previous[key]!.checkedAt) > Date.parse(stream.checkedAt) ? previous : { ...previous, [key]: stream });
        setNow(Date.now());
      },
    });
  }, [url, snapshotUrl]);

  useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(timer);
  }, []);

  const stateFor = (stream?: HlsStatus): HlsDisplayState => {
    if (!stream) return "UNKNOWN";
    if (!connected || now - Date.parse(stream.checkedAt) >= staleAfterMs) return "STALE";
    return stream.status === 1 ? "ONLINE" : "OFFLINE";
  };
  return {
    streams: Object.values(streams).map((stream) => ({ ...stream, state: stateFor(stream) })),
    connected,
    loaded,
    failed,
    now,
    stateFor,
  };
}
