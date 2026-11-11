"use client";
import { useEffect, useState } from "react";
import { startHlsFailureClient, type HlsFailure } from "./hls-failure-client.js";
export type { HlsFailure } from "./hls-failure-client.js";

const MAX_KEPT = 500; // a long-open tab must not grow without limit

/** Live dead-letter failures for a signed-in user. Keyed by failureId; a newer revision replaces an older one. */
/** `connection`: bump it to drop the stream and connect again (a Reconnect button). */
export function useHlsFailures(url: string, connection = 0) {
  const [failures, setFailures] = useState<Record<string, HlsFailure>>({});
  const [connected, setConnected] = useState(false);
  const [unauthorized, setUnauthorized] = useState(false);
  useEffect(() => {
    setFailures({}); setUnauthorized(false);
    return startHlsFailureClient({
      url,
      onConnected: setConnected,
      onUnauthorized: () => setUnauthorized(true),
      onFailure: failure => setFailures(current => {
        if ((current[failure.failureId]?.revision ?? -1) > failure.revision) return current;
        const next = { ...current, [failure.failureId]: failure };
        const keys = Object.keys(next);
        if (keys.length > MAX_KEPT) delete next[keys[0]!];
        return next;
      }),
    });
  }, [url, connection]);
  return { failures, connected, unauthorized };
}
