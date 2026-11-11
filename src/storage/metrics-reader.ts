import type { HlsStreamMetric } from "../types/hls-metric.types.js";

export interface MetricsHistoryQuery {
  serverIp: string;
  streamId: string;
  from?: Date;
  to?: Date;
  limit?: number; /** Default: 100. Maximum: 500. */
}

export interface StreamOfflineSummary {
  streamId: string;
  checks: number;
  /** Time covered by the checks. Each check covers the time until the next one, capped so a pause in monitoring is not counted. */
  observedMs: number;
  /** The part of observedMs the stream was offline. Unlike a count of checks, this does not depend on the check interval. */
  downtimeMs: number;
  /** 100 × (1 − downtime / observed), rounded down so any downtime shows below 100. */
  availabilityPercent: number;
  /** Times it went from online to offline in the window (an outage already running at the window start counts once). */
  outages: number;
  /** Mean time to recover: average outage length (downtime / outages). Null without outages. */
  mttrMs: number | null;
  /** Latency of online checks only: an offline check's latency is its timeout, not the stream's speed. */
  avgLatencyMs: number | null;
  p95LatencyMs: number | null;
  p99LatencyMs: number | null;
  lastOfflineAt: Date | null;
}
export interface ServerOfflineSummary extends Omit<StreamOfflineSummary, 'streamId'> {
  serverIp: string;
  streams: number;
  streamsWithOffline: number;
  /** Why its checks were offline, most common first. Rows stored before reasons existed count as UNKNOWN. */
  failureReasons: { reason: string; checks: number }[];
  /** Streams of this server with the most downtime, worst first (max 5). */
  worstStreams: StreamOfflineSummary[];
}

/** One outage: from the first offline check to the first online check after it. */
export interface StreamOutage {
  streamId: string;
  startedAt: Date;
  /** First online check after the outage; null while it is still running. */
  endedAt: Date | null;
  durationMs: number;
  offlineChecks: number;
  /** Most common failure reason during the outage (UNKNOWN for checks saved before reasons were recorded). */
  reason: string;
}
export interface ServerDetails {
  serverIp: string;
  /** Newest first, at most 100. */
  outages: StreamOutage[];
  /** Per stream, one entry per hour that has checks, oldest first. */
  timeline: { streamId: string; hours: { hour: Date; availabilityPercent: number; downtimeMs: number }[] }[];
}

export interface MetricsReader {
  readHistory(
    query: MetricsHistoryQuery,
    dataset?: string
  ): Promise<HlsStreamMetric[]>;
  /** Per-server downtime, availability, outages and latency in [from, to], least available server first. */
  serverReport(from: Date, to: Date, dataset?: string): Promise<ServerOfflineSummary[]>;
  /** One server's outage log and hourly availability per stream, for root-cause work. */
  serverDetails(serverIp: string, from: Date, to: Date): Promise<ServerDetails>;
}
