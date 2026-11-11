import type { HlsFailureReason } from './hls-health.types.js';

/** Database-neutral observation model; one observation per Redis message. */
export interface HlsStreamMetric {
  streamId: string;
  streamUrl: string;
  serverIp: string;
  createdAt: Date;
  receivedAt: Date;
  checkedAt: Date;
  ingestedAt: Date;
  pipelineDelayMs: number;
  health?: {
    status: 0 | 1;
    segmentLatencyMs: number;
    /** Offline checks only (stored since the failure-reason report; older rows have none). */
    reason?: HlsFailureReason;
  };
  redisStreamKey: string;
  redisMessageId: string;
}

export const HLS_METRICS_DATASET = "hls_stream_metrics";
/** Rows are physically deleted by a MongoDB TTL index on ingestedAt after this; history reads hide them at the same cutoff. */
export const HLS_METRICS_RETENTION_SECONDS = 7 * 24 * 60 * 60;
