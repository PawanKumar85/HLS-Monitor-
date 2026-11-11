import type { HlsStreamMetric } from "../types/hls-metric.types.js";

export interface MetricsWriter {
  writeBatch(
    observations: readonly HlsStreamMetric[],
    dataset: string
  ): Promise<void>;
}

export interface MetricsConnection {
  readonly name: string;
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  healthCheck(): Promise<boolean>;
}

export interface MetricsStore extends MetricsWriter, MetricsConnection { }
