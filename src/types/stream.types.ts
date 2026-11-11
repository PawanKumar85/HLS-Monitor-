import type { HlsHealthResult } from "./hls-health.types.js";

export type { HlsHealthResult };

export interface HlsStream {
    streamId: string;
    streamUrl: string;
}

export interface EnrichedHlsStream extends HlsStream {
    serverIp: string;
    createdAt: string;
}

export interface HlsConsumedStream {
    messageId: string;
    streamId: string;
    streamUrl: string;
    serverIp: string;
    createdAt: string;
    receivedAt: string;
    delay: number; // Delay in milliseconds: received - createdAt
    /** Probe completion timestamp; legacy/custom callers fall back to receivedAt. */
    checkedAt?: string;
    health?: HlsHealthResult | undefined;
}

export interface ConsumerOptions {
    groupName?: string | undefined;
    consumerName?: string | undefined;
    batchSize?: number | undefined;
    blockTimeoutMs?: number | undefined;
    /** Maximum simultaneous HLS checks per server. Default: 3. */
    healthCheckConcurrency?: number | undefined;
    /** Minimum pending idle age before reclaim. Default: 120000ms.
     * Must exceed expected processing, queue waiting, and persistence time. */
    pendingMinIdleMs?: number | undefined;
    /** Time between complete pending scans. Default: 30000ms. Requires Redis >= 6.2. */
    recoveryIntervalMs?: number | undefined;
}
