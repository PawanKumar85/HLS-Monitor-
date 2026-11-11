import { redactError } from "../dlq/redact.js";
import { BufferedMessageError, PartialMetricsWriteError, ProcessingFailure, type MessageContext, type MessageFailureHandler } from "../dlq/dead-letter.types.js";
import type { RetryPolicy } from "../dlq/retry-policy.js";
import type { MessageAcknowledger } from "../ports/messaging.js";
import { QueueRecordTooLargeError } from "../ports/queue-errors.js";
import type { MetricsWriter } from "../storage/metrics-store.js";
import { HLS_METRICS_DATASET, type HlsStreamMetric } from "../types/hls-metric.types.js";
import type { HlsConsumedStream } from "../types/stream.types.js";

type BufferedHlsStream = HlsConsumedStream & { checkedAt: string; processingContext?: MessageContext };

function copyContext(context: MessageContext): MessageContext {
  return {
    originalStreamKey: context.originalStreamKey,
    originalMessageId: context.originalMessageId,
    groupName: context.groupName,
    serverIp: context.serverIp,
    consumerName: context.consumerName,
    rawFields: [...context.rawFields],
  };
}

export interface BatchQueueOptions {
  /** Target server IP for this queue. Default: "unknown" */
  serverIp?: string;
  /** Maximum queue capacity in bytes. Default: 10MB (10 * 1024 * 1024) */
  maxSizeBytes?: number;
  /** Trigger flush ratio (e.g., 0.7 = 70% of maxSizeBytes = 7MB). Default: 0.7 */
  thresholdRatio?: number;
  /** Max time in ms before flushing partial batches. Default: 5000ms */
  flushIntervalMs?: number;
  /** Logical destination; each adapter maps it to a collection or table. */
  dataset?: string;
  /** @deprecated Use dataset. Preserved for existing callers. */
  collectionName?: string;
  /** Inject another database adapter without changing queue behavior. */
  store: MetricsWriter;
  acknowledger: MessageAcknowledger;
  failures?: MessageFailureHandler;
  retryPolicy?: RetryPolicy;
  /** Redis Consumer Group name for XACK. Default: "hls-workers" */
  groupName?: string;
}

/**
 * Server-specific in-memory unsorted batch queue.
 * - Collects stream records for a single server IP up to 10MB.
 * - Automatically sorts and flushes to storage at 70% capacity (7MB) or every 5s.
 * - Uses idempotent batch persistence and verified Redis XACK.
 */
export class UnsortedBatchQueue {
  public readonly serverIp: string;
  public readonly streamKey: string;
  public readonly maxSizeBytes: number;
  public readonly thresholdBytes: number;
  public readonly flushIntervalMs: number;
  public readonly dataset: string;
  /** @deprecated Use dataset. */
  public readonly collectionName: string;
  private readonly store: MetricsWriter;
  public readonly groupName: string;

  private queue: BufferedHlsStream[] = [];
  private readonly recordSizes = new WeakMap<BufferedHlsStream, number>();
  private currentSizeBytes = 0;
  private activeFlushPromise: Promise<void> | null = null;
  private flushTimer: NodeJS.Timeout | null = null;
  private inFlightBytes = 0;
  private retryAttempts = 0;
  private nextRetryAt = 0;
  private lastFailureMessage: string | null = null;
  private lastFailureAt: number | null = null;
  private state: "RUNNING" | "STOPPING" | "STOPPED" = "RUNNING";
  private stopPromise: Promise<void> | null = null;

  constructor(private readonly options: BatchQueueOptions) {
    this.serverIp = options.serverIp ?? "unknown";
    this.streamKey = `hls:streams:${this.serverIp}`;
    this.maxSizeBytes = options.maxSizeBytes ?? 10 * 1024 * 1024; // 10MB
    const ratio = options.thresholdRatio ?? 0.7; // 70%
    this.thresholdBytes = Math.floor(this.maxSizeBytes * ratio); // 7MB
    this.flushIntervalMs = options.flushIntervalMs ?? 5000;
    this.dataset = options.dataset ?? options.collectionName ?? HLS_METRICS_DATASET;
    this.collectionName = this.dataset;
    this.store = options.store;
    this.groupName = options.groupName ?? "hls-workers";

    if (!Number.isSafeInteger(this.maxSizeBytes) || this.maxSizeBytes <= 0 ||
        !Number.isFinite(ratio) || ratio <= 0 || ratio > 1 ||
        !Number.isSafeInteger(this.flushIntervalMs) || this.flushIntervalMs <= 0) {
      throw new RangeError("Invalid batch queue capacity, threshold, or flush interval");
    }

    this.startFlushTimer();
  }

  /**
   * Pushes a stream record into this server's batch queue.
   * If capacity reaches 70% (7MB of 10MB), automatically triggers a sort & flush.
   */
  public async push(item: HlsConsumedStream, context?: MessageContext): Promise<void> {
    this.assertRunning();
    if (context && (context.originalStreamKey !== this.streamKey || context.groupName !== this.groupName || context.originalMessageId !== item.messageId)) {
      throw new ProcessingFailure('QUEUE', 'ROUTING_MISMATCH', 'Message context does not match queue routing', false);
    }
    // Snapshot owned payloads before any capacity wait so caller mutations cannot
    // change routing, health, or the serialized capacity budget. Legacy callers
    // without probe time retain receive time instead of the later flush time.
    const queuedItem: BufferedHlsStream = {
      messageId: item.messageId,
      streamId: item.streamId,
      streamUrl: item.streamUrl,
      serverIp: item.serverIp,
      createdAt: item.createdAt,
      receivedAt: item.receivedAt,
      checkedAt: item.checkedAt ?? item.receivedAt,
      delay: item.delay,
      ...(item.health ? { health: { status: item.health.status, latency: item.health.latency, ...(item.health.reason ? { reason: item.health.reason } : {}) } } : {}),
      ...(context ? { processingContext: copyContext(context) } : {}),
    };
    if (!Number.isFinite(Date.parse(queuedItem.checkedAt))) {
      throw new RangeError("Invalid health observation checkedAt");
    }
    const itemBytes = Buffer.byteLength(JSON.stringify(queuedItem), "utf8");

    if (itemBytes > this.maxSizeBytes) {
      throw new QueueRecordTooLargeError();
    }
    while (this.currentSizeBytes + this.inFlightBytes + itemBytes > this.maxSizeBytes) {
      try { await this.flush(); }
      catch { throw new ProcessingFailure('PERSISTENCE', 'STORAGE_UNAVAILABLE', 'Queue is full while persistence is unavailable', true, false); }
      this.assertRunning();
    }

    this.recordSizes.set(queuedItem, itemBytes);
    this.queue.push(queuedItem);
    this.currentSizeBytes += itemBytes;

    const fillPercent = ((this.currentSizeBytes / this.maxSizeBytes) * 100).toFixed(1);

    // Trigger flush when queue reaches 70% (7MB)
    if (this.currentSizeBytes >= this.thresholdBytes && !this.activeFlushPromise) {
      console.log(
        `⚡ [${this.serverIp}] Queue at ${fillPercent}% (${(this.currentSizeBytes / 1024 / 1024).toFixed(2)}MB / ${(this.maxSizeBytes / 1024 / 1024).toFixed(2)}MB) -> Flushing`
      );
      try { await this.flush(); } catch { throw new BufferedMessageError(); }
    }
  }

  /**
   * Flushes the current batch:
   * 1. Atomic swap of in-memory queue
   * 2. Sorts batch by createdAt ascending
   * 3. Durable idempotent persistence through the storage adapter
   * 4. Strict Redis XACK with verified acknowledgment count
   */
  public async flush(): Promise<void> {
    if (this.queue.length === 0) {
      if (this.activeFlushPromise) {
        return this.activeFlushPromise;
      }
      return;
    }

    if (this.activeFlushPromise) {
      return this.activeFlushPromise;
    }

    this.activeFlushPromise = this.executeFlush().finally(() => {
      this.activeFlushPromise = null;
    });
    return this.activeFlushPromise;
  }

  private async executeFlush(): Promise<void> {
    // 1. Swap buffers; retain in-flight bytes in the capacity budget.
    const batchToFlush = this.queue;
    const batchBytes = this.currentSizeBytes;
    this.queue = [];
    this.currentSizeBytes = 0;
    this.inFlightBytes = batchBytes;

    let pendingRecords = batchToFlush;
    const startTime = performance.now();

    try {
      if (this.nextRetryAt > Date.now()) await new Promise((resolve) => setTimeout(resolve, this.nextRetryAt - Date.now()));
      // 2. Sort batch chronologically by createdAt (ascending)
      batchToFlush.sort(
        (a, b) => new Date(a.createdAt).getTime() - new Date(b.createdAt).getTime()
      );

      // 3. Deduplicate and map to database-neutral observations
      const uniqueItems = new Map<string, BufferedHlsStream>();
      for (const item of batchToFlush) {
        if (!uniqueItems.has(item.messageId)) uniqueItems.set(item.messageId, item);
      }
      const observations: HlsStreamMetric[] = [...uniqueItems.values()].map((item) => ({
        streamId: item.streamId,
        streamUrl: item.streamUrl,
        serverIp: this.serverIp,
        createdAt: new Date(item.createdAt),
        receivedAt: new Date(item.receivedAt),
        checkedAt: new Date(item.checkedAt),
        pipelineDelayMs: item.delay,
        ...(item.health ? {
          health: {
            status: item.health.status,
            segmentLatencyMs: item.health.latency,
            ...(item.health.reason ? { reason: item.health.reason } : {}),
          },
        } : {}),
        redisStreamKey: this.streamKey,
        redisMessageId: item.messageId,
        ingestedAt: new Date(),
      }));

      // Persist first; only authoritative per-record failures allow partial ACK.
      let failures: PartialMetricsWriteError['failures'] = [];
      try { await this.store.writeBatch(observations, this.dataset); }
      catch (error) {
        if (!(error instanceof PartialMetricsWriteError)) throw error;
        failures = error.failures;
      }
      const unique = [...uniqueItems.values()];
      const failedIndexes = new Set(failures.map((entry) => entry.index));
      const persistedIds = unique.filter((_, index) => !failedIndexes.has(index)).map((item) => item.messageId);
      if (persistedIds.length) {
        const count = await this.options.acknowledger.acknowledge(this.streamKey, this.groupName, persistedIds);
        if (count !== persistedIds.length) console.warn(`[${this.serverIp}] XACK count mismatch: expected ${persistedIds.length}, received ${count}`);
      }
      const pendingIds = new Set(failures.map(({ index }) => unique[index]!.messageId));
      pendingRecords = batchToFlush.filter((item) => pendingIds.has(item.messageId));
      for (const { index, failure } of failures) {
        const item = unique[index]!;
        if (this.options.failures) {
          const context = item.processingContext ? copyContext(item.processingContext) : {
            originalStreamKey: this.streamKey, originalMessageId: item.messageId, groupName: this.groupName,
            serverIp: this.serverIp, consumerName: 'batch-queue',
            rawFields: ['streamId', item.streamId, 'streamUrl', item.streamUrl, 'serverIp', this.serverIp, 'createdAt', item.createdAt],
          };
          try {
            if (await this.options.failures.failed(context, new ProcessingFailure('PERSISTENCE', failure.code, failure.message, failure.retryable, false))) {
              pendingRecords = pendingRecords.filter((record) => record.messageId !== item.messageId);
            }
          } catch { console.error(`[${this.serverIp}] DLQ unavailable; failed observation retained`); }
        }
      }
      if (pendingRecords.length) throw new Error('Unsettled persistence failures retained for retry');
      this.retryAttempts = 0;
      this.nextRetryAt = 0;

      const elapsed = Math.round(performance.now() - startTime);
      console.log(
        `💾 [${this.serverIp}] Settled batch: ${persistedIds.length} persisted identities, ${failures.length} terminal failures (${(batchBytes / 1024 / 1024).toFixed(2)}MB) in ${elapsed}ms`
      );
    } catch (error) {
      this.lastFailureMessage = redactError(error instanceof Error ? error.message : String(error));
      this.lastFailureAt = Date.now();
      console.error(`❌ [${this.serverIp}] Batch persistence/ACK incomplete; retained for retry: ${this.lastFailureMessage}`);
      // Retain the accepted byte budget for each unresolved snapshot on retry.
      this.queue = pendingRecords.concat(this.queue);
      this.currentSizeBytes += pendingRecords.reduce((sum, item) => sum + this.recordSizes.get(item)!, 0);
      this.retryAttempts++;
      this.nextRetryAt = Date.now() + (this.options.retryPolicy?.delay(this.retryAttempts) ?? 0);
      throw error;
    } finally {
      this.inFlightBytes = 0;
    }
  }

  /**
   * Periodic safety timer: flushes items if 70% capacity is not reached within interval
   */
  private startFlushTimer(): void {
    this.flushTimer = setInterval(() => {
      if (this.queue.length > 0 && !this.activeFlushPromise && Date.now() >= this.nextRetryAt) {
        void this.flush().catch((err) => {
          console.error(`[${this.serverIp}] Timer flush error:`, err);
        });
      }
    }, this.flushIntervalMs);

    this.flushTimer.unref();
  }

  /**
   * Returns queue memory and capacity metrics
   */
  public getStats() {
    return {
      serverIp: this.serverIp,
      streamKey: this.streamKey,
      itemCount: this.queue.length,
      currentSizeBytes: this.currentSizeBytes,
      inFlightBytes: this.inFlightBytes,
      totalSizeBytes: this.currentSizeBytes + this.inFlightBytes,
      state: this.state,
      retryAttempts: this.retryAttempts,
      nextRetryAt: this.nextRetryAt,
      lastFailureAt: this.lastFailureAt,
      lastFailureMessage: this.lastFailureMessage,
      currentSizeMb: (this.currentSizeBytes / 1024 / 1024).toFixed(2),
      maxSizeMb: (this.maxSizeBytes / 1024 / 1024).toFixed(2),
      fillPercentage: ((this.currentSizeBytes / this.maxSizeBytes) * 100).toFixed(1) + "%",
      isFlushing: this.activeFlushPromise !== null,
    };
  }

  /**
   * Graceful shutdown:
   * Closes admission, stops the timer, and drains accepted records.
   */
  private assertRunning(): void {
    if (this.state !== "RUNNING") throw new Error(`Queue is ${this.state}`);
  }

  /** Waits for active persistence and drains batches accepted while it was running. */
  public async drain(): Promise<void> {
    while (this.activeFlushPromise || this.queue.length > 0) {
      await this.flush();
    }
  }

  public stop(): Promise<void> {
    if (this.stopPromise) return this.stopPromise;
    this.state = "STOPPING";
    if (this.flushTimer) {
      clearInterval(this.flushTimer);
      this.flushTimer = null;
    }
    this.stopPromise = this.drain().then(() => {
      this.state = "STOPPED";
    }).catch((error) => {
      this.stopPromise = null; // Allow an explicit retry; admission stays closed.
      throw error;
    });
    return this.stopPromise;
  }
}

/**
 * Registry & Manager for server-wise batch queues.
 * Dynamically routes consumed stream records to their respective server queue.
 */
export class ServerBatchQueueManager {
  private queues = new Map<string, UnsortedBatchQueue>();
  private defaultOptions: BatchQueueOptions;
  private stopping = false;
  private stopPromise: Promise<void> | null = null;

  constructor(defaultOptions: BatchQueueOptions) {
    this.defaultOptions = defaultOptions;
  }

  /**
   * Retrieves or creates a dedicated UnsortedBatchQueue for a specific server IP
   */
  public getQueue(serverIp: string, groupName = this.defaultOptions.groupName ?? "hls-workers"): UnsortedBatchQueue {
    if (this.stopping) throw new Error("Batch queue manager is stopping or stopped");
    const key = groupName === (this.defaultOptions.groupName ?? "hls-workers")
      ? serverIp : JSON.stringify([serverIp, groupName]);
    let queue = this.queues.get(key);
    if (!queue) {
      queue = new UnsortedBatchQueue({
        ...this.defaultOptions,
        serverIp,
        groupName,
      });
      this.queues.set(key, queue);
    }
    return queue;
  }

  /**
   * Pushes a consumed stream record directly into its server's dedicated queue
   */
  public async push(item: HlsConsumedStream, groupName?: string, context?: MessageContext): Promise<void> {
    const queue = this.getQueue(item.serverIp, groupName);
    await queue.push(item, context);
  }

  /**
   * Flushes all server queues immediately
   */
  public async flushAll(): Promise<void> {
    const flushPromises = Array.from(this.queues.values()).map((q) => q.drain());
    await Promise.all(flushPromises);
  }

  /**
   * Gracefully stops all server queues, ensuring remaining records are flushed
   */
  public stopAll(): Promise<void> {
    if (this.stopPromise) return this.stopPromise;
    this.stopping = true;
    this.stopPromise = (async () => {
      const results = await Promise.allSettled(Array.from(this.queues.values(), (q) => q.stop()));
      const failures = results.filter((r) => r.status === "rejected");
      if (failures.length) throw new AggregateError(failures.map((r) => r.reason), "Queue shutdown failed");
      this.queues.clear();
    })().catch((error) => {
      this.stopPromise = null;
      throw error;
    });
    return this.stopPromise;
  }

  /**
   * Gets stats for all active server queues
   */
  /** Called only after scheduling and consumers have paused. Retire queue timers, then accept fresh queues. */
  public async reset(): Promise<void> {
    const results = await Promise.allSettled([...this.queues.values()].map(queue => queue.stop()));
    const failures = results.filter(result => result.status === 'rejected');
    if (failures.length) throw new AggregateError(failures.map(result => result.reason), 'Queue reset failed');
    this.queues.clear();
  }

  public getAllStats() {
    const stats: Record<string, ReturnType<UnsortedBatchQueue["getStats"]>> = {};
    for (const [serverIp, queue] of this.queues) {
      stats[serverIp] = queue.getStats();
    }
    return stats;
  }
}

