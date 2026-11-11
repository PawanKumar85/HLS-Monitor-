import { BufferedMessageError, type MessageFailureHandler, type MessageContext } from "../dlq/dead-letter.types.js";
import { randomUUID } from "node:crypto";
import { hostname } from "node:os";
import type { StreamReader, StreamSource, RawStreamMessage } from "../ports/messaging.js";
import type { HlsMessageHandler, StreamMessageProcessor } from "../ports/hls-processing.js";
import type { ConsumerOptions } from "../types/stream.types.js";

export interface HlsConsumerDependencies {
    source: StreamSource;
    processor: StreamMessageProcessor;
    failures?: MessageFailureHandler;
}

export class HlsConsumer {
    constructor(private readonly dependencies: HlsConsumerDependencies) {}

    private stopping = false;
    private cancelledServers = new Set<string>();
    private tasks = new Map<string, Promise<void>>();
    private defaultGroupName = "hls-workers";
    private activeStreams = new Set<string>();
    private readers = new Map<string, StreamReader>();

    /**
     * Initializes the consumer group for a server-specific stream if it doesn't already exist
     */
    async createGroup(serverIp: string, groupName = this.defaultGroupName): Promise<void> {
        const streamKey = `hls:streams:${serverIp}`;

        await this.dependencies.source.ensureGroup(streamKey, groupName);
    }

    /**
     * Starts continuous stream consumption loop for a given server IP.
     * Owns polling, recovery, concurrency, and reader lifecycle.
     * Each entry is delegated to an injected message processor.
     * A custom onMessage handler replaces the batch queue: it must persist durably
     * and ACK in options.groupName only after persistence. Delivery is at least once;
     * callbacks must tolerate duplicates. Returning alone does not ACK a message.
     */
    consume(
        serverIp: string,
        onMessage?: HlsMessageHandler,
        options: ConsumerOptions = {}
    ): Promise<void> {
        if (this.stopping) return Promise.reject(new Error("HLS consumer is stopping"));
        if (this.tasks.has(serverIp)) return Promise.reject(new Error("Consumer already running for server"));
        this.cancelledServers.delete(serverIp);
        const task = this.consumeStream(serverIp, onMessage, options).finally(() => {
            this.tasks.delete(serverIp);
        });
        this.tasks.set(serverIp, task);
        return task;
    }

    private async consumeStream(
        serverIp: string,
        onMessage?: HlsMessageHandler,
        options: ConsumerOptions = {}
    ): Promise<void> {
        const streamKey = `hls:streams:${serverIp}`;
        const groupName = options.groupName || this.defaultGroupName;
        const consumerName = options.consumerName || `${hostname()}-${process.pid}-${randomUUID()}`;
        const count = options.batchSize ?? 10;
        const concurrency = options.healthCheckConcurrency ?? 3;
        const recoveryInterval = options.recoveryIntervalMs ?? 30_000;
        const minIdle = options.pendingMinIdleMs ?? 120_000;
        const requestedBlock = options.blockTimeoutMs ?? 2000;
        for (const value of [count, concurrency, recoveryInterval, minIdle]) {
            if (!Number.isSafeInteger(value) || value <= 0) throw new RangeError("Consumer limits must be positive integers");
        }
        if (!Number.isSafeInteger(requestedBlock) || requestedBlock < 0) throw new RangeError("Invalid blockTimeoutMs");
        // Bound idle reads so recovery still runs when BLOCK 0 was requested.
        const blockMs = Math.min(requestedBlock || recoveryInterval, recoveryInterval);
        const isActive = () => !this.stopping && !this.cancelledServers.has(serverIp);
        await this.createGroup(serverIp, groupName);
        if (!isActive()) return;

        const reader = this.dependencies.source.createReader();
        this.readers.set(streamKey, reader);
        this.activeStreams.add(streamKey);
        // Custom handlers own durable persistence and subsequent ACK in groupName.
        // Returning from a callback never causes a consumer-level ACK.
        const processEntry = async (entry: RawStreamMessage) => {
            const [messageId] = entry;
            const context: MessageContext = { originalStreamKey: streamKey, originalMessageId: messageId, groupName, serverIp, consumerName, rawFields: entry[1] };
            if (this.dependencies.failures) {
                try { if (!await this.dependencies.failures.prepare(context)) return; }
                catch { console.error(`Retry preparation unavailable for ${streamKey}/${messageId}; left pending`); return; }
            }
            try {
                await this.dependencies.processor.process(serverIp, entry, groupName, onMessage, context);
            } catch (error) {
                if (!(error instanceof BufferedMessageError) && this.dependencies.failures) {
                    try { await this.dependencies.failures.failed(context, error); }
                    catch { console.error(`Failure handling unavailable for ${streamKey}/${messageId}; left pending`); }
                } else if (!(error instanceof BufferedMessageError)) {
                    console.error(`Error processing ${streamKey}/${messageId}; left pending for recovery`);
                }
            }
        };
        // Worker pool: a slow (timing-out) probe holds only its own slot. Free slots are refilled
        // right away instead of waiting for the slowest entry of a whole batch.
        const queued: RawStreamMessage[] = [];
        const inFlight = new Set<Promise<void>>();
        const fill = () => {
            while (inFlight.size < concurrency && queued.length && isActive()) {
                const task: Promise<void> = processEntry(queued.shift()!).finally(() => { inFlight.delete(task); fill(); });
                inFlight.add(task);
            }
        };
        const enqueue = (messages: RawStreamMessage[]) => { queued.push(...messages); fill(); };
        const drain = async () => { while (inFlight.size) await Promise.race(inFlight); };

        try {
            await reader.connect();
            let cursor = "0-0";
            let nextRecovery = 0; // Recover stale pending entries on startup.
            while (isActive()) {
                try {
                    if (Date.now() >= nextRecovery) {
                        // Claim only with nothing in flight, so XAUTOCLAIM never hands back entries we are still running.
                        await drain();
                        if (!isActive()) break;
                        const claimed = await reader.claim({
                            streamKey, groupName, consumerName, count, minIdleMs: minIdle, cursor,
                        });
                        cursor = claimed.cursor;
                        enqueue(claimed.messages);
                        if (claimed.deletedIds.length) {
                            console.warn(`[${streamKey}] Pending entries deleted from stream:`, claimed.deletedIds);
                        }
                        nextRecovery = cursor === "0-0" ? Date.now() + recoveryInterval : 0;
                    }
                    if (!isActive()) break;
                    // All slots busy (or claimed entries still waiting): wait for one slot, then loop.
                    if (queued.length || inFlight.size >= concurrency) { await Promise.race(inFlight); continue; }
                    const messages = await reader.read({
                        streamKey, groupName, consumerName, count: Math.min(count, concurrency - inFlight.size),
                        blockMs: Math.max(1, Math.min(blockMs, nextRecovery - Date.now() || 1)),
                    });
                    enqueue(messages);
                } catch (error) {
                    if (isActive()) {
                        console.error(`Consumer error on ${streamKey}:`, error);
                        await new Promise((resolve) => setTimeout(resolve, 1000));
                    }
                }
            }
        } finally {
            // Stop waits for running entries, as it did for a batch; unstarted queued ones stay pending in Redis.
            await Promise.allSettled(inFlight);
            reader.close();
            this.readers.delete(streamKey);
            this.activeStreams.delete(streamKey);
        }
    }

    /** Stop current runners without closing admission permanently (used during a confirmed reset). */
    async pause(): Promise<void> {
        await Promise.all([...this.tasks.keys()].map(server => this.stop(server)));
    }

    async stop(serverIp?: string): Promise<void> {
        if (!serverIp) this.stopping = true;
        else this.cancelledServers.add(serverIp);
        const keys = serverIp ? [`hls:streams:${serverIp}`] : [...this.activeStreams];
        for (const key of keys) {
            this.activeStreams.delete(key);
            // Interrupt BLOCK (including BLOCK 0); the shared ACK client stays open.
            this.readers.get(key)?.close();
        }
        const tasks = serverIp ? [this.tasks.get(serverIp)].filter((task) => task !== undefined) : [...this.tasks.values()];
        const results = await Promise.allSettled(tasks);
        const failures = results.filter((result) => result.status === "rejected");
        if (failures.length) throw new AggregateError(failures.map((result) => result.reason), "Consumer shutdown failed");
    }
}

