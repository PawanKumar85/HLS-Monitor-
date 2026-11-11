import { randomUUID } from "node:crypto";
import type { DeadLetterArchiveSource, FailureHistoryWriter } from "./dead-letter.types.js";

const BATCH = 100;
/** Per server per flush. A failure storm drains in one flush (up to 5,000 per server) instead of 100 every interval. */
const MAX_BATCHES = 50;

/**
 * Copies dead letters from Redis to MongoDB history, then removes them from Redis (never the other way round,
 * so a MongoDB outage only delays the copy). Runs every 5 s: new failures reach the DLQ page within ~10 s.
 */
export class DeadLetterProjector {
  private timer: NodeJS.Timeout | undefined;
  private active: Promise<void> | undefined;
  private readonly consumer = `archive-${randomUUID()}`;
  constructor(private readonly source: DeadLetterArchiveSource, private readonly history: FailureHistoryWriter, private readonly intervalMs = 5000) {}
  start(): void {
    if (this.timer) return;
    this.timer = setInterval(() => { void this.flush().catch(() => console.error('DLQ archive unavailable; Redis entries retained')); }, this.intervalMs);
    this.timer.unref();
    void this.flush().catch(() => console.error('DLQ archive unavailable; Redis entries retained'));
  }
  flush(): Promise<void> {
    if (this.active) return this.active;
    this.active = this.project().finally(() => { this.active = undefined; });
    return this.active;
  }
  private async project(): Promise<void> {
    for (const server of await this.source.servers()) {
      for (let batch = 0; batch < MAX_BATCHES; batch++) {
        const entries = await this.source.readArchiveBatch(server, this.consumer, BATCH);
        if (!entries.length) break;
        const records = entries.map(e => e.record);
        if (this.history.writeMany) await this.history.writeMany(records);
        else for (const record of records) await this.history.write(record);
        for (const { id } of entries) await this.source.archiveCompleted(server, id);
        if (entries.length < BATCH) break; // drained
      }
    }
  }
  async stop(): Promise<void> {
    if (this.timer) clearInterval(this.timer);
    this.timer = undefined;
    try { await this.flush(); }
    catch { console.error('DLQ archive shutdown deferred; unarchived events retained in Redis'); }
  }
}
