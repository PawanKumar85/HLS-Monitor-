import type { StreamInventoryRepository, StreamScheduleProjection } from './stream-inventory.js';
/** Single-document pending markers plus bounded reconciliation replace unsafe dual writes. */
export class InventorySyncWorker {
  private timer: NodeJS.Timeout | undefined;
  private active: Promise<void> | undefined;
  private stopped = true;
  private cursor: string | undefined;
  private readonly now: () => Date;
  constructor(private readonly repository: StreamInventoryRepository, private readonly projection: StreamScheduleProjection,
    private readonly options: { batchSize?: number; pollMs?: number; now?: () => Date } = {}) {
    if (!Number.isSafeInteger(options.batchSize ?? 20) || (options.batchSize ?? 20) < 2 || (options.batchSize ?? 20) > 100 ||
      !Number.isSafeInteger(options.pollMs ?? 1000) || (options.pollMs ?? 1000) < 100 || (options.pollMs ?? 1000) > 60_000) throw new RangeError('Invalid inventory sync limits');
    this.now = options.now ?? (() => new Date());
  }
  async start(): Promise<void> {
    if (!this.stopped) return;
    this.stopped = false;
    this.timer = setInterval(() => { void this.tick().catch(() => console.error('Inventory sync unavailable; DB registrations retained')); }, this.options.pollMs ?? 1000);
    this.timer.unref();
    await this.tick().catch(() => console.error('Inventory sync unavailable; DB registrations retained'));
  }
  tick(): Promise<void> {
    if (this.stopped) return Promise.resolve();
    if (this.active) return this.active;
    this.active = this.execute().finally(() => { this.active = undefined; });
    return this.active;
  }
  private async execute(): Promise<void> {
    const now = this.now();
    const budget = this.options.batchSize ?? 20;
    const pending = await this.repository.pending(Math.ceil(budget / 2), now);
    if (this.stopped) return;
    const page = await this.repository.scan(budget - pending.length, this.cursor);
    this.cursor = page.at(-1)?.id;
    const records = new Map([...pending, ...page].map(record => [record.id, record]));
    for (const record of records.values()) {
      if (this.stopped) break;
      if (record.pendingSync && record.nextSyncAt > now) continue;
      try {
        if (await this.projection.apply(record) === 'stale') throw new Error('Projection version is ahead of this registration');
        if (record.pendingSync) await this.repository.markSynced(record.id, record.version, this.now());
      } catch {
        const delay = Math.min(60_000, 1000 * 2 ** Math.min(6, record.syncAttempts));
        const failedAt = this.now();
        await this.repository.deferSync(record.id, record.version, new Date(failedAt.getTime() + delay));
      }
    }
  }
  async stop(): Promise<void> {
    this.stopped = true; if (this.timer) clearInterval(this.timer); this.timer = undefined;
    await this.active;
  }
}
