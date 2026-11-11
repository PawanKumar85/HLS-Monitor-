import type { ProbeJobProducer, StreamInventory } from './probe-contracts.js';

interface ConsumerRunner {
  consume(serverIp: string): Promise<void>;
}
/** Each instance may run a scheduler; atomic admission belongs to the producer port. */
export class ProbeScheduler {
  private timer: NodeJS.Timeout | undefined;
  private active: Promise<void> | undefined;
  private starting: Promise<void> | undefined;
  private stopped = true;
  private readonly consumers = new Map<string, Promise<void>>();
  constructor(
    private readonly inventory: StreamInventory,
    private readonly producer: ProbeJobProducer,
    private readonly runner: ConsumerRunner,
    private readonly options: { pollMs?: number; batchSize?: number; random?: () => number } = {},
  ) {
    if (!Number.isSafeInteger(options.pollMs ?? 500) || (options.pollMs ?? 500) < 1 || (options.pollMs ?? 500) > 2_147_483_647 ||
      !Number.isSafeInteger(options.batchSize ?? 100) || (options.batchSize ?? 100) < 1 || (options.batchSize ?? 100) > 500) throw new RangeError('Invalid scheduler limits');
  }
  private ensureConsumer(serverIp: string): void {
    if (this.stopped || this.consumers.has(serverIp)) return;
    const task = Promise.resolve().then(() => this.stopped ? undefined : this.runner.consume(serverIp)).catch(() => {
      if (!this.stopped) console.error('Scheduled HLS consumer stopped; retrying on the next scheduler tick');
    }).finally(() => { this.consumers.delete(serverIp); });
    this.consumers.set(serverIp, task);
  }
  start(): Promise<void> {
    if (this.starting) return this.starting;
    if (!this.stopped) return Promise.resolve();
    this.stopped = false;
    this.starting = this.initialize().catch(async error => {
      await this.stop(); throw error;
    }).finally(() => { this.starting = undefined; });
    return this.starting;
  }
  private async initialize(): Promise<void> {
    try {
      const configs = await this.inventory.list();
      if (this.stopped) return;
      for (const config of configs) if (config.enabled) this.ensureConsumer(config.serverIp);
      this.timer = setInterval(() => { void this.tick().catch(() => console.error('Probe scheduling unavailable; due jobs will retry')); }, this.options.pollMs ?? 500);
      this.timer.unref();
      await this.tick().catch(() => console.error('Probe scheduling unavailable; due jobs will retry'));
    } catch (error) { await this.stop(); throw error; }
  }
  private jitter(intervalMs: number): number {
    const random = (this.options.random ?? Math.random)();
    if (!Number.isFinite(random) || random < 0 || random >= 1) throw new RangeError('Invalid scheduler random value');
    return Math.floor(random * intervalMs * 0.1);
  }
  tick(): Promise<void> {
    if (this.stopped) return Promise.resolve();
    if (this.active) return this.active;
    this.active = (async () => {
      for (const { config } of await this.inventory.due(this.options.batchSize ?? 100)) {
        if (this.stopped) break;
        this.ensureConsumer(config.serverIp);
        try { await this.producer.enqueueDue(config, this.jitter(config.intervalMs)); }
        catch { console.error('Probe dispatch unavailable for a stream; other due streams will continue'); }
      }
    })().finally(() => { this.active = undefined; });
    return this.active;
  }
  /** Stop enqueueing first; the application's consumer lifecycle stops the runners. */
  async stop(): Promise<void> {
    this.stopped = true;
    if (this.timer) clearInterval(this.timer);
    this.timer = undefined;
    await this.active;
  }
}
