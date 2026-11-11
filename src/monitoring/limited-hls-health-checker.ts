import { ProcessingFailure } from '../dlq/dead-letter.types.js';
import type { HlsHealthChecker, HlsProbeIdentity } from '../ports/hls-processing.js';
/** Global network/probe concurrency, in addition to existing per-server limits. */
export class LimitedHlsHealthChecker implements HlsHealthChecker {
  private active = 0;
  private closed = false;
  private readonly waiting: { resolve(): void; reject(error: Error): void }[] = [];
  constructor(private readonly checker: HlsHealthChecker, private readonly limit = 8, private readonly maxWaiting = 128) {
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 1000 || !Number.isSafeInteger(maxWaiting) || maxWaiting < 0 || maxWaiting > 10000) throw new RangeError('Invalid global probe limits');
  }
  private unavailable() { return new ProcessingFailure('HEALTH_CHECK', 'PROBE_CAPACITY', 'Probe worker capacity unavailable', true, false); }
  async check(url: string, identity?: HlsProbeIdentity) {
    if (this.closed) throw this.unavailable();
    if (this.active >= this.limit) {
      if (this.waiting.length >= this.maxWaiting) throw this.unavailable();
      await new Promise<void>((resolve, reject) => { this.waiting.push({ resolve, reject }); });
    } else this.active++;
    try { if (this.closed) throw this.unavailable(); return await this.checker.check(url, identity); }
    finally {
      const next = this.waiting.shift();
      if (next) next.resolve(); else this.active--;
    }
  }
  stop(): void { this.closed = true; for (const waiter of this.waiting.splice(0)) waiter.reject(this.unavailable()); }
}
