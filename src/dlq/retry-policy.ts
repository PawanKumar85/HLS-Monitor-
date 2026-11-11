import type { HlsDeadLetter, RetrySettings } from "./dead-letter.types.js";

export class RetryPolicy {
  readonly settings: RetrySettings;
  constructor(settings: Partial<RetrySettings> = {}) {
    this.settings = { maxAttempts: 5, baseDelayMs: 1000, maxDelayMs: 60_000, ...settings };
    for (const n of Object.values(this.settings)) if (!Number.isSafeInteger(n) || n <= 0) throw new RangeError("Invalid retry configuration");
    if (this.settings.baseDelayMs > this.settings.maxDelayMs || this.settings.maxDelayMs > 2147483647) throw new RangeError("Invalid retry delay range");
  }
  delay(attempt: number): number { return Math.min(this.settings.maxDelayMs, this.settings.baseDelayMs * 2 ** Math.min(30, Math.max(0, attempt - 1))); }
  terminal(record: HlsDeadLetter): boolean { return !record.retryable || (record.bounded && record.attempts >= this.settings.maxAttempts); }
}
