import type { IncidentNotifier, IncidentStore } from './pipeline-monitor.js';

export const STALLED_INCIDENT_KEY = 'monitor:checks-stalled';

export interface CheckWatchdogOptions {
  /** When the newest check was saved (null: never). */
  lastSavedAt: () => Promise<Date | null>;
  /** True when at least one stream is registered and enabled, so checks are expected at all. */
  expectChecks: () => Promise<boolean>;
  incidents?: Pick<IncidentStore, 'open' | 'resolve'>;
  notify?: IncidentNotifier;
  /** No saved check for this long while streams exist = the monitor itself has stopped. Default 3 min. */
  stalledAfterMs?: number;
  now?: () => Date;
  log?: (message: string) => void;
}

/**
 * Watches the monitor itself. If the scheduler, consumers or batch writes stop, every stream turns STALE on the
 * dashboard but nothing alerts. This opens one incident (shown on the dashboard, sent to the webhook) when no
 * check has been saved for a while, and resolves it when checks arrive again.
 */
export class CheckWatchdog {
  private active: Promise<void> | undefined;
  private timer: NodeJS.Timeout | undefined;
  private state: { lastSavedAt: string | null; stalled: boolean; checkedAt: string | null } = { lastSavedAt: null, stalled: false, checkedAt: null };
  private readonly stalledAfterMs: number;
  private readonly now: () => Date;

  constructor(private readonly options: CheckWatchdogOptions) {
    this.stalledAfterMs = options.stalledAfterMs ?? 180_000;
    this.now = options.now ?? (() => new Date());
  }

  status() { return { ...this.state, stalledAfterMs: this.stalledAfterMs }; }

  tick(): Promise<void> {
    this.active ??= this.execute().finally(() => { this.active = undefined; });
    return this.active;
  }
  reset() { this.state = { lastSavedAt: null, stalled: false, checkedAt: null }; }
  private async execute(): Promise<void> {
    const now = this.now();
    const [last, expected] = await Promise.all([this.options.lastSavedAt(), this.options.expectChecks()]);
    const stalled = expected && (!last || now.getTime() - last.getTime() > this.stalledAfterMs);
    this.state = { lastSavedAt: last?.toISOString() ?? null, stalled, checkedAt: now.toISOString() };
    const { incidents, notify } = this.options;
    if (stalled) {
      const minutes = last ? Math.round((now.getTime() - last.getTime()) / 60_000) : null;
      const incident = { key: STALLED_INCIDENT_KEY, kind: 'OFFLINE' as const, channels: [],
        title: minutes === null ? 'Monitoring has saved no checks yet although streams are registered' : `Monitoring stopped: no check saved for ${minutes} min` };
      if (await incidents?.open(incident, now)) await notify?.({ type: 'opened', incident: { ...incident, openedAt: now } });
    } else {
      const resolved = await incidents?.resolve(STALLED_INCIDENT_KEY, now);
      if (resolved) await notify?.({ type: 'resolved', incident: resolved });
    }
  }

  start(intervalMs = 60_000): void {
    if (this.timer) return;
    const run = () => { void this.tick().catch(() => (this.options.log ?? console.error)('Check watchdog could not read monitoring state')); };
    this.timer = setInterval(run, intervalMs); this.timer.unref();
    // First look after one interval: right after startup no check can have been saved yet.
  }

  async stop(): Promise<void> { clearInterval(this.timer); this.timer = undefined; await this.active; }
}
