import type { Db } from 'mongodb';
import type { Redis } from 'ioredis';

export const RESET_COLLECTIONS = ['hls_stream_inventory', 'pipeline_channels', 'pipeline_probe_targets', 'hls_stream_metrics', 'hls_stream_hourly', 'pipeline_incidents', 'hls_processing_failures'] as const;
export interface MonitoringReset {
  readonly running: boolean;
  readonly generation: number;
  run(): Promise<void>;
}
/** Never drops the database or flushes Redis: accounts, sessions and notification settings survive. */
export class MonitoringResetService implements MonitoringReset {
  running = false;
  generation = 0;
  constructor(private readonly dependencies: {
    db: () => Db; redis: Pick<Redis, 'scan' | 'unlink'>;
    quiesce: () => Promise<void>; resume: () => Promise<void>; clearMemory: () => void;
  }) {}
  async run(): Promise<void> {
    if (this.running) throw new Error('A monitoring reset is already running');
    this.running = true;
    try {
      await this.dependencies.quiesce();
      // Persist the reset before clearing: file/legacy seeds must not resurrect removed streams on restart.
      await this.dependencies.db().collection<{ _id: string; resetAt: Date }>('app_settings').updateOne({ _id: 'monitoring-reset' },
        { $set: { resetAt: new Date() } }, { upsert: true });
      for (const name of RESET_COLLECTIONS) await this.dependencies.db().collection(name).deleteMany({});
      // Collect before unlinking so deleting keys cannot disturb SCAN traversal.
      const keys = new Set<string>();
      let cursor = '0';
      do {
        const page = await this.dependencies.redis.scan(cursor, 'MATCH', 'hls:*', 'COUNT', 500);
        cursor = page[0];
        for (const key of page[1]) keys.add(key);
      } while (cursor !== '0');
      const list = [...keys];
      for (let i = 0; i < list.length; i += 200) await this.dependencies.redis.unlink(...list.slice(i, i + 200));
      this.dependencies.clearMemory();
    } finally {
      this.generation++; // Invalidate cached reports even after a partial failure.
      try { await this.dependencies.resume(); }
      finally { this.running = false; }
    }
  }
}
