import type { Db } from "mongodb";
import { HLS_METRICS_DATASET, HLS_METRICS_RETENTION_SECONDS } from "../../types/hls-metric.types.js";

export const HLS_HOURLY_DATASET = "hls_stream_hourly";
const HOUR = 3_600_000;
/** Checks this close to an hour edge are read too, so a check's span and the "was it already offline?" test see across the edge. */
const MARGIN = 15 * 60_000;
/** Upper edges (ms) of the online-latency histogram used for p95/p99. The last bucket is everything slower. */
export const LATENCY_EDGES = [50, 100, 200, 300, 500, 750, 1000, 1500, 2000, 3000, 5000] as const;

/** One stream in one hour: everything the report needs, already summed. */
export interface HourlyStreamRollup {
  serverIp: string; streamId: string; hour: Date;
  checks: number; observedMs: number; downtimeMs: number; outages: number; lastOfflineAt: Date | null;
  latencyCount: number; latencySum: number; latencyMax: number | null; latencyBuckets: number[];
  reasons: Record<string, number>;
}

export const startOfHour = (date: Date) => new Date(Math.floor(date.getTime() / HOUR) * HOUR);

/** Approximate percentile from histogram counts: the upper edge of the bucket that holds it, never above the slowest check seen. */
export function percentileFromBuckets(buckets: readonly number[], p: number, max: number | null): number | null {
  const total = buckets.reduce((sum, n) => sum + n, 0);
  if (!total || max === null) return null;
  let seen = 0;
  for (let i = 0; i < buckets.length; i++) {
    seen += buckets[i]!;
    if (seen >= p * total) return Math.min(LATENCY_EDGES[i] ?? max, max);
  }
  return max;
}

/**
 * Hourly summaries of hls_stream_metrics, so a 7-day report reads ~streams × 168 documents instead of every check.
 * Each refresh recomputes whole hours from the raw checks and replaces them: re-running is safe, and a batch that
 * was retried or written late is simply counted on the next refresh. Downtime rules match the raw report:
 * each check covers the time until the stream's next check, capped at 3 × its usual gap (min 30 s).
 */
export class MongoMetricsRollup {
  private timer: NodeJS.Timeout | undefined;
  private active: Promise<void> | undefined;

  constructor(private readonly db: () => Db, private readonly now: () => Date = () => new Date(), private readonly log: (message: string) => void = console.error) {}

  async ensureIndexes(): Promise<void> {
    await this.db().collection(HLS_HOURLY_DATASET).createIndexes([
      { key: { hour: 1 }, name: "hour_ttl", expireAfterSeconds: HLS_METRICS_RETENTION_SECONDS },
      { key: { serverIp: 1, hour: 1 } },
    ]);
  }

  /** Recomputes one hour (any time inside it) from the raw checks. */
  async refreshHour(at: Date): Promise<void> {
    const hour = startOfHour(at);
    const end = new Date(hour.getTime() + HOUR);
    const now = this.now();
    const offline = { $eq: ["$health.status", 0] };
    const online = { $eq: ["$health.status", 1] };
    const latency = "$health.segmentLatencyMs";
    const stream = { serverIp: "$serverIp", streamId: "$streamId" };
    const buckets: Record<string, unknown> = {};
    LATENCY_EDGES.forEach((edge, i) => {
      buckets[`b${i}`] = { $sum: { $cond: [{ $and: [online, { $gt: [latency, i ? LATENCY_EDGES[i - 1]! : -1] }, { $lte: [latency, edge] }] }, 1, 0] } };
    });
    buckets[`b${LATENCY_EDGES.length}`] = { $sum: { $cond: [{ $and: [online, { $gt: [latency, LATENCY_EDGES.at(-1)!] }] }, 1, 0] } };
    await this.db().collection(HLS_METRICS_DATASET).aggregate([
      // ingestedAt is the indexed field; it trails checkedAt by seconds, well inside the margin.
      { $match: { ingestedAt: { $gte: new Date(hour.getTime() - MARGIN), $lt: new Date(end.getTime() + MARGIN) } } },
      { $setWindowFields: { partitionBy: stream, sortBy: { checkedAt: 1 }, output: {
        // The newest check covers the time until now. A check's whole span stays in its own hour (even past the edge),
        // so the time between the hour's last check and the next hour's first is counted once, not lost.
        nextAt: { $shift: { output: "$checkedAt", by: 1, default: now < end ? now : end } },
        previousStatus: { $shift: { output: "$health.status", by: -1, default: 1 } },
      } } },
      { $match: { checkedAt: { $gte: hour, $lt: end } } },
      { $set: { gapMs: { $max: [0, { $subtract: ["$nextAt", "$checkedAt"] }] } } },
      { $setWindowFields: { partitionBy: stream, output: { usualGapMs: { $median: { input: "$gapMs", method: "approximate" }, window: { documents: ["unbounded", "unbounded"] } } } } },
      { $set: { spanMs: { $min: ["$gapMs", { $max: [{ $multiply: ["$usualGapMs", 3] }, 30_000] }] } } },
      { $group: {
        _id: stream,
        checks: { $sum: 1 },
        observedMs: { $sum: "$spanMs" },
        downtimeMs: { $sum: { $cond: [offline, "$spanMs", 0] } },
        outages: { $sum: { $cond: [{ $and: [offline, { $ne: ["$previousStatus", 0] }] }, 1, 0] } },
        lastOfflineAt: { $max: { $cond: [offline, "$checkedAt", null] } },
        // Offline checks record their timeout as latency; latency figures use online checks only.
        latencyCount: { $sum: { $cond: [online, 1, 0] } },
        latencySum: { $sum: { $cond: [online, latency, 0] } },
        latencyMax: { $max: { $cond: [online, latency, null] } },
        reasonList: { $push: { $cond: [offline, { $ifNull: ["$health.reason", "UNKNOWN"] }, "$$REMOVE"] } },
        ...buckets,
      } },
      { $project: {
        _id: { serverIp: "$_id.serverIp", streamId: "$_id.streamId", hour: { $literal: hour } },
        serverIp: "$_id.serverIp", streamId: "$_id.streamId", hour: { $literal: hour },
        checks: 1, observedMs: 1, downtimeMs: 1, outages: 1, lastOfflineAt: 1, latencyCount: 1, latencySum: 1, latencyMax: 1,
        latencyBuckets: [...Array.from({ length: LATENCY_EDGES.length + 1 }, (_, i) => `$b${i}`)],
        reasons: { $arrayToObject: { $map: { input: { $setUnion: ["$reasonList"] }, as: "r",
          in: { k: "$$r", v: { $size: { $filter: { input: "$reasonList", cond: { $eq: ["$$this", "$$r"] } } } } } } } },
      } },
      { $merge: { into: HLS_HOURLY_DATASET, on: "_id", whenMatched: "replace", whenNotMatched: "insert" } },
    ], { allowDiskUse: true, maxTimeMS: 60_000 }).toArray();
  }

  /** First start, or rollups lost: rebuild every hour still in raw retention. Otherwise only the recent hours. */
  async catchUp(): Promise<void> {
    await this.ensureIndexes();
    const now = this.now();
    const empty = (await this.db().collection(HLS_HOURLY_DATASET).estimatedDocumentCount()) === 0;
    const hours = empty ? Math.ceil(HLS_METRICS_RETENTION_SECONDS / 3600) : 1;
    for (let i = hours; i >= 0; i--) await this.refreshHour(new Date(now.getTime() - i * HOUR));
  }

  /** Every minute: the current hour, and the previous one while late checks can still land in it. */
  private async tick(): Promise<void> {
    const now = this.now();
    if (now.getTime() - startOfHour(now).getTime() < MARGIN + 60_000) await this.refreshHour(new Date(now.getTime() - HOUR));
    await this.refreshHour(now);
  }

  start(intervalMs = 60_000): void {
    if (this.timer) return;
    const run = (job: () => Promise<void>) => { this.active ??= job().catch(() => this.log("Hourly report rollup failed; retrying next minute")).finally(() => { this.active = undefined; }); };
    run(() => this.catchUp());
    this.timer = setInterval(() => run(() => this.tick()), intervalMs);
    this.timer.unref();
  }

  async stop(): Promise<void> {
    clearInterval(this.timer); this.timer = undefined;
    await this.active;
  }
}
