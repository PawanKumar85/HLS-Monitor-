import type { Db, Filter } from "mongodb";
import { HLS_HOURLY_DATASET, LATENCY_EDGES, percentileFromBuckets, startOfHour, type HourlyStreamRollup } from "./mongo-metrics.rollup.js";
import type { ServerDetails, MetricsHistoryQuery, MetricsReader, ServerOfflineSummary, StreamOfflineSummary } from "../metrics-reader.js";
import { HLS_METRICS_DATASET, HLS_METRICS_RETENTION_SECONDS, type HlsStreamMetric } from "../../types/hls-metric.types.js";

/** MongoDB history queries, separate from write and connection lifecycle logic. */
export class MongoMetricsReader implements MetricsReader {
  constructor(
    private readonly db: Db,
    private readonly now: () => Date = () => new Date()
  ) { }

  async readHistory(
    query: MetricsHistoryQuery,
    dataset = HLS_METRICS_DATASET
  ): Promise<HlsStreamMetric[]> {
    const limit = query.limit ?? 100;

    if (typeof query.serverIp !== "string" || !query.serverIp ||
      typeof query.streamId !== "string" || !query.streamId ||
      !Number.isSafeInteger(limit) || limit < 1 || limit > 500) {
      throw new RangeError("History requires serverIp, streamId and a limit from 1 to 500");
    }

    for (const date of [query.from, query.to]) {
      if (date !== undefined && (!(date instanceof Date) || !Number.isFinite(date.getTime()))) {
        throw new RangeError("Invalid history date range");
      }
    }

    if (query.from && query.to && query.from > query.to) {
      throw new RangeError("Invalid history date range");
    }

    const cutoff = new Date(this.now().getTime() - HLS_METRICS_RETENTION_SECONDS * 1000);

    const filter: Filter<HlsStreamMetric> = {
      serverIp: query.serverIp,
      streamId: query.streamId,
      ingestedAt: { $gt: cutoff },
      ...((query.from || query.to) ? {
        checkedAt: { ...(query.from ? { $gte: query.from } : {}), ...(query.to ? { $lte: query.to } : {}) },
      } : {}),
    };

    return this.db.collection<HlsStreamMetric>(dataset)
      .find(filter, { projection: { _id: 0 } })
      .sort({ checkedAt: -1 }).limit(limit).toArray();
  }

  /**
   * Reads the hourly rollups (see mongo-metrics.rollup.ts), so cost depends on streams × hours, not on every check.
   * Windows are whole hours: "last 24 hours" covers the hour buckets starting from the hour `from` falls in.
   */
  async serverReport(from: Date, to: Date, _dataset = HLS_METRICS_DATASET): Promise<ServerOfflineSummary[]> {
    if (!(from instanceof Date) || !(to instanceof Date) || !(from <= to)) throw new RangeError("Invalid report date range");
    const cutoff = new Date(this.now().getTime() - HLS_METRICS_RETENTION_SECONDS * 1000);
    const start = startOfHour(from > cutoff ? from : cutoff);
    // Collapse hours on MongoDB: transfer one record per stream, while retaining exact
    // histogram and reason counts for the existing stream/server report calculations.
    const sums = Object.fromEntries(['checks', 'observedMs', 'downtimeMs', 'outages', 'latencyCount', 'latencySum']
      .map(field => [field, { $sum: `$${field}` }]));
    const bucketSums = Object.fromEntries(Array.from({ length: LATENCY_EDGES.length + 1 }, (_, i) =>
      [`b${i}`, { $sum: { $ifNull: [{ $arrayElemAt: ['$latencyBuckets', i] }, 0] } }]));
    const rows = await this.db.collection<HourlyStreamRollup>(HLS_HOURLY_DATASET).aggregate<Omit<HourlyStreamRollup, 'hour'>>([
      { $match: { hour: { $gte: start, $lte: to } } },
      { $group: {
        _id: { serverIp: '$serverIp', streamId: '$streamId' }, ...sums, ...bucketSums,
        lastOfflineAt: { $max: '$lastOfflineAt' }, latencyMax: { $max: '$latencyMax' },
        reasonGroups: { $push: { $objectToArray: { $ifNull: ['$reasons', {}] } } },
      } },
      { $set: { reasonEntries: { $reduce: { input: '$reasonGroups', initialValue: [], in: { $concatArrays: ['$$value', '$$this'] } } } } },
      { $project: {
        _id: 0, serverIp: '$_id.serverIp', streamId: '$_id.streamId',
        ...Object.fromEntries(Object.keys(sums).map(field => [field, 1])), lastOfflineAt: 1, latencyMax: 1,
        latencyBuckets: Array.from({ length: LATENCY_EDGES.length + 1 }, (_, i) => `$b${i}`),
        reasons: { $arrayToObject: { $map: {
          input: { $setUnion: [{ $map: { input: '$reasonEntries', as: 'entry', in: '$$entry.k' } }, []] }, as: 'reason',
          in: { k: '$$reason', v: { $sum: { $map: {
            input: { $filter: { input: '$reasonEntries', as: 'entry', cond: { $eq: ['$$entry.k', '$$reason'] } } },
            as: 'entry', in: '$$entry.v',
          } } } },
        } } },
      } },
    ], { allowDiskUse: true, maxTimeMS: 30_000 }).toArray();

    type Acc = Omit<HourlyStreamRollup, 'hour' | 'streamId' | 'serverIp'>;
    const empty = (): Acc => ({ checks: 0, observedMs: 0, downtimeMs: 0, outages: 0, lastOfflineAt: null, latencyCount: 0, latencySum: 0, latencyMax: null, latencyBuckets: [], reasons: {} });
    const add = (acc: Acc, r: Acc) => {
      acc.checks += r.checks; acc.observedMs += r.observedMs; acc.downtimeMs += r.downtimeMs; acc.outages += r.outages;
      if (r.lastOfflineAt && (!acc.lastOfflineAt || r.lastOfflineAt > acc.lastOfflineAt)) acc.lastOfflineAt = r.lastOfflineAt;
      acc.latencyCount += r.latencyCount; acc.latencySum += r.latencySum;
      if (r.latencyMax !== null && (acc.latencyMax === null || r.latencyMax > acc.latencyMax)) acc.latencyMax = r.latencyMax;
      r.latencyBuckets.forEach((n, i) => { acc.latencyBuckets[i] = (acc.latencyBuckets[i] ?? 0) + n; });
      for (const [reason, n] of Object.entries(r.reasons)) acc.reasons[reason] = (acc.reasons[reason] ?? 0) + n;
      return acc;
    };
    const availability = (a: Acc) => a.observedMs ? Math.floor((1 - a.downtimeMs / a.observedMs) * 10_000) / 100 : 100;
    const summary = (a: Acc) => ({
      checks: a.checks, observedMs: a.observedMs, downtimeMs: a.downtimeMs, availabilityPercent: availability(a), outages: a.outages,
      mttrMs: a.outages ? Math.round(a.downtimeMs / a.outages) : null,
      avgLatencyMs: a.latencyCount ? Math.round(a.latencySum / a.latencyCount) : null,
      p95LatencyMs: percentileFromBuckets(a.latencyBuckets, 0.95, a.latencyMax), p99LatencyMs: percentileFromBuckets(a.latencyBuckets, 0.99, a.latencyMax),
      lastOfflineAt: a.lastOfflineAt,
    });

    const streams = new Map<string, { serverIp: string; streamId: string; acc: Acc }>();
    for (const r of rows) {
      const k = JSON.stringify([r.serverIp, r.streamId]);
      const s = streams.get(k) ?? { serverIp: r.serverIp, streamId: r.streamId, acc: empty() };
      add(s.acc, r); streams.set(k, s);
    }
    const servers = new Map<string, { acc: Acc; streams: StreamOfflineSummary[] }>();
    for (const s of streams.values()) {
      const server = servers.get(s.serverIp) ?? { acc: empty(), streams: [] };
      add(server.acc, s.acc);
      server.streams.push({ streamId: s.streamId, ...summary(s.acc) });
      servers.set(s.serverIp, server);
    }
    return [...servers].map(([serverIp, { acc, streams: list }]): ServerOfflineSummary => ({
      serverIp, ...summary(acc),
      streams: list.length,
      streamsWithOffline: list.filter(s => s.downtimeMs > 0).length,
      failureReasons: Object.entries(acc.reasons).map(([reason, checks]) => ({ reason, checks })).sort((a, b) => b.checks - a.checks || a.reason.localeCompare(b.reason)),
      worstStreams: list.filter(s => s.downtimeMs > 0).sort((a, b) => b.downtimeMs - a.downtimeMs || b.outages - a.outages).slice(0, 5),
    })).sort((a, b) => a.availabilityPercent - b.availabilityPercent || b.downtimeMs - a.downtimeMs);
  }

  async serverDetails(serverIp: string, from: Date, to: Date): Promise<ServerDetails> {
    if (!(from instanceof Date) || !(to instanceof Date) || !(from <= to)) throw new RangeError("Invalid report date range");
    const cutoff = new Date(this.now().getTime() - HLS_METRICS_RETENTION_SECONDS * 1000);
    const since = from > cutoff ? from : cutoff;
    const offline = { $eq: ["$health.status", 0] };
    const [runs, hourly] = await Promise.all([
      // One server only: its raw checks in the window, cut into runs of consecutive offline checks.
      this.db.collection<HlsStreamMetric>(HLS_METRICS_DATASET).aggregate<{
        _id: { streamId: string; run: number }; startedAt: Date; lastOfflineAt: Date; nextAt: Date | null; offlineChecks: number; reasons: string[];
      }>([
        { $match: { serverIp, ingestedAt: { $gt: since, $lte: to } } },
        { $setWindowFields: { partitionBy: "$streamId", sortBy: { checkedAt: 1 }, output: {
          previousStatus: { $shift: { output: "$health.status", by: -1, default: 1 } },
          nextAt: { $shift: { output: "$checkedAt", by: 1, default: null } },
        } } },
        { $set: { starts: { $cond: [{ $and: [offline, { $ne: ["$previousStatus", 0] }] }, 1, 0] } } },
        { $setWindowFields: { partitionBy: "$streamId", sortBy: { checkedAt: 1 }, output: { run: { $sum: "$starts", window: { documents: ["unbounded", "current"] } } } } },
        { $match: { "health.status": 0 } },
        { $sort: { checkedAt: 1 } },
        { $group: { _id: { streamId: "$streamId", run: "$run" }, startedAt: { $min: "$checkedAt" }, lastOfflineAt: { $max: "$checkedAt" },
          // The last offline check's next check is the recovery (null while still offline).
          nextAt: { $last: "$nextAt" }, offlineChecks: { $sum: 1 }, reasons: { $push: { $ifNull: ["$health.reason", "UNKNOWN"] } } } },
        { $sort: { startedAt: -1 } },
        { $limit: 100 },
      ], { allowDiskUse: true, maxTimeMS: 30_000 }).toArray(),
      this.db.collection<HourlyStreamRollup>(HLS_HOURLY_DATASET)
        .find({ serverIp, hour: { $gte: startOfHour(since), $lte: to } }, { projection: { _id: 0, streamId: 1, hour: 1, observedMs: 1, downtimeMs: 1 }, sort: { hour: 1 }, maxTimeMS: 30_000 }).toArray(),
    ]);
    const mostCommon = (values: string[]) => [...values.reduce((m, v) => m.set(v, (m.get(v) ?? 0) + 1), new Map<string, number>())].sort((a, b) => b[1] - a[1])[0]?.[0] ?? "UNKNOWN";
    const timeline = new Map<string, ServerDetails["timeline"][number]["hours"]>();
    for (const h of hourly) {
      timeline.set(h.streamId, [...(timeline.get(h.streamId) ?? []), {
        hour: h.hour, downtimeMs: h.downtimeMs, availabilityPercent: h.observedMs ? Math.floor((1 - h.downtimeMs / h.observedMs) * 10_000) / 100 : 100 }]);
    }
    return {
      serverIp,
      outages: runs.map(r => ({ streamId: r._id.streamId, startedAt: r.startedAt, endedAt: r.nextAt,
        durationMs: (r.nextAt ?? to).getTime() - r.startedAt.getTime(), offlineChecks: r.offlineChecks, reason: mostCommon(r.reasons) })),
      timeline: [...timeline].map(([streamId, hours]) => ({ streamId, hours })).sort((a, b) => a.streamId.localeCompare(b.streamId)),
    };
  }
}
