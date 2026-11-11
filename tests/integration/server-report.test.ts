import { test } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { MongoClient } from 'mongodb';
import { MongoMetricsReader } from '../../src/storage/adapters/mongo-metrics.reader.js';
import { MongoMetricsRollup, percentileFromBuckets } from '../../src/storage/adapters/mongo-metrics.rollup.js';
const enabled = process.env['RUN_INTEGRATION'] === '1';
const MIN = 60_000;

test('server report: downtime by time, availability, outages, MTTR, and online-only latency percentiles', { skip: !enabled, timeout: 20000 }, async t => {
  assert.match(process.env['MONGO_DATABASE'] ?? '', /^gate_test_[a-zA-Z0-9_]+$/);
  const mongo = new MongoClient(process.env['MONGO_URI']!);
  const db = mongo.db(`gate_test_server_report_${randomUUID().replaceAll('-', '')}`);
  t.after(async () => { await db.dropDatabase(); await mongo.close(); });
  // Keep spans in a complete hour so expected durations do not depend on wall-clock minute.
  const now = new Date('2026-11-11T12:00:00Z');
  const at = (minutesAgo: number) => new Date(now.getTime() - minutesAgo * MIN);
  let n = 0;
  const row = (serverIp: string, streamId: string, status: 0 | 1, minutesAgo: number, latency = 200, reason?: string) => ({
    serverIp, streamId, streamUrl: 'http://x/live.m3u8', createdAt: at(minutesAgo), receivedAt: at(minutesAgo), checkedAt: at(minutesAgo),
    ingestedAt: at(minutesAgo), pipelineDelayMs: 5, health: { status, segmentLatencyMs: latency, ...(reason ? { reason } : {}) }, redisStreamKey: `hls:streams:${serverIp}`, redisMessageId: `${++n}-0`,
  });
  await db.collection('hls_stream_metrics').insertMany([
    // .10, checked every 10 min. a: online, then offline for the last 30 min (one outage, still running). b: always online.
    row('10.0.0.10', 'a', 1, 40), row('10.0.0.10', 'a', 0, 30, 3000, 'TIMEOUT'), row('10.0.0.10', 'a', 0, 20, 3000, 'TIMEOUT'), row('10.0.0.10', 'a', 0, 10, 3000, 'HTTP_404'),
    row('10.0.0.10', 'b', 1, 20), row('10.0.0.10', 'b', 1, 10),
    // .13 flaps every minute: two outages of 1 minute each.
    row('10.0.0.13', 'e', 0, 4, 3000), row('10.0.0.13', 'e', 1, 3), row('10.0.0.13', 'e', 0, 2, 3000), row('10.0.0.13', 'e', 1, 1),
    // .14 checked every minute, but monitoring paused for 40 min while it was offline: only 3 min (3 × usual gap) count.
    row('10.0.0.14', 'f', 1, 50), row('10.0.0.14', 'f', 1, 49), row('10.0.0.14', 'f', 0, 48, 3000), row('10.0.0.14', 'f', 1, 8), row('10.0.0.14', 'f', 1, 7),
    // .15 always online, latency 100..1000 ms, plus one offline timeout that must not count as latency.
    ...Array.from({ length: 10 }, (_, i) => row('10.0.0.15', 'g', 1, 30 - i, (i + 1) * 100)), row('10.0.0.15', 'g', 0, 15, 6000),
    // outside the 1-hour window: ignored
    row('10.0.0.15', 'g', 0, 120, 6000),
  ]);
  // The report reads hourly rollups: build them from the raw checks first (as the background job does).
  const rollup = new MongoMetricsRollup(() => db, () => now);
  await rollup.catchUp();
  await rollup.catchUp(); // re-running replaces, never double counts
  const report = await new MongoMetricsReader(db, () => now).serverReport(at(60), now);
  const server = (ip: string) => report.find(s => s.serverIp === ip)!;

  const s10 = server('10.0.0.10');
  assert.deepEqual([s10.checks, s10.observedMs, s10.downtimeMs, s10.availabilityPercent, s10.outages, s10.mttrMs, s10.streams, s10.streamsWithOffline],
    [6, 60 * MIN, 30 * MIN, 50, 1, 30 * MIN, 2, 1]);
  assert.equal(s10.avgLatencyMs, 200, 'offline checks (3000 ms timeouts) are not latency');
  assert.equal(s10.lastOfflineAt?.getTime(), at(10).getTime());
  assert.deepEqual(s10.worstStreams.map(w => [w.streamId, w.downtimeMs, w.availabilityPercent]), [['a', 30 * MIN, 25]]);
  assert.deepEqual(s10.failureReasons, [{ reason: 'TIMEOUT', checks: 2 }, { reason: 'HTTP_404', checks: 1 }], 'most common reason first');
  assert.deepEqual(server('10.0.0.13').failureReasons, [{ reason: 'UNKNOWN', checks: 2 }], 'rows stored before reasons existed');

  const s13 = server('10.0.0.13');
  assert.deepEqual([s13.outages, s13.downtimeMs, s13.mttrMs], [2, 2 * MIN, MIN], 'each flap is an outage; MTTR is the average outage length');

  const s14 = server('10.0.0.14');
  assert.equal(s14.downtimeMs, 3 * MIN, 'a 40-minute pause in monitoring is capped at 3 × the usual 1-minute gap');

  const s15 = server('10.0.0.15');
  assert.equal(s15.avgLatencyMs, 550);
  assert.ok(s15.p95LatencyMs! >= 900 && s15.p95LatencyMs! <= 1000, `p95 ${s15.p95LatencyMs}`);
  assert.ok(s15.p99LatencyMs! >= s15.p95LatencyMs! && s15.p99LatencyMs! <= 1000, `p99 ${s15.p99LatencyMs}: the 6000 ms timeout is excluded`);
  assert.equal(s15.availabilityPercent < 100, true, 'any downtime shows below 100');

  assert.deepEqual(report.map(s => s.serverIp).slice(0, 1), ['10.0.0.10'], 'least available server first');

  // Outage log: .10/a went offline 30 min ago and is still offline; most of its checks failed with TIMEOUT.
  const d10 = await new MongoMetricsReader(db, () => now).serverDetails('10.0.0.10', at(60), now);
  assert.deepEqual(d10.outages.map(o => [o.streamId, o.startedAt.getTime(), o.endedAt, o.offlineChecks, o.reason]), [['a', at(30).getTime(), null, 3, 'TIMEOUT']]);
  assert.equal(d10.outages[0]!.durationMs, 30 * MIN, 'a running outage lasts until now');
  assert.deepEqual(d10.timeline.map(t => t.streamId), ['a', 'b']);
  assert.ok(d10.timeline[0]!.hours.every(h => h.availabilityPercent <= 100) && d10.timeline[0]!.hours.some(h => h.downtimeMs > 0));
  // .13 flapped: two finished outages, newest first, each ended by the next online check.
  const d13 = await new MongoMetricsReader(db, () => now).serverDetails('10.0.0.13', at(60), now);
  assert.deepEqual(d13.outages.map(o => [o.startedAt.getTime(), o.endedAt?.getTime(), o.durationMs]), [[at(2).getTime(), at(1).getTime(), MIN], [at(4).getTime(), at(3).getTime(), MIN]]);
  for (let i = 1; i < report.length; i++) assert.ok(report[i - 1]!.availabilityPercent <= report[i]!.availabilityPercent);
});

test('percentileFromBuckets: upper edge of the bucket holding the percentile, never above the slowest check', () => {
  // 50 ms bucket: 90 checks, 1000 ms bucket: 9, >5000 ms: 1
  const buckets = [90, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0, 1];
  assert.equal(percentileFromBuckets(buckets, 0.5, 6200), 50);
  assert.equal(percentileFromBuckets(buckets, 0.95, 6200), 1000);
  assert.equal(percentileFromBuckets(buckets, 0.995, 6200), 6200);
  assert.equal(percentileFromBuckets([3], 0.99, 20), 20, 'capped at the slowest check');
  assert.equal(percentileFromBuckets([], 0.95, null), null);
});

test('report groups repeated hourly buckets without losing reasons, histograms or stream isolation', { skip: !enabled, timeout: 15000 }, async t => {
  assert.match(process.env['MONGO_DATABASE'] ?? '', /^gate_test_[a-zA-Z0-9_]+$/);
  const mongo = new MongoClient(process.env['MONGO_URI']!, { serverSelectionTimeoutMS: 5000 });
  const db = mongo.db(`gate_test_report_group_${randomUUID().replaceAll('-', '')}`);
  t.after(async () => { await db.dropDatabase(); await mongo.close(); });
  const now = new Date('2026-11-11T12:00:00Z');
  const hourly = (serverIp: string, streamId: string, hour: number, offline: boolean) => ({
    serverIp, streamId, hour: new Date(now.getTime() - hour * 3600000), checks: 10, observedMs: 1000,
    downtimeMs: offline ? 200 : 0, outages: offline ? 1 : 0, lastOfflineAt: offline ? new Date(now.getTime() - hour * 3600000) : null,
    latencyCount: 8, latencySum: 800, latencyMax: 100, latencyBuckets: [0, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    reasons: offline ? { TIMEOUT: 2, HTTP_404: 1 } : {},
  });
  await db.collection('hls_stream_hourly').insertMany([
    ...Array.from({ length: 168 }, (_, i) => hourly('a', 'same-id', i + 1, true)),
    hourly('a', 'online', 1, false), hourly('b', 'same-id', 1, false), hourly('a', 'outside', 200, true),
  ]);
  const report = await new MongoMetricsReader(db, () => now).serverReport(new Date(now.getTime() - 168 * 3600000), now);
  const a = report.find(s => s.serverIp === 'a')!;
  assert.equal(a.streams, 2);
  assert.equal(a.checks, 1690);
  assert.equal(a.downtimeMs, 33600);
  assert.equal(a.avgLatencyMs, 100);
  assert.equal(a.p95LatencyMs, 100);
  assert.equal(a.p99LatencyMs, 100);
  assert.deepEqual(a.failureReasons, [{ reason: 'TIMEOUT', checks: 336 }, { reason: 'HTTP_404', checks: 168 }]);
  assert.deepEqual(a.worstStreams.map(s => [s.streamId, s.checks, s.outages]), [['same-id', 1680, 168]]);
  assert.equal(a.lastOfflineAt?.getTime(), now.getTime() - 3600000);
  assert.equal(report.find(s => s.serverIp === 'b')!.checks, 10);
});
