import { test } from 'node:test';
import assert from 'node:assert/strict';
import { reportSummary } from '../frontend/src/lib/report-summary.js';
import type { ServerSummary } from '../frontend/src/api.js';
const server = (observedMs: number, downtimeMs: number, outages: number): ServerSummary => ({ serverIp: 'server.test', observedMs, downtimeMs, outages, availabilityPercent: 0, streams: 1, streamsWithOffline: downtimeMs ? 1 : 0, checks: 100, mttrMs: null, avgLatencyMs: null, p95LatencyMs: null, p99LatencyMs: null, lastOfflineAt: null, failureReasons: [], worstStreams: [] });
test('availability is time-weighted rather than the average of server percentages', () => {
 const summary=reportSummary([server(1000,1000,1),server(9000,0,0)]);
 assert.equal(summary.availability,90);assert.equal(summary.affected,1);assert.equal(summary.outages,1);assert.equal(summary.downtimeMs,1000);
});
test('no monitored time yields unavailable, not artificial 100 percent', () => {
 assert.equal(reportSummary([]).availability,null);
 assert.equal(reportSummary([server(0,0,0)]).availability,null);
});
test('small recorded downtime is not rounded up to perfect availability', () => {
 assert.equal(reportSummary([server(1000000,1,1)]).availability,99.99);
});
