import { test } from 'node:test';
import assert from 'node:assert/strict';
import Fastify from 'fastify';
import { registerStreamRoutes } from '../src/http/stream-routes.js';
import { StreamService } from '../src/inventory/stream-service.js';
import type { MetricsHistoryQuery, MetricsReader } from '../src/storage/metrics-reader.js';
import { MemoryInventory } from './helpers/stream-inventory.js';

const headers = {};

async function fixture() {
  const service = new StreamService(new MemoryInventory());
  const queries: MetricsHistoryQuery[] = []; const reports: [Date, Date][] = []; let failNext = false; const details: [string, Date, Date][] = [];
  const metrics: MetricsReader = {
    async readHistory(query) {
      queries.push(query);
      return [{ streamId: 'hmtv', serverIp: '192.168.1.10', streamUrl: 'http://u:secret@192.168.1.10/live.m3u8?token=x', createdAt: new Date(), receivedAt: new Date(),
        checkedAt: new Date('2026-11-10T10:00:00Z'), ingestedAt: new Date(), pipelineDelayMs: 12, health: { status: 0, segmentLatencyMs: 3001 },
        redisStreamKey: 'hls:streams:192.168.1.10', redisMessageId: '1-0' }];
    },
    async serverDetails(serverIp, from, to) { details.push([serverIp, from, to]); return { serverIp, outages: [], timeline: [] }; },
    async serverReport(from, to) { reports.push([from, to]); if (failNext) { failNext = false; throw new Error('mongo down'); } return []; },
  };
  const app = Fastify();
  registerStreamRoutes(app, { service, metrics });
  const created = await service.create({ serverIp: '192.168.1.10', streamId: 'hmtv', streamUrl: 'http://192.168.1.10/live.m3u8' });
  return { app, id: created.id, queries, reports, details, failOnce: () => { failNext = true; } };
}

test('stream history resolves the stream, hides the stored URL, and validates its range', async () => {
  const { app, id, queries } = await fixture();
  try {
    const result = await app.inject({ url: `/api/streams/${id}/history?from=2026-11-09T00:00:00Z&limit=50`, headers });
    assert.equal(result.statusCode, 200);
    assert.deepEqual(result.json(), { serverIp: '192.168.1.10', streamId: 'hmtv',
      items: [{ checkedAt: '2026-11-10T10:00:00.000Z', status: 0, latencyMs: 3001, pipelineDelayMs: 12 }] });
    assert.ok(!result.body.includes('secret') && !result.body.includes('token=x'));
    assert.deepEqual(queries[0], { serverIp: '192.168.1.10', streamId: 'hmtv', from: new Date('2026-11-09T00:00:00Z'), limit: 50 });
    assert.equal((await app.inject({ url: `/api/streams/${id}/history?from=2026-11-10T00:00:00Z&to=2026-11-09T00:00:00Z`, headers })).statusCode, 400);
    assert.equal((await app.inject({ url: `/api/streams/${id}/history?from=yesterday`, headers })).statusCode, 400);
    assert.equal((await app.inject({ url: '/api/streams/00000000-0000-0000-0000-000000000000/history', headers })).statusCode, 404);
  } finally { await app.close(); }
});

test('server report defaults to the last 24 hours and bounds the window to the 7-day retention', async () => {
  const { app, reports } = await fixture();
  try {
    const result = await app.inject({ url: '/api/servers/report', headers });
    assert.equal(result.statusCode, 200);
    assert.equal(result.json().hours, 24);
    assert.equal(reports[0]![1].getTime() - reports[0]![0].getTime(), 24 * 3_600_000);
    assert.equal((await app.inject({ url: '/api/servers/report?hours=168', headers })).statusCode, 200);
    assert.equal((await app.inject({ url: '/api/servers/report?hours=169', headers })).statusCode, 400);
    assert.equal((await app.inject({ url: '/api/servers/report?hours=0', headers })).statusCode, 400);
  } finally { await app.close(); }
});

test('server report: one query per window per minute for all tabs; a failed query is not cached', async () => {
  const { app, reports, failOnce } = await fixture();
  try {
    failOnce();
    assert.equal((await app.inject('/api/servers/report?hours=1')).statusCode, 503);
    assert.equal((await app.inject('/api/servers/report?hours=1')).statusCode, 200, 'the failure was not cached');
    await Promise.all([app.inject('/api/servers/report?hours=1'), app.inject('/api/servers/report?hours=1')]);
    assert.equal(reports.length, 2, 'later and simultaneous requests share the cached report');
    await app.inject('/api/servers/report?hours=24');
    assert.equal(reports.length, 3, 'each window has its own entry');
  } finally { await app.close(); }
});

test('server details: one server, URL-encoded name, hours bounded like the report', async () => {
  const { app, details } = await fixture();
  try {
    const ok = await app.inject('/api/servers/stream.ottlive.co.in/details?hours=168');
    assert.equal(ok.statusCode, 200); assert.equal(ok.json().serverIp, 'stream.ottlive.co.in');
    assert.equal(details[0]![2].getTime() - details[0]![1].getTime(), 168 * 3_600_000);
    assert.equal((await app.inject(`/api/servers/${encodeURIComponent('[2001:db8::1]')}/details`)).json().serverIp, '[2001:db8::1]');
    assert.equal((await app.inject('/api/servers/x/details?hours=169')).statusCode, 400);
  } finally { await app.close(); }
});
