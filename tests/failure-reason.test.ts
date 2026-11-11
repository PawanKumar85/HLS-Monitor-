import { test } from 'node:test';
import assert from 'node:assert/strict';
import { failureReason, inspectHls } from '../src/services/hls-health.service.js';
import { LiveHlsHealthChecker } from '../src/monitoring/live-hls-health-checker.js';

test('failure reasons: one short code per failure, network before HTTP before parse', () => {
  const f = (x: Partial<Parameters<typeof failureReason>[0]>) => failureReason({ stage: 'playlist-headers', reason: 'ProbeError', ...x });
  assert.equal(f({ code: 'ENOTFOUND', reason: 'TypeError' }), 'DNS');
  assert.equal(f({ code: 'ECONNREFUSED', reason: 'TypeError' }), 'REFUSED');
  assert.equal(f({ reason: 'TimeoutError' }), 'TIMEOUT');
  assert.equal(f({ httpStatus: 404 }), 'HTTP_404');
  assert.equal(f({ httpStatus: 503 }), 'HTTP_5XX');
  assert.equal(f({ httpStatus: 403 }), 'HTTP_4XX');
  assert.equal(f({ stage: 'playlist-parse' }), 'BAD_PLAYLIST');
  assert.equal(f({ stage: 'segment-body' }), 'EMPTY_SEGMENT');
  assert.equal(f({ reason: 'TypeError' }), 'NETWORK');
});

test('inspectHls tags 404s and ended playlists', async () => {
  const notFound = await inspectHls('http://o.test/a.m3u8', async () => new Response('gone', { status: 404 }));
  assert.deepEqual([notFound.health.status, notFound.health.reason], [0, 'HTTP_404']);
  const ended = await inspectHls('http://o.test/a.m3u8', async () => new Response('#EXTM3U\n#EXTINF:6,\nseg1.ts\n#EXT-X-ENDLIST\n'));
  assert.deepEqual([ended.health.status, ended.health.reason], [0, 'ENDED']);
});

test('live checker: a one-off blip stays online without a reason; a frozen playlist is FROZEN', async () => {
  const identity = { serverIp: 'o.test', streamId: 'a' };
  const freshness = { observe: async () => false, fail: async () => true };
  const failing = new LiveHlsHealthChecker({ get: async () => null }, freshness, async () => ({ health: { status: 0, latency: 3000, reason: 'TIMEOUT' } }));
  assert.deepEqual(await failing.check('http://o.test/a.m3u8', identity), { status: 1, latency: 3000 }, 'first failure in a row is a blip');
  freshness.fail = async () => false;
  assert.deepEqual(await failing.check('http://o.test/a.m3u8', identity), { status: 0, latency: 3000, reason: 'TIMEOUT' });
  const frozen = new LiveHlsHealthChecker({ get: async () => null }, freshness, async () => ({ health: { status: 1, latency: 90 }, progress: { fingerprint: 'x', targetDurationMs: 6000 } }));
  assert.deepEqual(await frozen.check('http://o.test/a.m3u8', identity), { status: 0, latency: 90, reason: 'FROZEN' });
});
