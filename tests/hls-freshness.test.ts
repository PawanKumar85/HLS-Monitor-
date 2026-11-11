import { test } from 'node:test';
import assert from 'node:assert/strict';
import { inspectHls } from '../src/services/hls-health.service.js';
import { LiveHlsHealthChecker } from '../src/monitoring/live-hls-health-checker.js';
import { ProcessingFailure } from '../src/dlq/dead-letter.types.js';
const media = (sequence: number, end = false) => `#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:${sequence}\n#EXTINF:2,\nsegment.ts?token=rotating\n${end ? '#EXT-X-ENDLIST' : ''}`;
const fetcher = (playlist: string): typeof fetch => async input => new Response(String(input).includes('.m3u8') ? playlist : new Uint8Array([1, 2, 3]));
test('media progress changes on sequence advancement, not rotating auth query strings', async () => {
  const first = await inspectHls('https://example.test/live.m3u8', fetcher(media(1)));
  const unchanged = await inspectHls('https://example.test/live.m3u8', fetcher(media(1).replace('rotating', 'new-token')));
  const next = await inspectHls('https://example.test/live.m3u8', fetcher(media(2)));
  assert.equal(first.health.status, 1); assert.equal(first.progress?.targetDurationMs, 2000);
  assert.equal(first.progress?.fingerprint, unchanged.progress?.fingerprint);
  assert.notEqual(first.progress?.fingerprint, next.progress?.fingerprint);
});
test('master probe follows a relative media playlist and requests the final media segment', async () => {
  const requests: string[] = [];
  const result = await inspectHls('https://example.test/master.m3u8', async input => {
    const url = String(input); requests.push(url);
    if (url.endsWith('master.m3u8')) return new Response('#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=100\nvideo/media.m3u8');
    if (url.includes('media.m3u8')) return new Response(media(7));
    return new Response(new Uint8Array([1]));
  });
  assert.equal(result.health.status, 1);
  assert.deepEqual(requests, ['https://example.test/master.m3u8', 'https://example.test/video/media.m3u8', 'https://example.test/video/segment.ts?token=rotating']);
});
test('ended, invalid, inaccessible and empty playlists/segments are normal offline observations', async () => {
  for (const probe of [fetcher(media(1, true)), fetcher('invalid'), async () => new Response('', { status: 503 }), async () => { throw new Error('network timeout'); },
    async (input: Parameters<typeof fetch>[0]) => new Response(String(input).includes('.m3u8') ? media(1) : ''),
  ]) assert.equal((await inspectHls('https://example.test/live.m3u8', probe)).health.status, 0);
});
test('frozen progress becomes offline, resumed progress becomes online; freshness infrastructure errors remain retryable', async () => {
  let fresh = false; let unavailable = false;
  const checker = new LiveHlsHealthChecker({ async get() { return null; } }, { async observe() { if (unavailable) throw new Error('Redis down'); return fresh; }, async fail() { return false; } },
    async () => ({ health: { status: 1, latency: 7 }, progress: { fingerprint: 'one', targetDurationMs: 2000 } }));
  const identity = { serverIp: 'server', streamId: 'one' };
  assert.equal((await checker.check('https://example.test/live', identity)).status, 0);
  fresh = true; assert.equal((await checker.check('https://example.test/live', identity)).status, 1);
  unavailable = true;
  await assert.rejects(checker.check('https://example.test/live', identity), error => {
    assert.ok(error instanceof ProcessingFailure); assert.equal(error.retryable, true); assert.equal(error.bounded, false); return true;
  });
});
test('a failed fetch asks the freshness store whether to keep showing online; infrastructure errors stay retryable', async () => {
  let keepOnline = true; let failCalls = 0; let unavailable = false;
  const checker = new LiveHlsHealthChecker({ async get() { return null; } },
    { async observe() { return true; }, async fail() { failCalls++; if (unavailable) throw new Error('Redis down'); return keepOnline; } },
    async () => ({ health: { status: 0, latency: 3000 } }));
  const identity = { serverIp: 'server', streamId: 'one' };
  assert.equal((await checker.check('https://example.test/live', identity)).status, 1); // first blip: still online
  keepOnline = false;
  assert.equal((await checker.check('https://example.test/live', identity)).status, 0); // second in a row: offline
  assert.equal((await checker.check('https://example.test/live')).status, 0); // no identity: raw result, store untouched
  assert.equal(failCalls, 2);
  unavailable = true;
  await assert.rejects(checker.check('https://example.test/live', identity), error => error instanceof ProcessingFailure && error.retryable);
});
test('segment check asks for a byte range and stops after the first media bytes', async () => {
  let pulls = 0; let rangeHeader: string | null = null;
  const probe = async (input: Parameters<typeof fetch>[0], init?: RequestInit) => {
    if (String(input).includes('.m3u8')) return new Response(media(1));
    rangeHeader = new Headers(init?.headers).get('range');
    // A server that ignores Range and would stream forever: we must not read it all.
    return new Response(new ReadableStream({ pull(controller) { pulls++; controller.enqueue(new Uint8Array(64 * 1024)); } }), { status: 200 });
  };
  assert.equal((await inspectHls('https://example.test/live.m3u8', probe)).health.status, 1);
  assert.equal(rangeHeader, 'bytes=0-1023');
  assert.ok(pulls <= 3, `read ${pulls} chunks`);
});

test('probe diagnostics identify playlist-header timeouts without exposing credentials', async () => {
  const result = await inspectHls('https://user:secret@example.test/live.m3u8?token=private', async () => { throw new DOMException('secret remote message', 'TimeoutError'); });
  assert.equal(result.health.status, 0);
  assert.equal(result.failure?.stage, 'playlist-headers');
  assert.equal(result.failure?.host, 'example.test');
  assert.equal(result.failure?.reason, 'TimeoutError');
  assert.ok(!JSON.stringify(result.failure).includes('secret'));
  assert.ok(!JSON.stringify(result.failure).includes('private'));
});

test('probe diagnostics distinguish segment-body failures and HTTP response errors', async () => {
  const result = await inspectHls('https://example.test/live.m3u8', async input => {
    if (String(input).includes('.m3u8')) return new Response(media(1));
    return new Response(new ReadableStream({ start(controller) { controller.error(new DOMException('timed out', 'TimeoutError')); } }));
  });
  assert.equal(result.failure?.stage, 'segment-body');
  assert.equal(result.failure?.reason, 'TimeoutError');
  assert.equal(result.failure?.httpStatus, 200);
  const denied = await inspectHls('https://example.test/live.m3u8', async () => new Response('', { status: 503 }));
  assert.equal(denied.failure?.stage, 'playlist-headers');
  assert.equal(denied.failure?.httpStatus, 503);
});

test('a range-honoring CDN returning 1 KiB is sufficient evidence of media availability', async () => {
  let cancelled = false;
  const result = await inspectHls('https://example.test/live.m3u8', async (input, init) => {
    if (String(input).includes('.m3u8')) return new Response(media(1));
    assert.equal(new Headers(init?.headers).get('range'), 'bytes=0-1023');
    return new Response(new ReadableStream({
      start(controller) { controller.enqueue(new Uint8Array(1024)); },
      cancel() { cancelled = true; },
    }), { status: 206, headers: { 'Content-Range': 'bytes 0-1023/2000000' } });
  });
  assert.equal(result.health.status, 1);
  assert.equal(cancelled, true);
});
