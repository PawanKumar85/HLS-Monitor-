import { test } from 'node:test';
import assert from 'node:assert/strict';
import Fastify from 'fastify';
import { registerFailureEvents } from '../src/http/failure-events.js';
import { registerStreamRoutes } from '../src/http/stream-routes.js';
import { parseFailureCursor, type FailureEvent, type FailureFeed, type FailureView } from '../src/dlq/failure-feed.js';
import { StreamService } from '../src/inventory/stream-service.js';
import { MemoryInventory } from './helpers/stream-inventory.js';
import { parseSseFrame, startHlsFailureClient, type HlsFailure } from '../frontend/src/lib/hls-failure-client.js';

const failure = (n: number): FailureView => ({ failureId: `f${n}`, serverIp: '192.168.1.10', originalMessageId: `${n}-0`, failureStage: 'HEALTH_CHECK',
  errorCode: 'TIMEOUT', errorMessage: 'Playlist timed out', attempts: 5, firstFailedAt: '2026-11-09T10:00:00.000Z', lastFailedAt: '2026-11-09T10:00:05.000Z',
  updatedAt: '2026-11-09T10:00:05.000Z', retryable: true, revision: n, resolution: 'OPEN', streamId: 'hmtv' });
const cursor = (n: number) => `${1_794_000_000_000 + n}-${n.toString(16).padStart(24, '0')}`;

/** In-memory feed: returns everything after the cursor, or the newest `limit` when there is none. */
function memoryFeed(events: FailureEvent[] = []) {
  const calls: (string | undefined)[] = [];
  const feed: FailureFeed = {
    async readAfter(after, limit) {
      calls.push(after);
      const start = after ? events.findIndex(e => e.cursor === after) + 1 : Math.max(0, events.length - limit);
      return events.slice(start, start + limit);
    },
  };
  return { feed, calls, events };
}

async function readUntil(body: ReadableStream<Uint8Array>, done: (text: string) => boolean) {
  const reader = body.pipeThrough(new TextDecoderStream()).getReader();
  let text = '';
  while (!done(text)) { const { value, done: end } = await reader.read(); if (end) break; text += value; }
  await reader.cancel();
  return text;
}

test('failure feed rejects a malformed cursor', async () => {
  const { feed } = memoryFeed();
  const app = Fastify();
  registerStreamRoutes(app, { service: new StreamService(new MemoryInventory()), failures: feed });
  try {
    const bad = await app.inject({ url: '/events/hls-failures', headers: { 'last-event-id': 'nope' } });
    assert.equal(bad.statusCode, 400);
  } finally { await app.close(); }
});

test('failure feed sends the newest failures, then live ones from its cursor, and resumes from Last-Event-ID', async () => {
  const { feed, calls, events } = memoryFeed([{ cursor: cursor(1), failure: failure(1) }]);
  const app = Fastify();
  registerFailureEvents(app, feed, { pollMs: 20, heartbeatMs: 60_000 });
  await app.listen({ port: 0, host: '127.0.0.1' });
  const base = `http://127.0.0.1:${(app.server.address() as import('node:net').AddressInfo).port}`;
  try {
    const first = await fetch(`${base}/events/hls-failures`);
    assert.equal(first.headers.get('content-type'), 'text/event-stream; charset=utf-8');
    setTimeout(() => events.push({ cursor: cursor(2), failure: failure(2) }), 50);
    const text = await readUntil(first.body!, t => t.includes('"f2"'));
    assert.match(text, new RegExp(`id: ${cursor(1)}\nevent: hls:failure\n`));
    assert.ok(text.indexOf('"f1"') < text.indexOf('"f2"'));
    assert.equal(calls[0], undefined); assert.ok(calls.includes(cursor(1)));

    const resumed = await fetch(`${base}/events/hls-failures`, { headers: { 'Last-Event-ID': cursor(1) } });
    const again = await readUntil(resumed.body!, t => t.includes('"f2"'));
    assert.ok(!again.includes('"f1"'));
  } finally { await app.close(); }
});

test('cursor format round-trips the stored projectedAt and ObjectId', () => {
  const parsed = parseFailureCursor(cursor(7));
  assert.equal(parsed?.at.getTime(), 1_794_000_000_007);
  assert.equal(parsed?.id, '7'.padStart(24, '0'));
  assert.equal(parseFailureCursor('1-xyz'), null);
});

test('browser client parses frames, resumes with Last-Event-ID after a drop, and stops on 401', async () => {
  assert.deepEqual(parseSseFrame(': connected'), { id: '', event: 'message', data: '' });
  const sent: (string | null)[] = [];
  const received: HlsFailure[] = [];
  let calls = 0;
  const stream = (text: string) => new Response(new ReadableStream({ start(c) { c.enqueue(new TextEncoder().encode(text)); c.close(); } }), { status: 200 });
  const fakeFetch = (async (_url: string, init: RequestInit) => {
    sent.push(new Headers(init.headers).get('last-event-id'));
    calls++;
    if (calls === 1) return stream(`retry: 3000\n: connected\n\nid: ${cursor(1)}\nevent: hls:failure\ndata: ${JSON.stringify(failure(1))}\n\n`);
    if (calls === 2) return stream(`id: ${cursor(2)}\nevent: hls:failure\ndata: ${JSON.stringify(failure(2))}\n\nevent: hls:heartbeat\ndata: 1\n\n`);
    return new Response('{}', { status: 401 });
  }) as typeof fetch;
  let unauthorized = false;
  const stop = startHlsFailureClient({ url: 'http://x/events/hls-failures', onFailure: f => received.push(f), onConnected() {}, onUnauthorized: () => { unauthorized = true; } }, fakeFetch, 5);
  try {
    const deadline = Date.now() + 2000;
    while (!unauthorized && Date.now() < deadline) await new Promise(r => setTimeout(r, 5));
    assert.deepEqual(received.map(f => f.failureId), ['f1', 'f2']);
    assert.deepEqual(sent, [null, cursor(1), cursor(2)]);
    assert.ok(unauthorized);
    await new Promise(r => setTimeout(r, 30));
    assert.equal(calls, 3); // no retry after 401
  } finally { stop(); }
});
