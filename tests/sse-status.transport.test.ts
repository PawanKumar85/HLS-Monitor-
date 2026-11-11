import { test } from 'node:test';
import assert from 'node:assert/strict';
import Fastify from 'fastify';
import { MemoryStatusStore } from './helpers/status-event-store.js';
import { SseStatusTransport } from '../src/realtime/adapters/sse-status.transport.js';
import { parseCursor } from '../src/realtime/status-event-store.js';
test('snapshot includes observations published without connected clients', async () => {
  const store = new MemoryStatusStore();
  const transport = new SseStatusTransport(store);
  const app = Fastify();
  await transport.register(app);
  try {
    await transport.publish({ type: 'hls:status', data: { serverIp: 's', streamId: 'a', status: 1, latencyMs: 2, checkedAt: new Date().toISOString() } });
    const response = await app.inject('/api/hls/status/snapshot');
    assert.equal(response.statusCode, 200);
    assert.equal(response.headers['cache-control'], 'no-store');
    assert.equal(response.json().sequence, 1);
    assert.equal(response.json().streams.length, 1);
    assert.equal((await store.readAfter(`${store.epoch}:0`, 100)).kind, 'events');
  } finally { await app.close(); }
});
test('cursor rejects malformed and unsafe sequence numbers', () => {
  assert.equal(parseCursor('bad'), null);
  assert.equal(parseCursor('00000000-0000-0000-0000-000000000000:9007199254740992'), null);
  assert.equal(parseCursor('00000000-0000-0000-0000-000000000000:0')?.sequence, 0);
});

test('failed snapshot returns 503 without exposing internal errors', async () => {
  const store = new MemoryStatusStore();
  store.snapshot = async () => { throw new Error('private connection details'); };
  const app = Fastify(); await new SseStatusTransport(store).register(app);
  try {
    const response = await app.inject('/api/hls/status/snapshot');
    assert.equal(response.statusCode, 503);
    assert.doesNotMatch(response.body, /private/);
  } finally { await app.close(); }
});

test('publication backlog stays bounded independently of persistence', async () => {
  const store = new MemoryStatusStore();
  let release!: () => void;
  const pending = new Promise<void>(resolve => { release = resolve; });
  store.append = () => pending;
  const transport = new SseStatusTransport(store);
  const event = { type: 'hls:status' as const, data: { serverIp: 's', streamId: 'a', status: 1 as const, latencyMs: 2, checkedAt: new Date().toISOString() } };
  const calls = Array.from({ length: 100 }, () => transport.publish(event));
  await assert.rejects(transport.publish(event), /busy/);
  release(); await Promise.all(calls);
  await transport.publish(event);
});

// Exercise independent response lifecycles without relying on OS socket buffers.
async function fakeApp(transport: SseStatusTransport) {
  const routes = new Map<string, (request: unknown, reply: unknown) => unknown>();
  let cleanup = async () => {};
  await transport.register({
    get(path: string, handler: (request: unknown, reply: unknown) => unknown) { routes.set(path, handler); },
    addHook(_name: string, handler: () => Promise<void>) { cleanup = handler; },
  } as unknown as import('fastify').FastifyInstance);
  return {
    connect(after: string, response: import('./helpers/sse-response.js').Response) {
      const reply = { raw: response, hijack() {}, getHeaders: () => ({}) };
      routes.get('/events/hls-status')!({ headers: {}, query: { after } }, reply);
    },
    cleanup: () => cleanup(),
  };
}
const eventually = async (check: () => boolean, timeoutMs = 1000) => {
  const deadline = Date.now() + timeoutMs;
  while (!check()) { if (Date.now() > deadline) throw new Error('Condition timed out'); await new Promise(resolve => setTimeout(resolve, 2)); }
};

test('heartbeat overlap and slow response do not gate another client or another app registration', async () => {
  const { Response } = await import('./helpers/sse-response.js');
  const store = new MemoryStatusStore();
  const transport = new SseStatusTransport(store, { pollMs: 5, heartbeatMs: 5, drainTimeoutMs: 500 });
  const firstApp = await fakeApp(transport); const secondApp = await fakeApp(transport);
  const cursor = (await store.snapshot()).cursor;
  const slow = new Response(); slow.block = true;
  const fast = new Response(); const other = new Response();
  try {
    firstApp.connect(cursor, slow); firstApp.connect(cursor, fast); secondApp.connect(cursor, other);
    await transport.publish({ type: 'hls:status', data: { serverIp: 's', streamId: 'one', status: 1, latencyMs: 3, checkedAt: new Date().toISOString() } });
    await eventually(() => fast.frames.join('').includes('"sequence":1') && other.frames.join('').includes('"sequence":1'));
    await eventually(() => fast.frames.join('').includes('hls:heartbeat'));
    assert.equal(slow.destroyed, false);
    assert.equal(slow.frames.length, 1); // Still blocked on the initial greeting.
    slow.drain();
    await eventually(() => slow.frames.join('').includes('"sequence":1'));
    assert.equal(slow.frames.filter(frame => frame.includes('"sequence":1')).length, 1);
    await firstApp.cleanup(); assert.equal(slow.destroyed, true); assert.equal(fast.destroyed, true); assert.equal(other.destroyed, false);
  } finally { await firstApp.cleanup(); await secondApp.cleanup(); }
});

test('simultaneous reconnects drain multiple replay batches promptly and idle feeds do not read Redis', async () => {
  const { Response } = await import('./helpers/sse-response.js');
  const store = new MemoryStatusStore();
  let reads = 0;
  const read = store.readAfter.bind(store);
  store.readAfter = (...args) => { reads++; return read(...args); };
  const transport = new SseStatusTransport(store, { pollMs: 500, heartbeatMs: 1000 });
  const app = await fakeApp(transport);
  const cursor = (await store.snapshot()).cursor;
  for (let i = 0; i < 250; i++) await store.append({ type: 'hls:status', data: { serverIp: 's', streamId: 'one', status: 1, latencyMs: i, checkedAt: new Date().toISOString() } });
  const responses = Array.from({ length: 8 }, () => new Response());
  try {
    assert.equal(reads, 0);
    for (const response of responses) app.connect(cursor, response);
    // Old pump needed a 500ms tick per batch. Catch-up now yields and continues.
    await eventually(() => responses.every(response => response.frames.some(frame => frame.includes('hls:ready'))), 400);
    for (const response of responses) {
      assert.equal(response.frames.filter(frame => frame.startsWith('id:')).length, 250);
      response.destroy();
    }
    await new Promise(resolve => setImmediate(resolve)); const stoppedReads = reads;
    await new Promise(resolve => setTimeout(resolve, 550)); assert.equal(reads, stoppedReads);
  } finally { await app.cleanup(); }
});

test('continuously growing replay backlog requests snapshot rather than staying behind forever', async () => {
  const { Response } = await import('./helpers/sse-response.js');
  const store = new MemoryStatusStore();
  const cursor = (await store.snapshot()).cursor;
  let sequence = 0;
  store.readAfter = async () => {
    await new Promise(resolve => setTimeout(resolve, 5));
    sequence++;
    return { kind: 'events', head: `${store.epoch}:${sequence + 1}`, events: [{ type: 'hls:status', epoch: store.epoch, sequence, cursor: `${store.epoch}:${sequence}`, data: { serverIp: 's', streamId: 'one', status: 1, latencyMs: 1, checkedAt: new Date().toISOString() } }] };
  };
  const app = await fakeApp(new SseStatusTransport(store, { pollMs: 5, heartbeatMs: 5, maxReplayMs: 15 }));
  const response = new Response();
  try {
    app.connect(cursor, response);
    await eventually(() => response.writableEnded);
    assert.match(response.frames.join(''), /replay_timeout/);
    assert.doesNotMatch(response.frames.join(''), /hls:ready/);
  } finally { await app.cleanup(); }
});

test('application authentication hooks apply to both snapshot and SSE before hijack', async () => {
  const app = Fastify();
  app.addHook('preHandler', async (_request, reply) => reply.code(401).send({ error: 'Unauthorized' }));
  await new SseStatusTransport(new MemoryStatusStore()).register(app);
  try {
    for (const path of ['/api/hls/status/snapshot', '/events/hls-status']) {
      const result = await app.inject(path); assert.equal(result.statusCode, 401);
      assert.doesNotMatch(result.headers['content-type'] ?? '', /text\/event-stream/);
    }
  } finally { await app.close(); }
});

test('shutdown destroys a resync response still waiting to finish on the network', async () => {
  const { Response } = await import('./helpers/sse-response.js');
  class PendingEndResponse extends Response {
    override end() { this.writableEnded = true; } // Accepted locally, peer has not closed.
  }
  const response = new PendingEndResponse();
  const app = await fakeApp(new SseStatusTransport(new MemoryStatusStore()));
  try {
    app.connect('invalid', response);
    await eventually(() => response.writableEnded);
    assert.equal(response.destroyed, false);
    await app.cleanup(); assert.equal(response.destroyed, true);
  } finally { await app.cleanup(); }
});

test('hung snapshot returns 503 rather than leaving an HTTP request pending', async () => {
  const store = new MemoryStatusStore(); store.snapshot = () => new Promise(() => {});
  const app = Fastify(); await new SseStatusTransport(store, { storeTimeoutMs: 15 }).register(app);
  try {
    const result = await app.inject('/api/hls/status/snapshot');
    assert.equal(result.statusCode, 503); assert.match(result.body, /unavailable/);
  } finally { await app.close(); }
});

test('hung replay closes its client and a late result cannot send frames', async () => {
  const { Response } = await import('./helpers/sse-response.js');
  const store = new MemoryStatusStore();
  const snapshot = await store.snapshot();
  let finish!: (value: Awaited<ReturnType<MemoryStatusStore['readAfter']>>) => void;
  store.readAfter = () => new Promise(resolve => { finish = resolve; });
  const app = await fakeApp(new SseStatusTransport(store, { storeTimeoutMs: 15 }));
  const response = new Response();
  try {
    app.connect(snapshot.cursor, response);
    await eventually(() => response.destroyed);
    const count = response.frames.length;
    finish({ kind: 'events', events: [], head: snapshot.cursor });
    await new Promise(resolve => setImmediate(resolve)); assert.equal(response.frames.length, count);
  } finally { await app.cleanup(); }
});

test('shutdown cancels a hung replay wait without awaiting store completion', { timeout: 1000 }, async () => {
  const { Response } = await import('./helpers/sse-response.js');
  const store = new MemoryStatusStore(); const snapshot = await store.snapshot();
  let fail!: (error: Error) => void;
  store.readAfter = () => new Promise((_, reject) => { fail = reject; });
  const app = await fakeApp(new SseStatusTransport(store, { storeTimeoutMs: 60_000, shutdownTimeoutMs: 15 }));
  const response = new Response(); app.connect(snapshot.cursor, response);
  await app.cleanup(); assert.equal(response.destroyed, true);
  fail(new Error('late store rejection')); await new Promise(resolve => setImmediate(resolve));
});

test('publication timeout retains the outstanding append slot until the store really settles', async () => {
  const store = new MemoryStatusStore(); let release!: () => void;
  const pending = new Promise<void>(resolve => { release = resolve; });
  store.append = () => pending;
  const transport = new SseStatusTransport(store, { storeTimeoutMs: 15 });
  const event = { type: 'hls:status' as const, data: { serverIp: 's', streamId: 'one', status: 1 as const, latencyMs: 1, checkedAt: new Date().toISOString() } };
  const results = await Promise.allSettled(Array.from({ length: 100 }, () => transport.publish(event)));
  assert.ok(results.every(result => result.status === 'rejected'));
  await assert.rejects(transport.publish(event), /busy/);
  release(); await new Promise(resolve => setImmediate(resolve));
  await transport.publish(event);
});
