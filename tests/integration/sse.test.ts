import { test } from 'node:test';
import assert from 'node:assert/strict';
import { get } from 'node:http';
import { once } from 'node:events';
import { randomUUID } from 'node:crypto';
import { Redis } from 'ioredis';
import Fastify from 'fastify';
import { RedisStatusEventStore } from '../../src/realtime/adapters/redis-status-event.store.js';
import { SseStatusTransport } from '../../src/realtime/adapters/sse-status.transport.js';

test('durable SSE snapshot handoff, replay, expired cursor reset and shutdown', { skip: process.env['RUN_INTEGRATION'] !== '1', timeout: 15000 }, async () => {
  const redis = new Redis({ host: '127.0.0.1', port: Number(process.env['REDIS_PORT'] ?? 6379), password: process.env['REDIS_PASSWORD'] ?? 'admin' });
  const store = new RedisStatusEventStore(redis, `test-${randomUUID()}`, 3);
  const app = Fastify();
  const transport = new SseStatusTransport(store);
  const requests: ReturnType<typeof get>[] = [];
  const event = (n: number) => ({ type: 'hls:status' as const, data: { streamId: 'one', serverIp: 'server', status: 1 as const, latencyMs: n, checkedAt: new Date(1700000000000 + n).toISOString() } });
  async function open(after: string, header?: string) {
    const request = get(`${base}/events/hls-status?after=${encodeURIComponent(after)}`, { headers: header ? { 'Last-Event-ID': header } : {} });
    requests.push(request);
    const [response] = await once(request, 'response');
    let text = '';
    response.setEncoding('utf8'); response.on('data', (chunk: string) => { text += chunk; }); response.on('error', () => {});
    return { response, text: () => text, wait: async (part: string) => {
      const deadline = Date.now() + 4000;
      while (!text.includes(part)) { if (Date.now() > deadline) throw new Error(`Missing ${part}: ${text}`); await new Promise(r => setTimeout(r, 10)); }
    } };
  }
  await transport.register(app);
  await app.listen({ host: '127.0.0.1', port: 0 });
  const address = app.server.address(); assert.ok(address && typeof address !== 'string');
  const base = `http://127.0.0.1:${address.port}`;
  try {
    const noCursor = await open('');
    await noCursor.wait('event: hls:resync');
    await transport.publish(event(1));
    const snapshot = (await app.inject('/api/hls/status/snapshot')).json();
    assert.equal(snapshot.streams[0].latencyMs, 1);
    await transport.publish(event(2));
    const live = await open(snapshot.cursor);
    await live.wait('event: hls:ready');
    assert.match(live.text(), /"sequence":2/);
    assert.doesNotMatch(live.text(), /"sequence":1/);
    await transport.publish(event(3)); await live.wait('"sequence":3');
    live.response.destroy();
    const resumed = await open(snapshot.cursor, `${snapshot.epoch}:2`);
    await resumed.wait('event: hls:ready');
    assert.match(resumed.text(), /"sequence":3/); assert.doesNotMatch(resumed.text(), /"sequence":2/);
    await transport.publish(event(4)); await transport.publish(event(5));
    const expired = await open(`${snapshot.epoch}:1`); await expired.wait('event: hls:resync');
    assert.deepEqual(await store.readAfter(`${randomUUID()}:0`, 100), { kind: 'reset' });
    const latest = await store.snapshot(); assert.equal(latest.sequence, 5); assert.equal(latest.streams[0]?.latencyMs, 5);
    await transport.publish(event(0)); assert.equal((await store.snapshot()).streams[0]?.latencyMs, 5);
    assert.equal(await redis.xlen(store.keys[2]!), 3);
    // New adapter/process can replay the same shared durable log.
    const other = new RedisStatusEventStore(redis, store.keys[0]!.split('{')[1]!.split('}')[0]!);
    const replay = await other.readAfter(`${snapshot.epoch}:4`, 100);
    assert.equal(replay.kind, 'events');
    if (replay.kind === 'events') assert.deepEqual(replay.events.map(e => e.sequence), [5, 6]);
    await resumed.wait('\"sequence\":6');
    await other.append(event(7));
    await resumed.wait('\"sequence\":7');
    await redis.del(...store.keys);
    const resetSnapshot = await store.snapshot();
    assert.notEqual(resetSnapshot.epoch, snapshot.epoch);
    assert.equal(resetSnapshot.sequence, 0);
    const oldEpoch = await open(`${snapshot.epoch}:7`); await oldEpoch.wait('event: hls:resync');
    const fresh = await open(resetSnapshot.cursor); await fresh.wait('event: hls:ready');
    const closed = new Promise<void>(resolve => fresh.response.once('close', resolve));
    await app.close(); await closed; assert.equal(fresh.response.destroyed, true);
  } finally {
    for (const request of requests) request.destroy();
    await app.close(); await redis.del(...store.keys); await redis.quit();
  }
});

test('real Redis status deadline rejects a blocked connection and recovers after its late command completes', { skip: process.env['RUN_INTEGRATION'] !== '1', timeout: 5000 }, async () => {
  const redis = new Redis({ host: '127.0.0.1', port: Number(process.env['REDIS_PORT'] ?? 6379), password: process.env['REDIS_PASSWORD'] ?? 'admin' });
  const blocked = redis.duplicate();
  const key = `status-timeout-blocker:${randomUUID()}`;
  const store = new RedisStatusEventStore(blocked, `timeout-${randomUUID()}`, 10, 10, 20);
  let blocker: Promise<unknown> | undefined;
  try {
    await blocked.ping();
    // Fault injection affects only this test connection, not the shared Redis server.
    blocker = blocked.blpop(key, 0);
    await assert.rejects(store.snapshot(), /Redis status command timed out/);
    await redis.lpush(key, 'release'); await blocker;
    const snapshot = await store.snapshot();
    assert.equal(snapshot.sequence, 0); assert.deepEqual(snapshot.streams, []);
  } finally {
    // Always release the intentionally blocked client before deleting its exact keys.
    if (blocker) { await redis.lpush(key, 'release'); await blocker; }
    await redis.del(key, ...store.keys);
    blocked.disconnect(); await redis.quit();
  }
});
