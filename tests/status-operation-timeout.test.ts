import { test } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import type { Redis } from 'ioredis';
import { RedisStatusEventStore } from '../src/realtime/adapters/redis-status-event.store.js';
import { withOperationTimeout } from '../src/realtime/operation-timeout.js';

const epoch = randomUUID();
const event = { type: 'hls:status' as const, data: { serverIp: 's', streamId: 'one', status: 1 as const, latencyMs: 1, checkedAt: new Date().toISOString() } };
test('all Redis status operations reject hung commands within their deadline', async () => {
  const redis = { eval: () => new Promise(() => {}) } as unknown as Redis;
  const store = new RedisStatusEventStore(redis, 'timeout-test', 10, 10, 10);
  const results = await Promise.allSettled([store.append(event), store.snapshot(), store.readAfter(`${epoch}:0`, 100)]);
  assert.ok(results.every(result => result.status === 'rejected' && /timed out/.test(String(result.reason))));
});

test('timed-out driver commands retain capacity slots until settling; late errors are consumed', async () => {
  let calls = 0; let healthy = false;
  const reject: ((error: Error) => void)[] = [];
  const redis = { eval: () => {
    calls++;
    return healthy ? Promise.resolve([epoch, '0', []]) : new Promise((_, fail) => { reject.push(fail); });
  } } as unknown as Redis;
  const store = new RedisStatusEventStore(redis, 'bounded-timeout-test', 10, 10, 15);
  const results = await Promise.allSettled(Array.from({ length: 100 }, () => store.snapshot()));
  assert.ok(results.every(result => result.status === 'rejected'));
  assert.equal(calls, 100);
  await assert.rejects(store.snapshot(), /capacity exceeded/); assert.equal(calls, 100);
  for (const fail of reject) fail(new Error('late connection failure'));
  await new Promise(resolve => setImmediate(resolve)); healthy = true;
  assert.equal((await store.snapshot()).cursor, `${epoch}:0`);
});

test('deadline wait aborts immediately and safely consumes later rejection', async () => {
  const controller = new AbortController();
  let fail!: (error: Error) => void;
  const operation = new Promise<void>((_, reject) => { fail = reject; });
  const result = withOperationTimeout(operation, 1000, 'Test operation', controller.signal);
  controller.abort();
  await assert.rejects(result, /cancelled/);
  fail(new Error('late failure'));
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(await withOperationTimeout(Promise.resolve('ok'), 1000, 'Test operation'), 'ok');
});
