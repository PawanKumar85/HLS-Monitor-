import { test } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { Redis } from 'ioredis';
import { buildApp } from '../../src/app.js';
import { hashPassword } from '../../src/auth/password.js';
import { RedisSessionStore } from '../../src/auth/redis-session-store.js';
import type { User, UserStore } from '../../src/auth/users.js';
import { RedisLoginLimiter } from '../../src/http/auth.js';
import { StreamService } from '../../src/inventory/stream-service.js';
import { MemoryInventory } from '../helpers/stream-inventory.js';
const enabled = process.env['RUN_INTEGRATION'] === '1';
const client = () => new Redis({ host: process.env['REDIS_HOST'] ?? '127.0.0.1', port: Number(process.env['REDIS_PORT'] ?? 6379), password: process.env['REDIS_PASSWORD'] ?? 'admin' });
const call = <T>(fn: (cb: (error?: unknown, value?: T) => void) => void) => new Promise<T>((resolve, reject) => fn((error, value) => error ? reject(error) : resolve(value as T)));

test('Redis session store: compare-and-set only writes the generation that was read', { skip: !enabled, timeout: 10000 }, async t => {
  const redis = client(); const prefix = `gate:test-session:${randomUUID()}:`;
  t.after(async () => { const keys = await redis.keys(`${prefix}*`); if (keys.length) await redis.del(...keys); await redis.quit(); });
  const store = new RedisSessionStore(redis, prefix);
  const session = (generation: string, value: number) => ({ value, __fastifySessionMetadata: { generation }, cookie: { originalMaxAge: 60_000 } });
  await call(cb => store.set('a', session('a', 1), cb));
  assert.equal((await call<{ value: number }>(cb => store.get('a', cb))).value, 1);
  await call(cb => store.compareAndSet('a', 'a', session('a', 2), cb));
  assert.equal((await call<{ value: number }>(cb => store.get('a', cb))).value, 2);
  await assert.rejects(call(cb => store.compareAndSet('a', 'stale', session('a', 3), cb)), (e: { code?: string }) => e.code === 'ENOENT');
  await assert.rejects(call(cb => store.compareAndSet('missing', 'missing', session('missing', 1), cb)), (e: { code?: string }) => e.code === 'ENOENT');
  assert.ok(await redis.pttl(`${prefix}a`) > 50_000);
  await call(cb => store.destroy('a', cb));
  assert.equal(await call(cb => store.get('a', cb)), null);
});

test('real app with Redis sessions: sign in, stay signed in across requests, sign out', { skip: !enabled, timeout: 15000 }, async t => {
  const redis = client(); const prefix = `gate:test-session:${randomUUID()}:`;
  const now = new Date();
  const user: User = { id: randomUUID(), username: 'ops', disabled: false, passwordHash: await hashPassword('correct horse battery'), createdAt: now, updatedAt: now };
  const users: UserStore = { async findByUsername(u) { return u.toLowerCase() === 'ops' ? user : null; }, async findById(id) { return id === user.id ? user : null; } };
  const app = await buildApp({
    dbManager: { async healthCheck() { return { redis: true, storage: true, storageProvider: 'test' }; }, async disconnectAll() {} },
    hlsConsumer: { async stop() {} }, batchQueueManager: { async stopAll() {} }, statusTransport: { async register() {} },
    streams: { service: new StreamService(new MemoryInventory()) },
    auth: { users, sessionStore: new RedisSessionStore(redis, prefix), secret: 'integration-session-secret-0123456789abcdef', limiter: new RedisLoginLimiter(redis) },
  });
  t.after(async () => { await app.close(); const keys = await redis.keys(`${prefix}*`); if (keys.length) await redis.del(...keys); await redis.del(`gate:login-fail:127.0.0.1:ops`); await redis.quit(); });
  const login = await app.inject({ method: 'POST', url: '/login', headers: { 'content-type': 'application/x-www-form-urlencoded' }, payload: 'username=ops&password=correct+horse+battery&next=%2F' });
  assert.equal(login.statusCode, 303); assert.equal(login.headers.location, '/');
  const cookie = String(login.headers['set-cookie']).split(';')[0]!;
  assert.equal((await redis.keys(`${prefix}*`)).length, 1);
  for (let i = 0; i < 3; i++) assert.equal((await app.inject({ url: '/api/me', headers: { cookie } })).json().username, 'ops'); // rolling saves use compareAndSet
  assert.equal((await app.inject({ method: 'POST', url: '/api/streams', headers: { cookie }, payload: { serverIp: '10.0.0.1', streamId: 'one', streamUrl: 'http://10.0.0.1/live.m3u8' } })).statusCode, 201);
  assert.equal((await app.inject({ method: 'POST', url: '/logout', headers: { cookie } })).statusCode, 303);
  assert.equal((await app.inject({ url: '/api/me', headers: { cookie } })).statusCode, 401);
  assert.equal((await redis.keys(`${prefix}*`)).length, 0);
});
