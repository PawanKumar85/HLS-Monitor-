import { test } from 'node:test';
import assert from 'node:assert/strict';
import { buildApp } from '../src/app.js';
import { StreamService } from '../src/inventory/stream-service.js';
import { MemoryInventory } from './helpers/stream-inventory.js';
import Fastify from 'fastify';
import { mkdtemp, mkdir, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { registerDashboard } from '../src/http/dashboard.js';
// Sign-in and roles are tested in auth.test.ts; these tests exercise the stream routes behind it.
const headers = {};
const payload = { serverIp: 'server', streamId: 'one', streamUrl: 'https://user:password@example.test/live?token=private' };
async function fixture(withApi = true) {
  const repository = new MemoryInventory(); const service = new StreamService(repository);
  const app = await buildApp({ dbManager: { async healthCheck() { return { redis: true, storage: true, storageProvider: 'test' }; }, async disconnectAll() {} },
    hlsConsumer: { async stop() {} }, batchQueueManager: { async stopAll() {} }, statusTransport: { async register() {} }, auth: 'disabled',
    ...(withApi ? { streams: { service } } : {}),
  });
  return { app, repository };
}
test('stream routes are never cached and are absent when not configured', async () => {
  const { app } = await fixture();
  try {
    const result = await app.inject('/api/streams');
    assert.equal(result.statusCode, 200); assert.equal(result.headers['cache-control'], 'no-store');
  } finally { await app.close(); }
  const disabled = await fixture(false);
  try { assert.equal((await disabled.app.inject('/api/streams')).statusCode, 404); } finally { await disabled.app.close(); }
});
test('POST persists pending registration; GET redacts URLs; PATCH and DELETE enforce version preconditions', async () => {
  const { app, repository } = await fixture();
  try {
    const created = await app.inject({ method: 'POST', url: '/api/streams', headers, payload });
    assert.equal(created.statusCode, 201); assert.equal(created.json().pendingSync, true);
    assert.equal(created.json().streamUrl, 'https://example.test/live'); assert.equal(created.json().streamUrlRedacted, true);
    assert.ok(!created.body.includes('password') && !created.body.includes('private'));
    const id = created.json().id; assert.equal(created.headers.etag, '"1"');
    assert.equal((await repository.get(id))?.config.streamUrl, payload.streamUrl);
    assert.equal((await app.inject({ method: 'POST', url: '/api/streams', headers, payload })).statusCode, 409);
    assert.equal((await app.inject({ method: 'GET', url: `/api/streams/${id}`, headers })).statusCode, 200);
    const missing = await app.inject({ method: 'PATCH', url: `/api/streams/${id}`, headers, payload: { intervalMs: 2000 } }); assert.equal(missing.statusCode, 428);
    const patched = await app.inject({ method: 'PATCH', url: `/api/streams/${id}`, headers: { ...headers, 'if-match': '"1"' }, payload: { intervalMs: 2000 } });
    assert.equal(patched.statusCode, 200); assert.equal(patched.json().version, 2);
    assert.equal((await repository.get(id))?.config.streamUrl, payload.streamUrl);
    assert.equal((await app.inject({ method: 'DELETE', url: `/api/streams/${id}`, headers: { ...headers, 'if-match': '1' } })).statusCode, 412);
    const deleted = await app.inject({ method: 'DELETE', url: `/api/streams/${id}`, headers: { ...headers, 'if-match': '2' } });
    assert.equal(deleted.statusCode, 202); assert.equal(deleted.json().version, 3);
    assert.equal((await repository.get(id))?.deleted, true); assert.equal((await repository.get(id))?.pendingSync, true);
    assert.equal((await app.inject({ method: 'GET', url: `/api/streams/${id}`, headers })).statusCode, 404);
    assert.deepEqual((await app.inject({ method: 'GET', url: '/api/streams', headers })).json().items, []);
    const recreated = await app.inject({ method: 'POST', url: '/api/streams', headers, payload });
    assert.equal(recreated.statusCode, 201); assert.equal(recreated.json().id, id); assert.equal(recreated.json().version, 4);
  } finally { await app.close(); }
});
test('invalid and immutable fields are rejected; list pagination is bounded', async () => {
  const { app } = await fixture();
  try {
    for (const value of [{ ...payload, streamUrl: 'file:///tmp/x' }, { ...payload, intervalMs: 0 }, { ...payload, version: 99 }]) assert.equal((await app.inject({ method: 'POST', url: '/api/streams', headers, payload: value })).statusCode, 400);
    const created = await app.inject({ method: 'POST', url: '/api/streams', headers, payload }); const id = created.json().id;
    assert.equal((await app.inject({ method: 'PATCH', url: `/api/streams/${id}`, headers: { ...headers, 'if-match': '1' }, payload: { serverIp: 'other', enabled: false } })).statusCode, 400);
    for (let i = 0; i < 2; i++) await app.inject({ method: 'POST', url: '/api/streams', headers, payload: { ...payload, streamId: `other-${i}` } });
    const first = (await app.inject({ method: 'GET', url: '/api/streams?limit=2', headers })).json();
    assert.equal(first.items.length, 2); assert.ok(first.nextCursor);
    const next = (await app.inject({ method: 'GET', url: `/api/streams?limit=2&cursor=${first.nextCursor}`, headers })).json();
    assert.equal(next.items.length, 1); assert.equal(new Set([...first.items, ...next.items].map(r => r.id)).size, 3);
    assert.equal((await app.inject({ method: 'GET', url: '/api/streams?limit=101', headers })).statusCode, 400);
  } finally { await app.close(); }
});
test('DB failure is a sanitized 503, never a successful registration', async () => {
  const { app, repository } = await fixture();
  repository.create = async () => { throw new Error('secret DB details'); };
  try {
    const result = await app.inject({ method: 'POST', url: '/api/streams', headers, payload });
    assert.equal(result.statusCode, 503); assert.ok(!result.body.includes('secret'));
  } finally { await app.close(); }
});
test('malformed or empty JSON is a 400 client error, not an inventory outage', async () => {
  const { app, repository } = await fixture();
  try {
    for (const body of ['{bad', '']) {
      const result = await app.inject({ method: 'POST', url: '/api/streams', headers: { ...headers, 'content-type': 'application/json' }, payload: body });
      assert.equal(result.statusCode, 400);
    }
    assert.equal(repository.records.size, 0);
  } finally { await app.close(); }
});
test('dashboard serves the Vite build: index with a self-only script CSP, hashed assets cached, 503 before a build', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'gate-dist-'));
  await mkdir(join(dir, 'assets'));
  await writeFile(join(dir, 'index.html'), '<!doctype html><title>GATE HLS monitor</title><script type="module" src="/assets/app-x1.js"></script>');
  await writeFile(join(dir, 'assets', 'app-x1.js'), 'console.log(1)');
  const app = Fastify(); await registerDashboard(app, pathToFileURL(`${dir}/`));
  const empty = Fastify(); await registerDashboard(empty, pathToFileURL(`${join(dir, 'missing')}/`));
  try {
    const page = await app.inject('/');
    assert.equal(page.statusCode, 200);
    assert.match(page.body, /<title>GATE HLS monitor<\/title>/);
    const csp = page.headers['content-security-policy'] as string;
    assert.match(csp, /script-src 'self'(;|$)/);
    assert.doesNotMatch(csp, /script-src[^;]*unsafe-inline/);
    assert.match(csp, /connect-src 'self'/);
    assert.equal(page.headers['referrer-policy'], 'same-origin');
    const asset = await app.inject('/assets/app-x1.js');
    assert.equal(asset.statusCode, 200); assert.match(String(asset.headers['cache-control']), /immutable/);
    assert.equal((await empty.inject('/')).statusCode, 503);
  } finally { await app.close(); await empty.close(); await rm(dir, { recursive: true }); }
});

test('POST without serverIp takes the origin server from the stream URL host', async () => {
  const { app } = await fixture();
  try {
    const created = await app.inject({ method: 'POST', url: '/api/streams', headers, payload: { streamId: 'hmtv', streamUrl: 'http://192.168.1.10:8080/live/index.m3u8?token=x' } });
    assert.equal(created.statusCode, 201); assert.equal(created.json().serverIp, '192.168.1.10');
    const explicit = await app.inject({ method: 'POST', url: '/api/streams', headers, payload: { serverIp: 'origin-a', streamId: 'cdn', streamUrl: 'https://cdn.example/live.m3u8' } });
    assert.equal(explicit.json().serverIp, 'origin-a'); // an explicit value still wins
    const bad = await app.inject({ method: 'POST', url: '/api/streams', headers, payload: { streamId: 'x', streamUrl: 'not a url' } });
    assert.equal(bad.statusCode, 400);
  } finally { await app.close(); }
});
