import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import vm from 'node:vm';
import { buildApp } from '../src/app.js';

test('manifest and install icons use correct MIME types, scope and image dimensions', async t => {
  const app = await buildApp({ auth: 'disabled', dbManager: { healthCheck: async () => ({ redis: true, storage: true, storageProvider: 'test' }), disconnectAll: async () => {} }, hlsConsumer: { stop: async () => {} }, batchQueueManager: { stopAll: async () => {} }, statusTransport: { register: async () => {} } });
  t.after(() => app.close());
  const response = await app.inject('/manifest.webmanifest');
  assert.equal(response.statusCode, 200); assert.match(String(response.headers['content-type']), /application\/manifest\+json/);
  const manifest = response.json(); assert.equal(manifest.display, 'standalone'); assert.equal(manifest.scope, '/'); assert.equal(manifest.start_url, '/');
  for (const icon of manifest.icons) {
    const asset = await app.inject(icon.src); assert.equal(asset.statusCode, 200); assert.match(String(asset.headers['content-type']), /image\/png/);
    const size = Number(icon.sizes.split('x')[0]); assert.equal(asset.rawPayload.readUInt32BE(16), size); assert.equal(asset.rawPayload.readUInt32BE(20), size);
  }
  const html = await app.inject('/'); assert.match(html.body, /rel="manifest"/);
  assert.match(String(html.headers['content-security-policy']), /worker-src 'self'/);
  assert.match(String(html.headers['content-security-policy']), /manifest-src 'self'/);
});

test('worker keeps private API and SSE requests network-only and uses public offline fallback for navigation', async () => {
  const handlers = new Map<string, (event: any) => void>();
  const cached: string[][] = []; let offline = false;
  const fallback = new Response('Offline: live data unavailable');
  const context = { URL, Response,
    self: { location: { origin: 'http://localhost' }, addEventListener: (type: string, handler: (event: any) => void) => handlers.set(type, handler), skipWaiting: async () => {}, clients: { claim: async () => {} } },
    caches: { open: async () => ({ addAll: async (urls: string[]) => { cached.push(urls); } }), keys: async () => [], match: async () => fallback },
    fetch: async () => { if (offline) throw new Error('Offline'); return new Response('network'); },
  };
  vm.runInNewContext(await readFile(new URL('../frontend/public/sw.js', import.meta.url), 'utf8'), context);
  let installed: Promise<unknown> | undefined;
  handlers.get('install')!({ waitUntil: (job: Promise<unknown>) => { installed = job; } }); await installed;
  assert.ok(cached[0]!.includes('/pwa/offline.html'));
  assert.ok(cached[0]!.every(url => url.startsWith('/pwa/') || url === '/manifest.webmanifest'));
  for (const path of ['/api/me', '/api/pipeline/status', '/events/hls-status', '/login', '/assets/app.js']) {
    let intercepted = false;
    handlers.get('fetch')!({ request: { url: `http://localhost${path}`, method: 'GET', mode: 'cors' }, respondWith: () => { intercepted = true; } });
    assert.equal(intercepted, false, path);
  }
  offline = true;
  let navigation: Promise<Response> | undefined;
  handlers.get('fetch')!({ request: { url: 'http://localhost/workspace', method: 'GET', mode: 'navigate' }, respondWith: (job: Promise<Response>) => { navigation = job; } });
  assert.equal(await (await navigation!).text(), 'Offline: live data unavailable');
  assert.ok(handlers.has('push')); assert.ok(handlers.has('notificationclick'));
});
