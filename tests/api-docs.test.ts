import { test } from 'node:test';
import assert from 'node:assert/strict';
import { buildApp } from '../src/app.js';
import { StreamService } from '../src/inventory/stream-service.js';
import { MemoryInventory } from './helpers/stream-inventory.js';

test('Scalar serves local assets and a shared specification for registered APIs', async t => {
  const app = await buildApp({
    auth: 'disabled', dbManager: { healthCheck: async () => ({ redis: true, storage: true, storageProvider: 'test' }), disconnectAll: async () => {} },
    hlsConsumer: { stop: async () => {} }, batchQueueManager: { stopAll: async () => {} },
    statusTransport: { register: async () => {} }, streams: { service: new StreamService(new MemoryInventory()) },
    pipeline: { channels: { list: async () => [], get: async () => null, create: async () => true, replace: async () => true, remove: async () => true },
      probes: { all: async () => new Map(), set: async () => {}, remove: async () => false }, monitor: { status: () => ({ evaluatedAt: null, intervalMs: 10000, channels: [] }) } },
  });
  t.after(() => app.close());
  for (const url of ['/docs/scalar/']) {
    const result = await app.inject(url);
    assert.equal(result.statusCode, 200, url);
    assert.match(String(result.headers['content-type']), /html/);
    const scripts = [...result.body.matchAll(/<script[^>]+src="([^"]+)"/g)].map(match => match[1]!);
    assert.ok(scripts.length > 0, `${url} has scripts`);
    for (const src of scripts) {
      assert.ok(!src.startsWith('http'), 'documentation scripts are hosted locally');
      const asset = await app.inject(new URL(src, `http://localhost${url}`).pathname);
      assert.equal(asset.statusCode, 200, src);
      assert.match(String(asset.headers['content-type']), /javascript/);
    }
  }
  assert.equal((await app.inject('/docs/swagger/')).statusCode, 404);
  const diagram = await app.inject('/docs/data-flow');
  assert.equal(diagram.statusCode, 200);
  assert.match(diagram.body, /HLS Data Flow/);
  assert.match(diagram.body, /<svg/);
  assert.match(diagram.body, /first 1 KB/);
  assert.match(String(diagram.headers['content-security-policy']), /default-src 'none'/);
  const response = await app.inject('/api/openapi.json');
  assert.equal(response.statusCode, 200);
  const spec = response.json();
  assert.equal(spec.openapi, '3.0.3');
  assert.deepEqual(spec.paths['/health'].get.security, []);
  assert.equal(spec.components.securitySchemes.sessionCookie.name, 'gate_session');
  assert.ok(spec.paths['/api/streams'].post.requestBody.content['application/json'].schema.properties.streamUrl);
  assert.ok(spec.paths['/api/pipeline/channels'].post.requestBody.content['application/json'].schema.properties.nodes);
  assert.ok(spec.paths['/api/pipeline/probes/{triggerId}/{nodeId}'].put.requestBody);
  const version = spec.paths['/api/streams/{id}'].patch.parameters.find((p: { name: string }) => p.name === 'If-Match');
  assert.equal(version.required, true);
  assert.ok(!Object.keys(spec.paths).some(path => path.startsWith('/docs') || path === '/' || path === '/api/openapi.json'));
});
