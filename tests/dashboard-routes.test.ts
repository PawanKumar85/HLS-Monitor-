import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import Fastify from 'fastify';
import { registerDashboard } from '../src/http/dashboard.js';

test('dashboard routes serve the SPA on direct navigation without swallowing API 404s', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'dashboard-routes-'));
  const app = Fastify();
  try {
    await mkdir(join(dir, 'assets'));
    await writeFile(join(dir, 'index.html'), '<html>Dashboard shell</html>');
    await writeFile(join(dir, 'sw.js'), 'self.addEventListener("push", () => {});');
    await registerDashboard(app, pathToFileURL(`${dir}/`));
    for (const url of ['/', '/workspace', '/create', '/reports', '/account', '/dlq', '/documentation', '/dashboard', '/overview', '/pipeline']) {
      const response = await app.inject(url);
      assert.equal(response.statusCode, 200, url);
      assert.match(response.headers['content-type'] ?? '', /text\/html/);
      assert.equal(response.body, '<html>Dashboard shell</html>');
      assert.equal(response.headers['cache-control'], 'no-cache');
      assert.ok(response.headers['content-security-policy']);
    }
    assert.equal((await app.inject('/api/missing')).statusCode, 404);
    const sw = await app.inject('/sw.js');
    assert.equal(sw.statusCode, 200); assert.match(String(sw.headers['content-type']), /javascript/); assert.match(sw.body, /push/);
    assert.equal((await app.inject({ url: '/streams', headers: { accept: 'text/html' } })).statusCode, 404, 'the Streams page is gone');
    const typo = await app.inject({ url: '/pipline', headers: { accept: 'text/html' } });
    assert.equal(typo.statusCode, 404); assert.equal(typo.body, '<html>Dashboard shell</html>', 'a browser gets the app and its Page not found view');
    for (const url of ['/api/missing', '/events/missing', '/assets/missing.js']) {
      const response = await app.inject({ url, headers: { accept: 'text/html' } });
      assert.equal(response.statusCode, 404, url); assert.deepEqual(response.json(), { error: 'Not found' }, url);
    }
    assert.equal((await app.inject({ method: 'POST', url: '/pipline', headers: { accept: 'text/html' } })).headers['content-type']?.includes('json'), true);
  } finally {
    await app.close();
    await rm(dir, { recursive: true, force: true });
  }
});

test('a frontend rebuild serves new asset URLs without restarting the backend', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'dashboard-rebuild-'));
  const app = Fastify();
  try {
    await mkdir(join(dir, 'assets'));
    await writeFile(join(dir, 'assets', 'old.js'), 'old bundle');
    await writeFile(join(dir, 'index.html'), '<script src="/assets/old.js"></script>');
    await registerDashboard(app, pathToFileURL(`${dir}/`));
    assert.match((await app.inject('/reports')).body, /old\.js/);
    assert.equal((await app.inject('/assets/old.js')).statusCode, 200);
    // Vite emits a new hashed bundle and replaces index.html on rebuild.
    await writeFile(join(dir, 'assets', 'new.js'), 'new bundle');
    await writeFile(join(dir, 'assets', 'new.css'), 'body { color: green; }');
    await writeFile(join(dir, 'index.html'), '<link href="/assets/new.css"><script src="/assets/new.js"></script>');
    await rm(join(dir, 'assets', 'old.js'));
    for (const url of ['/reports', '/pipeline', '/dashboard']) {
      const page = await app.inject(url);
      assert.match(page.body, /new\.js/);
      assert.match(page.body, /new\.css/);
      assert.doesNotMatch(page.body, /old\.js/);
      assert.equal(page.headers['cache-control'], 'no-cache');
    }
    assert.equal((await app.inject('/assets/new.js')).body, 'new bundle');
    assert.equal((await app.inject('/assets/new.css')).statusCode, 200);
    assert.equal((await app.inject('/assets/old.js')).statusCode, 404);
  } finally {
    await app.close();
    await rm(dir, { recursive: true, force: true });
  }
});
