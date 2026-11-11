import { test } from 'node:test';
import assert from 'node:assert/strict';
import { MemoryStore } from '@fastify/session';
import { buildApp } from '../src/app.js';
import { hashPassword, verifyPassword } from '../src/auth/password.js';
import type { User, UserStore, AccountPatch } from '../src/auth/users.js';
import { safeNext, type LoginLimiter } from '../src/http/auth.js';
import { StreamService } from '../src/inventory/stream-service.js';
import { MemoryInventory } from './helpers/stream-inventory.js';

const PASSWORD = 'correct horse battery';
const secret = 'a-test-session-secret-of-enough-length-0123456789';

class MemoryUsers implements UserStore {
  readonly users = new Map<string, User>();
  async add(username: string) {
    const now = new Date();
    this.users.set(username, { id: `id-${username}`, username, disabled: false, passwordHash: await hashPassword(PASSWORD), createdAt: now, updatedAt: now });
  }
  async findByUsername(username: string) { return this.users.get(username.trim().toLowerCase()) ?? null; }
  async findById(id: string) { return [...this.users.values()].find(u => u.id === id) ?? null; }
  async updateAccount(id: string, expectedPasswordHash: string, patch: AccountPatch) {
    const user = await this.findById(id);
    if (!user || user.disabled || user.passwordHash !== expectedPasswordHash) return 'conflict' as const;
    if (patch.username && [...this.users.values()].some(u => u.id !== id && u.username === patch.username)) return 'duplicate' as const;
    const updated = { ...user, ...patch, authVersion: (user.authVersion ?? 0) + (patch.passwordHash ? 1 : 0), updatedAt: new Date() };
    this.users.delete(user.username); this.users.set(updated.username, updated);
    return 'updated' as const;
  }

}
class MemoryLimiter implements LoginLimiter {
  readonly counts = new Map<string, number>();
  async blocked(key: string) { return (this.counts.get(key) ?? 0) >= 10; }
  async fail(key: string) { this.counts.set(key, (this.counts.get(key) ?? 0) + 1); }
  async clear(key: string) { this.counts.delete(key); }
}

async function fixture(resetDatabase?: import('../src/application/monitoring-reset.js').MonitoringReset, configurationTransfer?: import('../src/application/contracts.js').ApplicationServices['configurationTransfer']) {
  const users = new MemoryUsers();
  await users.add('ops'); await users.add('wall');
  const app = await buildApp({
    resetDatabase, configurationTransfer,
    dbManager: { async healthCheck() { return { redis: true, storage: true, storageProvider: 'test' }; }, async disconnectAll() {} },
    hlsConsumer: { async stop() {} }, batchQueueManager: { async stopAll() {} }, statusTransport: { async register() {} },
    streams: { service: new StreamService(new MemoryInventory()) },
    auth: { users, sessionStore: new MemoryStore(), secret, limiter: new MemoryLimiter() },
  });
  const login = async (username: string, password = PASSWORD, next = '/') => {
    const response = await app.inject({ method: 'POST', url: '/login', headers: { 'content-type': 'application/x-www-form-urlencoded', host: 'gate.test' },
      payload: new URLSearchParams({ username, password, next }).toString() });
    const cookie = String(response.headers['set-cookie'] ?? '').split(';')[0] ?? '';
    return { response, cookie };
  };
  return { app, users, login };
}
const stream = { serverIp: '10.0.0.1', streamId: 'one', streamUrl: 'http://10.0.0.1/live.m3u8' };

test('passwords are stored as salted scrypt hashes and checked in constant time', async () => {
  const hash = await hashPassword(PASSWORD);
  assert.match(hash, /^scrypt\$16384\$8\$1\$/);
  assert.notEqual(hash, await hashPassword(PASSWORD)); // salted
  assert.equal(await verifyPassword(PASSWORD, hash), true);
  assert.equal(await verifyPassword('wrong password!!', hash), false);
  assert.equal(await verifyPassword(PASSWORD, 'not-a-hash'), false);
  await assert.rejects(hashPassword('short'), RangeError);
});

test('without a session, pages redirect to sign-in and the API answers 401; /login and /health stay open', async () => {
  const { app } = await fixture();
  try {
    const page = await app.inject({ url: '/?q=x', headers: { accept: 'text/html' } });
    assert.equal(page.statusCode, 303); assert.equal(page.headers.location, '/login?next=%2F%3Fq%3Dx');
    const unknown = await app.inject({ url: '/pipline', headers: { accept: 'text/html' } });
    assert.equal(unknown.statusCode, 303, 'unknown pages are guarded too'); assert.equal(unknown.headers.location, '/login?next=%2Fpipline');
    for (const url of ['/api/streams', '/api/me', '/api/hls/status/snapshot', '/events/hls-status']) assert.equal((await app.inject(url)).statusCode, 401, url);
    assert.equal((await app.inject({ method: 'POST', url: '/api/streams', payload: stream })).statusCode, 401);
    assert.equal((await app.inject('/health')).statusCode, 200);
    for (const url of ['/manifest.webmanifest', '/pwa/offline.html', '/pwa/icon-192.png', '/pwa/icon-512.png', '/pwa/icon-maskable-512.png', '/pwa/apple-touch-icon.png']) assert.equal((await app.inject(url)).statusCode, 200, url);
    assert.notEqual((await app.inject('/sw.js')).statusCode, 401, 'the push service worker is public static code');
    const form = await app.inject('/login?error=invalid&next=/streams');
    assert.equal(form.statusCode, 200);
    assert.match(form.body, /<form method="post" action="\/login">/);
    assert.match(form.body, /don’t match/);
    assert.match(form.body, /name="next" value="\/streams"/);
    assert.doesNotMatch(String(form.headers['content-security-policy']), /script-src/); // default-src 'none': no script runs
    // Browsers send `Origin: null` on POSTs from a no-referrer page, which the CSRF check refuses: both pages must stay same-origin.
    assert.equal(form.headers['referrer-policy'], 'same-origin');
    assert.equal((await app.inject({ method: 'POST', url: '/login', headers: { origin: 'null', 'content-type': 'application/x-www-form-urlencoded' }, payload: 'username=ops&password=x' })).statusCode, 403);
  } finally { await app.close(); }
});

test('sign-in sets an httpOnly SameSite=Lax session cookie; every signed-in user can read and change streams', async () => {
  const { app, login } = await fixture();
  try {
    const wrong = await login('ops', 'not the password');
    assert.equal(wrong.response.statusCode, 303); assert.match(String(wrong.response.headers.location), /^\/login\?error=invalid/);
    assert.equal(wrong.cookie, ''); // no session for a failed sign-in

    const ops = await login('ops', PASSWORD, '/#streams');
    assert.equal(ops.response.statusCode, 303); assert.equal(ops.response.headers.location, '/#streams');
    const setCookie = String(ops.response.headers['set-cookie']);
    assert.match(setCookie, /HttpOnly/i); assert.match(setCookie, /SameSite=Lax/i); assert.match(setCookie, /gate_session=/);
    assert.deepEqual((await app.inject({ url: '/api/me', headers: { cookie: ops.cookie } })).json(), { id: 'id-ops', username: 'ops' });
    assert.equal((await app.inject({ method: 'POST', url: '/api/streams', headers: { cookie: ops.cookie }, payload: stream })).statusCode, 201);

    const wall = await login('WALL'); // usernames are case-insensitive
    assert.equal((await app.inject({ url: '/api/streams', headers: { cookie: wall.cookie } })).statusCode, 200);
    const second = await app.inject({ method: 'POST', url: '/api/streams', headers: { cookie: wall.cookie }, payload: { ...stream, streamId: 'two' } });
    assert.equal(second.statusCode, 201, 'no roles: any signed-in user can add streams');
  } finally { await app.close(); }
});

test('cross-site writes are refused and the post-login redirect never leaves the site', async () => {
  const { app, login } = await fixture();
  try {
    const { cookie } = await login('ops');
    const forged = await app.inject({ method: 'POST', url: '/api/streams', headers: { cookie, host: 'gate.test', origin: 'https://evil.example' }, payload: stream });
    assert.equal(forged.statusCode, 403);
    const same = await app.inject({ method: 'POST', url: '/api/streams', headers: { cookie, host: 'gate.test', origin: 'http://gate.test' }, payload: stream });
    assert.equal(same.statusCode, 201);
    for (const bad of ['//evil.example', 'https://evil.example', '/\\evil.example', 'javascript:alert(1)', '/login?x']) assert.equal(safeNext(bad), '/', bad);
    assert.equal(safeNext('/?hours=168#report'), '/?hours=168#report');
    assert.equal((await login('ops', PASSWORD, '//evil.example')).response.headers.location, '/');
  } finally { await app.close(); }
});

test('disabling a user and signing out both end the session at once', async () => {
  const { app, users, login } = await fixture();
  try {
    const ops = await login('ops');
    users.users.get('ops')!.disabled = true;
    assert.equal((await app.inject({ url: '/api/me', headers: { cookie: ops.cookie } })).statusCode, 401);
    assert.equal((await login('ops')).response.headers.location?.startsWith('/login?error=invalid'), true); // disabled users cannot sign in

    const wall = await login('wall');
    const out = await app.inject({ method: 'POST', url: '/logout', headers: { cookie: wall.cookie } });
    assert.equal(out.statusCode, 303); assert.equal(out.headers.location, '/login?error=signedout');
    assert.equal((await app.inject({ url: '/api/me', headers: { cookie: wall.cookie } })).statusCode, 401); // old cookie is dead
  } finally { await app.close(); }
});

test('behind a trusted gateway, failed sign-ins lock only the real client, not everyone behind the gateway', async () => {
  process.env['TRUST_PROXY'] = '127.0.0.1'; // app.inject connects from 127.0.0.1, like the gateway would
  const { app } = await fixture();
  delete process.env['TRUST_PROXY'];
  const from = (ip: string, password: string) => app.inject({ method: 'POST', url: '/login', headers: { 'content-type': 'application/x-www-form-urlencoded', host: 'gate.test', 'x-forwarded-for': ip },
    payload: new URLSearchParams({ username: 'ops', password, next: '/' }).toString() });
  try {
    for (let i = 0; i < 10; i++) await from('203.0.113.9', `wrong password ${i}`);
    assert.match(String((await from('203.0.113.9', PASSWORD)).headers.location), /error=locked/, 'the attacker is locked');
    assert.equal((await from('198.51.100.7', PASSWORD)).headers.location, '/', 'the real user behind the same gateway still signs in');
  } finally { await app.close(); }
});

test('behind a trusted gateway that rewrites Host, same-site writes pass and forged ones are still refused', async () => {
  process.env['TRUST_PROXY'] = '127.0.0.1';
  const { app, login } = await fixture();
  delete process.env['TRUST_PROXY'];
  try {
    const { cookie } = await login('ops');
    const via = (origin: string, forwardedHost?: string) => app.inject({ method: 'POST', url: '/api/streams',
      headers: { cookie, host: '10.0.0.5:3000', origin, ...(forwardedHost ? { 'x-forwarded-host': forwardedHost } : {}) }, payload: { ...stream, streamId: `s-${Math.random()}` } });
    assert.equal((await via('https://gate.example.com', 'gate.example.com')).statusCode, 201, 'public origin matches the forwarded host');
    assert.equal((await via('https://evil.example', 'gate.example.com')).statusCode, 403);
    assert.equal((await via('https://gate.example.com')).statusCode, 403, 'without X-Forwarded-Host the rewritten Host does not match');
  } finally { await app.close(); }
});

test('without a trusted proxy, X-Forwarded-Host is ignored', async () => {
  const { app, login } = await fixture();
  try {
    const { cookie } = await login('ops');
    const forged = await app.inject({ method: 'POST', url: '/api/streams', headers: { cookie, host: 'gate.test', origin: 'https://evil.example', 'x-forwarded-host': 'evil.example' }, payload: stream });
    assert.equal(forged.statusCode, 403);
  } finally { await app.close(); }
});

test('ten failed sign-ins lock that username for this client, even with the right password next', async () => {
  const { app, login } = await fixture();
  try {
    for (let i = 0; i < 10; i++) await login('ops', `wrong password ${i}`);
    const locked = await login('ops', PASSWORD);
    assert.match(String(locked.response.headers.location), /error=locked/);
    assert.equal(locked.cookie, '');
    assert.match((await login('wall')).response.headers.location ?? '', /^\/$/); // other usernames unaffected
  } finally { await app.close(); }
});


test('account username changes require the current password and cannot change another user', async () => {
  const { app, users, login } = await fixture();
  try {
    const { cookie } = await login('ops');
    const update = (payload: unknown, extra: Record<string, string> = {}) => app.inject({ method: 'PATCH', url: '/api/me', headers: { cookie, ...extra }, payload });
    assert.equal((await app.inject({ method: 'PATCH', url: '/api/me', payload: { username: 'renamed', currentPassword: PASSWORD } })).statusCode, 401);
    assert.equal((await update({ username: 'renamed', currentPassword: 'wrong' })).statusCode, 400);
    assert.equal((await update({ username: 'renamed', currentPassword: PASSWORD }, { origin: 'https://evil.test' })).statusCode, 403);
    assert.equal((await update({ id: 'id-wall', username: 'renamed', currentPassword: PASSWORD })).statusCode, 400);
    assert.equal((await update({ username: 'wall', currentPassword: PASSWORD })).statusCode, 409);
    const result = await update({ username: 'New.Name', currentPassword: PASSWORD });
    assert.equal(result.statusCode, 200);
    assert.deepEqual(result.json(), { id: 'id-ops', username: 'new.name' });
    assert.equal((await users.findById('id-wall'))?.username, 'wall');
    assert.equal((await login('ops')).response.headers.location?.startsWith('/login?error=invalid'), true);
    assert.equal((await login('new.name')).response.headers.location, '/');
    assert.ok(!result.body.includes('passwordHash'));
  } finally { await app.close(); }
});

test('password updates hash the password, rotate the current session and invalidate other sessions', async () => {
  const { app, users, login } = await fixture();
  try {
    const first = await login('ops'); const other = await login('ops');
    const newPassword = 'a completely new password';
    const result = await app.inject({ method: 'PATCH', url: '/api/me', headers: { cookie: first.cookie }, payload: { currentPassword: PASSWORD, newPassword } });
    assert.equal(result.statusCode, 200);
    const cookie = String(result.headers['set-cookie']).split(';')[0]!;
    assert.notEqual(cookie, first.cookie);
    assert.equal((await app.inject({ url: '/api/me', headers: { cookie } })).statusCode, 200);
    assert.equal((await app.inject({ url: '/api/me', headers: { cookie: other.cookie } })).statusCode, 401);
    assert.equal((await app.inject({ url: '/api/me', headers: { cookie: first.cookie } })).statusCode, 401);
    const user = await users.findById('id-ops');
    assert.notEqual(user?.passwordHash, newPassword);
    assert.equal(await verifyPassword(newPassword, user!.passwordHash), true);
    assert.equal((await login('ops', PASSWORD)).response.headers.location?.startsWith('/login?error=invalid'), true);
    assert.equal((await login('ops', newPassword)).response.headers.location, '/');
    assert.ok(!result.body.includes(newPassword));
  } finally { await app.close(); }
});

test('account input validation rejects weak passwords and username errors without changing credentials', async () => {
  const { app, login } = await fixture();
  try {
    const { cookie } = await login('ops');
    for (const payload of [{ currentPassword: PASSWORD }, { currentPassword: PASSWORD, newPassword: 'short' }, { currentPassword: PASSWORD, newPassword: PASSWORD }, { currentPassword: PASSWORD, username: 'bad name' }]) {
      const result = await app.inject({ method: 'PATCH', url: '/api/me', headers: { cookie }, payload });
      assert.equal(result.statusCode, 400);
    }
    assert.equal((await login('ops', PASSWORD)).response.headers.location, '/');
  } finally { await app.close(); }
});

test('repeated incorrect current passwords are rate-limited for account updates', async () => {
  const { app, login } = await fixture();
  try {
    const { cookie } = await login('ops');
    for (let i = 0; i < 10; i++) {
      assert.equal((await app.inject({ method: 'PATCH', url: '/api/me', headers: { cookie }, payload: { username: 'renamed', currentPassword: 'incorrect' } })).statusCode, 400);
    }
    assert.equal((await app.inject({ method: 'PATCH', url: '/api/me', headers: { cookie }, payload: { username: 'renamed', currentPassword: PASSWORD } })).statusCode, 429);
  } finally { await app.close(); }
});


test('documentation pages, assets and specification require the existing signed-in session', async () => {
  const { app, login } = await fixture();
  try {
    for (const url of ['/docs/scalar/', '/docs/scalar/js/scalar.js', '/docs/data-flow', '/documentation']) {
      assert.equal((await app.inject({ url, headers: { accept: 'text/html' } })).statusCode, 303, url);
    }
    assert.equal((await app.inject('/api/openapi.json')).statusCode, 401);
    const { cookie } = await login('ops');
    for (const url of ['/docs/scalar/', '/docs/data-flow', '/api/openapi.json']) {
      assert.equal((await app.inject({ url, headers: { cookie } })).statusCode, 200, url);
    }
  } finally { await app.close(); }
});


test('full monitoring reset requires authentication, same-origin request, RESET and current password', async () => {
  let resets = 0;
  const reset = { running: false, generation: 0, async run() { resets++; } };
  const { app, login } = await fixture(reset);
  try {
    const url = '/api/me/reset-database';
    assert.equal((await app.inject({ method: 'POST', url, payload: { currentPassword: PASSWORD, confirmation: 'RESET' } })).statusCode, 401);
    const { cookie } = await login('ops');
    const call = (currentPassword: string, confirmation = 'RESET', origin = 'http://gate.test') => app.inject({ method: 'POST', url, headers: { cookie, origin, host: 'gate.test' }, payload: { currentPassword, confirmation } });
    assert.equal((await call(PASSWORD, 'RESET', 'https://external.test')).statusCode, 403);
    assert.equal((await call(PASSWORD, 'wrong')).statusCode, 400);
    assert.equal((await call('incorrect password')).statusCode, 400);
    assert.equal(resets, 0);
    assert.equal((await call(PASSWORD)).statusCode, 200);
    assert.equal(resets, 1);
    assert.equal((await app.inject({ url: '/api/me', headers: { cookie } })).statusCode, 200, 'session survives');
  } finally { await app.close(); }
});

test('reset pauses API writes while waiting for existing writes and prevents simultaneous resets', async () => {
  let release!: () => void;
  let entered!: () => void;
  const started = new Promise<void>(resolve => { entered = resolve; });
  const { app, login } = await fixture({ running: false, generation: 0, async run() { entered(); await new Promise<void>(resolve => { release = resolve; }); } });
  const { cookie } = await login('ops');
  try {
    const first = app.inject({ method: 'POST', url: '/api/me/reset-database', headers: { cookie }, payload: { currentPassword: PASSWORD, confirmation: 'RESET' } });
    const result = first.then(value => value);
    await started;
    assert.equal((await app.inject({ method: 'POST', url: '/api/streams', headers: { cookie }, payload: stream })).statusCode, 503);
    assert.equal((await app.inject({ method: 'POST', url: '/api/me/reset-database', headers: { cookie }, payload: { currentPassword: PASSWORD, confirmation: 'RESET' } })).statusCode, 409);
    release(); assert.equal((await result).statusCode, 200);
    assert.equal((await app.inject({ url: '/api/me', headers: { cookie } })).statusCode, 200);
  } finally { release?.(); await app.close(); }
});


test('configuration export/preview/import require session; import additionally requires password and IMPORT', async () => {
  let imports = 0;
  const summary = { channels: 1, probes: 1, streams: 1 };
  const bundle = { format: 'hls-monitor-configuration', version: 1, channels: [], probes: [], streams: [] } as const;
  const transfer = { export: async () => ({ ...bundle, channels: [], probes: [], streams: [] }), preview: async () => ({ bundle: { ...bundle, channels: [], probes: [], streams: [] }, summary }), import: async () => { imports++; return summary; } };
  const { app, login } = await fixture(undefined, transfer);
  try {
    assert.equal((await app.inject('/api/me/configuration/export')).statusCode, 401);
    const { cookie } = await login('ops');
    const exported = await app.inject({ url: '/api/me/configuration/export', headers: { cookie } });
    assert.equal(exported.statusCode, 200); assert.match(String(exported.headers['content-disposition']), /attachment/);
    assert.equal(exported.headers['cache-control'], 'no-store');
    assert.equal((await app.inject({ method: 'POST', url: '/api/me/configuration/preview', headers: { cookie }, payload: bundle })).statusCode, 200);
    const call = (currentPassword: string, confirmation = 'IMPORT') => app.inject({ method: 'POST', url: '/api/me/configuration/import', headers: { cookie }, payload: { bundle, currentPassword, confirmation } });
    assert.equal((await call(PASSWORD, 'wrong')).statusCode, 400);
    assert.equal((await call('incorrect password')).statusCode, 400); assert.equal(imports, 0);
    assert.equal((await call(PASSWORD)).statusCode, 200); assert.equal(imports, 1);
  } finally { await app.close(); }
});
