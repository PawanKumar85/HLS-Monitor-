import { TransferValidationError, TransferConflictError } from '../application/configuration-transfer.js';
import { readFileSync } from 'node:fs';
import type { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';
import fastifyCookie from '@fastify/cookie';
import fastifyFormbody from '@fastify/formbody';
import fastifySession from '@fastify/session';
import { Authenticator } from '@fastify/passport';
import { Strategy as LocalStrategy } from 'passport-local';
import { DUMMY_HASH, hashPassword, verifyPassword } from '../auth/password.js';
import { normalizeUsername, type SessionUser, type UserStore } from '../auth/users.js';

declare module 'fastify' { interface PassportUser extends SessionUser {} }

/** Failed sign-ins per client and username; blocks guessing without letting one client lock others out. */
export interface LoginLimiter { blocked(key: string): Promise<boolean>; fail(key: string): Promise<void>; clear(key: string): Promise<void> }
export class RedisLoginLimiter implements LoginLimiter {
  constructor(private readonly redis: { get(key: string): Promise<string | null>; eval(script: string, numberOfKeys: number, ...args: (string | number)[]): Promise<unknown>; del(key: string): Promise<number> },
    private readonly max = 10, private readonly windowMs = 15 * 60_000) {}
  async blocked(key: string) { return Number(await this.redis.get(`gate:login-fail:${key}`) ?? 0) >= this.max; }
  async fail(key: string) {
    await this.redis.eval(`
      local count = redis.call('INCR', KEYS[1])
      if count == 1 or redis.call('PTTL', KEYS[1]) < 0 then
        redis.call('PEXPIRE', KEYS[1], ARGV[1])
      end
      return count
    `, 1, `gate:login-fail:${key}`, this.windowMs);
  }
  async clear(key: string) { await this.redis.del(`gate:login-fail:${key}`); }
}

export interface AuthOptions {
  configurationTransfer?: Pick<import('../application/configuration-transfer.js').ConfigurationTransfer, 'export' | 'preview' | 'import'>;
  resetDatabase?: import('../application/monitoring-reset.js').MonitoringReset;
  users: UserStore;
  sessionStore: unknown;
  secret: string;
  limiter: LoginLimiter;
  loginPage?: URL;
}

const SAFE = new Set(['GET', 'HEAD', 'OPTIONS']);
// /sw.js: the push service worker is static code the browser re-fetches on its own; it holds no data.
const PUBLIC = new Set(['/login', '/logout', '/health', '/sw.js', '/favicon.svg', '/manifest.webmanifest', '/pwa/offline.html', '/pwa/icon-192.png', '/pwa/icon-512.png', '/pwa/icon-maskable-512.png', '/pwa/apple-touch-icon.png']);
const MESSAGES: Record<string, string> = {
  invalid: 'That username and password don’t match. Check both and try again.',
  locked: 'Too many failed attempts. Wait 15 minutes, then try again.',
  unavailable: 'Sign-in is unavailable right now. Try again in a minute.',
  signedout: 'You are signed out.',
  expired: 'Your session ended. Sign in again to continue.',
};
const escape = (value: string) => value.replace(/[&<>"']/g, c => `&#${c.charCodeAt(0)};`);
/** Only same-site paths; never `//host` or a scheme, so the redirect cannot leave this site. */
export function safeNext(value: unknown): string {
  return typeof value === 'string' && /^\/(?![/\\])[^\s]*$/.test(value) && value.length <= 2048 && !value.startsWith('/login') ? value : '/';
}
const path = (request: FastifyRequest) => request.url.split('?')[0]!;

/**
 * Session login with passport-local. Everything except /login, /logout and /health needs a signed-in user;
 * every signed-in user can read and change everything.
 */
export async function registerAuth(app: FastifyInstance, options: AuthOptions): Promise<void> {
  if (typeof options.secret !== 'string' || options.secret.length < 32) throw new Error('SESSION_SECRET must be at least 32 characters');
  const template = readFileSync(options.loginPage ?? new URL('../../public/login.html', import.meta.url), 'utf8');

  await app.register(fastifyCookie);
  await app.register(fastifyFormbody, { bodyLimit: 4096 });
  await app.register(fastifySession, {
    secret: options.secret, cookieName: 'gate_session', saveUninitialized: false, rolling: true,
    store: options.sessionStore as never,
    // Lax keeps links into the dashboard working; cross-site writes are refused by the Origin check below.
    cookie: { httpOnly: true, sameSite: 'lax', secure: 'auto', path: '/', maxAge: 12 * 3600_000 },
  });

  const passport = new Authenticator();
  passport.use('local', new LocalStrategy((username, password, done) => {
    void (async () => {
      const user = await options.users.findByUsername(username);
      // Unknown users are checked against a dummy hash so the response time does not reveal which usernames exist.
      const valid = await verifyPassword(password, user?.passwordHash ?? await DUMMY_HASH);
      return valid && user && !user.disabled ? { id: user.id, username: user.username, authVersion: user.authVersion ?? 0 } : false;
    })().then(result => done(null, result), error => done(error));
  }));
  passport.registerUserSerializer(async (user: SessionUser) => ({ id: user.id, authVersion: user.authVersion ?? 0 }));
  // Version zero supports existing sessions; a password update invalidates every previous version.
  passport.registerUserDeserializer(async (session: string | { id: string; authVersion: number }) => {
    const id = typeof session === 'string' ? session : session?.id;
    const version = typeof session === 'string' ? 0 : session?.authVersion;
    const user = typeof id === 'string' ? await options.users.findById(id) : null;
    return user && !user.disabled && (user.authVersion ?? 0) === version ? { id: user.id, username: user.username, authVersion: user.authVersion ?? 0 } : null;
  });
  await app.register(passport.initialize());
  await app.register(passport.secureSession());

  // Runs after passport's own preValidation hook has loaded request.user.
  app.addHook('preValidation', async (request, reply) => {
    if (!SAFE.has(request.method)) {
      const origin = request.headers.origin;
      let sameSite = true;
      // request.host is X-Forwarded-Host when the request came through a trusted proxy (TRUST_PROXY), else the Host header.
      if (origin) { try { sameSite = new URL(origin).host === request.host; } catch { sameSite = false; } }
      if (!sameSite) return reply.code(403).send({ error: 'Cross-site request refused' });
    }
    if (PUBLIC.has(path(request))) return;
    const user = request.user;
    if (!user) {
      if (request.method === 'GET' && (request.headers.accept ?? '').includes('text/html')) {
        return reply.redirect(`/login?next=${encodeURIComponent(request.url)}`, 303);
      }
      return reply.code(401).header('Cache-Control', 'no-store').send({ error: 'Sign in required' });
    }
  });

  const page = (reply: FastifyReply, error?: string, next = '/') => reply
    .header('Content-Type', 'text/html; charset=utf-8')
    .header('Content-Security-Policy', "default-src 'none'; manifest-src 'self'; img-src 'self'; style-src 'unsafe-inline' https://fonts.googleapis.com; font-src https://fonts.gstatic.com; form-action 'self'; base-uri 'none'; frame-ancestors 'none'")
    .header('X-Content-Type-Options', 'nosniff')
    // Not no-referrer: with it the form would send `Origin: null`, which the CSRF check refuses.
    .header('Referrer-Policy', 'same-origin')
    .header('Cache-Control', 'no-store')
    .send(template
      .replace('{{message}}', error && MESSAGES[error] ? `<p class="message${error === 'signedout' ? '' : ' message--error'}" role="alert">${escape(MESSAGES[error])}</p>` : '')
      .replace('{{next}}', escape(safeNext(next))));

  app.get<{ Querystring: { error?: string; next?: string } }>('/login', async (request, reply) => {
    if (request.user) return reply.redirect(safeNext(request.query.next), 303);
    return page(reply, request.query.error, request.query.next);
  });

  const limiterKey = (request: FastifyRequest<{ Body: { username?: string } }>) =>
    `${request.ip}:${String(request.body?.username ?? '').trim().toLowerCase().slice(0, 64)}`;
  app.post<{ Body: { username?: string; password?: string; next?: string } }>('/login', {
    preHandler: async (request, reply) => {
      const next = encodeURIComponent(safeNext(request.body?.next));
      if (typeof request.body?.username !== 'string' || typeof request.body?.password !== 'string') return reply.redirect(`/login?error=invalid&next=${next}`, 303);
      if (await options.limiter.blocked(limiterKey(request))) return reply.redirect(`/login?error=locked&next=${next}`, 303);
    },
  }, passport.authenticate('local', async (request, reply, error, user) => {
    const body = request.body as { username?: string; next?: string };
    const next = safeNext(body.next);
    if (error) { request.log.error('Sign-in check failed'); return reply.redirect(`/login?error=unavailable&next=${encodeURIComponent(next)}`, 303); }
    if (!user) {
      await options.limiter.fail(limiterKey(request as FastifyRequest<{ Body: { username?: string } }>));
      return reply.redirect(`/login?error=invalid&next=${encodeURIComponent(next)}`, 303);
    }
    await options.limiter.clear(limiterKey(request as FastifyRequest<{ Body: { username?: string } }>));
    await request.logIn(user); // regenerates the session id, so a pre-login session id cannot be reused
    return reply.redirect(next, 303);
  }));

  app.post('/logout', async (request, reply) => {
    if (request.user) await request.logOut();
    await request.session.destroy();
    return reply.redirect('/login?error=signedout', 303);
  });

  app.get('/api/me', async (request, reply) => reply.header('Cache-Control', 'no-store').send({ id: request.user!.id, username: request.user!.username }));

  const transferError = (error: unknown, reply: FastifyReply) => {
    if (error instanceof TransferValidationError) return reply.code(400).send({ error: error.message });
    if (error instanceof TransferConflictError) return reply.code(409).send({ error: error.message });
    return reply.code(503).send({ error: 'Configuration transfer failed. Check database connectivity and validate again before retrying.' });
  };
  app.get('/api/me/configuration/export', async (_request, reply) => {
    reply.header('Cache-Control', 'no-store');
    if (!options.configurationTransfer) return reply.code(503).send({ error: 'Configuration export is unavailable' });
    try {
      const bundle = await options.configurationTransfer.export();
      return reply.header('Content-Disposition', 'attachment; filename="hls-monitor-configuration.json"').send(bundle);
    } catch (error) { return transferError(error, reply); }
  });
  app.post<{ Body: unknown }>('/api/me/configuration/preview', { bodyLimit: 10 * 1024 * 1024, schema: { summary: 'Validate configuration import without changes', tags: ['Account'], body: { type: 'object', required: ['format', 'version', 'channels', 'probes', 'streams'], properties: { format: { type: 'string', const: 'hls-monitor-configuration' }, version: { type: 'integer', const: 1 }, channels: { type: 'array', items: { type: 'object' } }, probes: { type: 'array', items: { type: 'object' } }, streams: { type: 'array', items: { type: 'object' } } } } } }, async (request, reply) => {
    reply.header('Cache-Control', 'no-store');
    if (!options.configurationTransfer) return reply.code(503).send({ error: 'Configuration import is unavailable' });
    try { return (await options.configurationTransfer.preview(request.body)).summary; }
    catch (error) { return transferError(error, reply); }
  });
  app.post<{ Body: { bundle: unknown; currentPassword: string; confirmation: string } }>('/api/me/configuration/import', {
    bodyLimit: 10 * 1024 * 1024,
    schema: { summary: 'Import monitoring configuration', tags: ['Account'], body: { type: 'object', additionalProperties: false,
      required: ['bundle', 'currentPassword', 'confirmation'], properties: { bundle: { type: 'object' }, currentPassword: { type: 'string', minLength: 1, maxLength: 1024 }, confirmation: { const: 'IMPORT', type: 'string' } },
    } },
  }, async (request, reply) => {
    reply.header('Cache-Control', 'no-store');
    if (!options.configurationTransfer) return reply.code(503).send({ error: 'Configuration import is unavailable' });
    const key = `account:${request.ip}:${request.user!.id}`;
    if (await options.limiter.blocked(key)) return reply.code(429).send({ error: 'Too many incorrect password attempts. Try again later.' });
    const user = await options.users.findById(request.user!.id);
    if (!user || user.disabled) return reply.code(401).send({ error: 'Sign in required' });
    if (!await verifyPassword(request.body.currentPassword, user.passwordHash)) { await options.limiter.fail(key); return reply.code(400).send({ error: 'Current password is incorrect' }); }
    await options.limiter.clear(key);
    try { return await options.configurationTransfer.import(request.body.bundle, user.username); }
    catch (error) { return transferError(error, reply); }
  });

  app.post<{ Body: { currentPassword: string; confirmation: string } }>('/api/me/reset-database', {
    schema: { summary: 'Reset all monitoring data', tags: ['Account'], body: {
      type: 'object', additionalProperties: false, required: ['currentPassword', 'confirmation'], properties: {
        currentPassword: { type: 'string', minLength: 1, maxLength: 1024 }, confirmation: { type: 'string', const: 'RESET' },
      },
    } },
  }, async (request, reply) => {
    reply.header('Cache-Control', 'no-store');
    if (!options.resetDatabase) return reply.code(503).send({ error: 'Database reset is unavailable' });
    if (options.resetDatabase.running) return reply.code(409).send({ error: 'A monitoring reset is already running' });
    const key = `account:${request.ip}:${request.user!.id}`;
    if (await options.limiter.blocked(key)) return reply.code(429).send({ error: 'Too many incorrect password attempts. Try again later.' });
    const user = await options.users.findById(request.user!.id);
    if (!user || user.disabled) return reply.code(401).send({ error: 'Sign in required' });
    if (!await verifyPassword(request.body.currentPassword, user.passwordHash)) {
      await options.limiter.fail(key);
      return reply.code(400).send({ error: 'Current password is incorrect' });
    }
    await options.limiter.clear(key);
    try { await options.resetDatabase.run(); }
    catch (error) {
      request.log.error({ err: error }, 'Monitoring database reset failed');
      return reply.code(503).send({ error: 'Reset did not finish. Some data may have been cleared. Check database connectivity and retry.' });
    }
    return { reset: true };
  });

  app.patch<{ Body: { currentPassword: string; username?: string; newPassword?: string } }>('/api/me', {
    schema: { body: { type: 'object', additionalProperties: false, required: ['currentPassword'], properties: {
      currentPassword: { type: 'string', minLength: 1, maxLength: 1024 }, username: { type: 'string', minLength: 3, maxLength: 64 }, newPassword: { type: 'string', minLength: 12, maxLength: 1024 },
    }, anyOf: [{ required: ['username'] }, { required: ['newPassword'] }] } },
  }, async (request, reply) => {
    reply.header('Cache-Control', 'no-store');
    const key = `account:${request.ip}:${request.user!.id}`;
    if (await options.limiter.blocked(key)) return reply.code(429).send({ error: 'Too many incorrect password attempts. Try again later.' });
    if (!options.users.updateAccount) return reply.code(503).send({ error: 'Account updates are unavailable' });
    const user = await options.users.findById(request.user!.id);
    if (!user || user.disabled) return reply.code(401).send({ error: 'Sign in required' });
    if (!await verifyPassword(request.body.currentPassword, user.passwordHash)) {
      await options.limiter.fail(key);
      return reply.code(400).send({ error: 'Current password is incorrect' });
    }
    let username: string | undefined;
    if (request.body.username !== undefined) {
      try { username = normalizeUsername(request.body.username); }
      catch (error) { return reply.code(400).send({ error: (error as Error).message }); }
    }
    if (request.body.newPassword !== undefined && await verifyPassword(request.body.newPassword, user.passwordHash)) return reply.code(400).send({ error: 'Choose a different new password' });
    const passwordHash = request.body.newPassword !== undefined ? await hashPassword(request.body.newPassword) : undefined;
    const result = await options.users.updateAccount(user.id, user.passwordHash, { ...(username !== undefined ? { username } : {}), ...(passwordHash !== undefined ? { passwordHash } : {}) });
    if (result === 'duplicate') return reply.code(409).send({ error: 'That username is already in use' });
    if (result === 'conflict') return reply.code(409).send({ error: 'Account changed during this request. Sign in again and retry.' });
    await options.limiter.clear(key);
    const updated = { id: user.id, username: username ?? user.username, authVersion: (user.authVersion ?? 0) + (passwordHash !== undefined ? 1 : 0) };
    await request.logIn(updated);
    return { id: updated.id, username: updated.username };
  });
}
