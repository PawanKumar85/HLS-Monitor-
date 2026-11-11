import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import type { FastifyInstance, FastifyReply } from 'fastify';
import fastifyStatic from '@fastify/static';

/** The React dashboard built by Vite (`npm run web:build` → frontend/dist). Works from src/ and dist/. */
export async function registerDashboard(app: FastifyInstance, dir = new URL('../../frontend/dist/', import.meta.url)): Promise<void> {
  // Vite emits every script as a file under /assets, so 'self' is enough: no inline script can run.
  const csp = [
    "default-src 'none'", "script-src 'self'", "worker-src 'self'", "manifest-src 'self'", "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com",
    'font-src https://fonts.gstatic.com', "connect-src 'self'", "img-src 'self' data:", "base-uri 'none'", "form-action 'self'", "frame-ancestors 'none'",
  ].join('; ');
  // Hashed file names change on every build, so assets can be cached for a year.
  await app.register(fastifyStatic, { root: fileURLToPath(new URL('assets/', dir)), prefix: '/assets/', immutable: true, maxAge: '365d', index: false, decorateReply: false });
  const sendApp = async (reply: FastifyReply, code = 200) => {
    // Builds replace the hashed assets while the backend may remain running.
    // Read the current manifest HTML so a rebuild never leaves clients on deleted asset URLs.
    let html: string;
    try { html = await readFile(new URL('index.html', dir), 'utf8'); }
    catch { return reply.code(503).header('Content-Type', 'text/plain; charset=utf-8').send('Dashboard not built. Run: npm run web:build'); }
    return reply.code(code)
      .header('Content-Type', 'text/html; charset=utf-8')
      .header('Content-Security-Policy', csp)
      .header('X-Content-Type-Options', 'nosniff')
      // Not no-referrer: with it browsers send `Origin: null` on same-site writes, which the CSRF check refuses.
      .header('Referrer-Policy', 'same-origin')
      .header('Cache-Control', 'no-cache')
      .send(html);
  };
  await app.register(fastifyStatic, { root: fileURLToPath(new URL('pwa/', dir)), prefix: '/pwa/', index: false, decorateReply: false, maxAge: '1h' });
  app.get('/manifest.webmanifest', async (_request, reply) => {
    try {
      return reply.header('Content-Type', 'application/manifest+json').header('Cache-Control', 'no-cache')
        .header('X-Content-Type-Options', 'nosniff').send(await readFile(new URL('manifest.webmanifest', dir), 'utf8'));
    } catch { return reply.code(404).send({ error: 'Not found' }); }
  });
  app.get('/favicon.svg', async (_request, reply) => {
    try {
      return reply.header('Content-Type', 'image/svg+xml').header('Cache-Control', 'public, max-age=3600')
        .header('X-Content-Type-Options', 'nosniff').send(await readFile(new URL('favicon.svg', dir), 'utf8'));
    } catch { return reply.code(404).send({ error: 'Not found' }); }
  });
  // The push service worker must be served from the root to control the whole site.
  app.get('/sw.js', async (_request, reply) => {
    try {
      return reply.header('Content-Type', 'text/javascript; charset=utf-8').header('Cache-Control', 'no-cache')
        .header('X-Content-Type-Options', 'nosniff').send(await readFile(new URL('sw.js', dir), 'utf8'));
    } catch { return reply.code(404).send({ error: 'Not found' }); }
  });
  for (const path of ['/', '/workspace', '/create', '/reports', '/account', '/dlq', '/documentation', '/dashboard', '/overview', '/pipeline']) app.get(path, async (_request, reply) => sendApp(reply));
  // A browser opening an unknown page gets the app, which shows its own "Page not found" (status stays 404).
  // API, SSE and asset paths keep a plain JSON 404.
  app.setNotFoundHandler(async (request, reply) => {
    const page = request.method === 'GET' && (request.headers.accept ?? '').includes('text/html') && !/^\/(api|events|assets)(\/|$)/.test(request.url);
    return page ? sendApp(reply, 404) : reply.code(404).send({ error: 'Not found' });
  });
}
