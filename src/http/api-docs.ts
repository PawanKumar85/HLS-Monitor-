import { readFile } from 'node:fs/promises';
import type { FastifyInstance } from 'fastify';
import swagger from '@fastify/swagger';
import scalar from '@scalar/fastify-api-reference';

const channelBody = {
  type: 'object', required: ['channelName', 'nodes'],
  properties: {
    channelName: { type: 'string', maxLength: 100 },
    nodes: { type: 'array', minItems: 1, maxItems: 30, items: {
      type: 'object', required: ['id', 'name', 'type'], properties: {
        id: { type: 'string', pattern: '^[a-z0-9-]{1,40}$' }, name: { type: 'string', maxLength: 100 },
        type: { type: 'string', enum: ['INGEST', 'SCTE_RELAY', 'TRANSCODER', 'EGRESS_CDN', 'FALLBACK_SOURCE'] },
        host: { type: 'string' }, port: { type: 'integer', minimum: 1, maximum: 65535 },
        isBackup: { type: 'boolean' }, transport: { type: 'string', enum: ['srt'] },
      },
    } },
    edges: { type: 'array', items: { type: 'object', required: ['fromNodeId', 'toNodeId', 'type'], properties: {
      fromNodeId: { type: 'string' }, toNodeId: { type: 'string' }, type: { type: 'string', enum: ['PRIMARY', 'FAILOVER', 'BYPASS', 'FALLBACK'] },
    } } },
  },
};

/** Install before application routes. Transform enriches docs without changing runtime validation/serialization. */
export async function registerApiSpecification(app: FastifyInstance): Promise<void> {
  await app.register(swagger, {
    openapi: {
      openapi: '3.0.3',
      info: { title: 'HLS Monitor API', version: '1.0.0', description: 'Manage channels, probe targets, streams, reports and processing failures. Sign in through /login first; interactive requests use your existing session cookie. Writes must come from the same origin. SSE feeds provide live events and stay open until disconnected.' },
      servers: [{ url: '/' }],
      components: { securitySchemes: { sessionCookie: { type: 'apiKey', in: 'cookie', name: 'gate_session', description: 'HttpOnly session cookie issued by the sign-in page. Use the browser session; do not paste credentials into the docs.' } } },
      security: [{ sessionCookie: [] }],
      tags: ['Account', 'Streams', 'Pipeline', 'Reports', 'Failures', 'Push notifications', 'Live events', 'Health'].map(name => ({ name })),
    },
    transform: ({ schema, url, route }) => {
      if (!(url.startsWith('/api/') || url.startsWith('/events/') || url === '/health')) return { schema: { ...schema, hide: true }, url };
      const method = String(Array.isArray(route.method) ? route.method[0] : route.method).toUpperCase();
      const tag = url === '/health' ? 'Health' : url.startsWith('/api/me') ? 'Account' : url.startsWith('/events/') || url.startsWith('/api/hls/') ? 'Live events' : url.startsWith('/api/pipeline/') ? 'Pipeline' : url.startsWith('/api/failures') ? 'Failures' : url.startsWith('/api/push/') ? 'Push notifications' : url.startsWith('/api/servers') ? 'Reports' : 'Streams';
      const documented = { ...schema, tags: schema?.tags ?? [tag], summary: schema?.summary ?? `${method} ${url}`, security: url === '/health' ? [] : [{ sessionCookie: [] }] };
      if ((method === 'POST' || method === 'PUT') && /^\/api\/pipeline\/channels(?:\/:triggerId)?$/.test(url)) {
        documented.body = channelBody;
        documented.description = 'Save an acyclic channel topology with exactly one main CDN egress node. Probe targets are configured separately. Channel IDs are derived from the name at creation and stay fixed when renamed.';
      }
      if (method === 'PUT' && url.startsWith('/api/pipeline/probes/')) documented.body = {
        type: 'object', description: 'Choose either an HLS URL or a TCP host:port target. SRT server ping is separate from stream-health probing.',
        oneOf: [
          { type: 'object', required: ['hls'], properties: { hls: { type: 'string', example: 'https://example.com/live/index.m3u8' } } },
          { type: 'object', required: ['tcp'], properties: { tcp: { type: 'string', example: 'example.com:1935' } } },
        ],
      };
      if (url === '/api/streams/:id' && ['PATCH', 'DELETE'].includes(method)) {
        documented.headers = { type: 'object', required: ['If-Match'], properties: {
          'If-Match': { type: 'string', description: 'Current stream version from ETag. Missing: 428; outdated: 412.', example: '"1"' },
        } };
      }
      if (url === '/api/push/subscribe') documented.body = {
        type: 'object', required: ['endpoint', 'keys'], properties: {
          endpoint: { type: 'string', format: 'uri' }, expirationTime: { type: 'number', nullable: true },
          keys: { type: 'object', required: ['p256dh', 'auth'], properties: { p256dh: { type: 'string' }, auth: { type: 'string' } } },
        },
      };
      if (url.startsWith('/events/')) documented.description = 'Server-sent events (text/event-stream). This is a long-lived connection. Use EventSource rather than waiting for a complete JSON response.';
      return { schema: documented, url };
    },
  });
}

/** Auth hooks are already registered when the documentation pages and assets are mounted. */
export async function registerApiDocs(app: FastifyInstance): Promise<void> {
  app.get('/docs/data-flow', { schema: { hide: true } }, async (_request, reply) => {
    const html = await readFile(new URL('../../notes/GATE HLS Data Flow.html', import.meta.url), 'utf8');
    return reply.header('Content-Type', 'text/html; charset=utf-8').header('Cache-Control', 'no-cache')
      .header('X-Content-Type-Options', 'nosniff')
      .header('Content-Security-Policy', "default-src 'none'; style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; font-src https://fonts.gstatic.com; img-src 'self' data:; base-uri 'none'; frame-ancestors 'none'")
      .send(html);
  });
  app.get('/api/openapi.json', { schema: { hide: true } }, async (_request, reply) => {
    reply.header('Cache-Control', 'no-store');
    return app.swagger();
  });
  await app.register(scalar, {
    routePrefix: '/docs/scalar',
    configuration: { title: 'HLS Monitor API', url: '/api/openapi.json', theme: 'kepler', darkMode: true, proxyUrl: '' },
  });
}
