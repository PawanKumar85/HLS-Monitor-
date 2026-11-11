import type { FastifyInstance, FastifyRequest } from 'fastify';
import { parseSubscription, type WebPushService } from '../notify/web-push.js';
import type { SessionUser } from '../auth/users.js';

export type PushApi = Pick<WebPushService, 'publicKey' | 'subscribe' | 'unsubscribe' | 'isSubscribed' | 'send'>;
const userId = (request: FastifyRequest) => (request as { user?: SessionUser }).user?.id ?? 'anonymous';
const endpointBody = { type: 'object', required: ['endpoint'], additionalProperties: false, properties: { endpoint: { type: 'string', minLength: 1, maxLength: 2048 } } };

/**
 * Background alerts on this device. Endpoints are capability URLs, so they travel in request bodies, never in
 * URLs (which end up in logs). Sign-in is enforced app-wide by registerAuth.
 */
export function registerPushRoutes(app: FastifyInstance, push: PushApi): void {
  app.register(async routes => {
    routes.addHook('onRequest', async (_request, reply) => { reply.header('Cache-Control', 'no-store'); });
    routes.get('/api/push/key', async () => ({ publicKey: await push.publicKey() }));
    routes.post('/api/push/subscribe', { bodyLimit: 4096 }, async (request, reply) => {
      let subscription; try { subscription = parseSubscription(request.body); } catch (error) { return reply.code(400).send({ error: (error as Error).message }); }
      await push.subscribe(userId(request), subscription);
      return reply.code(201).send({ subscribed: true });
    });
    routes.post<{ Body: { endpoint: string } }>('/api/push/unsubscribe', { schema: { body: endpointBody } }, async (request, reply) => {
      await push.unsubscribe(request.body.endpoint);
      return reply.code(204).send();
    });
    routes.post<{ Body: { endpoint: string } }>('/api/push/status', { schema: { body: endpointBody } }, async request => ({ subscribed: await push.isSubscribed(request.body.endpoint) }));
    routes.post('/api/push/test', async request => ({
      delivered: await push.send({ title: 'Test alert from HLS monitor', body: 'Background alerts work on this device.', tag: 'test', url: '/', critical: false }, userId(request)),
    }));
  });
}
