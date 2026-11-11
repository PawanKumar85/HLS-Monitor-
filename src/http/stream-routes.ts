import type { FastifyInstance } from 'fastify';
import { redactUrl } from '../dlq/redact.js';
import { InventoryConflictError, InventoryNotFoundError, InventoryValidationError, InventoryVersionError,
  type NewStream, type StreamManagement, type StreamPatch, type StreamRegistration } from '../inventory/stream-inventory.js';
import type { FailureFeed } from '../dlq/failure-feed.js';
import { registerFailureEvents } from './failure-events.js';
import type { MetricsReader } from '../storage/metrics-reader.js';

export interface StreamApi {
  reportGeneration?: () => number;
  service: StreamManagement; failures?: FailureFeed; metrics?: MetricsReader;
  /** Puts a dead-lettered message back for processing. False when it is no longer dead-lettered. */
  replay?: (target: { originalStreamKey: string; originalMessageId: string; groupName: string; serverIp: string }) => Promise<boolean>;
}
const REPORT_CACHE_MS = 60_000;
function date(value: string | undefined, name: string): Date | undefined {
  if (value === undefined) return undefined;
  const parsed = new Date(value);
  if (!Number.isFinite(parsed.getTime())) throw new InventoryValidationError(`${name} must be an ISO date`);
  return parsed;
}
const uuid = '^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$';
const properties = {
  serverIp: { type: 'string', minLength: 1, maxLength: 255 }, streamId: { type: 'string', minLength: 1, maxLength: 1024 },
  streamUrl: { type: 'string', minLength: 1, maxLength: 8192 }, intervalMs: { type: 'integer', minimum: 1000, maximum: 2147483647 },
  staleAfterMs: { type: 'integer', minimum: 1000, maximum: 2147483647 }, enabled: { type: 'boolean' },
};
const params = { type: 'object', required: ['id'], properties: { id: { type: 'string', pattern: uuid } }, additionalProperties: false };
function view(record: StreamRegistration) {
  const streamUrl = redactUrl(record.config.streamUrl);
  return { id: record.id, ...record.config, streamUrl, streamUrlRedacted: streamUrl !== record.config.streamUrl,
    version: record.version, pendingSync: record.pendingSync, createdAt: record.createdAt, updatedAt: record.updatedAt, syncedAt: record.syncedAt ?? null };
}
class MissingVersionError extends Error {}
function version(header: string | string[] | undefined): number {
  if (header === undefined) throw new MissingVersionError();
  const match = typeof header === 'string' && /^(?:"([1-9]\d*)"|([1-9]\d*))$/.exec(header);
  const value = match ? Number(match[1] ?? match[2]) : Number.NaN;
  if (!Number.isSafeInteger(value) || value < 1 || value >= Number.MAX_SAFE_INTEGER) throw new InventoryValidationError('Invalid If-Match version');
  return value;
}
/** Stream API. Sign-in is enforced for the whole app by registerAuth (src/http/auth.ts), not here. */
export function registerStreamRoutes(app: FastifyInstance, api: StreamApi): void {
  app.register(async routes => {
    routes.addHook('onRequest', async (_request, reply) => { reply.header('Cache-Control', 'no-store'); });
    if (api.failures) registerFailureEvents(routes, api.failures);
    const failures = api.failures;
    if (failures?.summary) routes.get('/api/failures/summary', async () => failures.summary!());
    if (failures?.search) routes.get<{ Querystring: { resolution?: string; stage?: string; q?: string; before?: string; limit?: number } }>('/api/failures', {
      schema: { querystring: { type: 'object', additionalProperties: false, properties: {
        resolution: { enum: ['OPEN', 'REPLAY_REQUESTED', 'RESOLVED'] }, stage: { enum: ['PARSE', 'HEALTH_CHECK', 'QUEUE', 'PERSISTENCE'] },
        q: { type: 'string', maxLength: 200 }, before: { type: 'string', maxLength: 64 }, limit: { type: 'integer', minimum: 1, maximum: 200 } } } },
    }, async (request, reply) => {
      try { return await failures.search!(request.query); }
      catch (error) { if (error instanceof RangeError) return reply.code(400).send({ error: error.message }); throw error; }
    });
    if (failures?.replayTarget && api.replay) routes.post<{ Params: { failureId: string } }>('/api/failures/:failureId/replay', {
      schema: { params: { type: 'object', required: ['failureId'], properties: { failureId: { type: 'string', minLength: 1, maxLength: 128 } } } },
    }, async (request, reply) => {
      const target = await failures.replayTarget!(request.params.failureId);
      if (!target) return reply.code(404).send({ error: 'Failure not found' });
      const notWaiting = 'This failure is no longer waiting in the dead-letter queue (already replayed or resolved).';
      try {
        if (!await api.replay!(target)) return reply.code(409).send({ error: notWaiting });
      } catch (error) {
        // The replay script refuses these two states; they are answers for the operator, not server faults.
        const message = error instanceof Error ? error.message : '';
        if (message.includes('not dead-lettered')) return reply.code(409).send({ error: notWaiting });
        if (message.includes('Original message no longer exists')) return reply.code(409).send({ error: 'The original message is gone from Redis, so it cannot be replayed.' });
        throw error;
      }
      return reply.code(202).send({ replayRequested: true });
    });
    routes.setErrorHandler((error, request, reply) => {
      const details = error && typeof error === 'object' ? error as { validation?: unknown; statusCode?: number } : {};
      if (error instanceof MissingVersionError) return reply.code(428).send({ error: 'If-Match version required' });
      if (error instanceof InventoryValidationError || details.validation) return reply.code(400).send({ error: error instanceof InventoryValidationError ? error.message : 'Invalid stream request' });
      if (error instanceof InventoryNotFoundError) return reply.code(404).send({ error: error.message });
      if (error instanceof InventoryConflictError) return reply.code(409).send({ error: error.message });
      if (error instanceof InventoryVersionError) return reply.code(412).send({ error: error.message });
      // Fastify client errors (bad JSON, empty body, 413, 415) are not inventory outages.
      if (details.statusCode && details.statusCode >= 400 && details.statusCode < 500) return reply.code(details.statusCode).send({ error: details.statusCode === 413 ? 'Request body too large' : 'Invalid stream request' });
      request.log.error('Stream inventory operation unavailable');
      return reply.code(503).send({ error: 'Stream inventory unavailable' });
    });
    routes.get<{ Querystring: { limit?: number; cursor?: string } }>('/api/streams', {
      schema: { querystring: { type: 'object', additionalProperties: false, properties: { limit: { type: 'integer', minimum: 1, maximum: 100 }, cursor: { type: 'string', pattern: uuid } } } },
    }, async request => {
      const page = await api.service.list(request.query.limit, request.query.cursor);
      return { items: page.items.map(view), ...(page.nextCursor ? { nextCursor: page.nextCursor } : {}) };
    });
    routes.get<{ Params: { id: string } }>('/api/streams/:id', { schema: { params } }, async (request, reply) => {
      const record = await api.service.get(request.params.id);
      return reply.header('ETag', `"${record.version}"`).send(view(record));
    });
    routes.post<{ Body: NewStream }>('/api/streams', { bodyLimit: 16 * 1024,
      schema: { body: { type: 'object', additionalProperties: false, required: ['streamId', 'streamUrl'], properties } },
    }, async (request, reply) => {
      const record = await api.service.create(request.body);
      return reply.code(201).header('Location', `/api/streams/${record.id}`).header('ETag', `"${record.version}"`).send(view(record));
    });
    routes.patch<{ Params: { id: string }; Body: StreamPatch }>('/api/streams/:id', { bodyLimit: 16 * 1024,
      schema: { params, body: { type: 'object', minProperties: 1, additionalProperties: false, properties: { streamUrl: properties.streamUrl, intervalMs: properties.intervalMs, staleAfterMs: properties.staleAfterMs, enabled: properties.enabled } } },
    }, async (request, reply) => {
      const record = await api.service.patch(request.params.id, version(request.headers['if-match']), request.body);
      return reply.header('ETag', `"${record.version}"`).send(view(record));
    });
    routes.delete<{ Params: { id: string } }>('/api/streams/:id', { schema: { params } }, async (request, reply) => {
      const record = await api.service.remove(request.params.id, version(request.headers['if-match']));
      return reply.code(202).header('ETag', `"${record.version}"`).send({ id: record.id, version: record.version, pendingSync: record.pendingSync });
    });
    const metrics = api.metrics;
    if (!metrics) return;
    routes.get<{ Params: { id: string }; Querystring: { from?: string; to?: string; limit?: number } }>('/api/streams/:id/history', {
      schema: { params, querystring: { type: 'object', additionalProperties: false, properties: {
        from: { type: 'string', maxLength: 64 }, to: { type: 'string', maxLength: 64 }, limit: { type: 'integer', minimum: 1, maximum: 500 } } } },
    }, async request => {
      const { serverIp, streamId } = (await api.service.get(request.params.id)).config;
      const from = date(request.query.from, 'from'); const to = date(request.query.to, 'to');
      if (from && to && from > to) throw new InventoryValidationError('from must be before to');
      const rows = await metrics.readHistory({ serverIp, streamId, ...(from ? { from } : {}), ...(to ? { to } : {}), ...(request.query.limit ? { limit: request.query.limit } : {}) });
      // Newest first. streamUrl is left out: stored URLs may carry tokens.
      return { serverIp, streamId, items: rows.map(row => ({ checkedAt: row.checkedAt, status: row.health?.status ?? null,
        latencyMs: row.health?.segmentLatencyMs ?? null, pipelineDelayMs: row.pipelineDelayMs })) };
    });
    routes.get<{ Params: { serverIp: string }; Querystring: { hours?: number } }>('/api/servers/:serverIp/details', {
      schema: { params: { type: 'object', required: ['serverIp'], properties: { serverIp: { type: 'string', minLength: 1, maxLength: 255 } } },
        querystring: { type: 'object', additionalProperties: false, properties: { hours: { type: 'integer', minimum: 1, maximum: 168 } } } },
    }, async request => {
      const to = new Date(); const hours = request.query.hours ?? 24;
      return metrics.serverDetails(request.params.serverIp, new Date(to.getTime() - hours * 3_600_000), to);
    });

    // Every open Reports tab refreshes each minute; share one computed report per window for 60 s, and let
    // simultaneous requests wait for the same query. shortcut: cache is per process; each instance keeps its own.
    let reportGeneration = api.reportGeneration?.();
    const reports = new Map<number, { at: number; value: Promise<unknown> }>();
    routes.get<{ Querystring: { hours?: number } }>('/api/servers/report', {
      schema: { querystring: { type: 'object', additionalProperties: false, properties: { hours: { type: 'integer', minimum: 1, maximum: 168 } } } },
    }, async request => {
      if (reportGeneration !== api.reportGeneration?.()) { reports.clear(); reportGeneration = api.reportGeneration?.(); }
      const hours = request.query.hours ?? 24;
      const cached = reports.get(hours);
      if (cached && Date.now() - cached.at < REPORT_CACHE_MS) return cached.value;
      const value = (async () => {
        const to = new Date(); const from = new Date(to.getTime() - hours * 3_600_000);
        return { from, to, hours, servers: await metrics.serverReport(from, to) };
      })();
      reports.set(hours, { at: Date.now(), value });
      value.catch(() => { if (reports.get(hours)?.value === value) reports.delete(hours); }); // never cache a failure
      return value;
    });
  });
}
