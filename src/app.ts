import { registerApiSpecification, registerApiDocs } from './http/api-docs.js';
import { registerStreamRoutes } from "./http/stream-routes.js";
import { registerPipelineRoutes } from "./http/pipeline-routes.js";
import { registerPushRoutes } from "./http/push-routes.js";
import { registerDashboard } from "./http/dashboard.js";
import { registerAuth } from "./http/auth.js";
import Fastify, { type FastifyRequest, type FastifyInstance } from "fastify";
import type { ApplicationServices } from "./application/contracts.js";

export async function buildApp(services: ApplicationServices): Promise<FastifyInstance> {
  const app = Fastify({
    logger: true,
    // Behind the API gateway: its IPs or CIDRs (comma-separated). Then request.ip is the real client, so the
    // login limiter counts each client separately. Unset = trust no proxy (X-Forwarded-For is ignored).
    trustProxy: process.env['TRUST_PROXY'] || false,
    ajv: { customOptions: { removeAdditional: false } },
  });

  const { dbManager, statusTransport, hlsConsumer, batchQueueManager } = services;

  await registerApiSpecification(app);

  let resetting = false;
  let resetJob: Promise<void> | undefined;
  const writes = new Set<FastifyRequest>();
  const drained = new Set<() => void>();
  app.addHook('onRequest', async (request, reply) => {
    const path = request.url.split('?')[0];
    if (path === '/api/me/reset-database' || path === '/api/me/configuration/import') return;
    if (resetting && /^\/(api|events)(\/|$)/.test(request.url)) return reply.code(503).header('Retry-After', '5').send({ error: 'Data maintenance in progress. Try again shortly.' });
    if (!['GET', 'HEAD', 'OPTIONS'].includes(request.method) && request.url.startsWith('/api/')) writes.add(request);
  });
  app.addHook('onResponse', async request => {
    writes.delete(request);
    if (!writes.size) { for (const done of drained) done(); drained.clear(); }
  });
  async function exclusive<T>(work: () => Promise<T>): Promise<T> {
    if (resetting) throw new Error('Data maintenance is already running');
    resetting = true;
    const job = (async () => {
      if (writes.size) await new Promise<void>((resolve, reject) => {
        const done = () => { clearTimeout(timer); resolve(); };
        const timer = setTimeout(() => { drained.delete(done); reject(new Error('Active writes did not drain')); }, 15000);
        drained.add(done);
      });
      return work();
    })().finally(() => { resetting = false; resetJob = undefined; });
    resetJob = job.then(() => {}, () => {});
    return job;
  }
  const reset = services.resetDatabase;
  const guardedReset = reset && {
    get running() { return resetting || reset.running; }, get generation() { return reset.generation; },
    run: () => exclusive(() => reset.run()),
  };
  const transfer = services.configurationTransfer;
  const guardedTransfer = transfer && {
    export: () => exclusive(() => transfer.export()), preview: (bundle: unknown) => transfer.preview(bundle),
    import: (bundle: unknown, by: string) => exclusive(() => transfer.import(bundle, by)),
  };

  // Auth first, so its guard sits in front of every route registered below.
  if (services.auth !== 'disabled') await registerAuth(app, { ...services.auth, ...(guardedReset ? { resetDatabase: guardedReset } : {}), ...(guardedTransfer ? { configurationTransfer: guardedTransfer } : {}) });

  await registerApiDocs(app);

  // Each selected transport registers its endpoints and owns connection cleanup.
  await statusTransport.register(app);
  if (services.streams) registerStreamRoutes(app, services.streams);
  if (services.pipeline) registerPipelineRoutes(app, services.pipeline);
  if (services.push) registerPushRoutes(app, services.push);
  await registerDashboard(app);

  app.addHook('onReady', async () => {
    await services.monitoring?.start();
    services.failureProjector?.start();
  });

  // Health check endpoint with database status
  app.get("/health", async (_request, reply) => {
    const health = await dbManager.healthCheck();
    const isHealthy = health.redis && health.storage;

    return reply.status(isHealthy ? 200 : 503).send({
      status: isHealthy ? "healthy" : "unhealthy",
      timestamp: new Date().toISOString(),
      databases: health,
      // Informational: a stalled monitor is an incident and an alert, not a reason to take this instance out of service.
      ...(services.checkWatchdog ? { checks: services.checkWatchdog.status() } : {}),
    });
  });

  // Ensure graceful shutdown: flush pending queues first, then disconnect databases
  app.addHook("onClose", async () => {
    await resetJob?.catch(() => {});
    await services.monitoring?.stop();
    await hlsConsumer.stop();
    await batchQueueManager.stopAll();
    await services.failureProjector?.stop();
    await dbManager.disconnectAll();
  });

  return app;
}
