import { ConfigurationTransfer } from './configuration-transfer.js';
import { MonitoringResetService } from './monitoring-reset.js';
import { SrtReachabilityMonitor } from '../monitoring/srt-reachability.js';
import { MongoStreamInventoryRepository } from "../inventory/adapters/mongo-stream-inventory.repository.js";
import { MongoUserStore } from "../auth/users.js";
import { RedisSessionStore } from "../auth/redis-session-store.js";
import { RedisLoginLimiter } from "../http/auth.js";
import { StreamService } from "../inventory/stream-service.js";
import { InventorySyncWorker } from "../inventory/inventory-sync-worker.js";
import { LimitedHlsHealthChecker } from "../monitoring/limited-hls-health-checker.js";
import { loadProbeInventory } from "../monitoring/load-probe-inventory.js";
import { RedisProbeScheduleStore } from "../monitoring/adapters/redis-probe-schedule.store.js";
import { RedisPlaylistFreshnessStore } from "../monitoring/adapters/redis-playlist-freshness.store.js";
import { LiveHlsHealthChecker } from "../monitoring/live-hls-health-checker.js";
import { ProbeScheduler } from "../monitoring/probe-scheduler.js";
import { RedisStatusEventStore } from "../realtime/adapters/redis-status-event.store.js";
import { mongoConfig } from "../config/mongo.config.js";
import { RedisDeadLetterService } from "../dlq/adapters/redis-dead-letter.service.js";
import { MongoFailureHistoryStore } from "../dlq/adapters/mongo-failure-history.store.js";
import { FailureHandler } from "../dlq/failure-handler.js";
import { RetryPolicy } from "../dlq/retry-policy.js";
import { TrackingAcknowledger } from "../dlq/tracking-acknowledger.js";
import { DeadLetterProjector } from "../dlq/dead-letter-projector.js";
import { redisConfig } from "../config/redis.config.js";
import { DatabaseManager } from "../config/index.js";
import { createMetricsStore } from "../storage/create-metrics-store.js";
import { createStatusTransport } from "../realtime/create-status-transport.js";
import { RedisStreamAdapter } from "../messaging/adapters/redis-stream.adapter.js";
import { ServerBatchQueueManager } from "../services/batch-queue.service.js";
import { HlsMessageProcessor } from "../services/hls-message-processor.service.js";
import { HlsConsumer } from "../consumers/hls.consumer.js";
import { MongoChannelStore } from "../monitoring/adapters/mongo-channel.store.js";
import { PipelineMonitor, webhookNotifier } from "../monitoring/pipeline-monitor.js";
import { MongoPipelineIncidentStore } from "../monitoring/adapters/mongo-pipeline-incident.store.js";
import { MongoProbeTargetStore } from "../monitoring/adapters/mongo-probe-target.store.js";
import { PipelineProbeService } from "../monitoring/pipeline-probe.service.js";
import { PipelineRcaEngine } from "../services/pipeline-rca.service.js";
import { MongoMetricsRollup } from "../storage/adapters/mongo-metrics.rollup.js";
import { CheckWatchdog } from "../monitoring/check-watchdog.js";
import { WebPushService, allNotifiers } from "../notify/web-push.js";
import { HLS_METRICS_DATASET } from "../types/hls-metric.types.js";

/** Composition root: concrete adapters are constructed and shared only here. */
export function createApplicationRuntime() {
  const store = createMetricsStore();

  const statusEvents = new RedisStatusEventStore(redisConfig.getClient());
  const statusTransport = createStatusTransport(statusEvents);

  const messaging = new RedisStreamAdapter(redisConfig.getClient());

  const retryPolicy = new RetryPolicy({
    maxAttempts: Number(process.env['DLQ_MAX_ATTEMPTS'] ?? 5),
    baseDelayMs: Number(process.env['DLQ_RETRY_BASE_MS'] ?? 1000),
    maxDelayMs: Number(process.env['DLQ_RETRY_MAX_MS'] ?? 60_000),
  });
  const deadLetters = new RedisDeadLetterService(redisConfig.getClient());
  const failures = new FailureHandler(deadLetters, retryPolicy);

  const acknowledger = new TrackingAcknowledger(messaging, deadLetters);

  const failureHistory = new MongoFailureHistoryStore(mongoConfig);
  const failureProjector = new DeadLetterProjector(deadLetters, failureHistory);
  const batchQueueManager = new ServerBatchQueueManager({ store, acknowledger, failures, retryPolicy });
  const probeNamespace = process.env['HLS_PROBE_NAMESPACE'] ?? 'default';
  const probeScheduleStore = new RedisProbeScheduleStore(redisConfig.getClient(), probeNamespace);
  const streamInventory = new MongoStreamInventoryRepository(mongoConfig, probeNamespace);
  const streamService = new StreamService(streamInventory);
  const inventorySync = new InventorySyncWorker(streamInventory, probeScheduleStore, {
    batchSize: Number(process.env['HLS_SYNC_BATCH_SIZE'] ?? 20),
    pollMs: Number(process.env['HLS_SYNC_POLL_MS'] ?? 1000),
  });
  const playlistFreshness = new RedisPlaylistFreshnessStore(redisConfig.getClient(), probeNamespace);
  // Read config from the Redis projection so probes don't depend on Mongo per check.
  const liveChecker = new LiveHlsHealthChecker(probeScheduleStore, playlistFreshness);
  // Sized for 200 streams every 10 s even when all are down and each probe hits its 6 s timeout (200 × 6 s / 64 ≈ 19 s).
  const healthChecker = new LimitedHlsHealthChecker(liveChecker, Number(process.env['HLS_MAX_CONCURRENT_PROBES'] ?? 64), Number(process.env['HLS_MAX_WAITING_PROBES'] ?? 256));
  const processor = new HlsMessageProcessor(healthChecker, statusTransport, batchQueueManager);
  const hlsConsumer = new HlsConsumer({ source: messaging, processor, failures });
  // Per-server parallel checks: one dead server with 20 streams must still finish a round in ~12 s, not 40 s.
  const serverProbeConcurrency = Number(process.env['HLS_SERVER_PROBE_CONCURRENCY'] ?? 10);
  const probeScheduler = new ProbeScheduler(probeScheduleStore, probeScheduleStore, {
    consume: serverIp => hlsConsumer.consume(serverIp, undefined, { healthCheckConcurrency: serverProbeConcurrency }),
  }, {
    // 50 per 500 ms = 100 jobs/s, room for 200 streams at 10 s (the old 10 had no headroom).
    batchSize: Number(process.env['HLS_SCHEDULER_BATCH_SIZE'] ?? 50),
    random: () => 0,
  });
  const monitoringFlag = process.env['HLS_MONITORING_ENABLED'];
  if (monitoringFlag !== undefined && !['true', 'false'].includes(monitoringFlag)) throw new Error('HLS_MONITORING_ENABLED must be true or false');
  const configFile = process.env['HLS_STREAMS_FILE'];
  // On by default now that streams are managed from the signed-in dashboard; HLS_MONITORING_ENABLED=false turns it off.
  const monitoringEnabled = monitoringFlag !== 'false';
  // Multi-hop pipeline: users enter probe targets in the dashboard (MongoDB); hops without one stay UNPROBED.
  // HLS targets become streams, so the regular stream flow checks them and the monitor reads their live status.
  const pipelineChannels = new MongoChannelStore(mongoConfig);
  const probeTargetStore = new MongoProbeTargetStore(mongoConfig);
  const pipelineProbes = new PipelineProbeService(probeTargetStore, {
    findIdentity: (serverIp, streamId) => streamInventory.findIdentity(serverIp, streamId),
    create: value => streamService.create(value),
    patch: (id, version, patch) => streamService.patch(id, version, patch),
    remove: (id, version) => streamService.remove(id, version),
  });
  const srtReachability = new SrtReachabilityMonitor(() => pipelineChannels.list());
  const pipelineIncidents = new MongoPipelineIncidentStore(mongoConfig);
  const webhook = process.env['PIPELINE_ALERT_WEBHOOK_URL'];
  // Incident alerts go to the webhook (if set) and to every device that turned on background alerts.
  const push = new WebPushService(() => mongoConfig.getDb(), { ...(process.env['VAPID_SUBJECT'] ? { subject: process.env['VAPID_SUBJECT'] } : {}) });
  const notify = allNotifiers(webhook ? webhookNotifier(webhook) : undefined, push.notifier());
  const pipelineMonitor = new PipelineMonitor({
    channels: () => pipelineChannels.list(), targets: () => pipelineProbes.all(), rca: new PipelineRcaEngine(), incidents: pipelineIncidents,
    liveStatus: async () => (await statusEvents.snapshot()).streams,
    notify,
    intervalMs: Number(process.env['PIPELINE_INTERVAL_MS'] ?? 10_000),
    holdDownMs: Number(process.env['PIPELINE_HOLD_DOWN_MS'] ?? 30_000),
  });
  // Hourly summaries the server report reads; refreshed every minute, rebuilt from raw checks on first start.
  const metricsRollup = new MongoMetricsRollup(() => mongoConfig.getDb());
  // Alerts when the monitor itself stops saving checks (scheduler, consumers or batch writes broken).
  const checkWatchdog = new CheckWatchdog({
    lastSavedAt: async () => (await mongoConfig.getDb().collection(HLS_METRICS_DATASET).find({}, { projection: { ingestedAt: 1 }, sort: { ingestedAt: -1 }, limit: 1, maxTimeMS: 5000 }).next())?.['ingestedAt'] ?? null,
    expectChecks: async () => (await streamService.list(100)).items.some(r => r.config.enabled),
    incidents: pipelineIncidents,
    notify,
    stalledAfterMs: Number(process.env['CHECKS_STALLED_AFTER_MS'] ?? 180_000),
  });
  const monitoring = {
    async start() {
      await streamInventory.initialize();
      metricsRollup.start(); // also when monitoring is off: the report still reads stored checks
      if (!monitoringEnabled) return;
      const wasReset = await mongoConfig.getDb().collection<{ _id: string }>('app_settings').findOne({ _id: 'monitoring-reset' });
      if (configFile && !wasReset) await loadProbeInventory(configFile, { upsert: config => streamService.seed(config) });
      // Migrate legacy Redis registrations without overwriting DB/API versions.
      for (const config of wasReset ? [] : await probeScheduleStore.legacy()) await streamService.seed(config);
      // Pipeline HLS targets saved before they became streams, or whose stream was deleted, get it back.
      await pipelineProbes.relinkAll().catch(() => console.error('Pipeline: could not re-register probe target streams; they are retried on the next start'));
      await inventorySync.start();
      await probeScheduler.start();
      pipelineMonitor.start();
      srtReachability.start();
      checkWatchdog.start();
    },
    async stop() { await checkWatchdog.stop(); await metricsRollup.stop(); await srtReachability.stop(); await pipelineMonitor.stop(); await probeScheduler.stop(); await inventorySync.stop(); healthChecker.stop(); },
  };
  const dbManager = new DatabaseManager(store, redisConfig);
  const users = new MongoUserStore(mongoConfig);
  const resetDatabase = new MonitoringResetService({
    db: () => mongoConfig.getDb(), redis: redisConfig.getClient(),
    async quiesce() {
      await checkWatchdog.stop();
      await metricsRollup.stop();
      await srtReachability.stop();
      await pipelineMonitor.stop();
      await probeScheduler.stop();
      await inventorySync.stop();
      await hlsConsumer.pause();
      await batchQueueManager.reset();
      await failureProjector.stop();
    },
    clearMemory() { pipelineMonitor.reset(); srtReachability.reset(); checkWatchdog.reset(); },
    async resume() {
      metricsRollup.start();
      failureProjector.start();
      if (monitoringEnabled) {
        await inventorySync.start(); await probeScheduler.start();
        pipelineMonitor.start(); srtReachability.start(); checkWatchdog.start();
      }
    },
  });
  const configurationTransfer = new ConfigurationTransfer(pipelineChannels, probeTargetStore, streamService);
  const auth = { configurationTransfer, resetDatabase, users, sessionStore: new RedisSessionStore(redisConfig.getClient()), secret: process.env['SESSION_SECRET'] ?? '', limiter: new RedisLoginLimiter(redisConfig.getClient()) };
  const pipeline = { srtReachability, channels: pipelineChannels, probes: pipelineProbes, monitor: pipelineMonitor, incidents: pipelineIncidents };
  return { configurationTransfer, resetDatabase, push, checkWatchdog, auth, users, pipeline, pipelineMonitor, streams: { reportGeneration: () => resetDatabase.generation, service: streamService, failures: failureHistory, metrics: store, replay: (target: Parameters<RedisDeadLetterService['replay']>[0]) => deadLetters.replay(target) }, streamService, inventorySync, probeScheduleStore, streamInventory, playlistFreshness, probeScheduler, monitoring, healthChecker, statusEvents, deadLetters, failures, acknowledger, failureProjector, retryPolicy, store, statusTransport, messaging, processor, hlsConsumer, batchQueueManager, dbManager };
}
