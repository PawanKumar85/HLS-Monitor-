import { test, mock } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { Redis } from 'ioredis';
import { MongoClient } from 'mongodb';
import { RedisDeadLetterService } from '../../src/dlq/adapters/redis-dead-letter.service.js';
import { MongoFailureHistoryStore } from '../../src/dlq/adapters/mongo-failure-history.store.js';
import { FailureHandler } from '../../src/dlq/failure-handler.js';
import { RetryPolicy } from '../../src/dlq/retry-policy.js';
import { DeadLetterProjector } from '../../src/dlq/dead-letter-projector.js';
import { TrackingAcknowledger } from '../../src/dlq/tracking-acknowledger.js';
import { ProcessingFailure, type MessageContext } from '../../src/dlq/dead-letter.types.js';
import { RedisStreamAdapter } from '../../src/messaging/adapters/redis-stream.adapter.js';
import { MongoMetricsStore } from '../../src/storage/adapters/mongo-metrics.store.js';
import { HlsMessageProcessor } from '../../src/services/hls-message-processor.service.js';
import { HlsConsumer } from '../../src/consumers/hls.consumer.js';
import { ServerBatchQueueManager, UnsortedBatchQueue } from '../../src/services/batch-queue.service.js';
import { parseHlsStreamMessage } from '../../src/utils/parse-hls-stream-message.js';

async function until(check: () => Promise<boolean> | boolean) {
  const end = Date.now() + 10000;
  while (!await check()) { if (Date.now() > end) throw new Error('Integration timeout'); await new Promise((resolve) => setTimeout(resolve, 10)); }
}

test('real Redis DLQ, retries, partial writes, archival and controlled replay', { skip: process.env['RUN_INTEGRATION'] !== '1', timeout: 60000 }, async (t) => {
  assert.match(process.env['MONGO_DATABASE'] ?? '', /^gate_test_[a-zA-Z0-9_]+$/);
  assert.ok(process.env['MONGO_URI']);
  const prefix = `dlq-test-${randomUUID()}`;
  const redis = new Redis({ host: process.env['REDIS_HOST'] ?? '127.0.0.1', port: Number(process.env['REDIS_PORT'] ?? 6379), password: process.env['REDIS_PASSWORD'] ?? 'admin', lazyConnect: true });
  const mongo = new MongoClient(process.env['MONGO_URI']!);
  const db = mongo.db(`${process.env['MONGO_DATABASE']}_dlq_${randomUUID().replaceAll('-', '')}`);
  const connection = { getDb: () => db, async connect() { await mongo.connect(); return db; }, async disconnect() {} };
  const store = new MongoMetricsStore(connection);
  const ledger = new RedisDeadLetterService(redis);
  const policy = new RetryPolicy({ maxAttempts: 3, baseDelayMs: 1, maxDelayMs: 2 });
  const failures = new FailureHandler(ledger, policy);
  const messaging = new RedisStreamAdapter(redis);
  const acknowledger = new TrackingAcknowledger(messaging, ledger);
  const keys = new Set<string>();
  const servers = new Set<string>();
  const queues: UnsortedBatchQueue[] = [];
  const consumers: HlsConsumer[] = [];
  t.after(async () => {
    mock.restoreAll();
    await Promise.allSettled(consumers.map((c) => c.stop()));
    await Promise.allSettled(queues.map((q) => q.stop()));
    if (keys.size) await redis.del(...keys);
    if (servers.size) await redis.srem(ledger.registryKey, ...servers);
    await db.dropDatabase();
    await mongo.close();
    redis.disconnect();
  });
  await redis.connect();
  await store.connect();
  async function message(suffix: string, streamId = 'channel', groupName = 'group') {
    const serverIp = `${prefix}-${suffix}`;
    servers.add(serverIp);
    const stream = `hls:streams:${serverIp}`;
    keys.add(stream); keys.add(`hls:dlq:${serverIp}`);
    await messaging.ensureGroup(stream, groupName);
    const fields = ['streamId', streamId, 'streamUrl', 'https://user:password@example.test/live?token=private', 'serverIp', serverIp, 'createdAt', new Date().toISOString()];
    const id = (await redis.xadd(stream, '*', ...fields))!;
    await redis.xreadgroup('GROUP', groupName, 'reader', 'COUNT', 1, 'STREAMS', stream, '>');
    const context: MessageContext = { originalStreamKey: stream, originalMessageId: id, groupName, serverIp, consumerName: 'reader', rawFields: fields };
    keys.add(ledger.metadataKey(stream, groupName, id));
    return context;
  }
  const pending = async (ctx: MessageContext) => Number((await redis.xpending(ctx.originalStreamKey, ctx.groupName) as unknown[])[0]);

  await t.test('immediate poison-message transfer is idempotent and sanitized', async () => {
    const ctx = await message('poison');
    const error = new ProcessingFailure('PARSE', 'INVALID_MESSAGE', 'Bad https://user:password@example.test/live?token=private', false);
    assert.equal(await failures.failed(ctx, error), true);
    assert.equal(await pending(ctx), 0);
    assert.equal(await redis.xlen(`hls:dlq:${ctx.serverIp}`), 1);
    assert.equal(await failures.failed(ctx, error), true);
    assert.equal(await redis.xlen(`hls:dlq:${ctx.serverIp}`), 1);
    const raw = JSON.stringify(await redis.xrange(`hls:dlq:${ctx.serverIp}`, '-', '+'));
    assert.ok(!raw.includes('password') && !raw.includes('private'));
  });

  await t.test('known DLQ transfer reconciles a leftover pending ACK without appending again', async () => {
    const ctx = await message('reconcile-ack');
    await failures.failed(ctx, new ProcessingFailure('PARSE', 'INVALID', 'bad', false));
    await redis.xclaim(ctx.originalStreamKey, ctx.groupName, 'interrupted-ack', 0, ctx.originalMessageId, 'FORCE', 'JUSTID');
    assert.equal(await pending(ctx), 1);
    assert.equal(await failures.prepare(ctx), false);
    assert.equal(await pending(ctx), 0);
    assert.equal(await redis.xlen(`hls:dlq:${ctx.serverIp}`), 1);
  });

  await t.test('failure identity and ACK remain isolated across consumer groups', async () => {
    const ctx = await message('groups');
    const other = { ...ctx, groupName: 'second-group' };
    keys.add(ledger.metadataKey(other.originalStreamKey, other.groupName, other.originalMessageId));
    await messaging.ensureGroup(other.originalStreamKey, other.groupName);
    await redis.xreadgroup('GROUP', other.groupName, 'other', 'COUNT', 1, 'STREAMS', other.originalStreamKey, '>');
    await failures.failed(ctx, new ProcessingFailure('PARSE', 'INVALID', 'bad', false));
    assert.equal(await pending(ctx), 0);
    assert.equal(await pending(other), 1);
    await failures.failed(other, new ProcessingFailure('PARSE', 'INVALID', 'bad', false));
    assert.equal(await pending(other), 0);
    assert.equal(await redis.xlen(`hls:dlq:${ctx.serverIp}`), 2);
  });

  await t.test('DLQ WRONGTYPE failure never ACKs; retry finishes the original failure transfer', async () => {
    const ctx = await message('dlq-down');
    await redis.set(`hls:dlq:${ctx.serverIp}`, 'wrong type');
    await assert.rejects(failures.failed(ctx, new ProcessingFailure('PARSE', 'INVALID', 'bad', false)), /WRONGTYPE/);
    assert.equal(await pending(ctx), 1);
    await redis.del(`hls:dlq:${ctx.serverIp}`);
    assert.equal(await new FailureHandler(new RedisDeadLetterService(redis), policy).prepare(ctx), false);
    assert.equal(await pending(ctx), 0);
    assert.equal((await ledger.get(ctx))?.record.failureStage, 'PARSE');
  });

  await t.test('failure attempts survive handler restart and stop at the configured limit', async () => {
    const ctx = await message('retry');
    for (let i=1; i<=3; i++) {
      const replacement = new FailureHandler(new RedisDeadLetterService(redis), policy);
      assert.equal(await replacement.failed(ctx, new ProcessingFailure('HEALTH_CHECK', 'WORKER_EXCEPTION', 'worker failed')), i===3);
      assert.equal((await ledger.get(ctx))?.record.attempts, i);
    }
    assert.equal(await pending(ctx), 0);
    assert.equal(await redis.xlen(`hls:dlq:${ctx.serverIp}`), 1);
  });

  await t.test('lost transfer response retries without a duplicate DLQ event', async () => {
    const ctx = await message('lost-reply');
    const original = redis.eval.bind(redis);
    const fault = mock.method(redis, 'eval', async (...args: Parameters<typeof original>) => {
      const result = await original(...args);
      if (String(args[0]).includes("local pending = redis.call('XPENDING'")) throw new Error('reply lost');
      return result;
    });
    try { await assert.rejects(failures.failed(ctx, new ProcessingFailure('PARSE', 'INVALID', 'bad', false)), /reply lost/); }
    finally { fault.mock.restore(); }
    const state = (await ledger.get(ctx))!;
    await ledger.moveToDeadLetter(ctx, state.record);
    assert.equal(await pending(ctx), 0);
    assert.equal(await redis.xlen(`hls:dlq:${ctx.serverIp}`), 1);
  });

  await t.test('consumer routes invalid Redis fields through PARSE DLQ without probing', async () => {
    const ctx = await message('invalid-fields', '');
    const processor = new HlsMessageProcessor({ async check() { throw new Error('must not probe an invalid message'); } }, { publish() {} }, { async push() { throw new Error('must not queue'); } });
    const consumer = new HlsConsumer({ source: messaging, processor, failures });
    consumers.push(consumer);
    const running = consumer.consume(ctx.serverIp, undefined, { groupName: ctx.groupName, pendingMinIdleMs: 1, recoveryIntervalMs: 5, blockTimeoutMs: 5 });
    void running.catch(() => {});
    await until(async () => await pending(ctx) === 0);
    await consumer.stop(); await running;
    assert.equal((await ledger.get(ctx))?.record.failureStage, 'PARSE');
    assert.equal((await ledger.get(ctx))?.record.attempts, 1);
  });

  await t.test('consumer retries unexpected probe exceptions and dead-letters after three attempts', async () => {
    const ctx = await message('worker-error');
    let probes = 0;
    const processor = new HlsMessageProcessor({ async check() { probes++; throw new Error('worker crashed'); } }, { publish() {} }, { async push() { throw new Error('must not queue'); } });
    const consumer = new HlsConsumer({ source: messaging, processor, failures });
    consumers.push(consumer);
    const running = consumer.consume(ctx.serverIp, undefined, { groupName: ctx.groupName, pendingMinIdleMs: 1, recoveryIntervalMs: 5, blockTimeoutMs: 5 });
    void running.catch(() => {});
    await until(async () => await pending(ctx) === 0);
    await consumer.stop(); await running;
    assert.equal(probes, 3);
    assert.equal((await ledger.get(ctx))?.record.failureStage, 'HEALTH_CHECK');
  });

  await t.test('unordered validation failure isolates bad record and ACKs the persisted good record', async () => {
    await db.createCollection('partial_metrics', { validator: { streamId: { $ne: 'bad' } } });
    const good = await message('partial', 'good');
    const bad = await message('partial', 'bad');
    const queue = new UnsortedBatchQueue({ store, acknowledger, failures, retryPolicy: policy, dataset: 'partial_metrics', serverIp: good.serverIp, groupName: good.groupName });
    queues.push(queue);
    await queue.push(parseHlsStreamMessage(good.originalMessageId, good.rawFields), good);
    await queue.push(parseHlsStreamMessage(bad.originalMessageId, bad.rawFields), bad);
    await queue.stop();
    assert.equal(await pending(good), 0);
    assert.equal(await db.collection('partial_metrics').countDocuments(), 1);
    assert.equal(await redis.xlen(`hls:dlq:${good.serverIp}`), 1);
    assert.equal((await ledger.get(bad))?.record.errorCode, 'SCHEMA_VALIDATION');
    assert.equal(await ledger.get(good), null);
  });

  await t.test('global storage outage retains buffered pending messages instead of mass-DLQ', async () => {
    const ctx = await message('outage');
    let down = true;
    const queue = new UnsortedBatchQueue({ store: { async writeBatch() { if (down) throw new Error('DB offline'); } }, acknowledger, failures, retryPolicy: policy, serverIp: ctx.serverIp, groupName: ctx.groupName });
    queues.push(queue);
    await queue.push(parseHlsStreamMessage(ctx.originalMessageId, ctx.rawFields), ctx);
    for (let i=0; i<5; i++) await assert.rejects(queue.flush());
    assert.equal(await pending(ctx), 1);
    assert.equal(await redis.xlen(`hls:dlq:${ctx.serverIp}`), 0);
    assert.equal(queue.getStats().itemCount, 1);
    down = false;
    await queue.stop();
    assert.equal(await pending(ctx), 0);
  });

  await t.test('offline health is persisted normally and never dead-lettered', async () => {
    const ctx = await message('offline');
    const manager = new ServerBatchQueueManager({ store, acknowledger, failures, retryPolicy: policy, groupName: ctx.groupName });
    const processor = new HlsMessageProcessor({ async check() { return { status: 0, latency: 3000 }; } }, { publish() {} }, manager);
    const consumer = new HlsConsumer({ source: messaging, processor, failures });
    consumers.push(consumer);
    const running = consumer.consume(ctx.serverIp, undefined, { groupName: ctx.groupName, pendingMinIdleMs: 1, recoveryIntervalMs: 5, blockTimeoutMs: 5 });
    void running.catch(() => {});
    await until(() => manager.getQueue(ctx.serverIp).getStats().itemCount > 0);
    await consumer.stop(); await running; await manager.stopAll();
    assert.equal(await pending(ctx), 0);
    assert.equal(await redis.xlen(`hls:dlq:${ctx.serverIp}`), 0);
    assert.equal((await db.collection('hls_stream_metrics').findOne({ redisMessageId: ctx.originalMessageId, serverIp: ctx.serverIp }))?.['health'].status, 0);
  });

  let archiveOffline = true;
  const history = new MongoFailureHistoryStore(connection);
  const projector = new DeadLetterProjector({
    async servers() { return (await ledger.servers()).filter((server) => server.startsWith(prefix)); },
    readArchiveBatch: (server, consumer, count) => ledger.readArchiveBatch(server, consumer, count),
    archiveCompleted: (server, id) => ledger.archiveCompleted(server, id),
  }, { async write(record) { if (archiveOffline) throw new Error('history offline'); await history.write(record); } });

  await t.test('failure history outage retains Redis events; successful archive persists then trims', async () => {
    await assert.rejects(projector.flush(), /history offline/);
    archiveOffline = false;
    await projector.flush();
    assert.ok(await db.collection('hls_processing_failures').countDocuments() >= 4);
    for (const server of servers) assert.equal(await redis.xlen(`hls:dlq:${server}`), 0);
  });

  await t.test('an older projection cannot overwrite a newer resolution revision', async () => {
    const ctx = await message('revision');
    await failures.failed(ctx, new ProcessingFailure('PARSE', 'INVALID', 'bad', false));
    const record = (await ledger.get(ctx))!.record;
    await history.write({ ...record, revision: record.revision + 1, resolution: 'RESOLVED' });
    await history.write(record);
    assert.equal((await db.collection('hls_processing_failures').findOne({ originalStreamKey: ctx.originalStreamKey }))?.['resolution'], 'RESOLVED');
  });

  await t.test('archive limit holds across reclaimed, own-pending and fresh Redis deliveries', async () => {
    let ctx!: MessageContext;
    for (let index = 0; index < 7; index++) {
      ctx = await message('archive-limit');
      await failures.failed(ctx, new ProcessingFailure('PARSE', 'INVALID', 'bad', false));
    }
    const key = `hls:dlq:${ctx.serverIp}`;
    await redis.xgroup('CREATE', key, ledger.archiveGroup, '0');
    const old = await redis.xreadgroup('GROUP', ledger.archiveGroup, 'old-archive', 'COUNT', 2, 'STREAMS', key, '>') as [string, [string, string[]][]][];
    await redis.xreadgroup('GROUP', ledger.archiveGroup, 'current-archive', 'COUNT', 2, 'STREAMS', key, '>');
    for (const [id] of old[0]![1]) await redis.xclaim(key, ledger.archiveGroup, 'old-archive', 0, id, 'IDLE', 61_000);
    const seen = new Set<string>();
    for (let round = 0; round < 5 && seen.size < 7; round++) {
      const entries = await ledger.readArchiveBatch(ctx.serverIp, 'current-archive', 3);
      assert.ok(entries.length <= 3);
      for (const entry of entries) {
        assert.equal(seen.has(entry.id), false);
        await history.write(entry.record);
        await ledger.archiveCompleted(ctx.serverIp, entry.id);
        seen.add(entry.id);
      }
    }
    assert.equal(seen.size, 7);
    assert.equal(await redis.xlen(key), 0);
  });

  await t.test('replay audit append failure never reintroduces the source into pending', async () => {
    const ctx = await message('replay-append-failure');
    await failures.failed(ctx, new ProcessingFailure('PARSE', 'INVALID', 'bad', false));
    const before = (await ledger.get(ctx))!;
    const dlq = `hls:dlq:${ctx.serverIp}`;
    const lastId = (await redis.xrevrange(dlq, '+', '-', 'COUNT', 1))[0]![0];
    // Force a real XADD runtime error after all TYPE checks have passed.
    await redis.xsetid(dlq, '18446744073709551615-18446744073709551615');
    await assert.rejects(ledger.replay(ctx), /ID|id|exhausted/);
    assert.equal(await pending(ctx), 0);
    assert.deepEqual(await ledger.get(ctx), before);
    await redis.xsetid(dlq, lastId);
    assert.equal(await ledger.replay(ctx), true);
    assert.equal(await pending(ctx), 1);
  });

  await t.test('failure after replay claim leaves recoverable intent and retry does not append another audit', async () => {
    const ctx = await message('replay-claim-failure');
    await failures.failed(ctx, new ProcessingFailure('PARSE', 'INVALID', 'bad', false));
    const original = redis.eval.bind(redis);
    const fault = mock.method(redis, 'eval', async (...args: Parameters<typeof original>) => {
      const script = String(args[0]);
      const finish = "redis.call('HSET', KEYS[3], 'state', 'replaying')";
      if (script.includes(finish)) args[0] = script.replace(finish, "error('injected failure after replay claim')");
      return original(...args);
    });
    try { await assert.rejects(ledger.replay(ctx), /injected failure/); }
    finally { fault.mock.restore(); }
    const intent = (await ledger.get(ctx))!;
    assert.equal(intent.state, 'replay_requested');
    assert.equal(intent.record.generation, 1);
    assert.equal(intent.record.attempts, 0);
    assert.equal(intent.record.failureId, `${ledger.metadataKey(ctx.originalStreamKey, ctx.groupName, ctx.originalMessageId)}:1`);
    assert.equal(await pending(ctx), 1);
    assert.equal(await failures.prepare(ctx), true);
    assert.equal(await pending(ctx), 1, 'prepare must not ACK the replay intent');
    const auditCount = await redis.xlen(`hls:dlq:${ctx.serverIp}`);
    assert.equal(await ledger.replay(ctx), true);
    assert.equal(await ledger.replay(ctx), false);
    assert.equal(await redis.xlen(`hls:dlq:${ctx.serverIp}`), auditCount);
    assert.equal((await ledger.get(ctx))!.record.revision, intent.record.revision);
  });

  await t.test('old generation cannot dead-letter and ACK a scheduled replay', async () => {
    const ctx = await message('stale-transfer');
    await failures.failed(ctx, new ProcessingFailure('PARSE', 'INVALID', 'bad', false));
    const old = (await ledger.get(ctx))!.record;
    await ledger.replay(ctx);
    await assert.rejects(ledger.moveToDeadLetter(ctx, old), /Stale DLQ transfer generation/);
    assert.equal(await pending(ctx), 1);
    assert.equal((await ledger.get(ctx))!.state, 'replaying');
  });

  await t.test('operator replay preserves original identity and archives successful resolution', async () => {
    const ctx = await message('replay');
    await failures.failed(ctx, new ProcessingFailure('PARSE', 'INVALID', 'bad', false));
    assert.equal(await ledger.replay(ctx), true);
    assert.equal(await ledger.replay(ctx), false);
    const scheduled = (await ledger.get(ctx))!;
    assert.equal(scheduled.record.generation, 1);
    assert.equal(scheduled.record.attempts, 0);
    assert.equal(await redis.hget(ledger.metadataKey(ctx.originalStreamKey, ctx.groupName, ctx.originalMessageId), 'generation'), '1');
    assert.equal(await redis.xlen(ctx.originalStreamKey), 1);
    const manager = new ServerBatchQueueManager({ store, acknowledger, failures, retryPolicy: policy, groupName: ctx.groupName });
    const processor = new HlsMessageProcessor({ async check() { return { status: 1, latency: 1 }; } }, { publish() {} }, manager);
    const consumer = new HlsConsumer({ source: messaging, processor, failures });
    consumers.push(consumer);
    const running = consumer.consume(ctx.serverIp, undefined, { groupName: ctx.groupName, pendingMinIdleMs: 1, recoveryIntervalMs: 5, blockTimeoutMs: 5 });
    void running.catch(() => {});
    await until(() => manager.getQueue(ctx.serverIp).getStats().itemCount > 0);
    await consumer.stop(); await running; await manager.stopAll();
    await projector.flush();
    assert.equal(await pending(ctx), 0);
    assert.equal(await ledger.get(ctx), null);
    assert.equal((await db.collection('hls_processing_failures').findOne({ originalStreamKey: ctx.originalStreamKey }))?.['resolution'], 'RESOLVED');
    const resolved = await db.collection('hls_processing_failures').findOne({ originalStreamKey: ctx.originalStreamKey });
    assert.equal(resolved?.['generation'], 1);
    assert.equal(resolved?.['failureId'], scheduled.record.failureId);
    assert.equal(await db.collection('hls_stream_metrics').countDocuments({ redisMessageId: ctx.originalMessageId, serverIp: ctx.serverIp }), 1);
  });
});
