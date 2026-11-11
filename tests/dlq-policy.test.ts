import { test } from 'node:test';
import assert from 'node:assert/strict';
import { FailureHandler } from '../src/dlq/failure-handler.js';
import { RetryPolicy } from '../src/dlq/retry-policy.js';
import { ProcessingFailure, type FailureLedger, type HlsDeadLetter, type MessageContext, type RetryState } from '../src/dlq/dead-letter.types.js';
import { redactError, redactFields } from '../src/dlq/redact.js';
import { UnsortedBatchQueue } from '../src/services/batch-queue.service.js';
import { PartialMetricsWriteError } from '../src/dlq/dead-letter.types.js';
import { DeadLetterProjector } from '../src/dlq/dead-letter-projector.js';

const context: MessageContext = { originalStreamKey: 'hls:streams:server', originalMessageId: '1-0', serverIp: 'server', groupName: 'group', consumerName: 'worker', rawFields: [] };
function fakeLedger() {
  let saved: RetryState | null = null;
  let moves = 0;
  const ledger: FailureLedger = {
    async get() { return saved; },
    async clearSucceeded() { saved = null; },
    async recordFailure(ctx, error) {
      const record: HlsDeadLetter = { ...ctx, failureId: 'failure', failureStage: error.stage, errorCode: error.code, errorMessage: error.message, attempts: (saved?.record.attempts ?? 0) + 1,
        firstFailedAt: saved?.record.firstFailedAt ?? 'first', lastFailedAt: 'last', updatedAt: 'last', retryable: error.retryable, bounded: error.bounded, generation: 0, revision: 1, resolution: 'OPEN' };
      saved = { state: 'retry', nextRetryAt: 1000, record };
      return saved;
    },
    async moveToDeadLetter() { moves++; saved!.state = 'dead_lettered'; },
  };
  return { ledger, state: () => saved, moves: () => moves };
}

test('retry policy enforces exponential capped delay and defaults to five attempts', () => {
  const policy = new RetryPolicy();
  assert.deepEqual([1,2,3,7,99].map((n) => policy.delay(n)), [1000,2000,4000,60000,60000]);
  assert.equal(policy.settings.maxAttempts, 5);
  assert.throws(() => new RetryPolicy({ maxAttempts: 0 }), RangeError);
});

test('permanent parse failures dead-letter immediately; transient worker failures are bounded', async () => {
  const fake = fakeLedger();
  const handler = new FailureHandler(fake.ledger, new RetryPolicy({ maxAttempts: 2 }), () => 0);
  assert.equal(await handler.prepare(context), true);
  assert.equal(await handler.failed(context, new ProcessingFailure('HEALTH_CHECK', 'WORKER', 'failed')), false);
  assert.equal(await handler.prepare(context), false, 'backoff prevents early processing');
  assert.equal(await handler.failed(context, new ProcessingFailure('HEALTH_CHECK', 'WORKER', 'failed')), true);
  assert.equal(fake.moves(), 1);
  assert.equal(await handler.prepare(context), false);
  const permanent = fakeLedger();
  assert.equal(await new FailureHandler(permanent.ledger).failed(context, new ProcessingFailure('PARSE', 'INVALID', 'bad', false)), true);
  assert.equal(permanent.moves(), 1);
});

test('infrastructure outages are retryable without a mass-DLQ attempt threshold', async () => {
  const fake = fakeLedger();
  const handler = new FailureHandler(fake.ledger, new RetryPolicy({ maxAttempts: 1 }));
  for (let i=0; i<6; i++) assert.equal(await handler.failed(context, new ProcessingFailure('PERSISTENCE', 'OFFLINE', 'offline', true, false)), false);
  assert.equal(fake.moves(), 0);
});

test('partial batch failure ACKs persisted records, dead-letters only the failed record', async () => {
  const acknowledgements: string[][] = [];
  const failures: string[] = [];
  const queue = new UnsortedBatchQueue({
    store: { async writeBatch() { throw new PartialMetricsWriteError([{ index: 1, failure: new ProcessingFailure('PERSISTENCE', 'SCHEMA_VALIDATION', 'bad', false) }]); } },
    acknowledger: { async acknowledge(_s, _g, ids) { acknowledgements.push([...ids]); return ids.length; } },
    failures: { async prepare() { return true; }, async failed(ctx) { failures.push(ctx.originalMessageId); return true; } },
  });
  const base = { streamId: 'channel', streamUrl: 'https://example.test/a.m3u8', serverIp: 'server', createdAt: '2026-11-09', receivedAt: '2026-11-09', delay: 0 };
  await queue.push({ ...base, messageId: '1-0' });
  await queue.push({ ...base, messageId: '2-0' });
  await queue.stop();
  assert.deepEqual(acknowledgements, [['1-0']]);
  assert.deepEqual(failures, ['2-0']);
  assert.equal(queue.getStats().itemCount, 0);
});

test('failed DLQ delivery keeps only the unresolved record buffered for retry', async () => {
  let broken = true;
  const writes: string[][] = [];
  const queue = new UnsortedBatchQueue({
    store: { async writeBatch(records) {
      writes.push(records.map((r) => r.redisMessageId));
      if (broken) throw new PartialMetricsWriteError([{ index: records.length - 1, failure: new ProcessingFailure('PERSISTENCE', 'SCHEMA_VALIDATION', 'bad', false) }]);
    } },
    acknowledger: { async acknowledge(_s, _g, ids) { return ids.length; } },
    failures: { async prepare() { return true; }, async failed() { throw new Error('DLQ unavailable'); } },
  });
  const base = { streamId: 'channel', streamUrl: 'https://example.test/a.m3u8', serverIp: 'server', createdAt: '2026-11-09', receivedAt: '2026-11-09', delay: 0 };
  await queue.push({ ...base, messageId: '1-0' }); await queue.push({ ...base, messageId: '2-0' });
  await assert.rejects(queue.flush());
  assert.equal(queue.getStats().itemCount, 1);
  broken = false;
  await queue.stop();
  assert.deepEqual(writes, [['1-0','2-0'], ['2-0']]);
});

test('failure archival never trims before history persistence succeeds', async () => {
  const fake = fakeLedger();
  const state = await fake.ledger.recordFailure(context, new ProcessingFailure('PARSE', 'BAD', 'bad', false), new RetryPolicy().settings);
  const operations: string[] = [];
  const projector = new DeadLetterProjector({
    async servers() { return ['server']; },
    async readArchiveBatch() { return [{ id: '1-0', record: state.record }]; },
    async archiveCompleted() { operations.push('trim'); },
  }, { async write() { operations.push('history'); throw new Error('Mongo offline'); } });
  await assert.rejects(projector.flush());
  assert.deepEqual(operations, ['history']);
});

test('RCA payloads redact URL credentials, query tokens and secret fields', () => {
  const redacted = redactFields(['streamUrl', 'https://user:password@example.test/live?token=secret#private', 'authorization', 'Bearer secret']);
  assert.deepEqual(redacted, ['streamUrl', 'https://example.test/live', 'authorization', '[redacted]']);
  assert.ok(!redactError('Failed https://user:secret@example.test/live?token=private password=hunter').includes('hunter'));
});
