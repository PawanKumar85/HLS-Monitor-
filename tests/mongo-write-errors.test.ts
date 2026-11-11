import { test } from 'node:test';
import assert from 'node:assert/strict';
import { MongoBulkWriteError, type BulkWriteResult, type Db, type WriteError } from 'mongodb';
import { MongoMetricsStore } from '../src/storage/adapters/mongo-metrics.store.js';
import { MongoWriteErrorClassifier } from '../src/storage/adapters/mongo-write-error-classifier.js';
import { PartialMetricsWriteError } from '../src/storage/persistence-errors.js';
import type { HlsStreamMetric } from '../src/types/hls-metric.types.js';

const observations: HlsStreamMetric[] = Array.from({ length: 3 }, (_, index) => ({
  redisStreamKey: 'hls:streams:server', redisMessageId: `${index + 1}-0`, serverIp: 'server', streamId: 'channel', streamUrl: 'https://example.test/live',
  createdAt: new Date(1700000000000), receivedAt: new Date(), checkedAt: new Date(), ingestedAt: new Date(), pipelineDelayMs: 0,
}));
function bulkError(failures = [{ index: 1, code: 121 }], successful = 3 - failures.length, concern = false) {
  // Real driver error, with a controlled result to exercise defensive validation.
  const result = { ok: 1, matchedCount: successful, upsertedCount: 0, insertedCount: 0, deletedCount: 0,
    getWriteConcernError: () => concern ? { code: 64 } : undefined,
  } as unknown as BulkWriteResult;
  return new MongoBulkWriteError({ message: 'bulk failed', code: failures[0]?.code ?? 1, writeErrors: failures as WriteError[] }, result);
}
function fixture(error: unknown, lookup: (filter: unknown, options: unknown) => Promise<unknown> = async () => null) {
  let lookups = 0; let disconnects = 0; let indexError: Error | undefined;
  const collection = {
    createIndexes: async () => { if (indexError) throw indexError; }, listIndexes: () => ({ toArray: async () => [] }),
    bulkWrite: async () => { throw error; }, findOne: async (filter: unknown, options: unknown) => { lookups++; return lookup(filter, options); },
  };
  const db = { collection: () => collection } as unknown as Db;
  const store = new MongoMetricsStore({ connect: async () => db, getDb: () => db, disconnect: async () => { disconnects++; } });
  return { store, lookups: () => lookups, disconnects: () => disconnects, failIndexes: (failure: Error) => { indexError = failure; } };
}

test('only complete, unique, safe operation indexes yield partial failures', async () => {
  const classifier = new MongoWriteErrorClassifier();
  assert.equal(classifier.authoritativeFailures(bulkError(), 3)?.length, 1);
  for (const indexes of [[1, 1], [-1], [3], [1.5], [Number.MAX_SAFE_INTEGER + 1]]) {
    const error = bulkError(indexes.map(index => ({ index, code: 121 })));
    const { store } = fixture(error);
    await assert.rejects(store.writeBatch(observations), thrown => thrown === error);
  }
  for (const count of [0, -1, 1.5, Number.NaN, Number.MAX_SAFE_INTEGER + 1]) {
    const error = bulkError([{ index: 1, code: 121 }], count);
    await assert.rejects(fixture(error).store.writeBatch(observations), thrown => thrown === error);
  }
});

test('write concern, ambiguous labels, unexpected counts and non-bulk failures retain original batch error', async () => {
  const concern = bulkError(undefined, undefined, true);
  const label = bulkError(); label.addErrorLabel('UnknownTransactionCommitResult');
  const directConcern = bulkError(); directConcern.writeConcernError = { code: 64 };
  const unexpected = bulkError(); Object.defineProperty(unexpected.result, 'insertedCount', { value: 1 });
  for (const error of [concern, label, directConcern, unexpected, new Error('network disconnected'), bulkError([])]) {
    const f = fixture(error);
    await assert.rejects(f.store.writeBatch(observations), thrown => thrown === error);
    assert.equal(f.lookups(), 0);
  }
});

test('schema validation is permanent; permission, authentication, conflict and unknown errors remain retryable', async () => {
  for (const code of [121, 13, 18, 112, 50, 99999]) {
    await assert.rejects(fixture(bulkError([{ index: 1, code }])).store.writeBatch(observations), error => {
      assert.ok(error instanceof PartialMetricsWriteError);
      assert.equal(error.failures.length, 1);
      assert.equal(error.failures[0]?.index, 1);
      assert.equal(error.failures[0]?.failure.retryable, code !== 121);
      return true;
    });
  }
});

test('duplicate reconciliation checks original identity and stable producer fields using primary majority read', async () => {
  const f = fixture(bulkError([{ index: 1, code: 11000 }]), async (filter, options) => {
    const observation = observations[1]!;
    assert.deepEqual(filter, { redisStreamKey: observation.redisStreamKey, redisMessageId: observation.redisMessageId,
      serverIp: observation.serverIp, streamId: observation.streamId, streamUrl: observation.streamUrl, createdAt: observation.createdAt });
    assert.deepEqual(options, { projection: { _id: 1 }, readPreference: 'primary', readConcern: { level: 'majority' } });
    return { _id: 'persisted' };
  });
  await f.store.writeBatch(observations); assert.equal(f.lookups(), 1);
});

test('unconfirmed duplicate stays retryable; failed reconciliation retries the original whole batch', async () => {
  const error = bulkError([{ index: 1, code: 11000 }]);
  await assert.rejects(fixture(error).store.writeBatch(observations), thrown => {
    assert.ok(thrown instanceof PartialMetricsWriteError);
    assert.equal(thrown.failures[0]?.failure.code, 'DUPLICATE_KEY_UNCONFIRMED');
    assert.equal(thrown.failures[0]?.failure.retryable, true); return true;
  });
  await assert.rejects(fixture(error, async () => { throw new Error('lookup failed'); }).store.writeBatch(observations), thrown => thrown === error);
});

test('reconciled duplicate is removed from mixed validation failures without changing original indexes', async () => {
  const error = bulkError([{ index: 0, code: 11000 }, { index: 2, code: 121 }]);
  await assert.rejects(fixture(error, async () => ({ _id: 'persisted' })).store.writeBatch(observations), thrown => {
    assert.ok(thrown instanceof PartialMetricsWriteError);
    assert.deepEqual(thrown.failures.map(failure => failure.index), [2]); return true;
  });
});

test('index initialization failure does not close the shared connection', async () => {
  const f = fixture(new Error('unused')); const indexError = new Error('index initialization failed'); f.failIndexes(indexError);
  await assert.rejects(f.store.connect(), error => error === indexError);
  assert.equal(f.disconnects(), 0);
  await f.store.disconnect(); assert.equal(f.disconnects(), 1);
});
