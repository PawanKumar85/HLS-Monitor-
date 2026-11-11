import { MongoBulkWriteError, type WriteError } from 'mongodb';
import type { MetricsWriteFailure } from '../persistence-errors.js';

type Classification = { kind: 'duplicate' } | { kind: 'failure'; failure: MetricsWriteFailure['failure'] };

/** Mongo-specific error semantics stay out of the database-neutral batch queue. */
export class MongoWriteErrorClassifier {
  authoritativeFailures(error: unknown, batchSize: number): readonly WriteError[] | null {
    if (!(error instanceof MongoBulkWriteError) || error.err || error.writeConcernError ||
      error.result.getWriteConcernError() || error.hasErrorLabel('UnknownTransactionCommitResult') ||
      error.result.ok !== 1) return null;
    const failures = Array.isArray(error.writeErrors) ? error.writeErrors : [error.writeErrors];
    const { matchedCount, upsertedCount, insertedCount, deletedCount } = error.result;
    if (!failures.length || insertedCount !== 0 || deletedCount !== 0 ||
      ![matchedCount, upsertedCount].every(count => Number.isSafeInteger(count) && count >= 0)) return null;
    const indexes = failures.map(failure => failure?.index);
    if (new Set(indexes).size !== failures.length || !failures.every(failure =>
      failure && Number.isSafeInteger(failure.index) && failure.index >= 0 && failure.index < batchSize &&
      Number.isSafeInteger(failure.code) && failure.code > 0)) return null;
    return matchedCount + upsertedCount + failures.length === batchSize ? failures : null;
  }

  classify(code: number): Classification {
    if (code === 11000) return { kind: 'duplicate' };
    if (code === 121) return { kind: 'failure', failure: {
      code: 'SCHEMA_VALIDATION', message: 'Observation failed database schema validation', retryable: false,
    } };
    const infrastructure: Record<number, [string, string]> = {
      13: ['DATABASE_UNAUTHORIZED', 'Database permissions require operator intervention'],
      18: ['DATABASE_AUTHENTICATION', 'Database credentials require operator intervention'],
      50: ['DATABASE_TIMEOUT', 'Database operation exceeded its time limit'],
      112: ['WRITE_CONFLICT', 'Database write conflict; retry with backoff'],
    };
    const [name, message] = infrastructure[code] ?? [String(code), 'Database write failure; outcome requires retry'];
    // Infrastructure/configuration failures must not become permanent observation failures.
    return { kind: 'failure', failure: { code: name, message, retryable: true } };
  }
}
