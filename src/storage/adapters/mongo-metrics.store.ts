import { MongoWriteErrorClassifier } from "./mongo-write-error-classifier.js";
import { PartialMetricsWriteError, type MetricsWriteFailure } from "../persistence-errors.js";
import { MongoMetricsReader } from "./mongo-metrics.reader.js";
import type { MetricsHistoryQuery, MetricsReader, ServerDetails, ServerOfflineSummary } from "../metrics-reader.js";
import type { MongoConfig } from "../../config/mongo.config.js";
import { ensureHlsMetricsIndexes } from "./mongo-metrics.indexes.js";
import { HLS_METRICS_DATASET, type HlsStreamMetric } from "../../types/hls-metric.types.js";
import type { MetricsStore } from "../metrics-store.js";

/** All MongoDB write semantics and schema initialization live behind this adapter. */
export class MongoMetricsStore implements MetricsStore, MetricsReader {
  readonly name = "mongodb";
  private readonly classifier = new MongoWriteErrorClassifier();

  constructor(
    private readonly connection: Pick<MongoConfig, "connect" | "disconnect" | "getDb">,
    private readonly now: () => Date = () => new Date()
  ) { }

  async connect(): Promise<void> {
    const db = await this.connection.connect();

    // Shared connection cleanup belongs to application startup/shutdown ownership.
    await ensureHlsMetricsIndexes(db);
  }

  async disconnect(): Promise<void> {
    await this.connection.disconnect();
  }

  async healthCheck(): Promise<boolean> {
    try {
      const result = await this.connection
        .getDb()
        .command({ ping: 1 });

      return result.ok === 1;
    } catch {
      return false;
    }
  }

  async readHistory(
    query: MetricsHistoryQuery,
    dataset = HLS_METRICS_DATASET
  ): Promise<HlsStreamMetric[]> {
    return new MongoMetricsReader(
      this.connection.getDb(),
      this.now
    ).readHistory(query, dataset);
  }

  async serverReport(from: Date, to: Date, dataset = HLS_METRICS_DATASET): Promise<ServerOfflineSummary[]> {
    return new MongoMetricsReader(this.connection.getDb(), this.now).serverReport(from, to, dataset);
  }

  async serverDetails(serverIp: string, from: Date, to: Date): Promise<ServerDetails> {
    return new MongoMetricsReader(this.connection.getDb(), this.now).serverDetails(serverIp, from, to);
  }

  async writeBatch(
    observations: readonly HlsStreamMetric[],
    dataset = HLS_METRICS_DATASET
  ): Promise<void> {
    if (!observations.length) return;

    const db = this.connection.getDb();

    await ensureHlsMetricsIndexes(db, dataset);

    const collection = db.collection<HlsStreamMetric>(dataset);
    try {
      await collection.bulkWrite(
        observations.map((observation) => ({
          updateOne: {
            filter: {
              redisStreamKey: observation.redisStreamKey,
              redisMessageId: observation.redisMessageId,
            },
            update: { $setOnInsert: observation },
            upsert: true,
          },
        })),
        { ordered: false, writeConcern: { w: 'majority' } }
      );
    } catch (error) {
      const confirmed = this.classifier.authoritativeFailures(error, observations.length);
      if (!confirmed) throw error;
      const failures: MetricsWriteFailure[] = [];
      for (const failure of confirmed) {
        const classification = this.classifier.classify(failure.code);
        if (classification.kind === 'failure') {
          failures.push({ index: failure.index, failure: classification.failure });
          continue;
        }
        const observation = observations[failure.index]!;
        try {
          // A duplicate on an unrelated unique index is not proof of persistence.
          // Check stable producer fields too; retry-time health/timestamps may differ.
          const persisted = await collection.findOne({
            redisStreamKey: observation.redisStreamKey,
            redisMessageId: observation.redisMessageId,
            serverIp: observation.serverIp,
            streamId: observation.streamId,
            streamUrl: observation.streamUrl,
            createdAt: observation.createdAt,
          }, { projection: { _id: 1 }, readPreference: 'primary', readConcern: { level: 'majority' } });
          if (persisted) continue;
        } catch {
          // Failed reconciliation is ambiguous: preserve the original batch error.
          throw error;
        }
        failures.push({ index: failure.index, failure: {
          code: 'DUPLICATE_KEY_UNCONFIRMED', message: 'Duplicate key requires persistence reconciliation', retryable: true,
        } });
      }
      if (failures.length) throw new PartialMetricsWriteError(failures);
      // Every failed write was a reconciled duplicate; all identities are persisted.
    }
  }
}
