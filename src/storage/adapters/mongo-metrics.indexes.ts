import type { Db, IndexDescription } from "mongodb";

import { HLS_METRICS_DATASET, HLS_METRICS_RETENTION_SECONDS, type HlsStreamMetric } from "../../types/hls-metric.types.js";

// Backward-compatible exports; the observation model is independent of MongoDB.
export { HLS_METRICS_RETENTION_SECONDS, type HlsStreamMetric } from "../../types/hls-metric.types.js";
export const HLS_METRICS_COLLECTION = HLS_METRICS_DATASET;

const indexPromises = new WeakMap<Db, Map<string, Promise<unknown>>>();

/** Shared by startup and custom queues. A failed index build can be retried. */
export async function ensureHlsMetricsIndexes(
  db: Db,
  collectionName = HLS_METRICS_COLLECTION
): Promise<void> {
  let collections = indexPromises.get(db);

  if (!collections) {
    collections = new Map();
    indexPromises.set(db, collections);
  }

  let pending = collections.get(collectionName);

  if (!pending) {
    const indexes: IndexDescription[] = [
      { key: { redisStreamKey: 1, redisMessageId: 1 }, unique: true },
      { key: { serverIp: 1, streamId: 1, checkedAt: -1 } },
    ];

    pending = (async () => {
      const collection = db.collection<HlsStreamMetric>(collectionName);
      await collection.createIndexes(indexes);

      // Physical retention: MongoDB's TTL monitor (runs about every 60 s) deletes rows once ingestedAt is older than the window.
      const ttl = (await collection.listIndexes().toArray()).find((index) =>
        Object.keys(index.key).length === 1 && index.key["ingestedAt"] === 1
      );
      if (!ttl) {
        await collection.createIndexes([{ key: { ingestedAt: 1 }, name: "ingestedAt_1", expireAfterSeconds: HLS_METRICS_RETENTION_SECONDS }]);
      } else if (ttl.expireAfterSeconds !== HLS_METRICS_RETENTION_SECONDS) {
        // An older window (or a plain ingestedAt index) is changed in place; no rebuild, no drop.
        await db.command({ collMod: collectionName, index: { name: ttl.name, expireAfterSeconds: HLS_METRICS_RETENTION_SECONDS } });
      }
    })();

    collections.set(collectionName, pending);
  }

  try {
    await pending;
  } catch (error) {
    if (collections.get(collectionName) === pending) collections.delete(collectionName);
    throw error;
  }
}
