import { mongoConfig } from "../config/mongo.config.js";
import { MongoMetricsStore } from "./adapters/mongo-metrics.store.js";
import type { MetricsReader } from "./metrics-reader.js";
import type { MetricsStore } from "./metrics-store.js";

/** Application composition point. Add a SQL adapter here when implementing it. */
export function createMetricsStore(
  provider = process.env["METRICS_STORE"] ?? "mongodb"
): MetricsStore & MetricsReader {
  switch (provider) {
    case "mongodb": return new MongoMetricsStore(mongoConfig);
    default: throw new Error(`Unsupported METRICS_STORE: ${provider}`);
  }
}

