import type { StreamApi } from "../http/stream-routes.js";
import type { AuthOptions } from "../http/auth.js";
import type { PipelineApiOptions } from "../http/pipeline-routes.js";
import type { PushApi } from "../http/push-routes.js";
import type { StatusTransport } from "../realtime/status-transport.js";

export interface DatabaseHealth {
  redis: boolean;
  storage: boolean;
  storageProvider: string;
}
/** HTTP application depends only on lifecycle operations it actually uses. */
export interface ApplicationServices {
  configurationTransfer?: Pick<import('./configuration-transfer.js').ConfigurationTransfer, 'export' | 'preview' | 'import'>;
  resetDatabase?: import('./monitoring-reset.js').MonitoringReset;
  dbManager: {
    healthCheck(): Promise<DatabaseHealth>;
    disconnectAll(): Promise<void>;
  };
  hlsConsumer: { stop(): Promise<void> };
  batchQueueManager: { stopAll(): Promise<void> };
  streams?: StreamApi;
  pipeline?: PipelineApiOptions;
  /** Web Push background alerts; absent in tests that do not need it. */
  push?: PushApi;
  /** Sign-in for every page and API. 'disabled' is for tests only and must be chosen explicitly. */
  auth: AuthOptions | 'disabled';
  monitoring?: { start(): Promise<void>; stop(): Promise<void> };
  failureProjector?: { start(): void; stop(): Promise<void> };
  /** Reported by /health: whether checks are still being saved. */
  checkWatchdog?: { status(): { lastSavedAt: string | null; stalled: boolean; checkedAt: string | null; stalledAfterMs: number } };
  statusTransport: Pick<StatusTransport, "register">;
}
