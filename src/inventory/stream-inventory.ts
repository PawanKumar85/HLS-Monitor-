import type { StreamProbeConfig, StreamProbeRegistration } from '../monitoring/probe-contracts.js';
export interface StreamRegistration {
  id: string;
  config: StreamProbeConfig;
  version: number;
  deleted: boolean;
  pendingSync: boolean;
  syncAttempts: number;
  nextSyncAt: Date;
  createdAt: Date;
  updatedAt: Date;
  syncedAt?: Date;
}
export type StreamPatch = Partial<Pick<StreamProbeConfig, 'streamUrl' | 'intervalMs' | 'staleAfterMs' | 'enabled'>>;
export interface InventoryPage { items: StreamRegistration[]; nextCursor?: string }
export interface StreamInventoryRepository {
  initialize(): Promise<void>;
  create(config: StreamProbeConfig): Promise<StreamRegistration>;
  get(id: string): Promise<StreamRegistration | null>;
  findIdentity(serverIp: string, streamId: string): Promise<StreamRegistration | null>;
  list(limit: number, cursor?: string): Promise<InventoryPage>;
  update(id: string, version: number, config: StreamProbeConfig, deleted: boolean): Promise<StreamRegistration | null>;
  pending(limit: number, now: Date): Promise<StreamRegistration[]>;
  scan(limit: number, cursor?: string): Promise<StreamRegistration[]>;
  markSynced(id: string, version: number, now: Date): Promise<boolean>;
  deferSync(id: string, version: number, next: Date): Promise<void>;
}
export interface StreamScheduleProjection {
  apply(record: Pick<StreamRegistration, 'id' | 'config' | 'version' | 'deleted'>): Promise<'applied' | 'stale'>;
}
export type NewStream = Omit<StreamProbeRegistration, 'serverIp'> & { serverIp?: string };
export interface StreamManagement {
  /** serverIp may be left out: it is then the stream URL's host. */
  create(value: NewStream): Promise<StreamRegistration>;
  get(id: string): Promise<StreamRegistration>;
  list(limit?: number, cursor?: string): Promise<InventoryPage>;
  patch(id: string, version: number, value: StreamPatch): Promise<StreamRegistration>;
  remove(id: string, version: number): Promise<StreamRegistration>;
}
export class InventoryConflictError extends Error {}
export class InventoryNotFoundError extends Error {}
export class InventoryVersionError extends Error {}
export class InventoryValidationError extends Error {}
