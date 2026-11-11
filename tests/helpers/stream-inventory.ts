import { randomUUID } from 'node:crypto';
import { InventoryConflictError, type StreamInventoryRepository, type StreamRegistration } from '../../src/inventory/stream-inventory.js';
import type { StreamProbeConfig } from '../../src/monitoring/probe-contracts.js';
export class MemoryInventory implements StreamInventoryRepository {
  readonly records = new Map<string, StreamRegistration>();
  async initialize() {}
  async create(config: StreamProbeConfig) {
    const previous = [...this.records.values()].find(r => r.config.serverIp === config.serverIp && r.config.streamId === config.streamId);
    if (previous && !previous.deleted) throw new InventoryConflictError('Stream is already registered');
    const time = new Date();
    const record: StreamRegistration = { id: previous?.id ?? randomUUID(), config, version: (previous?.version ?? 0) + 1,
      deleted: false, pendingSync: true, syncAttempts: 0, nextSyncAt: time, createdAt: previous?.createdAt ?? time, updatedAt: time };
    this.records.set(record.id, structuredClone(record)); return structuredClone(record);
  }
  async get(id: string) { return structuredClone(this.records.get(id) ?? null); }
  async findIdentity(server: string, stream: string) { return structuredClone([...this.records.values()].find(r => r.config.serverIp === server && r.config.streamId === stream) ?? null); }
  async list(limit: number, cursor?: string) {
    const items = (await this.scan(10000, cursor)).filter(r => !r.deleted).slice(0, limit + 1);
    const more = items.length > limit; if (more) items.pop();
    return { items, ...(more ? { nextCursor: items.at(-1)!.id } : {}) };
  }
  async update(id: string, version: number, config: StreamProbeConfig, deleted: boolean) {
    const record = this.records.get(id); if (!record || record.version !== version || record.deleted) return null;
    record.config = structuredClone(config); record.version++; record.deleted = deleted; record.pendingSync = true;
    record.syncAttempts = 0; record.nextSyncAt = new Date(); record.updatedAt = new Date(); delete record.syncedAt;
    return structuredClone(record);
  }
  async pending(limit: number, now: Date) { return structuredClone([...this.records.values()].filter(r => r.pendingSync && r.nextSyncAt <= now).sort((a, b) => a.id.localeCompare(b.id)).slice(0, limit)); }
  async scan(limit: number, cursor?: string) { return structuredClone([...this.records.values()].filter(r => !cursor || r.id > cursor).sort((a, b) => a.id.localeCompare(b.id)).slice(0, limit)); }
  async markSynced(id: string, version: number, now: Date) {
    const record = this.records.get(id); if (!record || record.version !== version || !record.pendingSync) return false;
    record.pendingSync = false; record.syncedAt = now; record.syncAttempts = 0; return true;
  }
  async deferSync(id: string, version: number, next: Date) {
    const record = this.records.get(id); if (!record || record.version !== version) return;
    record.pendingSync = true; record.nextSyncAt = next; record.syncAttempts++;
  }
}
