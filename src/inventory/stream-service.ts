import { normalizeProbeConfig, type StreamProbeConfig, type StreamProbeRegistration } from '../monitoring/probe-contracts.js';
import { InventoryNotFoundError, InventoryValidationError, InventoryVersionError, type StreamInventoryRepository, type NewStream, type StreamManagement, type StreamPatch } from './stream-inventory.js';
export class StreamService implements StreamManagement {
  constructor(private readonly repository: StreamInventoryRepository) {}
  async create(value: NewStream) {
    let config: StreamProbeConfig;
    // The origin server defaults to the host in the stream URL (http://192.168.1.10/live.m3u8 -> 192.168.1.10).
    try { config = normalizeProbeConfig({ ...value, serverIp: value.serverIp ?? new URL(value.streamUrl).hostname }); }
    catch { throw new InventoryValidationError('Invalid stream configuration'); }
    return this.repository.create(config);
  }
  async get(id: string) {
    const record = await this.repository.get(id);
    if (!record || record.deleted) throw new InventoryNotFoundError('Stream not found');
    return record;
  }
  async list(limit = 50, cursor?: string) {
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 100) throw new InventoryValidationError('limit must be between 1 and 100');
    return this.repository.list(limit, cursor);
  }
  async patch(id: string, version: number, patch: StreamPatch) {
    const current = await this.get(id);
    this.checkVersion(current.version, version);
    const allowed = ['streamUrl', 'intervalMs', 'staleAfterMs', 'enabled'];
    if (!patch || typeof patch !== 'object' || !Object.keys(patch).length || Object.keys(patch).some(key => !allowed.includes(key))) throw new InventoryValidationError('Invalid stream patch');
    let config;
    try { config = normalizeProbeConfig({ ...current.config, ...patch }); }
    catch { throw new InventoryValidationError('Invalid stream configuration'); }
    const updated = await this.repository.update(id, version, config, false);
    if (!updated) throw new InventoryVersionError('Stream changed; fetch the latest version');
    return updated;
  }
  async remove(id: string, version: number) {
    const current = await this.get(id);
    this.checkVersion(current.version, version);
    const updated = await this.repository.update(id, version, { ...current.config, enabled: false }, true);
    if (!updated) throw new InventoryVersionError('Stream changed; fetch the latest version');
    return updated;
  }
  private checkVersion(current: number, version: number): void {
    if (!Number.isSafeInteger(version) || version < 1 || version >= Number.MAX_SAFE_INTEGER || current !== version) throw new InventoryVersionError('Stream changed; fetch the latest version');
  }
  /** Seed only absent identities; restart must not overwrite API changes/deletions. */
  async seed(value: StreamProbeRegistration): Promise<void> {
    const config = normalizeProbeConfig(value);
    if (await this.repository.findIdentity(config.serverIp, config.streamId)) return;
    try { await this.repository.create(config); }
    catch (error) {
      if (!await this.repository.findIdentity(config.serverIp, config.streamId)) throw error;
    }
  }
}
