import { pipelineStream, targetKey, type ProbeTarget, type ProbeTargetStore } from '../config/pipeline-probes.js';
import type { NewStream, StreamPatch, StreamRegistration } from '../inventory/stream-inventory.js';

export interface LinkedStreams {
  findIdentity(serverIp: string, streamId: string): Promise<StreamRegistration | null>;
  create(value: NewStream): Promise<StreamRegistration>;
  patch(id: string, version: number, patch: StreamPatch): Promise<StreamRegistration>;
  remove(id: string, version: number): Promise<StreamRegistration>;
}

/**
 * Saving a probe target. An HLS target is also registered as a stream (see pipelineStream), so the
 * regular pipeline checks it; the pipeline monitor then reads that stream's live status. TCP targets
 * are only stored: the monitor connects to them itself, because stream consumers check HLS only.
 */
export class PipelineProbeService implements ProbeTargetStore {
  constructor(private readonly store: ProbeTargetStore, private readonly streams: LinkedStreams) {}

  all() { return this.store.all(); }

  async set(triggerId: string, nodeId: string, target: ProbeTarget, by: string | null): Promise<void> {
    const previous = (await this.store.all()).get(targetKey(triggerId, nodeId));
    if (previous?.kind === 'hls') {
      const old = pipelineStream(triggerId, nodeId, previous.url);
      if (target.kind !== 'hls' || pipelineStream(triggerId, nodeId, target.url).serverIp !== old.serverIp) await this.unlink(old);
    }
    if (target.kind === 'hls') await this.link(triggerId, nodeId, target.url);
    await this.store.set(triggerId, nodeId, target, by);
  }

  async remove(triggerId: string, nodeId: string): Promise<boolean> {
    const previous = (await this.store.all()).get(targetKey(triggerId, nodeId));
    if (previous?.kind === 'hls') await this.unlink(pipelineStream(triggerId, nodeId, previous.url));
    return this.store.remove(triggerId, nodeId);
  }

  /** At startup: re-registers HLS targets whose stream is missing (for example deleted from the Streams page). */
  async relinkAll(): Promise<void> {
    for (const [key, target] of await this.store.all()) {
      if (target.kind !== 'hls') continue;
      const slash = key.indexOf('/');
      await this.link(key.slice(0, slash), key.slice(slash + 1), target.url);
    }
  }

  private async link(triggerId: string, nodeId: string, url: string): Promise<void> {
    const identity = pipelineStream(triggerId, nodeId, url);
    const existing = await this.streams.findIdentity(identity.serverIp, identity.streamId);
    if (!existing || existing.deleted) { await this.streams.create({ ...identity, streamUrl: url }); return; }
    if (existing.config.streamUrl !== url || !existing.config.enabled) await this.streams.patch(existing.id, existing.version, { streamUrl: url, enabled: true });
  }

  private async unlink(identity: { serverIp: string; streamId: string }): Promise<void> {
    const existing = await this.streams.findIdentity(identity.serverIp, identity.streamId);
    if (existing && !existing.deleted) await this.streams.remove(existing.id, existing.version);
  }
}
