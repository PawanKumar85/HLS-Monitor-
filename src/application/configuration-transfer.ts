import { parseChannel, type ChannelStore } from '../config/pipeline-channels.js';
import { parseProbeTarget, pipelineStream, targetKey, type ProbeTarget, type ProbeTargetStore } from '../config/pipeline-probes.js';
import { normalizeProbeConfig, type StreamProbeConfig } from '../monitoring/probe-contracts.js';
import type { StreamManagement } from '../inventory/stream-inventory.js';
import type { ChannelTopology } from '../types/pipeline.types.js';

export class TransferValidationError extends Error {}
export class TransferConflictError extends Error {}
export interface ConfigurationBundle {
  format: 'hls-monitor-configuration'; version: 1; exportedAt?: string;
  channels: ChannelTopology[];
  probes: { triggerId: string; nodeId: string; target: ProbeTarget }[];
  streams: StreamProbeConfig[];
}
const object = (value: unknown): Record<string, unknown> => {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new TransferValidationError('Expected a configuration object');
  return value as Record<string, unknown>;
};
export function parseConfiguration(value: unknown): ConfigurationBundle {
  try {
    const input = object(value);
    if (input.format !== 'hls-monitor-configuration' || input.version !== 1) throw new Error('Unsupported file format or version');
    if (!Array.isArray(input.channels) || input.channels.length > 500 || !Array.isArray(input.probes) || input.probes.length > 15000 || !Array.isArray(input.streams) || input.streams.length > 10000) throw new Error('Invalid configuration lists or capacity exceeded');
    const channels = input.channels.map(raw => {
      const row = object(raw);
      if (typeof row.triggerId !== 'string' || !/^[a-z0-9_-]{1,60}$/.test(row.triggerId)) throw new Error('Invalid channel ID');
      return parseChannel(row, row.triggerId);
    });
    const ids = new Set(channels.map(c => c.triggerId));
    if (ids.size !== channels.length) throw new Error('Duplicate channel IDs');
    const probes = input.probes.map(raw => {
      const row = object(raw); const channel = channels.find(c => c.triggerId === row.triggerId);
      if (!channel || typeof row.nodeId !== 'string' || !channel.nodes.some(n => n.id === row.nodeId)) throw new Error('Probe references an unknown channel or node');
      const target = object(row.target);
      const parsed = target.kind === 'hls' ? parseProbeTarget({ hls: target.url }) : target.kind === 'tcp' ? parseProbeTarget({ tcp: `${String(target.host).includes(':') ? `[${target.host}]` : target.host}:${target.port}` }) : null;
      if (!parsed) throw new Error('Invalid probe kind');
      return { triggerId: channel.triggerId, nodeId: row.nodeId, target: parsed };
    });
    if (new Set(probes.map(p => targetKey(p.triggerId, p.nodeId))).size !== probes.length) throw new Error('Duplicate probe targets');
    const streams = input.streams.map(raw => normalizeProbeConfig(object(raw) as unknown as StreamProbeConfig));
    const identity = (s: { serverIp: string; streamId: string }) => JSON.stringify([s.serverIp, s.streamId]);
    if (new Set(streams.map(identity)).size !== streams.length) throw new Error('Duplicate stream identities');
    for (const probe of probes) {
      if (probe.target.kind !== 'hls') continue;
      const key = pipelineStream(probe.triggerId, probe.nodeId, probe.target.url);
      const stream = streams.find(s => identity(s) === identity(key));
      if (stream && stream.streamUrl !== probe.target.url) throw new Error('HLS probe and linked stream URLs do not match');
      if (!stream) streams.push(normalizeProbeConfig({ ...key, streamUrl: probe.target.url }));
    }
    if (streams.length > 10000) throw new Error('Stream capacity exceeded');
    return { format: 'hls-monitor-configuration', version: 1, channels, probes, streams };
  } catch (error) { throw new TransferValidationError((error as Error).message); }
}
export class ConfigurationTransfer {
  private importing = false;
  constructor(private readonly channels: ChannelStore, private readonly probes: ProbeTargetStore, private readonly streams: StreamManagement) {}
  private async allStreams() {
    const rows: StreamProbeConfig[] = []; let cursor: string | undefined;
    do { const page = await this.streams.list(100, cursor); rows.push(...page.items.map(r => r.config)); if (rows.length > 10000) throw new TransferValidationError('Configuration export exceeds 10000 streams'); cursor = page.nextCursor; } while (cursor);
    return rows;
  }
  async export(): Promise<ConfigurationBundle> {
    const [channels, targets, streams] = await Promise.all([this.channels.list(), this.probes.all(), this.allStreams()]);
    return { format: 'hls-monitor-configuration', version: 1, exportedAt: new Date().toISOString(), channels,
      probes: [...targets].filter(([key]) => channels.some(c => c.nodes.some(n => targetKey(c.triggerId, n.id) === key))).map(([key, target]) => {
        const slash = key.indexOf('/'); return { triggerId: key.slice(0, slash), nodeId: key.slice(slash + 1), target };
      }), streams };
  }
  async preview(value: unknown) {
    const bundle = parseConfiguration(value);
    const [channels, streams, targets] = await Promise.all([this.channels.list(), this.allStreams(), this.probes.all()]);
    if (bundle.channels.some(c => channels.some(old => old.triggerId === c.triggerId)) || bundle.streams.some(s => streams.some(old => old.serverIp === s.serverIp && old.streamId === s.streamId)) || bundle.probes.some(p => targets.has(targetKey(p.triggerId, p.nodeId)))) throw new TransferConflictError('Existing channels or streams conflict with this file. Import adds new configuration only; existing data is never overwritten.');
    return { bundle, summary: { channels: bundle.channels.length, probes: bundle.probes.length, streams: bundle.streams.length } };
  }
  async import(value: unknown, by: string) {
    if (this.importing) throw new TransferConflictError('Another import is in progress');
    this.importing = true;
    const undo: (() => Promise<unknown>)[] = [];
    try {
      const { bundle, summary } = await this.preview(value);
      for (const stream of bundle.streams) {
        const row = await this.streams.create(stream);
        undo.push(() => this.streams.remove(row.id, row.version));
      }
      for (const probe of bundle.probes) {
        await this.probes.set(probe.triggerId, probe.nodeId, probe.target, by);
        undo.push(() => this.probes.remove(probe.triggerId, probe.nodeId));
      }
      for (const channel of bundle.channels) {
        if (!await this.channels.create(channel)) throw new TransferConflictError('Channel changed during import. Please validate again.');
        undo.push(() => this.channels.remove(channel.triggerId));
      }
      return summary;
    } catch (error) {
      const rollbackErrors: unknown[] = [];
      for (const rollback of undo.reverse()) try { await rollback(); } catch (e) { rollbackErrors.push(e); }
      if (rollbackErrors.length) throw new Error('Import failed and cleanup was incomplete. Inspect channels and streams before retrying.');
      throw error;
    } finally { this.importing = false; }
  }
}
