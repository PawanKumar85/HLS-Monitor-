import { connect } from 'node:net';
import type { HlsStatusEvent } from '../types/hls-status.types.js';
import { pipelineStream, targetKey, type ProbeTargets } from '../config/pipeline-probes.js';
import type { PipelineRcaEngine } from '../services/pipeline-rca.service.js';
import type { ChannelPipelineVerdict, ChannelTopology, HopObservation } from '../types/pipeline.types.js';

/** Drain every worker before propagating a failure, so rounds never overlap background work. */
async function forEachBounded<T>(items: readonly T[], limit: number, work: (item: T) => Promise<void>): Promise<void> {
  let next = 0;
  let failure: unknown;
  let failed = false;
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (next < items.length) {
      const item = items[next++]!;
      try { await work(item); }
      catch (error) { if (!failed) { failed = true; failure = error; } }
    }
  }));
  if (failed) throw failure;
}

export type IncidentKind = 'CONVICT' | 'OFFLINE' | 'ON_FALLBACK';
export interface PipelineIncident { key: string; kind: IncidentKind; title: string; channels: string[]; openedAt: Date; resolvedAt?: Date | null }
export interface IncidentStore {
  /** Opens the incident, or refreshes its channel list if already open. True only when this call opened it. */
  open(incident: Omit<PipelineIncident, 'openedAt' | 'resolvedAt'>, at: Date): Promise<boolean>;
  /** Resolves it if open. Returns the incident only when this call resolved it. */
  resolve(key: string, at: Date): Promise<PipelineIncident | null>;
  openKeys(): Promise<string[]>;
  list(since: Date): Promise<PipelineIncident[]>;
}
export type IncidentNotifier = (event: { type: 'opened' | 'resolved'; incident: PipelineIncident }) => Promise<void>;

/**
 * One incident per broken thing, not per channel: a host that is the convict for five channels
 * (a shared transcoder or CDN) is one incident listing five channels.
 */
export function incidentsFrom(verdicts: readonly ChannelPipelineVerdict[], channels: readonly ChannelTopology[]): Omit<PipelineIncident, 'openedAt' | 'resolvedAt'>[] {
  const byKey = new Map<string, Omit<PipelineIncident, 'openedAt' | 'resolvedAt'>>();
  const add = (key: string, kind: IncidentKind, title: string, channel: string) => {
    const incident = byKey.get(key) ?? { key, kind, title, channels: [] };
    if (!incident.channels.includes(channel)) incident.channels.push(channel);
    byKey.set(key, incident);
  };
  for (const verdict of verdicts) {
    if (!verdict.alertSuppression.alertRecommended) continue;
    const channel = channels.find(c => c.triggerId === verdict.triggerId)!;
    if (verdict.overallState === 'CRITICAL_CONVICT') {
      for (const id of verdict.convictNodeIds) {
        const node = channel.nodes.find(n => n.id === id)!;
        const where = node.host ? `${node.host}${node.port ? `:${node.port}` : ''}` : null;
        add(where ? `node:${where}` : `node:${channel.triggerId}/${id}`, 'CONVICT', where ? `${node.name} (${where}) is down` : `${node.name} of ${channel.channelName} is down`, channel.channelName);
      }
    } else if (verdict.overallState === 'OFFLINE') {
      add(`channel:${channel.triggerId}:offline`, 'OFFLINE', `${channel.channelName} is offline; the cause is in a hop that is not probed`, channel.channelName);
    } else if (verdict.overallState === 'ON_FALLBACK') {
      add(`channel:${channel.triggerId}:fallback`, 'ON_FALLBACK', `${channel.channelName} is on the VOD/filler fallback`, channel.channelName);
    }
  }
  return [...byKey.values()];
}

export function tcpReachable(host: string, port: number, timeoutMs = 3000): Promise<boolean> {
  return new Promise(resolve => {
    const socket = connect({ host, port });
    const done = (ok: boolean) => { socket.destroy(); resolve(ok); };
    socket.setTimeout(timeoutMs, () => done(false));
    socket.once('connect', () => done(true));
    socket.once('error', () => done(false));
  });
}

/** Posts incidents to a webhook. Slack incoming webhooks read `text`; other receivers get the full incident. */
export function webhookNotifier(url: string, log: (message: string) => void = console.error): IncidentNotifier {
  if (!/^https?:\/\//.test(url)) throw new Error('PIPELINE_ALERT_WEBHOOK_URL must be an http(s) URL');
  return async ({ type, incident }) => {
    const text = type === 'opened'
      ? `[${incident.kind === 'CONVICT' ? 'CRITICAL' : 'WARNING'}] ${incident.title}. Channels: ${incident.channels.join(', ')}.`
      : `[RESOLVED] ${incident.title}. Down for ${Math.max(1, Math.round(((incident.resolvedAt?.getTime() ?? Date.now()) - incident.openedAt.getTime()) / 60_000))} min.`;
    try {
      const response = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ text, type, incident }), signal: AbortSignal.timeout(5000) });
      if (!response.ok) log(`Pipeline alert webhook answered HTTP ${response.status}`);
    } catch { log('Pipeline alert webhook unreachable'); }
  };
}

export interface PipelineMonitorOptions {
  /** Read at the start of every round, like targets: channels added or edited in the dashboard apply from the next round. */
  channels: () => Promise<readonly ChannelTopology[]>;
  /** Read at the start of every round, so a target saved from the dashboard is used from the next round. */
  targets: () => Promise<ProbeTargets>;
  rca: PipelineRcaEngine;
  /**
   * Latest status of every stream (the live status snapshot). HLS hops are registered as streams, so the
   * regular stream flow checks them, with its 2-failure rule and frozen-playlist detection; this only reads the result.
   */
  liveStatus: () => Promise<HlsStatusEvent['data'][]>;
  /** A stream status older than this is unknown (UNPROBED), not live: its checks stopped arriving. Default 60 s. */
  maxStatusAgeMs?: number;
  tcpConcurrency?: number;
  incidentConcurrency?: number;
  checkTcp?: (host: string, port: number) => Promise<boolean>;
  incidents?: IncidentStore;
  notify?: IncidentNotifier;
  intervalMs?: number;
  holdDownMs?: number;
  now?: () => number;
  log?: (message: string) => void;
}

/**
 * Probes every hop of every channel on a timer and keeps the latest verdicts; the API only reads them.
 * shortcut: each instance probes on its own (≈70 hops every 10 s); elect one prober if many instances run.
 */
export class PipelineMonitor {
  private verdicts: ChannelPipelineVerdict[] = [];
  private evaluatedAt: Date | null = null;
  private readonly lastDownAt = new Map<string, number>();
  private readonly targetIdentities = new Map<string, string>();
  private readonly tcpState = new Map<string, { fails: number; confirmed: boolean }>();
  private timer: NodeJS.Timeout | undefined;
  private active: Promise<void> | undefined;
  private readonly intervalMs: number;
  private readonly holdDownMs: number;
  private readonly now: () => number;
  private readonly log: (message: string) => void;

  constructor(private readonly options: PipelineMonitorOptions) {
    this.intervalMs = options.intervalMs ?? 10_000;
    this.holdDownMs = options.holdDownMs ?? 30_000;
    if (![this.intervalMs, this.holdDownMs].every(n => Number.isSafeInteger(n) && n >= 1000)) throw new RangeError('Pipeline intervals must be whole milliseconds, 1000 or more');
    for (const limit of [options.tcpConcurrency ?? 20, options.incidentConcurrency ?? 4]) {
      if (!Number.isSafeInteger(limit) || limit < 1 || limit > 100) throw new RangeError('Concurrency must be an integer from 1 to 100');
    }
    this.now = options.now ?? Date.now;
    this.log = options.log ?? console.error;
  }

  start(): void {
    if (this.timer) return;
    this.timer = setInterval(() => { void this.round().catch(() => this.log('Pipeline probe round failed; retrying next interval')); }, this.intervalMs);
    this.timer.unref();
    void this.round().catch(() => this.log('Pipeline probe round failed; retrying next interval'));
  }

  async stop(): Promise<void> {
    clearInterval(this.timer); this.timer = undefined;
    await this.active?.catch(() => {});
  }

  reset(): void {
    this.verdicts = []; this.evaluatedAt = null;
    this.lastDownAt.clear(); this.tcpState.clear(); this.targetIdentities.clear();
  }

  status() { return { evaluatedAt: this.evaluatedAt?.toISOString() ?? null, intervalMs: this.intervalMs, channels: this.verdicts }; }

  /** One probe round; a round still running is reused, never overlapped. */
  round(): Promise<void> {
    this.active ??= this.execute().finally(() => { this.active = undefined; });
    return this.active;
  }

  private async observe(targets: ProbeTargets, live: ReadonlyMap<string, HlsStatusEvent['data']> | null, channel: ChannelTopology, nodeId: string): Promise<HopObservation> {
    const target = targets.get(targetKey(channel.triggerId, nodeId));
    if (!target) return null;
    if (target.kind === 'hls') {
      const { serverIp, streamId } = pipelineStream(channel.triggerId, nodeId, target.url);
      const latest = live?.get(JSON.stringify([serverIp, streamId]));
      // Status store unavailable, not checked yet, or checks stopped: unknown, never "down".
      if (!latest || this.now() - Date.parse(latest.checkedAt) > (this.options.maxStatusAgeMs ?? 60_000)) return null;
      return latest.status === 1;
    }
    // Same rule as HLS hops: a hop that was live turns down only after two failed checks in a row.
    const reachable = await (this.options.checkTcp ?? tcpReachable)(target.host, target.port);
    const key = targetKey(channel.triggerId, nodeId);
    const state = this.tcpState.get(key) ?? { fails: 0, confirmed: false };
    if (reachable) { state.fails = 0; state.confirmed = true; }
    else if (++state.fails >= 2 || !state.confirmed) state.confirmed = false;
    this.tcpState.set(key, state);
    return state.confirmed;
  }

  private async execute(): Promise<void> {
    const { rca } = this.options;
    const [channels, targets] = await Promise.all([this.options.channels(), this.options.targets()]);
    const identities = new Map(channels.flatMap(channel => channel.nodes.map(node => {
      const key = targetKey(channel.triggerId, node.id);
      const target = targets.get(key);
      return [key, JSON.stringify(target?.kind === 'tcp' ? ['tcp', target.host, target.port] : target?.kind === 'hls' ? ['hls', target.url] : null)] as const;
    })));
    for (const [key, identity] of this.targetIdentities) {
      if (identities.get(key) !== identity) {
        this.tcpState.delete(key);
        this.lastDownAt.delete(key);
      }
    }
    this.targetIdentities.clear();
    for (const [key, identity] of identities) this.targetIdentities.set(key, identity);
    let live: Map<string, HlsStatusEvent['data']> | null = null;
    try { live = new Map((await this.options.liveStatus()).map(s => [JSON.stringify([s.serverIp, s.streamId]), s])); }
    catch { this.log('Pipeline: live stream status unavailable; HLS hops are unknown this round'); }
    const now = this.now();
    const observed = channels.map(() => new Map<string, HopObservation>());
    const jobs = channels.flatMap((channel, index) => channel.nodes.map(node => ({ channel, index, node })));
    await forEachBounded(jobs, this.options.tcpConcurrency ?? 20, async ({ channel, index, node }) => {
      observed[index]!.set(node.id, await this.observe(targets, live, channel, node.id));
    });

    this.verdicts = channels.map((channel, i) => {
      const observations = observed[i]!;
      const recovered = new Set<string>();
      for (const [nodeId, live] of observations) {
        const key = targetKey(channel.triggerId, nodeId);
        if (live === true && now - (this.lastDownAt.get(key) ?? -Infinity) < this.holdDownMs) recovered.add(nodeId);
        if (live === false) this.lastDownAt.set(key, now);
      }
      return rca.evaluate(channel, observations, recovered, new Date(now));
    });
    this.evaluatedAt = new Date(now);
    await this.syncIncidents(channels, new Date(now));
  }

  private async syncIncidents(channels: readonly ChannelTopology[], at: Date): Promise<void> {
    const { incidents, notify } = this.options;
    if (!incidents) return;
    const wanted = incidentsFrom(this.verdicts, channels);
    const keys = new Set(wanted.map(i => i.key));
    const resolvedKeys = (await incidents.openKeys()).filter(key => !keys.has(key) && /^(node|channel):/.test(key));
    await forEachBounded(resolvedKeys, this.options.incidentConcurrency ?? 4, async key => {
      const resolved = await incidents.resolve(key, at);
      if (resolved) await notify?.({ type: 'resolved', incident: resolved });
    });
    await forEachBounded(wanted, this.options.incidentConcurrency ?? 4, async incident => {
      if (await incidents.open(incident, at)) await notify?.({ type: 'opened', incident: { ...incident, openedAt: at } });
    });
  }
}
