import { inspectHls } from './hls-health.service.js';
import { tcpReachable } from '../monitoring/pipeline-monitor.js';
import { targetKey, type ProbeTarget, type ProbeTargets } from '../config/pipeline-probes.js';
import type { ChannelTopology, HopObservation, PipelineNode } from '../types/pipeline.types.js';

export interface HopProbeResult {
  nodeId: string;
  isLive: HopObservation;
  latencyMs: number | null;
  reason?: string | undefined;
  checkedAt: Date;
  target?: { kind: 'hls' | 'tcp'; endpoint: string } | undefined;
}

export interface PipelineProberOptions {
  fetcher?: typeof fetch;
  checkTcp?: (host: string, port: number, timeoutMs?: number) => Promise<boolean>;
  defaultTimeoutMs?: number;
  now?: () => Date;
}

/**
 * Concurrently pings each hop in a channel pipeline.
 * - HLS hops reuse inspectHls for playlist + media segment byte verification.
 * - TCP hops use direct socket connection reachability.
 * - Unconfigured hops are marked UNPROBED (null).
 */
export class PipelineProberService {
  private readonly fetcher: typeof fetch;
  private readonly checkTcp: (host: string, port: number, timeoutMs?: number) => Promise<boolean>;
  private readonly defaultTimeoutMs: number;
  private readonly now: () => Date;

  constructor(options: PipelineProberOptions = {}) {
    this.fetcher = options.fetcher ?? fetch;
    this.checkTcp = options.checkTcp ?? tcpReachable;
    this.defaultTimeoutMs = options.defaultTimeoutMs ?? 3000;
    this.now = options.now ?? (() => new Date());
  }

  /**
   * Probes a single node in a channel.
   */
  async probeHop(
    node: PipelineNode,
    target: ProbeTarget | undefined,
    timeoutMs = this.defaultTimeoutMs,
  ): Promise<HopProbeResult> {
    const checkedAt = this.now();

    if (!target) {
      return {
        nodeId: node.id,
        isLive: null,
        latencyMs: null,
        reason: 'UNPROBED',
        checkedAt,
      };
    }

    if (target.kind === 'hls') {
      try {
        const inspection = await inspectHls(target.url, this.fetcher);
        return {
          nodeId: node.id,
          isLive: inspection.health.status === 1,
          latencyMs: inspection.health.latency,
          reason: inspection.health.reason,
          checkedAt,
          target: { kind: 'hls', endpoint: target.url },
        };
      } catch (error) {
        return {
          nodeId: node.id,
          isLive: false,
          latencyMs: null,
          reason: error instanceof Error ? error.name : 'PROBE_ERROR',
          checkedAt,
          target: { kind: 'hls', endpoint: target.url },
        };
      }
    }

    if (target.kind === 'tcp') {
      const start = performance.now();
      try {
        const isLive = await this.checkTcp(target.host, target.port, timeoutMs);
        const latencyMs = Math.round(performance.now() - start);
        return {
          nodeId: node.id,
          isLive,
          latencyMs,
          reason: isLive ? undefined : 'REFUSED',
          checkedAt,
          target: { kind: 'tcp', endpoint: `${target.host}:${target.port}` },
        };
      } catch (error) {
        return {
          nodeId: node.id,
          isLive: false,
          latencyMs: Math.round(performance.now() - start),
          reason: error instanceof Error ? error.name : 'TCP_ERROR',
          checkedAt,
          target: { kind: 'tcp', endpoint: `${target.host}:${target.port}` },
        };
      }
    }

    return {
      nodeId: node.id,
      isLive: null,
      latencyMs: null,
      reason: 'UNPROBED',
      checkedAt,
    };
  }

  /**
   * Concurrently probes every hop in a single channel pipeline.
   * Each hop is executed in parallel with timeout protection.
   */
  async probeChannel(
    channel: ChannelTopology,
    targets: ProbeTargets | ReadonlyMap<string, ProbeTarget>,
    timeoutMs = this.defaultTimeoutMs,
  ): Promise<Map<string, HopProbeResult>> {
    const results = await Promise.all(
      channel.nodes.map(node => {
        const target = targets.get(targetKey(channel.triggerId, node.id));
        return this.probeHop(node, target, timeoutMs);
      }),
    );

    return new Map(results.map(r => [r.nodeId, r]));
  }

  /**
   * Concurrently probes all channels in parallel.
   */
  async probeChannels(
    channels: readonly ChannelTopology[],
    targets: ProbeTargets | ReadonlyMap<string, ProbeTarget>,
    timeoutMs = this.defaultTimeoutMs,
  ): Promise<Map<string, Map<string, HopProbeResult>>> {
    const results = await Promise.all(
      channels.map(async ch => [ch.triggerId, await this.probeChannel(ch, targets, timeoutMs)] as const),
    );
    return new Map(results);
  }

  /**
   * Converts hop probe results to the Map<string, HopObservation> format expected by PipelineRcaEngine.
   */
  toObservations(results: Map<string, HopProbeResult> | Iterable<HopProbeResult>): Map<string, HopObservation> {
    const map = new Map<string, HopObservation>();
    const iterable = results instanceof Map ? results.values() : results;
    for (const res of iterable) {
      map.set(res.nodeId, res.isLive);
    }
    return map;
  }
}

