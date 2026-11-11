import type {
  ChannelPipelineVerdict,
  ChannelState,
  ChannelTopology,
  HopEvaluation,
  HopObservation,
  PipelineNode,
} from '../types/pipeline.types.js';

const DOWN = new Set(['CONVICT', 'CASCADED']);

/**
 * Topological root-cause analysis for one channel, from one probe round. Pure: no I/O, no clock.
 *
 * - A down hop whose primary upstream is down is CASCADED and points at the nearest convict above it.
 * - A down hop whose primary upstream is live (or not probed) is the CONVICT: the first point of failure.
 * - recentlyRecovered: hops that came back within the hold-down window. HLS needs a few segments to
 *   flow downstream, so a hop still down right after its upstream recovered is CASCADED, not a new convict.
 * - Unprobed hops (UDP/SRT, or no target configured) are never convicted and never assumed healthy.
 */
export class PipelineRcaEngine {
  public evaluate(
    channel: ChannelTopology,
    observations: ReadonlyMap<string, HopObservation>,
    recentlyRecovered: ReadonlySet<string> = new Set(),
    now = new Date(),
  ): ChannelPipelineVerdict {
    const incoming = new Map(channel.nodes.map(n => [n.id, channel.edges.filter(e => e.toNodeId === n.id)]));
    const evaluations = new Map<string, HopEvaluation>();
    const convicts: string[] = [];

    for (const node of this.topologicalOrder(channel)) {
      const isLive = observations.get(node.id) ?? null;
      const edges = incoming.get(node.id) ?? [];
      const primaries = edges.filter(e => e.type === 'PRIMARY').map(e => evaluations.get(e.fromNodeId)!);
      const primaryDown = primaries.filter(p => DOWN.has(p.status));
      const base = { nodeId: node.id, nodeName: node.name, nodeType: node.type, isLive };
      let evaluation: HopEvaluation;

      if (isLive === null) {
        evaluation = { ...base, status: 'UNPROBED', details: 'No probe for this hop (UDP/SRT feed, or no target configured)' };
      } else if (isLive) {
        if (!primaryDown.length) {
          evaluation = { ...base, status: 'HEALTHY' };
        } else {
          const alternates = edges.filter(e => e.type === 'FAILOVER' || e.type === 'BYPASS').map(e => observations.get(e.fromNodeId) ?? null);
          const fallbacks = edges.filter(e => e.type === 'FALLBACK');
          if (alternates.some(live => live === true)) evaluation = { ...base, status: 'FAILOVER_ACTIVE', details: 'Fed by a backup or bypass while the primary upstream is down' };
          else if (fallbacks.length) evaluation = { ...base, status: 'FALLBACK_ACTIVE', details: 'Primary upstream down; only the VOD/filler fallback can be feeding this hop' };
          // A live hop with every known feed down: one of the upstream probes is probably wrong.
          else evaluation = { ...base, status: 'HEALTHY', details: 'Live although its primary upstream reports down; check the upstream probe' };
        }
      } else if (node.isBackup || node.type === 'FALLBACK_SOURCE') {
        evaluation = { ...base, status: 'STANDBY', details: 'Backup or fallback source is down: redundancy lost, no outage on its own' };
      } else if (primaryDown.length) {
        const convictNodeId = primaryDown[0]!.status === 'CONVICT' ? primaryDown[0]!.nodeId : primaryDown[0]!.convictNodeId;
        evaluation = { ...base, status: 'CASCADED', convictNodeId, details: `Down because ${this.name(channel, convictNodeId)} is down` };
      } else if (primaries.some(p => recentlyRecovered.has(p.nodeId))) {
        evaluation = { ...base, status: 'CASCADED', details: 'Upstream recovered moments ago; waiting for it to reach this hop' };
      } else {
        const unverified = primaries.some(p => p.status === 'UNPROBED');
        evaluation = { ...base, status: 'CONVICT', details: unverified ? 'First probed hop that is down; the hop above it is not probed, so the failure may start earlier' : 'First point of failure (root cause)' };
        convicts.push(node.id);
      }
      evaluations.set(node.id, evaluation);
    }

    const egress = channel.nodes.find(n => n.type === 'EGRESS_CDN') ?? channel.nodes.at(-1)!;
    const isEgressLive = observations.get(egress.id) ?? null;
    const hops = [...evaluations.values()];
    const has = (status: HopEvaluation['status']) => hops.some(h => h.status === status);
    let overallState: ChannelState;
    if (isEgressLive === null) overallState = 'UNVERIFIED';
    else if (isEgressLive) overallState = has('FALLBACK_ACTIVE') ? 'ON_FALLBACK' : has('FAILOVER_ACTIVE') || convicts.length ? 'DEGRADED_FAILOVER' : 'OPTIMAL';
    else overallState = convicts.length ? 'CRITICAL_CONVICT' : 'OFFLINE';

    const suppressedCount = hops.filter(h => h.status === 'CASCADED').length;
    const first = convicts[0] ?? null;
    const label = (id: string) => this.label(channel.nodes.find(n => n.id === id)!);
    const rootCauseSummary =
      overallState === 'CRITICAL_CONVICT' ? `Root cause: ${convicts.map(label).join(' and ')} offline.${suppressedCount ? ` ${suppressedCount} downstream hop${suppressedCount === 1 ? '' : 's'} affected.` : ''}`
      : overallState === 'OFFLINE' ? 'Egress is down, but no probed hop explains it; the cause is in a hop that is not probed.'
      : overallState === 'ON_FALLBACK' ? 'Egress is up but fed by the VOD/filler fallback: viewers do not see the live channel.'
      : overallState === 'DEGRADED_FAILOVER' ? (first ? `${label(first)} is down; a backup path keeps the channel live.` : 'Channel is live on a backup path.')
      : overallState === 'UNVERIFIED' ? 'Egress is not probed, so viewer impact is unknown.'
      : null;

    return {
      triggerId: channel.triggerId,
      channelName: channel.channelName,
      isEgressLive,
      overallState,
      rootCauseConvictNodeId: first,
      convictNodeIds: convicts,
      rootCauseSummary,
      hopEvaluations: hops,
      evaluatedAt: now.toISOString(),
      alertSuppression: {
        suppressedCount,
        alertRecommended: overallState === 'CRITICAL_CONVICT' || overallState === 'OFFLINE' || overallState === 'ON_FALLBACK',
      },
    };
  }

  private name(channel: ChannelTopology, id: string | undefined) { return channel.nodes.find(n => n.id === id)?.name ?? 'an upstream hop'; }
  private label(node: PipelineNode) { return node.host ? `${node.name} (${node.host}${node.port ? `:${node.port}` : ''})` : node.name; }

  /** Kahn's algorithm. parseChannel rejects loops, so the fallback order is never used in practice. */
  private topologicalOrder(channel: ChannelTopology): PipelineNode[] {
    const inDegree = new Map(channel.nodes.map(n => [n.id, 0]));
    for (const e of channel.edges) inDegree.set(e.toNodeId, (inDegree.get(e.toNodeId) ?? 0) + 1);
    const queue = channel.nodes.filter(n => inDegree.get(n.id) === 0).map(n => n.id);
    const order: string[] = [];
    while (queue.length) {
      const id = queue.shift()!; order.push(id);
      for (const e of channel.edges) if (e.fromNodeId === id) { inDegree.set(e.toNodeId, inDegree.get(e.toNodeId)! - 1); if (inDegree.get(e.toNodeId) === 0) queue.push(e.toNodeId); }
    }
    if (order.length !== channel.nodes.length) return channel.nodes;
    const byId = new Map(channel.nodes.map(n => [n.id, n]));
    return order.map(id => byId.get(id)!);
  }
}
