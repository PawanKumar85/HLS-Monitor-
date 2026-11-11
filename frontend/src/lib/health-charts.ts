import type { ChannelPipelineVerdict, Topology, SavedProbe, HopEvaluation } from '../Pipeline';
import { serverDomain } from './workspace-graph';
export type HealthBucket = 'healthy' | 'degraded' | 'offline' | 'unknown';
export interface ChartHop { id: string; channel: string; name: string; role: string; server: string; srt: boolean; state: HealthBucket }
export interface HealthRow { id: string; label: string; healthy: number; degraded: number; offline: number; unknown: number; total: number; hops: ChartHop[] }
export const ROLES = ['Main', 'Backup', 'SCTE', 'Transcoding', 'Final / CDN', 'VOD fallback'];
export function hopHealth(hop?: HopEvaluation): HealthBucket {
  if (!hop || hop.status === 'UNPROBED' || hop.isLive === null) return 'unknown';
  if (hop.status === 'FAILOVER_ACTIVE' || hop.status === 'FALLBACK_ACTIVE') return 'degraded';
  if (hop.status === 'CONVICT' || hop.status === 'CASCADED' || hop.status === 'STANDBY' || hop.isLive === false) return 'offline';
  return 'healthy';
}
export function healthCharts(channels: Topology[], probes: SavedProbe[], verdicts: ChannelPipelineVerdict[], fresh: boolean) {
  const targets = new Map(probes.map(p => [JSON.stringify([p.triggerId, p.nodeId]), p]));
  const evaluations = new Map(verdicts.flatMap(c => c.hopEvaluations.map(h => [JSON.stringify([c.triggerId, h.nodeId]), h] as const)));
  const hops: ChartHop[] = channels.flatMap(channel => channel.nodes.map(node => {
    const id = JSON.stringify([channel.triggerId, node.id]);
    const role = node.type === 'FALLBACK_SOURCE' ? 'VOD fallback' : node.isBackup ? 'Backup' : node.type === 'INGEST' ? 'Main' : node.type === 'SCTE_RELAY' ? 'SCTE' : node.type === 'TRANSCODER' ? 'Transcoding' : 'Final / CDN';
    return { id, srt: node.transport === 'srt', channel: channel.channelName, name: node.name, role, server: serverDomain(node, targets.get(id)) ?? 'Server not configured', state: fresh ? hopHealth(evaluations.get(id)) : 'unknown' };
  }));
  function group(key: (hop: ChartHop) => string): HealthRow[] {
    const groups = new Map<string, HealthRow>();
    for (const hop of hops) {
      const id = key(hop);
      const row = groups.get(id) ?? { id, label: id, healthy: 0, degraded: 0, offline: 0, unknown: 0, total: 0, hops: [] };
      row[hop.state]++; row.total++; row.hops.push(hop); groups.set(id, row);
    }
    return [...groups.values()].sort((a, b) => b.offline - a.offline || b.degraded - a.degraded || a.label.localeCompare(b.label));
  }
  const byChannel = channels.map(channel => {
    const members = hops.filter(h => JSON.parse(h.id)[0] === channel.triggerId);
    return { id: channel.triggerId, label: channel.channelName, total: members.length, healthy: members.filter(h => h.state === 'healthy').length, degraded: members.filter(h => h.state === 'degraded').length, offline: members.filter(h => h.state === 'offline').length, unknown: members.filter(h => h.state === 'unknown').length, hops: members };
  }).sort((a, b) => b.offline - a.offline || b.degraded - a.degraded || a.label.localeCompare(b.label));
  const roles = group(h => h.role);
  return { servers: group(h => h.server), channels: byChannel, roles: ROLES.map(role => roles.find(r => r.id === role) ?? { id: role, label: role, healthy: 0, degraded: 0, offline: 0, unknown: 0, total: 0, hops: [] }) };
}
