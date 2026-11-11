import type { ChannelPipelineVerdict, Topology } from '../Pipeline';
export const CHANNEL_LABELS: Record<ChannelPipelineVerdict['overallState'], string> = { OPTIMAL: 'Healthy', DEGRADED_FAILOVER: 'On backup', ON_FALLBACK: 'VOD fallback', CRITICAL_CONVICT: 'Root cause detected', OFFLINE: 'Offline', UNVERIFIED: 'Unverified' };
const priority = { CRITICAL_CONVICT: 0, OFFLINE: 0, ON_FALLBACK: 1, DEGRADED_FAILOVER: 2, UNVERIFIED: 3, OPTIMAL: 4 };
export function dashboardSummary(verdicts: ChannelPipelineVerdict[], configured: Topology[] | null) {
  const ids = configured ? new Set(configured.map(c => c.triggerId)) : null;
  const channels = verdicts.filter(c => !ids || ids.has(c.triggerId));
  const known = new Set(channels.map(c => c.triggerId));
  const pending = configured?.filter(c => !known.has(c.triggerId)) ?? [];
  return {
    total: configured?.length ?? channels.length,
    healthy: channels.filter(c => c.overallState === 'OPTIMAL').length,
    critical: channels.filter(c => ['CRITICAL_CONVICT', 'OFFLINE', 'ON_FALLBACK'].includes(c.overallState)).length,
    backup: channels.filter(c => c.overallState === 'DEGRADED_FAILOVER').length,
    unknown: channels.filter(c => c.overallState === 'UNVERIFIED').length + pending.length,
    unprobed: channels.reduce((sum, c) => sum + c.hopEvaluations.filter(h => h.status === 'UNPROBED').length, 0),
    pending,
    channels: [...channels].sort((a, b) => priority[a.overallState] - priority[b.overallState] || a.channelName.localeCompare(b.channelName)),
  };
}

// Shared with the backend (push notifications link to the same Reports view).
export { incidentServer } from '../../../src/monitoring/incident-server';
