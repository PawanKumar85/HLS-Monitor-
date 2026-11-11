import type { ChannelPipelineVerdict, Topology, TopologyNode, HopEvaluation } from '../Pipeline';
export interface SignalHop extends HopEvaluation { backup: boolean; fallback: boolean; host?: string; feeds: { name: string; type: string }[] }
export interface SignalChannel extends Omit<ChannelPipelineVerdict, 'hopEvaluations'> { hopEvaluations: SignalHop[] }
/** Saved topology is authoritative: new or unprobed nodes must never disappear from the path view. */
export function signalPaths(verdicts: ChannelPipelineVerdict[], topology: Topology[] | null): SignalChannel[] {
  const definitions = topology ?? verdicts.map(c => ({ triggerId: c.triggerId, channelName: c.channelName, nodes: c.hopEvaluations.map(h => ({ id: h.nodeId, name: h.nodeName, type: h.nodeType } as TopologyNode)), edges: [] }));
  return definitions.map(channel => {
    const verdict = verdicts.find(c => c.triggerId === channel.triggerId);
    const evaluations = new Map(verdict?.hopEvaluations.map(h => [h.nodeId, h]));
    const hopEvaluations = channel.nodes.map(node => ({
      ...(evaluations.get(node.id) ?? { nodeId: node.id, nodeName: node.name, nodeType: node.type, isLive: null, status: 'UNPROBED' as const, details: 'Waiting for a verified probe result' }),
      nodeName: node.name, nodeType: node.type,
      backup: !!node.isBackup, fallback: node.type === 'FALLBACK_SOURCE',
      ...(node.host ? { host: node.host } : {}),
      feeds: channel.edges.filter(e => e.fromNodeId === node.id).map(edge => ({ name: channel.nodes.find(n => n.id === edge.toNodeId)?.name ?? edge.toNodeId, type: edge.type })),
    }));
    return { triggerId: channel.triggerId, channelName: channel.channelName, overallState: verdict?.overallState ?? 'UNVERIFIED', rootCauseSummary: verdict?.rootCauseSummary ?? null, convictNodeIds: verdict?.convictNodeIds ?? [], alertSuppression: verdict?.alertSuppression ?? { suppressedCount: 0, alertRecommended: false }, hopEvaluations };
  });
}
