/**
 * Broadcaster-to-Client Pipeline & Topology Types
 */

export type PipelineNodeType =
  | 'INGEST'
  | 'SCTE_RELAY'
  | 'TRANSCODER'
  | 'EGRESS_CDN'
  | 'FALLBACK_SOURCE';

export type PipelineEdgeType =
  | 'PRIMARY'
  | 'FAILOVER'
  | 'BYPASS'
  | 'FALLBACK';

export type HopDiagnosticStatus =
  | 'HEALTHY'
  | 'CONVICT'          // Root cause: first failed hop whose primary upstream is live (or not probed)
  | 'CASCADED'         // Down because a primary upstream hop is down, or upstream recovered moments ago
  | 'FAILOVER_ACTIVE'  // Primary upstream down; a live backup or bypass feeds this hop
  | 'FALLBACK_ACTIVE'  // Primary upstream down; only the VOD/filler fallback can be feeding this hop
  | 'STANDBY'          // A backup or fallback source that is down: no outage, but redundancy is lost
  | 'UNPROBED';        // No probe target (UDP/SRT, or not configured) or the probe infrastructure failed

/** Topology only, entered in the dashboard (see config/pipeline-channels.ts). How a hop is probed: config/pipeline-probes.ts. */
export interface PipelineNode {
  id: string;
  name: string;
  type: PipelineNodeType;
  host?: string;
  port?: number;
  isBackup?: boolean;
  transport?: 'srt';
}

export interface PipelineEdge {
  fromNodeId: string;
  toNodeId: string;
  type: PipelineEdgeType;
}

export interface ChannelTopology {
  triggerId: string;
  channelName: string;
  nodes: PipelineNode[];
  edges: PipelineEdge[];
}

/** One hop's confirmed state after a probe round: true live, false down, null not probed or unknown. */
export type HopObservation = boolean | null;

export interface HopEvaluation {
  nodeId: string;
  nodeName: string;
  nodeType: PipelineNodeType;
  isLive: HopObservation;
  status: HopDiagnosticStatus;
  convictNodeId?: string | undefined;
  details?: string | undefined;
}

export type ChannelState =
  | 'OPTIMAL'
  | 'DEGRADED_FAILOVER'  // viewers fine, a backup or bypass carries the channel
  | 'ON_FALLBACK'        // egress up but fed by VOD/filler: viewers do not see the live channel
  | 'CRITICAL_CONVICT'   // egress down, root cause found
  | 'OFFLINE'            // egress down, no root cause among probed hops
  | 'UNVERIFIED';        // egress not probed: nothing can be said about viewers

export interface ChannelPipelineVerdict {
  triggerId: string;
  channelName: string;
  isEgressLive: HopObservation;
  overallState: ChannelState;
  /** First convict in topological order; convictNodeIds lists every independent one. */
  rootCauseConvictNodeId: string | null;
  convictNodeIds: string[];
  rootCauseSummary: string | null;
  hopEvaluations: HopEvaluation[];
  evaluatedAt: string;
  alertSuppression: {
    suppressedCount: number;
    alertRecommended: boolean;
  };
}
