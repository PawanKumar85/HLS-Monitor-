/** Why a check was offline. Stored with each offline check so the report can show what breaks per server. */
export type HlsFailureReason = 'TIMEOUT' | 'DNS' | 'REFUSED' | 'HTTP_404' | 'HTTP_4XX' | 'HTTP_5XX' | 'BAD_PLAYLIST' | 'EMPTY_SEGMENT' | 'ENDED' | 'FROZEN' | 'NETWORK';
export interface HlsHealthResult {
  status: 0 | 1;
  latency: number;
  /** Set only when status is 0. */
  reason?: HlsFailureReason;
}
