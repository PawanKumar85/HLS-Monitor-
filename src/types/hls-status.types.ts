/** Transport-neutral live observation. Delivery is independent of persistence. */
export interface HlsStatusEvent {
  type: "hls:status";
  data: {
    streamId: string;
    serverIp: string;
    status: 0 | 1;
    latencyMs: number;
    checkedAt: string;
  };
}
