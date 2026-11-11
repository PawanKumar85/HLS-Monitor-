import type { MessageContext } from "../dlq/dead-letter.types.js";
import type { HlsConsumedStream } from "../types/stream.types.js";
import type { HlsHealthResult } from "../types/hls-health.types.js";
import type { RawStreamMessage } from "./messaging.js";

export interface HlsProbeIdentity { serverIp: string; streamId: string }
export interface HlsHealthChecker {
  check(streamUrl: string, identity?: HlsProbeIdentity): Promise<HlsHealthResult>;
}
export interface ConsumedStreamSink {
  push(message: HlsConsumedStream, groupName?: string, context?: MessageContext): Promise<void>;
}
export type HlsMessageHandler = (message: HlsConsumedStream, health: HlsHealthResult) => void | Promise<void>;
export interface StreamMessageProcessor {
  process(serverIp: string, entry: RawStreamMessage, groupName: string, handler?: HlsMessageHandler, context?: MessageContext): Promise<void>;
}
