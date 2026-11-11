import { validateHlsUrl } from "./validator.js";
import { resolveServerIp } from "./network.js";
import { parseHlsStreamMessage } from "./parse-hls-stream-message.js";
export { parseHlsStreamMessage } from "./parse-hls-stream-message.js";
import type {
  HlsStream,
  EnrichedHlsStream,
  HlsConsumedStream,
  ConsumerOptions,
} from "../types/stream.types.js";

/**
 * Universal Stage 3 Producer Decorator:
 * Externally accepts `HlsStream` (caller provides `streamId` & `streamUrl`).
 * Internally validates URL and enriches with `serverIp` and `createdAt` before invoking the method.
 * Supports any additional method arguments via rest parameters.
 */
export function EnrichStream<This, Args extends any[], Result>(
  originalMethod: (
    this: This,
    stream: any,
    ...rest: Args
  ) => Result,
  _context: ClassMethodDecoratorContext<
    This,
    (this: This, stream: HlsStream, ...rest: Args) => Result
  >
) {
  return async function (
    this: This,
    stream: HlsStream,
    ...rest: Args
  ): Promise<Awaited<Result>> {
    validateHlsUrl(stream.streamUrl);

    const serverIp = await resolveServerIp(stream.streamUrl);

    const enriched: EnrichedHlsStream = {
      ...stream,
      serverIp,
      createdAt: new Date().toISOString(),
    };

    return await originalMethod.call(this, enriched, ...rest);
  };
}

// Alias for convenience
export const Decorder = EnrichStream;

/**
 * Universal Stage 3 Consumer Decorator for `consume()`:
 * Automatically intercepts incoming Redis stream messages in `consume()`,
 * parses raw fields, computes `receivedAt`, calculates latency `delay`,
 * and forwards the parsed `HlsConsumedStream` to the user's `onMessage` handler.
 */
export function DecodeHlsStream<This, Result>(
  originalMethod: (
    this: This,
    serverIp: string,
    onMessage: any,
    options?: ConsumerOptions
  ) => Result,
  _context: ClassMethodDecoratorContext<
    This,
    (
      this: This,
      serverIp: string,
      onMessage: (msg: HlsConsumedStream) => Promise<void> | void,
      options?: ConsumerOptions
    ) => Result
  >
) {
  return async function (
    this: This,
    serverIp: string,
    onMessage: (msg: HlsConsumedStream) => Promise<void> | void,
    options: ConsumerOptions = {}
  ): Promise<Awaited<Result>> {
    const wrappedHandler = async (messageId: string, rawFields: string[]) => {
      const consumed = parseHlsStreamMessage(messageId, rawFields);
      await onMessage(consumed);
    };

    return await originalMethod.call(this, serverIp, wrappedHandler as any, options);
  };
}
