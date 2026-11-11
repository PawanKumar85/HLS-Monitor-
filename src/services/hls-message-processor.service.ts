import { BufferedMessageError, ProcessingFailure, type FailureStage, type MessageContext } from "../dlq/dead-letter.types.js";
import { MeasureLatency } from "../decorators/measure-latency.decorator.js";
import { redactError } from "../dlq/redact.js";
import type { ConsumedStreamSink, HlsHealthChecker, HlsProbeIdentity, HlsMessageHandler, StreamMessageProcessor } from "../ports/hls-processing.js";
import type { RawStreamMessage } from "../ports/messaging.js";
import { QueueRecordTooLargeError } from "../ports/queue-errors.js";
import type { StatusPublisher } from "../realtime/status-transport.js";
import { parseHlsStreamMessage } from "../utils/parse-hls-stream-message.js";

function logPublicationFailure(error: unknown): void {
  const message = error instanceof Error ? error.message : String(error);
  console.error("HLS status publication failed:", redactError(message));
}

/** Processes one observation; polling/recovery and persistence batching are separate. */
export class HlsMessageProcessor implements StreamMessageProcessor {
  constructor(
    private readonly checker: HlsHealthChecker,
    private readonly publisher: StatusPublisher,
    private readonly sink: ConsumedStreamSink
  ) {}

  @MeasureLatency
  private async checkStreamHealth(url: string, identity: HlsProbeIdentity) { return this.checker.check(url, identity); }

  async process(serverIp: string, [messageId, fields]: RawStreamMessage, groupName: string, handler?: HlsMessageHandler, context?: MessageContext): Promise<void> {
    let stage: FailureStage = 'PARSE';
    try {
      const message = parseHlsStreamMessage(messageId, fields);
      message.serverIp = serverIp;
      stage = 'HEALTH_CHECK';
      const health = await this.checkStreamHealth(message.streamUrl, { serverIp, streamId: message.streamId });
      message.health = health;
      message.checkedAt = new Date().toISOString();
      try {
        const publication = this.publisher.publish({
          type: "hls:status",
          data: {
            streamId: message.streamId, serverIp, status: health.status,
            latencyMs: health.latency, checkedAt: message.checkedAt,
          },
        });
        void Promise.resolve(publication).catch(logPublicationFailure);
      } catch (error) {
        logPublicationFailure(error);
      }
      stage = 'QUEUE';
      if (handler) await handler(message, health);
      else await this.sink.push(message, groupName, context);
    } catch (error) {
      if (error instanceof BufferedMessageError || error instanceof ProcessingFailure) throw error;
      const oversized = stage === 'QUEUE' && error instanceof QueueRecordTooLargeError;
      const code = stage === 'PARSE' ? 'INVALID_MESSAGE' : oversized ? 'QUEUE_RECORD_TOO_LARGE' : 'WORKER_EXCEPTION';
      throw new ProcessingFailure(stage, code, error instanceof Error ? error.message : String(error), stage !== 'PARSE' && !oversized);
    }
  }
}
