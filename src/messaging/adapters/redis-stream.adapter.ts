import type { Redis } from "ioredis";
import type { MessageAcknowledger, RawStreamMessage, StreamReader, StreamReadOptions, StreamSource } from "../../ports/messaging.js";

class RedisStreamReader implements StreamReader {
  constructor(private readonly client: Redis) { }

  async connect(): Promise<void> {
    if (this.client.status === "wait") await this.client.connect();
  }

  async read(options: StreamReadOptions & { blockMs: number }): Promise<RawStreamMessage[]> {
    const response = await this.client.xreadgroup(
      "GROUP", options.groupName, options.consumerName, "COUNT", options.count,
      "BLOCK", options.blockMs, "STREAMS", options.streamKey, ">"
    ) as [string, RawStreamMessage[]][] | null;
    return response?.flatMap(([, messages]) => messages) ?? [];
  }

  async claim(options: StreamReadOptions & { minIdleMs: number; cursor: string }) {
    const response = await this.client.xautoclaim(
      options.streamKey, options.groupName, options.consumerName,
      options.minIdleMs, options.cursor, "COUNT", options.count
    ) as [string, RawStreamMessage[], string[]?];
    return { cursor: response[0], messages: response[1], deletedIds: response[2] ?? [] };
  }

  close(): void { this.client.disconnect(); }
}

/** Redis command syntax and connection handling stay outside application logic. */
export class RedisStreamAdapter implements StreamSource, MessageAcknowledger {
  constructor(private readonly client: Redis) { }
  async ensureGroup(streamKey: string, groupName: string): Promise<void> {
    try {
      await this.client.xgroup("CREATE", streamKey, groupName, "0", "MKSTREAM");
    } catch (error) {
      if (!(error instanceof Error && error.message.includes("BUSYGROUP"))) throw error;
    }
  }
  createReader(): StreamReader { return new RedisStreamReader(this.client.duplicate()); }
  async acknowledge(streamKey: string, groupName: string, ids: readonly string[]): Promise<number> {
    if (!ids.length) return 0;
    return this.client.xack(streamKey, groupName, ...ids);
  }
}
