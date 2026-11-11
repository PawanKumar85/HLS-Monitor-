import type { Redis } from "ioredis";
import type {
  ConsumerConfig,
  ConsumerRunConfig,
  KafkaMessage,
} from "../types/kafka.types.js";
import { KafkaAdmin } from "./admin.js";

interface SubscribedTopic {
  topic: string;
  fromBeginning: boolean;
}

export class KafkaConsumer {
  private groupId: string;
  private consumerId: string;
  private autoCommit: boolean;
  private batchSize: number;
  private blockTimeoutMs: number;
  private isRunning = false;
  private subscribedTopics: SubscribedTopic[] = [];

  constructor(
    private redis: Redis,
    private admin: KafkaAdmin,
    config: ConsumerConfig
  ) {
    this.groupId = config.groupId;
    this.consumerId = config.consumerId || `consumer-${Math.random().toString(36).substring(2, 9)}`;
    this.autoCommit = config.autoCommit ?? true;
    this.batchSize = config.batchSize ?? 10;
    this.blockTimeoutMs = config.blockTimeoutMs ?? 2000;
  }

  public async subscribe(subscription: { topic: string; fromBeginning?: boolean }): Promise<void> {
    this.subscribedTopics.push({
      topic: subscription.topic,
      fromBeginning: subscription.fromBeginning ?? false,
    });
  }

  private async initializeConsumerGroups(): Promise<string[]> {
    const streamKeys: string[] = [];

    for (const sub of this.subscribedTopics) {
      const numPartitions = await this.admin.getTopicPartitions(sub.topic);
      const startId = sub.fromBeginning ? "0" : "$";

      for (let p = 0; p < numPartitions; p++) {
        const streamKey = KafkaAdmin.getStreamKey(sub.topic, p);
        streamKeys.push(streamKey);

        try {
          // XGROUP CREATE <key> <groupname> <id> MKSTREAM
          await this.redis.xgroup("CREATE", streamKey, this.groupId, startId, "MKSTREAM");
        } catch (err: unknown) {
          // BusyGroup error is expected if the group already exists
          const message = err instanceof Error ? err.message : String(err);
          if (!message.includes("BUSYGROUP")) {
            console.error(`[KafkaConsumer] Error creating group for ${streamKey}:`, err);
          }
        }
      }
    }

    return streamKeys;
  }

  public async run(config: ConsumerRunConfig): Promise<void> {
    const streamKeys = await this.initializeConsumerGroups();
    if (streamKeys.length === 0) {
      throw new Error("No topics subscribed. Call subscribe() before run().");
    }

    this.isRunning = true;

    // Run continuous event loop
    while (this.isRunning) {
      try {
        const ids = streamKeys.map(() => ">");

        // XREADGROUP GROUP <group> <consumer> BLOCK <ms> COUNT <count> STREAMS <streams...> <ids...>
        const response = (await this.redis.xreadgroup(
          "GROUP",
          this.groupId,
          this.consumerId,
          "COUNT",
          this.batchSize,
          "BLOCK",
          this.blockTimeoutMs,
          "STREAMS",
          ...streamKeys,
          ...ids
        )) as [string, [string, string[]][]][] | null;

        if (!response || response.length === 0) {
          continue;
        }

        for (const [streamKey, rawMessages] of response) {
          const { topic, partition } = this.parseStreamKey(streamKey);

          for (const [offset, fields] of rawMessages) {
            const message = this.parseMessage(topic, partition, offset, fields);

            const commit = async () => {
              await this.redis.xack(streamKey, this.groupId, offset);
            };

            await config.eachMessage({
              topic,
              partition,
              message,
              commit,
            });

            if (this.autoCommit) {
              await commit();
            }
          }
        }
      } catch (err) {
        if (!this.isRunning) break;
        console.error("[KafkaConsumer] Polling loop error:", err);
        // Short pause before retrying
        await new Promise((res) => setTimeout(res, 500));
      }
    }
  }

  public async stop(): Promise<void> {
    this.isRunning = false;
  }

  private parseStreamKey(streamKey: string): { topic: string; partition: number } {
    // kafka:topic:<topic>:p:<partition>
    const parts = streamKey.split(":");
    const topic = parts[2] || "unknown";
    const partition = parseInt(parts[4] || "0", 10);
    return { topic, partition };
  }

  private parseMessage(
    topic: string,
    partition: number,
    offset: string,
    fields: string[]
  ): KafkaMessage {
    const data: Record<string, string> = {};
    for (let i = 0; i < fields.length; i += 2) {
      const k = fields[i];
      const v = fields[i + 1];
      if (k !== undefined && v !== undefined) {
        data[k] = v;
      }
    }

    let parsedValue: unknown = data["value"];
    try {
      if (data["value"]) {
        parsedValue = JSON.parse(data["value"]);
      }
    } catch {
      // keep raw string
    }

    let headers: Record<string, string> | undefined;
    if (data["headers"]) {
      try {
        headers = JSON.parse(data["headers"]);
      } catch {
        // ignore
      }
    }

    return {
      topic,
      partition,
      offset,
      key: data["key"] || undefined,
      value: parsedValue,
      timestamp: Number(data["timestamp"] || Date.now()),
      headers,
    };
  }
}
