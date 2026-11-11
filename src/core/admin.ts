import type { Redis } from "ioredis";
import type { TopicConfig, TopicMetadata } from "../types/kafka.types.js";

export class KafkaAdmin {
  constructor(private redis: Redis) {}

  /**
   * Helper: Redis stream key format: kafka:topic:<topic>:p:<partition>
   */
  public static getStreamKey(topic: string, partition: number): string {
    return `kafka:topic:${topic}:p:${partition}`;
  }

  /**
   * Metadata key stores partition count for a topic: kafka:meta:topic:<topic>
   */
  public static getMetaKey(topic: string): string {
    return `kafka:meta:topic:${topic}`;
  }

  public async createTopics(topics: TopicConfig[]): Promise<void> {
    for (const config of topics) {
      const numPartitions = config.numPartitions ?? 1;
      const metaKey = KafkaAdmin.getMetaKey(config.topic);

      await this.redis.hset(metaKey, {
        topic: config.topic,
        partitions: numPartitions.toString(),
        createdAt: Date.now().toString(),
      });

      // Initialize streams for each partition
      for (let p = 0; p < numPartitions; p++) {
        const streamKey = KafkaAdmin.getStreamKey(config.topic, p);
        // Create group with MKSTREAM so stream is created
        try {
          await this.redis.xgroup("CREATE", streamKey, "default-init", "$", "MKSTREAM");
          await this.redis.xgroup("DESTROY", streamKey, "default-init");
        } catch {
          // Stream/group may already exist
        }
      }
    }
  }

  public async getTopicPartitions(topic: string): Promise<number> {
    const metaKey = KafkaAdmin.getMetaKey(topic);
    const count = await this.redis.hget(metaKey, "partitions");
    return count ? parseInt(count, 10) : 1;
  }

  public async listTopics(): Promise<string[]> {
    const keys: string[] = await this.redis.keys("kafka:meta:topic:*");
    return keys.map((k: string) => k.replace("kafka:meta:topic:", ""));
  }

  public async deleteTopic(topic: string): Promise<void> {
    const numPartitions = await this.getTopicPartitions(topic);
    const streamKeys: string[] = [];

    for (let p = 0; p < numPartitions; p++) {
      streamKeys.push(KafkaAdmin.getStreamKey(topic, p));
    }

    if (streamKeys.length > 0) {
      await this.redis.del(...streamKeys);
    }
    await this.redis.del(KafkaAdmin.getMetaKey(topic));
  }

  public async fetchTopicMetadata(topic: string): Promise<TopicMetadata> {
    const numPartitions = await this.getTopicPartitions(topic);
    const partitionInfo: TopicMetadata["partitions"] = [];

    for (let p = 0; p < numPartitions; p++) {
      const streamKey = KafkaAdmin.getStreamKey(topic, p);
      try {
        const info = (await this.redis.xinfo("STREAM", streamKey)) as (string | number)[];
        const lengthIdx = info.indexOf("length");
        const firstIdx = info.indexOf("first-entry");
        const lastIdx = info.indexOf("last-entry");

        const length = lengthIdx !== -1 ? Number(info[lengthIdx + 1]) : 0;
        const firstEntry =
          firstIdx !== -1 ? (info[firstIdx + 1] as unknown as [string, string[]]) : null;
        const lastEntry =
          lastIdx !== -1 ? (info[lastIdx + 1] as unknown as [string, string[]]) : null;

        partitionInfo.push({
          partition: p,
          streamKey,
          length,
          firstOffset: firstEntry ? firstEntry[0] : undefined,
          lastOffset: lastEntry ? lastEntry[0] : undefined,
        });
      } catch {
        partitionInfo.push({
          partition: p,
          streamKey,
          length: 0,
          firstOffset: undefined,
          lastOffset: undefined,
        });
      }
    }

    return {
      topic,
      partitions: partitionInfo,
    };
  }
}
