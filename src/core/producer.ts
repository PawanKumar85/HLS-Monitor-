import type { Redis } from "ioredis";
import type { ProducerRecord, RecordMetadata } from "../types/kafka.types.js";
import { KafkaAdmin } from "./admin.js";
import { hashKey, RoundRobinPartitioner } from "../utils/partitioner.js";

export class KafkaProducer {
  private roundRobin = new RoundRobinPartitioner();
  private partitionCountCache = new Map<string, number>();

  constructor(
    private redis: Redis,
    private admin: KafkaAdmin
  ) {}

  public async send<T = unknown>(record: ProducerRecord<T>): Promise<RecordMetadata> {
    const { topic, key, value, headers } = record;

    let numPartitions = this.partitionCountCache.get(topic);
    if (!numPartitions) {
      numPartitions = await this.admin.getTopicPartitions(topic);
      this.partitionCountCache.set(topic, numPartitions);
    }

    let partition: number;
    if (typeof record.partition === "number") {
      partition = record.partition;
    } else if (key) {
      partition = hashKey(key, numPartitions);
    } else {
      partition = this.roundRobin.getNextPartition(numPartitions);
    }

    const streamKey = KafkaAdmin.getStreamKey(topic, partition);
    const timestamp = Date.now();

    const fields: string[] = [
      "key",
      key ?? "",
      "value",
      typeof value === "string" ? value : JSON.stringify(value),
      "timestamp",
      timestamp.toString(),
    ];

    if (headers) {
      fields.push("headers", JSON.stringify(headers));
    }

    const offset = await this.redis.xadd(streamKey, "*", ...fields);

    return {
      topic,
      partition,
      offset: offset as string,
      timestamp,
    };
  }

  public async sendBatch<T = unknown>(records: ProducerRecord<T>[]): Promise<RecordMetadata[]> {
    const results: RecordMetadata[] = [];
    for (const record of records) {
      const meta = await this.send(record);
      results.push(meta);
    }
    return results;
  }
}

