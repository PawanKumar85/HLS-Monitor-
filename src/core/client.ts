import type { Redis } from "ioredis";
import { redisConfig, RedisConfig } from "../config/redis.config.js";
import { KafkaAdmin } from "./admin.js";
import { KafkaProducer } from "./producer.js";
import { KafkaConsumer } from "./consumer.js";
import type { ConsumerConfig } from "../types/kafka.types.js";

export class KafkaMimic {
  private redisConfig: RedisConfig;
  private redis: Redis;
  private adminClient: KafkaAdmin;
  private consumers: KafkaConsumer[] = [];

  constructor(customConfig?: RedisConfig) {
    this.redisConfig = customConfig ?? redisConfig;
    this.redis = this.redisConfig.getClient();
    this.adminClient = new KafkaAdmin(this.redis);
  }

  public async connect(): Promise<void> {
    await this.redisConfig.connect();
  }

  public producer(): KafkaProducer {
    return new KafkaProducer(this.redis, this.adminClient);
  }

  public consumer(config: ConsumerConfig): KafkaConsumer {
    // Each consumer uses its own duplicated Redis connection for blocking reads
    const consumerRedis = this.redis.duplicate();
    const consumer = new KafkaConsumer(consumerRedis, this.adminClient, config);
    this.consumers.push(consumer);
    return consumer;
  }

  public admin(): KafkaAdmin {
    return this.adminClient;
  }

  public async disconnect(): Promise<void> {
    for (const consumer of this.consumers) {
      await consumer.stop();
    }
    await this.redisConfig.disconnect();
  }
}
