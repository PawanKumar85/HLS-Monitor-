export interface KafkaMessage<T = unknown> {
  topic: string;
  partition: number;
  offset: string;
  key?: string | undefined;
  value: T;
  timestamp: number;
  headers?: Record<string, string> | undefined;
}

export interface ProducerRecord<T = unknown> {
  topic: string;
  partition?: number | undefined;
  key?: string | undefined;
  value: T;
  headers?: Record<string, string> | undefined;
}

export interface RecordMetadata {
  topic: string;
  partition: number;
  offset: string;
  timestamp: number;
}

export interface ConsumerConfig {
  groupId: string;
  consumerId?: string | undefined;
  autoCommit?: boolean | undefined;
  batchSize?: number | undefined;
  blockTimeoutMs?: number | undefined;
}

export interface EachMessagePayload<T = unknown> {
  topic: string;
  partition: number;
  message: KafkaMessage<T>;
  commit: () => Promise<void>;
}

export interface ConsumerRunConfig {
  eachMessage: (payload: EachMessagePayload) => Promise<void>;
}

export interface TopicConfig {
  topic: string;
  numPartitions?: number | undefined;
}

export interface TopicMetadata {
  topic: string;
  partitions: {
    partition: number;
    streamKey: string;
    length: number;
    firstOffset?: string | undefined;
    lastOffset?: string | undefined;
  }[];
}
