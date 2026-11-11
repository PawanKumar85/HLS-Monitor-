export type RawStreamMessage = [id: string, fields: string[]];

export interface StreamReadOptions {
  streamKey: string;
  groupName: string;
  consumerName: string;
  count: number;
}

export interface StreamReader {
  connect(): Promise<void>;
  read(options: StreamReadOptions & { blockMs: number }): Promise<RawStreamMessage[]>;
  claim(options: StreamReadOptions & { minIdleMs: number; cursor: string }): Promise<{
    cursor: string;
    messages: RawStreamMessage[];
    deletedIds: string[];
  }>;
  /** Interrupt a blocked read and release its dedicated connection. */
  close(): void;
}

export interface StreamSource {
  ensureGroup(streamKey: string, groupName: string): Promise<void>;
  createReader(): StreamReader;
}

export interface MessageAcknowledger {
  acknowledge(streamKey: string, groupName: string, ids: readonly string[]): Promise<number>;
}
