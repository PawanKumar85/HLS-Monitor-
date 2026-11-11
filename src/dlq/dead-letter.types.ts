export type FailureStage = "PARSE" | "HEALTH_CHECK" | "QUEUE" | "PERSISTENCE";
export interface MessageContext {
  originalStreamKey: string;
  originalMessageId: string;
  groupName: string;
  serverIp: string;
  consumerName: string;
  rawFields: string[];
}
export interface HlsDeadLetter extends MessageContext {
  failureId: string;
  failureStage: FailureStage;
  errorCode: string;
  errorMessage: string;
  attempts: number;
  firstFailedAt: string;
  lastFailedAt: string;
  updatedAt: string;
  retryable: boolean;
  bounded: boolean;
  generation: number;
  revision: number;
  resolution: "OPEN" | "REPLAY_REQUESTED" | "RESOLVED";
  streamId?: string;
  streamUrl?: string;
}
export interface RetryState {
  state: "retry" | "dead_lettered" | "replay_requested" | "replaying";
  nextRetryAt: number;
  record: HlsDeadLetter;
}
export interface RetrySettings { maxAttempts: number; baseDelayMs: number; maxDelayMs: number }
export interface FailureLedger {
  get(context: MessageContext): Promise<RetryState | null>;
  recordFailure(context: MessageContext, failure: ProcessingFailure, settings: RetrySettings): Promise<RetryState>;
  moveToDeadLetter(context: MessageContext, record: HlsDeadLetter): Promise<void>;
  clearSucceeded(streamKey: string, groupName: string, ids: readonly string[]): Promise<void>;
}
export interface MessageFailureHandler {
  prepare(context: MessageContext): Promise<boolean>;
  failed(context: MessageContext, error: unknown): Promise<boolean>;
}
export class ProcessingFailure extends Error {
  constructor(
    readonly stage: FailureStage, readonly code: string, message: string,
    readonly retryable = true, readonly bounded = true
  ) { super(message); this.name = "ProcessingFailure"; }
}
/** The batch owns this message already; consumer must not dead-letter it again. */
export class BufferedMessageError extends Error {
  constructor() { super("Message retained by batch queue for persistence retry"); }
}
export { PartialMetricsWriteError } from "../storage/persistence-errors.js";
export interface FailureHistoryWriter {
  write(record: HlsDeadLetter): Promise<void>;
  /** Same as write for each record, in one round trip. Optional: the projector falls back to write. */
  writeMany?(records: readonly HlsDeadLetter[]): Promise<void>;
}
export interface DeadLetterArchiveSource {
  servers(): Promise<string[]>;
  readArchiveBatch(serverIp: string, consumer: string, count: number): Promise<{ id: string; record: HlsDeadLetter }[]>;
  archiveCompleted(serverIp: string, id: string): Promise<void>;
}
