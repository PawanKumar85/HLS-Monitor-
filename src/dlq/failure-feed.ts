import type { HlsDeadLetter } from "./dead-letter.types.js";

/** What the operator UI sees: no raw message fields, consumer or group internals. */
export type FailureView = Pick<HlsDeadLetter, 'failureId' | 'serverIp' | 'originalMessageId' | 'failureStage' | 'errorCode' | 'errorMessage' |
  'attempts' | 'firstFailedAt' | 'lastFailedAt' | 'updatedAt' | 'retryable' | 'revision' | 'resolution' | 'streamId' | 'streamUrl'>;
export interface FailureEvent { cursor: string; failure: FailureView }
export interface FailureSummary {
  total: number;
  byResolution: Record<string, number>;
  /** Open failures only, by the stage that failed. */
  openByStage: Record<string, number>;
}
export interface FailureQuery { resolution?: string; stage?: string; q?: string; before?: string; limit?: number }
export interface FailureFeed {
  /** No cursor: the newest `limit` failures, oldest first. With a cursor: changes after it, in order. */
  readAfter(cursor: string | undefined, limit: number): Promise<FailureEvent[]>;
  /** Counts over the whole history, not just what a page has loaded. */
  summary?(): Promise<FailureSummary>;
  /** Server-side filter and search, newest first, paged with `next`. */
  search?(query: FailureQuery): Promise<{ items: FailureView[]; next?: string }>;
  /** What a replay needs to find the original Redis entry. */
  replayTarget?(failureId: string): Promise<{ originalStreamKey: string; originalMessageId: string; groupName: string; serverIp: string } | null>;
}

/** Cursor = "<projectedAt ms>-<Mongo ObjectId hex>". */
export function parseFailureCursor(cursor: string): { at: Date; id: string } | null {
  const match = /^(\d{1,15})-([a-f0-9]{24})$/.exec(cursor);
  return match ? { at: new Date(Number(match[1])), id: match[2]! } : null;
}

export function failureView(record: HlsDeadLetter): FailureView {
  const { failureId, serverIp, originalMessageId, failureStage, errorCode, errorMessage, attempts, firstFailedAt, lastFailedAt,
    updatedAt, retryable, revision, resolution, streamId, streamUrl } = record;
  return { failureId, serverIp, originalMessageId, failureStage, errorCode, errorMessage, attempts, firstFailedAt, lastFailedAt,
    updatedAt, retryable, revision, resolution, ...(streamId !== undefined ? { streamId } : {}), ...(streamUrl !== undefined ? { streamUrl } : {}) };
}
