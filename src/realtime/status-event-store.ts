import type { HlsStatusEvent } from '../types/hls-status.types.js';

export interface StatusCursor {
  cursor: string;
  sequence: number;
  epoch: string;
}

export interface RecordedStatus
  extends HlsStatusEvent, StatusCursor { }

export interface StatusSnapshot extends StatusCursor {
  streams: HlsStatusEvent["data"][];
}

export type StatusReplay = {
  kind: 'events';
  events: RecordedStatus[];
  head: string
} | { kind: 'reset' };

export interface StatusEventStore {
  append(event: HlsStatusEvent): Promise<void>;
  snapshot(): Promise<StatusSnapshot>;
  readAfter(cursor: string, limit: number): Promise<StatusReplay>;
}

export function parseCursor(cursor: string): { epoch: string; sequence: number } | null {
  const match = /^([a-f0-9-]{36}):(0|[1-9]\d{0,15})$/.exec(cursor);
  if (!match) return null;
  const sequence = Number(match[2]);
  return Number.isSafeInteger(sequence) ? { epoch: match[1]!, sequence } : null;
}
