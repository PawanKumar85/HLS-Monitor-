import { randomUUID } from 'node:crypto';
import type { HlsStatusEvent } from '../../src/types/hls-status.types.js';
import { parseCursor, type RecordedStatus, type StatusEventStore, type StatusReplay } from '../../src/realtime/status-event-store.js';
export class MemoryStatusStore implements StatusEventStore {
  epoch = randomUUID();
  events: RecordedStatus[] = [];
  latest = new Map<string, HlsStatusEvent['data']>();
  async append(event: HlsStatusEvent) {
    const sequence = this.events.length + 1;
    this.events.push({ ...event, epoch: this.epoch, sequence, cursor: `${this.epoch}:${sequence}` });
    this.latest.set(JSON.stringify([event.data.serverIp, event.data.streamId]), event.data);
  }
  async snapshot() { return { epoch: this.epoch, sequence: this.events.length, cursor: `${this.epoch}:${this.events.length}`, streams: [...this.latest.values()] }; }
  async readAfter(cursor: string, limit: number): Promise<StatusReplay> {
    const parsed = parseCursor(cursor);
    if (!parsed || parsed.epoch !== this.epoch || parsed.sequence > this.events.length) return { kind: 'reset' };
    return { kind: 'events', events: this.events.slice(parsed.sequence, parsed.sequence + limit), head: `${this.epoch}:${this.events.length}` };
  }
}
