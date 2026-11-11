import type { ServerResponse } from 'node:http';

/** One bounded, ordered writer for data, heartbeat and control frames. */
export class SseClientWriter {
  private tail: Promise<unknown> = Promise.resolve();
  private queuedBytes = 0;
  private accepting = true;
  private closed = false;

  constructor(
    private readonly response: ServerResponse,
    private readonly onClose: () => void,
    private readonly drainTimeoutMs = 5000,
    private readonly maxBufferedBytes = 1024 * 1024,
  ) {
    response.once('close', this.close);
    response.once('error', this.close);
  }

  readonly close = (): void => {
    if (this.closed) return;
    this.closed = true;
    this.accepting = false;
    this.onClose();
    if (!this.response.destroyed) this.response.destroy();
  };

  send(frame: string): Promise<boolean> {
    const bytes = Buffer.byteLength(frame);
    if (!this.accepting || this.response.destroyed || this.response.writableEnded) return Promise.resolve(false);
    if (this.queuedBytes + this.response.writableLength + bytes > this.maxBufferedBytes) {
      this.close(); return Promise.resolve(false);
    }
    this.queuedBytes += bytes;
    const operation = this.tail.then(async () => {
      this.queuedBytes -= bytes;
      if (this.closed || this.response.destroyed || this.response.writableEnded) return false;
      try {
        if (this.response.writableNeedDrain && !await this.waitForDrain()) { this.close(); return false; }
        if (this.closed) return false;
        // false still means accepted; never resend the frame.
        if (!this.response.write(frame) && !await this.waitForDrain()) { this.close(); return false; }
        return !this.closed;
      } catch { this.close(); return false; }
    });
    this.tail = operation;
    return operation;
  }

  async end(): Promise<void> {
    this.accepting = false;
    await this.tail;
    if (!this.closed && !this.response.writableEnded) this.response.end();
  }

  private waitForDrain(): Promise<boolean> {
    if (this.closed || this.response.destroyed || this.response.writableEnded) return Promise.resolve(false);
    if (!this.response.writableNeedDrain) return Promise.resolve(true);
    return new Promise(resolve => {
      const finish = (ok: boolean) => {
        clearTimeout(timer);
        this.response.off('drain', drain);
        this.response.off('close', close);
        this.response.off('error', close);
        resolve(ok);
      };
      const drain = () => finish(true);
      const close = () => finish(false);
      const timer = setTimeout(() => finish(false), this.drainTimeoutMs);
      this.response.once('drain', drain);
      this.response.once('close', close);
      this.response.once('error', close);
    });
  }
}
