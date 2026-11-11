import { EventEmitter } from 'node:events';
export class Response extends EventEmitter {
  destroyed = false;
  writableEnded = false;
  writableNeedDrain = false;
  writableLength = 0;
  frames: string[] = [];
  block = false;
  write(frame: string) { this.frames.push(frame); if (this.block) { this.writableNeedDrain = true; return false; } return true; }
  destroy() { if (!this.destroyed) { this.destroyed = true; this.emit('close'); } }
  end() { this.writableEnded = true; this.emit('close'); }
  drain() { this.block = false; this.writableNeedDrain = false; this.emit('drain'); }
  setHeader() {}
  writeHead() {}
  flushHeaders() {}
}
