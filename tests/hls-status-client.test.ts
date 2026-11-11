import { test } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { startHlsStatusClient, type HlsStatus, type StatusEventSource } from '../frontend/src/lib/hls-status-client.js';
class Source implements StatusEventSource {
  readyState = 1;
  onerror: ((event: Event) => void) | null = null;
  listeners = new Map<string, (event: MessageEvent<string>) => void>();
  constructor(readonly url: string) {}
  addEventListener(type: string, listener: (event: MessageEvent<string>) => void) { this.listeners.set(type, listener); }
  emit(type: string, data: unknown, id = '') { this.listeners.get(type)?.({ data: JSON.stringify(data), lastEventId: id } as MessageEvent<string>); }
  close() { this.readyState = 2; }
  error() { this.onerror?.({} as Event); }
}
const flush = async () => { for (let i = 0; i < 5; i++) await Promise.resolve(); };
function fixture(pending = false) {
  let epoch = randomUUID(); let sequence = 3; let calls = 0;
  const sources: Source[] = []; const applied: HlsStatus[] = []; const connected: boolean[] = [];
  const stop = startHlsStatusClient({ url: '/events/hls-status', snapshotUrl: '/api/hls/status/snapshot', baseUrl: 'https://example.test/dashboard',
    onSnapshot: () => {}, onStatus: status => applied.push(status), onConnected: value => connected.push(value),
  }, { open: url => { const source = new Source(url); sources.push(source); return source; }, now: () => Date.now(),
    fetchSnapshot: async (_url, signal) => {
      calls++;
      if (pending) await new Promise((_, reject) => signal.addEventListener('abort', () => reject(new Error('aborted')), { once: true }));
      return { epoch, sequence, cursor: `${epoch}:${sequence}`, streams: [] };
    } });
  return { sources, applied, connected, stop, calls: () => calls, reset: () => { epoch = randomUUID(); sequence = 0; },
    cursor: (n: number) => `${epoch}:${n}`,
    status: (n: number) => ({ type: 'hls:status', epoch, sequence: n, cursor: `${epoch}:${n}`, data: { streamId: 'one', serverIp: 's', status: 1, latencyMs: n, checkedAt: new Date().toISOString() } }),
  };
}
test('fresh client snapshots first, applies handoff replay, ignores duplicates and reopens from last applied cursor on gap', async () => {
  const f = fixture();
  try {
    assert.equal(f.sources.length, 0); await flush();
    const source = f.sources[0]!; assert.equal(new URL(source.url).searchParams.get('after'), f.cursor(3));
    source.emit('hls:status', f.status(4), f.cursor(4)); source.emit('hls:status', f.status(4), f.cursor(4));
    source.emit('hls:ready', { cursor: f.cursor(4) }); assert.equal(f.connected.at(-1), true);
    source.emit('hls:status', f.status(7), f.cursor(7));
    assert.equal(f.applied.length, 1); assert.equal(source.readyState, 2);
    assert.equal(new URL(f.sources[1]!.url).searchParams.get('after'), f.cursor(4));
    assert.equal(f.calls(), 1);
    f.sources[1]!.emit('hls:status', f.status(5), f.cursor(5)); assert.equal(f.applied.length, 2);
  } finally { f.stop(); }
});
test('lost resync control falls back to snapshot after repeated errors; transient errors preserve native replay', async () => {
  const f = fixture();
  try {
    await flush(); const source = f.sources[0]!;
    source.emit('hls:ready', { cursor: f.cursor(3) }); source.error();
    assert.equal(f.calls(), 1); assert.equal(source.readyState, 1); assert.equal(f.connected.at(-1), false);
    source.error(); source.error(); await flush(); assert.equal(f.calls(), 2); assert.equal(source.readyState, 2);
  } finally { f.stop(); }
});
test('expired cursor control, epoch reset, malformed event and terminal EventSource error fetch fresh snapshots', async () => {
  for (const mode of ['resync', 'epoch', 'malformed', 'closed'] as const) {
    const f = fixture();
    try {
      await flush(); const source = f.sources[0]!;
      if (mode === 'resync') source.emit('hls:resync', { reason: 'cursor_unavailable' });
      if (mode === 'epoch') { f.reset(); source.emit('hls:status', f.status(1), f.cursor(1)); }
      if (mode === 'malformed') source.emit('hls:status', {});
      if (mode === 'closed') { source.close(); source.error(); }
      await flush(); assert.equal(f.calls(), 2, mode); assert.equal(f.sources.length, 2);
    } finally { f.stop(); }
  }
});
test('watchdog escapes replay that never reaches ready even while heartbeats arrive', async t => {
  t.mock.timers.enable({ apis: ['setTimeout', 'setInterval', 'Date'], now: 1000 });
  const f = fixture();
  try {
    await flush();
    for (let i = 0; i < 7; i++) { f.sources[0]!.emit('hls:heartbeat', {}); t.mock.timers.tick(5000); await flush(); }
    assert.equal(f.calls(), 2);
  } finally { f.stop(); t.mock.timers.reset(); }
});
test('cleanup aborts pending snapshot and never opens a stream afterwards', async () => {
  const f = fixture(true); f.stop(); await flush(); assert.equal(f.sources.length, 0);
});

test('snapshot timeout retries without opening a cursorless stream', async t => {
  t.mock.timers.enable({ apis: ['setTimeout', 'setInterval', 'Date'], now: 1000 });
  const f = fixture(true);
  try {
    t.mock.timers.tick(10_000); await flush();
    assert.equal(f.calls(), 1); assert.equal(f.sources.length, 0);
    t.mock.timers.tick(3000); await flush(); assert.equal(f.calls(), 2);
  } finally { f.stop(); await flush(); t.mock.timers.reset(); }
});
