import { MemoryStatusStore } from "./helpers/status-event-store.js";
import { createStatusTransport } from "../src/realtime/create-status-transport.js";
import { test, mock } from 'node:test';
import assert from 'node:assert/strict';
import { redisClient } from '../src/config/redis.config.js';
import { HlsMessageProcessor } from '../src/services/hls-message-processor.service.js';
import { RedisStreamAdapter } from '../src/messaging/adapters/redis-stream.adapter.js';
import { checkHls } from '../src/services/hls-health.service.js';
import type { HlsStatusEvent } from '../src/types/hls-status.types.js';
import type { HlsConsumedStream } from '../src/types/stream.types.js';
import { HlsConsumer } from '../src/consumers/hls.consumer.js';

const statusTransport = createStatusTransport(new MemoryStatusStore());
const checker = { check: checkHls };
const unsortedBatchQueue = { async push(_message: HlsConsumedStream, _group?: string) {} };
function createConsumer() {
  return new HlsConsumer({ source: new RedisStreamAdapter(redisClient), processor: new HlsMessageProcessor(checker, statusTransport, unsortedBatchQueue) });
}

test('shutdown interrupts a blocking read and waits for consumer exit', async () => {
  let entered!: () => void;
  const reading = new Promise<void>((r) => { entered = r; });
  let rejectRead!: (error: Error) => void;
  const blocking = {
    xautoclaim: async () => ["0-0", [], []],
    xreadgroup: () => { entered(); return new Promise((_resolve, reject) => { rejectRead = reject; }); },
    disconnect: mock.fn(() => rejectRead(new Error('disconnected'))),
  };
  mock.method(redisClient, 'xgroup', async () => 'OK');
  mock.method(redisClient, 'duplicate', () => blocking);
  try {
    const consumer = createConsumer();
    const running = consumer.consume('server', undefined, { blockTimeoutMs: 0 });
    await reading;
    await consumer.stop();
    await running;
    assert.ok(blocking.disconnect.mock.callCount() >= 1);
    await assert.rejects(consumer.consume('another'), /stopping/);
  } finally { mock.restoreAll(); }
});

test('shutdown waits for group initialization and prevents starting a reader', async () => {
  let finish!: () => void;
  const init = new Promise<void>((r) => { finish = r; });
  mock.method(redisClient, 'xgroup', () => init);
  const duplicate = mock.method(redisClient, 'duplicate', () => { throw new Error('unexpected reader'); });
  try {
    const consumer = createConsumer();
    const running = consumer.consume('server');
    let stopped = false;
    const stopping = consumer.stop().then(() => { stopped = true; });
    await new Promise(setImmediate);
    assert.equal(stopped, false);
    finish();
    await Promise.all([running, stopping]);
    assert.equal(duplicate.mock.callCount(), 0);
  } finally { mock.restoreAll(); }
});

test('pending scan follows empty cursors, retries handler failures, bounds checks, and never auto ACKs', async () => {
  const consumer = createConsumer();
  const entry = (id: string) => [id, ['streamId', id, 'streamUrl', 'https://example.test/live.m3u8', 'serverIp', 'wrong', 'createdAt', '2026-01-01T00:00:00Z']];
  const cursors: string[] = [];
  let now = 1000;
  let claims = 0;
  let inFlight = 0;
  let peak = 0;
  let failed = false;
  let shutdown!: Promise<void>;
  const handled: string[] = [];
  const blocking = {
    status: 'wait',
    connect: mock.fn(async () => {}),
    disconnect: mock.fn(),
    xautoclaim: async (...args: unknown[]) => {
      cursors.push(args[4] as string);
      assert.equal(args[3], 120000);
      claims++;
      if (claims === 1) return ['10-0', []]; // Empty page is not end of scan (Redis 6.2 reply).
      if (claims === 2) return ['0-0', [entry('1-0'), entry('2-0'), entry('3-0')], []];
      return ['0-0', [entry('1-0')], []];
    },
    xreadgroup: async () => { now += 30001; return null; },
  };
  mock.method(Date, 'now', () => now);
  mock.method(redisClient, 'xgroup', async () => 'OK');
  mock.method(redisClient, 'duplicate', () => blocking);
  const ack = mock.method(redisClient, 'xack', async () => { throw new Error('premature ACK'); });
  mock.method(checker, 'check', async () => {
    peak = Math.max(peak, ++inFlight);
    await new Promise(setImmediate);
    inFlight--;
    return { status: 1, latency: 1 };
  });
  try {
    await consumer.consume('server', async (message) => {
      assert.equal(message.serverIp, 'server');
      if (message.messageId === '1-0' && !failed) { failed = true; throw new Error('handler failed'); }
      handled.push(message.messageId);
      if (claims === 3) shutdown = consumer.stop();
    }, { healthCheckConcurrency: 2 });
    await shutdown;
    assert.deepEqual(cursors, ['0-0', '10-0', '0-0']);
    assert.equal(peak, 2);
    assert.deepEqual(handled.sort(), ['1-0', '2-0', '3-0']);
    assert.equal(ack.mock.callCount(), 0);
    assert.equal(blocking.connect.mock.callCount(), 1);
    assert.ok(blocking.disconnect.mock.callCount() > 0);
  } finally { mock.restoreAll(); }
});

test('failed duplicate connect is cleaned up and per-server stop cancels initialization', async () => {
  const consumer = createConsumer();
  const blocking = { status: 'wait', connect: async () => { throw new Error('connect failed'); }, disconnect: mock.fn() };
  mock.method(redisClient, 'xgroup', async () => 'OK');
  mock.method(redisClient, 'duplicate', () => blocking);
  try {
    await assert.rejects(consumer.consume('server'), /connect failed/);
    assert.equal(blocking.disconnect.mock.callCount(), 1);
    let finish!: () => void;
    mock.method(redisClient, 'xgroup', () => new Promise<void>((resolve) => { finish = resolve; }));
    const task = consumer.consume('server');
    const stopping = consumer.stop('server');
    finish();
    await Promise.all([task, stopping]);
    assert.equal(blocking.disconnect.mock.callCount(), 1);
  } finally { mock.restoreAll(); }
});


test('broadcast failure still queues new messages in the actual group without immediate ACK', async () => {
  const consumer = createConsumer();
  let shutdown!: Promise<void>;
  const blocking = {
    status: 'ready',
    xautoclaim: async () => ['0-0', [], []],
    xreadgroup: (() => { let delivered = false; return async () => { if (delivered) { await new Promise(r => setTimeout(r, 5)); return null; } delivered = true; return [['hls:streams:server', [['1-0', ['streamId', 'channel', 'streamUrl', 'https://example.test/live.m3u8', 'createdAt', '2026-01-01T00:00:00Z']]]]]; }; })(), // XREADGROUP '>' delivers an entry once
    disconnect: mock.fn(),
  };
  mock.method(redisClient, 'xgroup', async () => 'OK');
  mock.method(redisClient, 'duplicate', () => blocking);
  mock.method(checker, 'check', async () => ({ status: 1, latency: 1 }));
  const ack = mock.method(redisClient, 'xack', async () => 1);
  const broadcast = mock.method(statusTransport, 'publish', (event: HlsStatusEvent) => {
    assert.equal(event.type, 'hls:status');
    assert.equal(event.data.streamId, 'channel');
    assert.equal(event.data.status, 1);
    throw new Error('Status transport unavailable');
  });
  const push = mock.method(unsortedBatchQueue, 'push', async (message: HlsConsumedStream, group?: string) => {
    assert.equal(broadcast.mock.callCount(), 1);
    assert.equal(message.serverIp, 'server');
    assert.equal(message.checkedAt, broadcast.mock.calls[0]?.arguments[0].data.checkedAt);
    assert.equal(group, 'custom');
    shutdown = consumer.stop();
  });
  try {
    await consumer.consume('server', undefined, { groupName: 'custom' });
    await shutdown;
    assert.equal(push.mock.callCount(), 1);
    assert.equal(ack.mock.callCount(), 0);
  } finally { mock.restoreAll(); }
});

test('reset pause drains blocking readers and permits consuming again afterwards', async () => {
  let entered!: () => void;
  let reading = new Promise<void>(resolve => { entered = resolve; });
  mock.method(redisClient, 'xgroup', async () => 'OK');
  mock.method(redisClient, 'duplicate', () => {
    let rejectRead!: (error: Error) => void;
    return { xautoclaim: async () => ['0-0', [], []],
      xreadgroup: () => { entered(); return new Promise((_resolve, reject) => { rejectRead = reject; }); },
      disconnect: () => rejectRead?.(new Error('paused')) };
  });
  const consumer = createConsumer();
  try {
    const first = consumer.consume('server'); await reading;
    await consumer.pause(); await first;
    reading = new Promise<void>(resolve => { entered = resolve; });
    const second = consumer.consume('server'); await reading;
    await consumer.stop(); await second;
  } finally { await consumer.stop(); mock.restoreAll(); }
});
