import { MemoryStatusStore } from "./helpers/status-event-store.js";
import { test, mock } from 'node:test';
import assert from 'node:assert/strict';
import { HlsConsumer } from '../src/consumers/hls.consumer.js';
import { redisClient } from '../src/config/redis.config.js';
import { HlsMessageProcessor } from '../src/services/hls-message-processor.service.js';
import { RedisStreamAdapter } from '../src/messaging/adapters/redis-stream.adapter.js';
import { createStatusTransport } from '../src/realtime/create-status-transport.js';
import type { HlsConsumedStream } from '../src/types/stream.types.js';
import type { StatusPublisher } from '../src/realtime/status-transport.js';
import type { HlsStatusEvent } from '../src/types/hls-status.types.js';

for (const mode of ['success', 'throw', 'reject', 'pending'] as const) {
  test(`injected status publisher (${mode}) cannot gate queue persistence or consumer shutdown`, { timeout: 3000 }, async () => {
    const events: string[] = [];
    let observation: HlsStatusEvent | undefined;
    let release!: () => void;
    const pending = new Promise<void>((resolve) => { release = resolve; });
    const publisher: StatusPublisher = {
      publish(event) {
        events.push('publish');
        observation = event;
        if (mode === 'throw') throw new Error('transport error');
        if (mode === 'reject') return Promise.reject(new Error('async transport error'));
        if (mode === 'pending') return pending;
      },
    };
    const checker = { check: async () => ({ status: 0 as const, latency: 3000 }) };
    const unsortedBatchQueue = { async push(_message: HlsConsumedStream, _group?: string) {} };
    const consumer = new HlsConsumer({ source: new RedisStreamAdapter(redisClient), processor: new HlsMessageProcessor(checker, publisher, unsortedBatchQueue) });
    let shutdown!: Promise<void>;
    const reader = {
      status: 'ready',
      xautoclaim: async () => ['0-0', [], []],
      xreadgroup: (() => { let delivered = false; return async () => { if (delivered) { await new Promise(r => setTimeout(r, 5)); return null; } delivered = true; return [['hls:streams:server', [['1-0', ['streamId', 'channel', 'streamUrl', 'https://example.test/live.m3u8', 'createdAt', '2026-11-08T00:00:00Z']]]]]; }; })(), // XREADGROUP '>' delivers an entry once
      disconnect: mock.fn(),
    };
    mock.method(redisClient, 'xgroup', async () => 'OK');
    mock.method(redisClient, 'duplicate', () => reader);
    const ack = mock.method(redisClient, 'xack', async () => { throw new Error('consumer must not ACK'); });
    mock.method(unsortedBatchQueue, 'push', async (message: HlsConsumedStream, group?: string) => {
      events.push('queue');
      assert.equal(group, 'custom');
      assert.equal(message.checkedAt, observation?.data.checkedAt);
      shutdown = consumer.stop();
    });
    try {
      await consumer.consume('server', undefined, { groupName: 'custom' });
      await shutdown;
      assert.deepEqual(events, ['publish', 'queue']);
      assert.equal(observation?.type, 'hls:status');
      assert.equal(observation?.data.status, 0);
      assert.equal(observation?.data.latencyMs, 3000);
      assert.equal(ack.mock.callCount(), 0);
    } finally { release(); mock.restoreAll(); }
  });
}

test('transport factory selects SSE and rejects unimplemented providers', () => {
  assert.equal(createStatusTransport(new MemoryStatusStore()).name, 'sse');
  assert.equal(createStatusTransport(new MemoryStatusStore(), 'sse').name, 'sse');
  assert.throws(() => createStatusTransport(new MemoryStatusStore(), 'websocket'), /Unsupported STATUS_TRANSPORT/);
  assert.throws(() => createStatusTransport(new MemoryStatusStore(), 'socket.io'), /Unsupported STATUS_TRANSPORT/);
});
