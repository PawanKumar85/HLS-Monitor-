import { MemoryStatusStore } from "./helpers/status-event-store.js";
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { buildApp } from '../src/app.js';
import type { ApplicationServices } from '../src/application/contracts.js';
import { SseStatusTransport } from '../src/realtime/adapters/sse-status.transport.js';

test('app shutdown waits for injected consumers, queues, then database disconnect', async () => {
  const events: string[] = [];
  let finish!: () => void;
  const gate = new Promise<void>((resolve) => { finish = resolve; });
  const services: ApplicationServices = { auth: 'disabled',
    statusTransport: new SseStatusTransport(new MemoryStatusStore()),
    hlsConsumer: { async stop() { events.push('consumer stopping'); await gate; events.push('consumer stopped'); } },
    batchQueueManager: { async stopAll() { events.push('queues drained'); } },
    dbManager: {
      async healthCheck() { return { redis: true, storage: true, storageProvider: 'test' }; },
      async disconnectAll() { events.push('databases closed'); },
    },
  };
  const app = await buildApp(services);
  await app.ready();
  assert.equal((await app.inject({ method: 'GET', url: '/health' })).statusCode, 200);
  const closing = app.close();
  await new Promise(setImmediate);
  assert.deepEqual(events, ['consumer stopping']);
  finish();
  await closing;
  assert.deepEqual(events, ['consumer stopping', 'consumer stopped', 'queues drained', 'databases closed']);
});

test('failed queue drain does not close persistence connections', async () => {
  let disconnected = false;
  const services: ApplicationServices = { auth: 'disabled',
    statusTransport: new SseStatusTransport(new MemoryStatusStore()),
    hlsConsumer: { async stop() {} },
    batchQueueManager: { async stopAll() { throw new Error('persistence failed'); } },
    dbManager: {
      async healthCheck() { return { redis: true, storage: true, storageProvider: 'test' }; },
      async disconnectAll() { disconnected = true; },
    },
  };
  const app = await buildApp(services);
  await app.ready();
  await assert.rejects(app.close(), /persistence failed/);
  assert.equal(disconnected, false);
});

test('failure projector starts with app readiness and stops after queue drain', async () => {
  const events: string[] = [];
  const app = await buildApp({ auth: 'disabled',
    statusTransport: { async register() {} },
    hlsConsumer: { async stop() { events.push('consumer'); } },
    batchQueueManager: { async stopAll() { events.push('queues'); } },
    failureProjector: { start() { events.push('archive start'); }, async stop() { events.push('archive stop'); } },
    dbManager: {
      async healthCheck() { return { redis: true, storage: true, storageProvider: 'test' }; },
      async disconnectAll() { events.push('databases'); },
    },
  });
  await app.ready();
  assert.deepEqual(events, ['archive start']);
  await app.close();
  assert.deepEqual(events, ['archive start', 'consumer', 'queues', 'archive stop', 'databases']);
});

test('monitoring starts with readiness and stops enqueueing before consumers and queue drain', async () => {
  const calls: string[] = [];
  const app = await buildApp({ auth: 'disabled',
    dbManager: { async healthCheck() { return { redis: true, storage: true, storageProvider: 'test' }; }, async disconnectAll() { calls.push('disconnect'); } },
    statusTransport: { async register() {} },
    monitoring: { async start() { calls.push('monitoring start'); }, async stop() { calls.push('monitoring stop'); } },
    hlsConsumer: { async stop() { calls.push('consumer stop'); } },
    batchQueueManager: { async stopAll() { calls.push('drain'); } },
  });
  await app.ready(); await app.close();
  assert.deepEqual(calls, ['monitoring start', 'monitoring stop', 'consumer stop', 'drain', 'disconnect']);
});
