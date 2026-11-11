import { test } from 'node:test';
import assert from 'node:assert/strict';
import { Response } from './helpers/sse-response.js';
import type { ServerResponse } from 'node:http';
import { SseClientWriter } from '../src/realtime/sse-client-writer.js';
const tick = () => new Promise<void>(resolve => setImmediate(resolve));
test('writer serializes data, heartbeat and resync across drain without duplicating accepted data', async () => {
  const response = new Response(); response.block = true;
  let closed = 0;
  const writer = new SseClientWriter(response as unknown as ServerResponse, () => closed++, 100);
  const data = writer.send('data');
  const heartbeat = writer.send('heartbeat');
  const resync = writer.send('resync');
  await tick(); assert.deepEqual(response.frames, ['data']); assert.equal(closed, 0);
  response.drain();
  assert.deepEqual(await Promise.all([data, heartbeat, resync]), [true, true, true]);
  assert.deepEqual(response.frames, ['data', 'heartbeat', 'resync']);
  await writer.end(); writer.close(); writer.close(); assert.equal(closed, 1);
  assert.equal(response.listenerCount('drain'), 0);
});
test('drain timeout and disconnect release pending writers and clean listeners', async () => {
  for (const mode of ['timeout', 'disconnect'] as const) {
    const response = new Response(); response.block = true;
    let closed = 0;
    const writer = new SseClientWriter(response as unknown as ServerResponse, () => closed++, 15);
    const data = writer.send('data'); const heartbeat = writer.send('heartbeat');
    await tick(); if (mode === 'disconnect') writer.close();
    assert.deepEqual(await Promise.all([data, heartbeat]), [false, false]);
    assert.equal(closed, 1); assert.equal(response.listenerCount('drain'), 0);
    assert.deepEqual(response.frames, ['data']);
  }
});
test('queued and response bytes are bounded together', async () => {
  const response = new Response(); response.block = true;
  const writer = new SseClientWriter(response as unknown as ServerResponse, () => {}, 100, 8);
  const first = writer.send('12345');
  assert.equal(await writer.send('6789'), false);
  assert.equal(await first, false); assert.equal(response.destroyed, true);
});
