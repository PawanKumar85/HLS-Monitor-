import { test } from 'node:test';
import assert from 'node:assert/strict';
import Fastify from 'fastify';
import { DeadLetterProjector } from '../src/dlq/dead-letter-projector.js';
import { registerStreamRoutes } from '../src/http/stream-routes.js';
import { StreamService } from '../src/inventory/stream-service.js';
import { MemoryInventory } from './helpers/stream-inventory.js';
import type { HlsDeadLetter } from '../src/dlq/dead-letter.types.js';

const record = (i: number) => ({ failureId: `f${i}`, revision: 1 }) as unknown as HlsDeadLetter;

test('archive: a failure storm drains in one flush, in bulk, and Redis entries go only after MongoDB has them', async () => {
  const pending = new Map(Array.from({ length: 250 }, (_, i) => [`${i}-0`, record(i)]));
  const acked: string[] = []; const writes: number[] = [];
  const projector = new DeadLetterProjector({
    async servers() { return ['10.0.0.1']; },
    async readArchiveBatch(_server, _consumer, count) { return [...pending].filter(([id]) => !acked.includes(id)).slice(0, count).map(([id, r]) => ({ id, record: r })); },
    async archiveCompleted(_server, id) { acked.push(id); },
  }, { async write() { throw new Error('use writeMany'); }, async writeMany(records) { writes.push(records.length); } });
  await projector.flush();
  assert.deepEqual(writes, [100, 100, 50], 'three bulk writes, one flush');
  assert.equal(acked.length, 250);

  // MongoDB down: nothing is removed from Redis.
  const kept: string[] = [];
  const offline = new DeadLetterProjector({
    async servers() { return ['10.0.0.1']; },
    async readArchiveBatch() { return [{ id: '1-0', record: record(1) }]; },
    async archiveCompleted(_s, id) { kept.push(id); },
  }, { async write() {}, async writeMany() { throw new Error('mongo down'); } });
  await assert.rejects(offline.flush());
  assert.deepEqual(kept, []);
});

test('failure API: summary, server-side search with validated filters, and replay', async () => {
  const replayed: string[] = []; const queries: unknown[] = [];
  const app = Fastify();
  registerStreamRoutes(app, {
    service: new StreamService(new MemoryInventory()),
    failures: {
      async readAfter() { return []; },
      async summary() { return { total: 3, byResolution: { OPEN: 2, RESOLVED: 1 }, openByStage: { PERSISTENCE: 2 } }; },
      async search(query) { queries.push(query); if (query.before === 'bad') throw new RangeError('Invalid failure cursor'); return { items: [], next: '1-aaaaaaaaaaaaaaaaaaaaaaaa' }; },
      async replayTarget(id) { const ids: Record<string, string> = { f1: '1-0', gone: '2-0', fresh: '3-0', lost: '4-0', broken: '5-0' };
        return ids[id] ? { originalStreamKey: 'hls:streams:10.0.0.1', originalMessageId: ids[id]!, groupName: 'g', serverIp: '10.0.0.1' } : null; },
    },
    replay: async target => {
      replayed.push(target.originalMessageId);
      if (target.originalMessageId === '3-0') throw new Error('Message is not dead-lettered');
      if (target.originalMessageId === '4-0') throw new Error('Original message no longer exists; explicit repair is required');
      if (target.originalMessageId === '5-0') throw new Error('redis down');
      return target.originalMessageId === '1-0';
    },
  });
  try {
    assert.equal((await app.inject('/api/failures/summary')).json().byResolution.OPEN, 2);
    assert.equal((await app.inject('/api/failures?resolution=OPEN&stage=PERSISTENCE&q=timeout&limit=20')).json().next, '1-aaaaaaaaaaaaaaaaaaaaaaaa');
    assert.deepEqual({ ...(queries[0] as object) }, { resolution: 'OPEN', stage: 'PERSISTENCE', q: 'timeout', limit: 20 });
    assert.equal((await app.inject('/api/failures?resolution=MAYBE')).statusCode, 400);
    assert.equal((await app.inject('/api/failures?before=bad')).statusCode, 400);
    assert.equal((await app.inject({ method: 'POST', url: '/api/failures/f1/replay' })).statusCode, 202);
    assert.equal((await app.inject({ method: 'POST', url: '/api/failures/gone/replay' })).statusCode, 409, 'already replayed or resolved');
    assert.equal((await app.inject({ method: 'POST', url: '/api/failures/missing/replay' })).statusCode, 404);
    assert.equal((await app.inject({ method: 'POST', url: '/api/failures/fresh/replay' })).statusCode, 409, 'not dead-lettered (never was, or still retrying)');
    const lost = await app.inject({ method: 'POST', url: '/api/failures/lost/replay' });
    assert.equal(lost.statusCode, 409); assert.match(lost.json().error, /gone from Redis/);
    assert.equal((await app.inject({ method: 'POST', url: '/api/failures/broken/replay' })).statusCode, 503, 'a real fault stays a server error');
    assert.deepEqual(replayed, ['1-0', '2-0', '3-0', '4-0', '5-0']);
  } finally { await app.close(); }
});
