import { test } from 'node:test';
import assert from 'node:assert/strict';
import { MemoryInventory } from './helpers/stream-inventory.js';
import { StreamService } from '../src/inventory/stream-service.js';
import { InventorySyncWorker } from '../src/inventory/inventory-sync-worker.js';
import { normalizeProbeConfig } from '../src/monitoring/probe-contracts.js';
const config = normalizeProbeConfig({ serverIp: 'server', streamId: 'one', streamUrl: 'https://example.test/live' });
test('sync clears only matching versions; concurrent PATCH remains pending', async () => {
  const repo = new MemoryInventory(); const service = new StreamService(repo); const record = await service.create(config);
  let updated = false;
  const worker = new InventorySyncWorker(repo, { async apply() { if (!updated) { updated = true; await service.patch(record.id, 1, { intervalMs: 2000 }); } return 'applied'; } }, { pollMs: 60_000 });
  try {
    await worker.start(); const current = (await repo.get(record.id))!;
    assert.equal(current.version, 2); assert.equal(current.pendingSync, true);
    await worker.tick(); assert.equal((await repo.get(record.id))?.pendingSync, false);
  } finally { await worker.stop(); }
});
test('projection outage preserves registration and backs off; reconciliation repairs already-synced records', async () => {
  const repo = new MemoryInventory(); const record = await repo.create(config);
  let now = Date.now() + 10; let failing = true; const projected = new Set<string>();
  const worker = new InventorySyncWorker(repo, { async apply(record) { if (failing) throw new Error('Redis unavailable'); projected.add(record.id); return 'applied'; } }, { pollMs: 60_000, now: () => new Date(now) });
  try {
    await worker.start(); assert.equal((await repo.get(record.id))?.pendingSync, true); assert.equal((await repo.get(record.id))?.syncAttempts, 1);
    failing = false; now += 2000; await worker.tick(); assert.equal((await repo.get(record.id))?.pendingSync, false);
    projected.clear(); await worker.tick(); await worker.tick(); assert.equal(projected.has(record.id), true);
  } finally { await worker.stop(); }
});
test('sync applies a bounded batch, serializes overlapping ticks and stops further admission', async () => {
  const repo = new MemoryInventory(); for (let i = 0; i < 20; i++) await repo.create({ ...config, streamId: `${i}` });
  let applied = 0;
  const worker = new InventorySyncWorker(repo, { async apply() { applied++; return 'applied'; } }, { batchSize: 4, pollMs: 60_000 });
  await worker.start(); assert.ok(applied <= 4 && applied > 0);
  const tick = worker.tick(); assert.equal(worker.tick(), tick); await tick;
  await worker.stop(); const count = applied; await worker.tick(); assert.equal(applied, count);
});
test('JSON seeding cannot overwrite API edits or resurrect deleted registrations', async () => {
  const repo = new MemoryInventory(); const service = new StreamService(repo);
  await service.seed(config); const record = (await repo.findIdentity(config.serverIp, config.streamId))!;
  await service.patch(record.id, 1, { intervalMs: 2000 }); await service.seed(config);
  assert.equal((await repo.get(record.id))?.config.intervalMs, 2000);
  await service.remove(record.id, 2); await service.seed(config); assert.equal((await repo.get(record.id))?.deleted, true);
});

test('backoff starts when a slow failed projection settles, not when its batch started', async () => {
  const repo = new MemoryInventory(); const record = await repo.create(config); let now = Date.now() + 100;
  const worker = new InventorySyncWorker(repo, { async apply() { now += 5000; throw new Error('slow failure'); } }, { pollMs: 60_000, now: () => new Date(now) });
  try { await worker.start(); assert.equal((await repo.get(record.id))!.nextSyncAt.getTime(), now + 1000); }
  finally { await worker.stop(); }
});
