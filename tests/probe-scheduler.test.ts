import { loadProbeInventory } from '../src/monitoring/load-probe-inventory.js';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { normalizeProbeConfig, type StreamInventory, type DueProbe } from '../src/monitoring/probe-contracts.js';
import { ProbeScheduler } from '../src/monitoring/probe-scheduler.js';
const config = normalizeProbeConfig({ serverIp: 'server', streamId: 'one', streamUrl: 'https://example.test/live' });

test('inventory config normalizes defaults and rejects invalid URLs, flags and timing', () => {
  assert.equal(config.intervalMs, 10_000); assert.equal(config.staleAfterMs, 30_000); assert.equal(config.enabled, true);
  for (const value of [{ ...config, streamUrl: 'file:///tmp/x' }, { ...config, intervalMs: 0 }, { ...config, serverIp: '' }, { ...config, staleAfterMs: Number.NaN }, { ...config, enabled: 'true' }]) {
    assert.throws(() => normalizeProbeConfig(value as unknown as typeof config));
  }
});

test('scheduler starts each server once, serializes ticks, applies jitter and stops admission before consumer shutdown', async () => {
  const due: DueProbe[] = [{ config, nextCheckAt: 0 }, { config: { ...config, streamId: 'two' }, nextCheckAt: 0 }];
  let reads = 0; let queued = 0; let started = 0; let release!: () => void;
  const gate = new Promise<void>(resolve => { release = resolve; });
  let block = false;
  const inventory: StreamInventory = { async upsert() {}, async get() { return config; }, async list() { return due.map(entry => entry.config); }, async due() { reads++; if (block) await gate; return due; } };
  const scheduler = new ProbeScheduler(inventory, { async enqueueDue(_config, jitter) { assert.equal(jitter, 500); queued++; return 'queued'; } },
    { consume() { started++; return new Promise(() => {}); } }, { pollMs: 60_000, random: () => 0.5 });
  await scheduler.start(); assert.equal(started, 1); assert.equal(queued, 2);
  block = true;
  const active = scheduler.tick(); assert.equal(scheduler.tick(), active);
  const before = reads;
  const stopping = scheduler.stop(); release(); await Promise.all([active, stopping]);
  assert.equal(queued, 2); // Work discovered after stop is not dispatched.
  await scheduler.tick(); assert.equal(reads, before);
});

test('JSON config validation completes before any inventory write', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'gate-probes-')); const file = join(directory, 'streams.json');
  let writes = 0;
  const inventory: StreamInventory = { async upsert() { writes++; }, async get() { return null; }, async list() { return []; }, async due() { return []; } };
  const scheduler = new ProbeScheduler(inventory, { async enqueueDue() { return 'queued'; } }, { async consume() {} });
  try {
    await writeFile(file, JSON.stringify([config, { ...config, streamUrl: 'invalid' }]));
    await assert.rejects(loadProbeInventory(file, inventory)); assert.equal(writes, 0);
    await writeFile(file, JSON.stringify([config])); await loadProbeInventory(file, inventory); await scheduler.start(); assert.equal(writes, 1);
  } finally { await scheduler.stop(); await rm(directory, { recursive: true }); }
});

test('concurrent starts share initialization and stop during startup cannot launch late consumers', async () => {
  let finish!: (configs: typeof config[]) => void; let lists = 0; let consumers = 0; let due = 0;
  const inventory: StreamInventory = {
    async upsert() {}, async get() { return null; },
    list() { lists++; return new Promise(resolve => { finish = resolve; }); },
    async due() { due++; return []; },
  };
  const scheduler = new ProbeScheduler(inventory, { async enqueueDue() { return 'queued'; } }, { async consume() { consumers++; } });
  const starting = scheduler.start(); assert.equal(scheduler.start(), starting); assert.equal(lists, 1);
  await scheduler.stop(); finish([config]); await starting;
  assert.equal(consumers, 0); assert.equal(due, 0);
});

test('stop during dispatch prevents a deferred consumer launch', async () => {
  let launched = 0; let stopping: Promise<void> | undefined;
  const inventory: StreamInventory = { async upsert() {}, async get() { return null; }, async list() { return []; }, async due() { return [{ config, nextCheckAt: 0 }]; } };
  let scheduler!: ProbeScheduler;
  scheduler = new ProbeScheduler(inventory, { async enqueueDue() { stopping = scheduler.stop(); return 'queued'; } }, { async consume() { launched++; } });
  await scheduler.start(); await stopping; assert.equal(launched, 0);
});

test('one dispatch failure does not starve other due streams', async t => {
  t.mock.method(console, 'error', () => {});
  const configs = [config, { ...config, serverIp: 'other-server', streamId: 'two' }];
  const inventory: StreamInventory = { async upsert() {}, async get() { return null; }, async list() { return []; }, async due() { return configs.map(config => ({ config, nextCheckAt: 0 })); } };
  const dispatched: string[] = [];
  const scheduler = new ProbeScheduler(inventory, { async enqueueDue(config) {
    dispatched.push(config.streamId); if (config.streamId === 'one') throw new Error('One stream key has wrong type'); return 'queued';
  } }, { consume() { return new Promise(() => {}); } });
  try { await scheduler.start(); assert.deepEqual(dispatched, ['one', 'two']); }
  finally { await scheduler.stop(); }
});
