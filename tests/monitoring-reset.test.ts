import { test } from 'node:test';
import assert from 'node:assert/strict';
import type { Db } from 'mongodb';
import { MonitoringResetService, RESET_COLLECTIONS } from '../src/application/monitoring-reset.js';

function fixture(fail = '') {
  const events: string[] = [];
  let release: (() => void) | undefined;
  let waiting = false;
  const db = { collection(name: string) { return {
    async updateOne() { events.push(`marker:${name}`); },
    async deleteMany() { events.push(`delete:${name}`); if (fail === name) throw new Error('storage failed'); },
  }; } } as unknown as Db;
  const reset = new MonitoringResetService({ db: () => db,
    redis: { async scan(cursor: string) { events.push(`scan:${cursor}`); return cursor === '0' ? ['1', ['hls:streams:a', 'hls:retry:one']] : ['0', ['hls:status:{default}:latest']]; },
      async unlink(...keys: string[]) { events.push(`unlink:${keys.join(',')}`); return keys.length; } } as never,
    async quiesce() { events.push('pause'); if (waiting) await new Promise<void>(resolve => { release = resolve; }); },
    async resume() { events.push('resume'); }, clearMemory() { events.push('memory'); },
  });
  return { reset, events, wait: () => { waiting = true; }, release: () => release!() };
}
test('reset drains before deletion, clears only monitoring data, preserves accounts and resumes workers', async () => {
  const { reset, events } = fixture();
  await reset.run();
  assert.equal(events[0], 'pause');
  assert.deepEqual(events.filter(e => e.startsWith('delete:')), RESET_COLLECTIONS.map(name => `delete:${name}`));
  assert.ok(events.indexOf('scan:1') < events.findIndex(e => e.startsWith('unlink:')));
  assert.deepEqual(events.slice(-2), ['memory', 'resume']);
  assert.ok(!events.some(e => /delete:.*(users|app_settings|push_subscriptions)|session|FLUSH/.test(e)));
  assert.equal(reset.running, false); assert.equal(reset.generation, 1);
});
test('concurrent resets are rejected; failure invalidates reports and resumes without claiming success', async () => {
  const f = fixture('hls_stream_metrics'); f.wait();
  const first = f.reset.run();
  assert.equal(f.reset.running, true);
  await assert.rejects(f.reset.run(), /already running/);
  f.release(); await assert.rejects(first, /storage failed/);
  assert.equal(f.reset.running, false); assert.equal(f.reset.generation, 1);
  assert.equal(f.events.at(-1), 'resume');
  assert.ok(!f.events.includes('memory'));
  assert.ok(!f.events.some(e => e.startsWith('unlink:')));
});
