import { test } from 'node:test';
import assert from 'node:assert/strict';
import { CheckWatchdog, STALLED_INCIDENT_KEY } from '../src/monitoring/check-watchdog.js';
import type { PipelineIncident } from '../src/monitoring/pipeline-monitor.js';

test('watchdog: opens one incident when checks stop being saved, resolves it when they return, ignores an idle system', async () => {
  let now = new Date('2026-11-12T10:00:00Z');
  let last: Date | null = new Date('2026-11-12T09:59:30Z');
  let streams = true;
  const open = new Map<string, PipelineIncident>(); const events: string[] = [];
  const watchdog = new CheckWatchdog({
    lastSavedAt: async () => last, expectChecks: async () => streams, stalledAfterMs: 180_000, now: () => now,
    incidents: {
      async open(i, at) { if (open.has(i.key)) return false; open.set(i.key, { ...i, openedAt: at }); return true; },
      async resolve(key, at) { const i = open.get(key); if (!i) return null; open.delete(key); return { ...i, resolvedAt: at }; },
    },
    notify: async e => { events.push(`${e.type}:${e.incident.title}`); },
  });
  await watchdog.tick();
  assert.deepEqual([watchdog.status().stalled, events], [false, []], 'last check 30 s ago: fine');

  now = new Date('2026-11-12T10:05:00Z'); await watchdog.tick(); await watchdog.tick();
  assert.equal(watchdog.status().stalled, true);
  assert.deepEqual(events, ['opened:Monitoring stopped: no check saved for 6 min'], 'opened once, not every minute');
  assert.ok(open.has(STALLED_INCIDENT_KEY));

  last = new Date('2026-11-12T10:04:50Z'); await watchdog.tick();
  assert.deepEqual(events.at(-1), 'resolved:Monitoring stopped: no check saved for 6 min');
  assert.equal(open.size, 0);

  streams = false; last = null; now = new Date('2026-11-12T11:00:00Z'); await watchdog.tick();
  assert.equal(watchdog.status().stalled, false, 'no enabled streams: no checks are expected');
});
