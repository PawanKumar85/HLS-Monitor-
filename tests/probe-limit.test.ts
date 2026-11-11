import { test } from 'node:test';
import assert from 'node:assert/strict';
import { LimitedHlsHealthChecker } from '../src/monitoring/limited-hls-health-checker.js';
import { ProcessingFailure } from '../src/dlq/dead-letter.types.js';
test('global probe limit bounds active checks, waiting backlog and rejects overload without terminal failure', async () => {
  let active = 0; let peak = 0; const releases: (() => void)[] = [];
  const checker = new LimitedHlsHealthChecker({ async check() {
    active++; peak = Math.max(peak, active); await new Promise<void>(resolve => releases.push(resolve)); active--; return { status: 1, latency: 1 };
  } }, 2, 1);
  const calls = [checker.check('one'), checker.check('two'), checker.check('three')];
  await assert.rejects(checker.check('four'), error => { assert.ok(error instanceof ProcessingFailure); assert.equal(error.bounded, false); return true; });
  assert.equal(peak, 2); releases.shift()!();
  await new Promise(resolve => setImmediate(resolve)); assert.equal(peak, 2);
  for (const release of releases.splice(0)) release(); await Promise.all(calls); checker.stop();
});
test('stopping the checker rejects waiting probes without launching their network requests', async () => {
  let release!: () => void; let calls = 0;
  const checker = new LimitedHlsHealthChecker({ async check() { calls++; await new Promise<void>(resolve => { release = resolve; }); return { status: 1, latency: 1 }; } }, 1, 1);
  const first = checker.check('one'); const waiting = checker.check('two');
  const rejected = assert.rejects(waiting, ProcessingFailure); checker.stop(); await rejected;
  release(); await first; assert.equal(calls, 1); await assert.rejects(checker.check('three'), ProcessingFailure);
});
