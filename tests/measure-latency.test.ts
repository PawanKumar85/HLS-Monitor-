import { test, mock } from 'node:test';
import assert from 'node:assert/strict';
import { MeasureLatency } from '../src/decorators/measure-latency.decorator.js';

// Exercise standard decorator syntax, including receiver and async settlement.
test('MeasureLatency preserves receiver, arguments and result; logs after settlement', async () => {
  let finish!: () => void;
  const pending = new Promise<void>((resolve) => { finish = resolve; });
  const result = { status: 0, latency: 5 };
  class Checker {
    readonly url = 'https://example.test/live.m3u8';
    @MeasureLatency
    async check(url: string, attempt: number) {
      assert.equal(url, this.url);
      assert.equal(attempt, 2);
      await pending;
      return result;
    }
  }
  const log = mock.method(console, 'log', () => {});
  let now = 10;
  mock.method(performance, 'now', () => now);
  try {
    const checker = new Checker();
    const checking = checker.check(checker.url, 2);
    assert.equal(log.mock.callCount(), 0);
    now = 35;
    finish();
    assert.equal(await checking, result);
    assert.deepEqual(log.mock.calls[0]?.arguments, ['check: 25.00ms']);
    assert.equal(log.mock.callCount(), 1);
  } finally { mock.restoreAll(); }
});

test('MeasureLatency logs failures and propagates the original rejection', async () => {
  const error = new Error('health checker failed');
  class Checker {
    @MeasureLatency
    async check(): Promise<never> { throw error; }
  }
  const log = mock.method(console, 'log', () => {});
  try {
    await assert.rejects(new Checker().check(), (actual) => actual === error);
    assert.equal(log.mock.callCount(), 1);
    assert.match(String(log.mock.calls[0]?.arguments[0]), /^check: \d+\.\d{2}ms$/);
  } finally { mock.restoreAll(); }
});
