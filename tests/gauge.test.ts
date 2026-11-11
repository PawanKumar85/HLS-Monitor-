import { test } from 'node:test';
import assert from 'node:assert/strict';
import { gaugeValue } from '../frontend/src/lib/gauge.js';
test('speedometer maps counts to a 270 degree arc', () => {
  assert.deepEqual(gaugeValue(0, 4, true), { ratio: 0, percent: 0, angle: 135 });
  assert.deepEqual(gaugeValue(2, 4, true), { ratio: 0.5, percent: 50, angle: 270 });
  assert.deepEqual(gaugeValue(4, 4, true), { ratio: 1, percent: 100, angle: 405 });
});
test('missing, stale or empty data has no artificial zero percent', () => {
  assert.equal(gaugeValue(0, 0, true), null);
  assert.equal(gaugeValue(2, 4, false), null);
  assert.equal(gaugeValue(NaN, 4, true), null);
});
test('gauge safely bounds percentages', () => {
  assert.equal(gaugeValue(5, 4, true)?.percent, 100);
  assert.equal(gaugeValue(-1, 4, true)?.percent, 0);
});
