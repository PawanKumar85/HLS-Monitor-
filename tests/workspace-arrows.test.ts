import { test } from 'node:test';
import assert from 'node:assert/strict';
import { arrowLayout, readArrowLayouts, reconnectArrow } from '../frontend/src/lib/workspace-arrows.js';

const edge = { id: 'channel-hop-link', source: 'origin', target: 'cdn', label: 'Channel · PRIMARY' };
test('arrow endpoints can move to any side while preserving the server pair', () => {
  for (const sourceHandle of ['top', 'right', 'bottom', 'left']) {
    for (const targetHandle of ['top', 'right', 'bottom', 'left']) {
      const result = reconnectArrow(edge, { source: 'origin', target: 'cdn', sourceHandle, targetHandle });
      assert.equal(result?.sourceSide, sourceHandle);
      assert.equal(result?.targetSide, targetHandle);
      assert.equal(result?.source, 'origin');
      assert.equal(result?.target, 'cdn');
    }
  }
});
test('workspace rejects topology changes and unknown endpoints', () => {
  assert.equal(reconnectArrow(edge, { source: 'other', target: 'cdn', sourceHandle: 'top', targetHandle: 'left' }), null);
  assert.equal(reconnectArrow(edge, { source: 'origin', target: 'other', sourceHandle: 'top', targetHandle: 'left' }), null);
  assert.equal(reconnectArrow(edge, { source: 'origin', target: 'cdn', sourceHandle: null, targetHandle: 'left' }), null);
});
test('arrow preferences survive storage and stale or invalid preferences are ignored', () => {
  const custom = { ...arrowLayout(edge), sourceSide: 'bottom' as const, targetSide: 'top' as const, curvature: 0.8 };
  const restored = readArrowLayouts(JSON.stringify({ [edge.id]: custom, broken: { source: 'a', curvature: 'NaN' } }));
  assert.deepEqual(arrowLayout(edge, restored[edge.id]), custom);
  assert.equal(Object.keys(restored).length, 1);
  assert.deepEqual(arrowLayout({ ...edge, target: 'new-server' }, custom), { source: 'origin', target: 'new-server', sourceSide: 'right', targetSide: 'left', curvature: 0.25 });
  assert.deepEqual(readArrowLayouts('not json'), {});
  assert.deepEqual(readArrowLayouts('[]'), {});
  assert.deepEqual(arrowLayout(edge), { source: 'origin', target: 'cdn', sourceSide: 'right', targetSide: 'left', curvature: 0.25 });
});
