import { test } from 'node:test';
import assert from 'node:assert/strict';
import { flowEdge } from '../frontend/src/lib/pipeline-canvas-edge.js';

test('saved bypass links reopen on the bypass handle with a curved, dashed path', () => {
  const edge = flowEdge('main', 'transcoder', 'BYPASS');
  assert.equal(edge.sourceHandle, 'bottom');
  assert.equal(edge.targetHandle, 'left');
  assert.equal(edge.data?.kind, 'BYPASS');
  assert.equal(edge.label, 'BYPASS');
  assert.equal(edge.pathOptions?.curvature, 0.7);
  assert.equal(edge.style?.strokeDasharray, '7 4');
});

test('other hop types use the main handle and restore a solid path after changing type', () => {
  for (const type of ['PRIMARY', 'FAILOVER', 'FALLBACK'] as const) {
    const edge = flowEdge('main', 'transcoder', type);
    assert.equal(edge.sourceHandle, 'right');
    assert.equal(edge.data?.kind, type);
    assert.equal(edge.style?.strokeDasharray, undefined);
    assert.equal(edge.pathOptions?.curvature, 0.25);
  }
});

test('links retain all four mouse-selected connection sides when changing link type', () => {
  for (const source of ['top', 'right', 'bottom', 'left']) {
    for (const target of ['top', 'right', 'bottom', 'left']) {
      const edge = flowEdge('a', 'b', 'PRIMARY', source, target);
      const changed = flowEdge(edge.source, edge.target, 'BYPASS', edge.sourceHandle, edge.targetHandle);
      assert.equal(changed.sourceHandle, source);
      assert.equal(changed.targetHandle, target);
    }
  }
});
