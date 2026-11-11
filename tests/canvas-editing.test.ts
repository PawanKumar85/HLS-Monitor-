import { test } from 'node:test';
import assert from 'node:assert/strict';
import { arrangeNodes, canvasSignature } from '../frontend/src/lib/canvas-editing.js';

const nodes = ['main', 'scte', 'transcoder', 'cdn', 'backup'].map(id => ({ id, position: { x: -100, y: -100 }, data: { name: id }, selected: false }));
const edges = [
  { id: 'a', source: 'main', target: 'scte' },
  { id: 'b', source: 'scte', target: 'transcoder' },
  { id: 'c', source: 'transcoder', target: 'cdn' },
  { id: 'd', source: 'main', target: 'transcoder' },
  { id: 'e', source: 'backup', target: 'transcoder' },
];

test('auto-arrange orders the primary chain despite bypasses and separates backup nodes', () => {
  const result = arrangeNodes(nodes, edges);
  const position = (id: string) => result.find(n => n.id === id)!.position;
  assert.ok(position('main').x < position('scte').x);
  assert.ok(position('scte').x < position('transcoder').x);
  assert.ok(position('transcoder').x < position('cdn').x);
  assert.notDeepEqual(position('main'), position('backup'));
  assert.equal(nodes[0]!.position.x, -100, 'original snapshot remains unchanged for undo');
  assert.deepEqual(arrangeNodes(result, edges), result, 'layout is stable');
});

test('auto-arrange handles empty graphs and disconnected drafts', () => {
  assert.deepEqual(arrangeNodes([], []), []);
  const result = arrangeNodes(nodes, []);
  assert.equal(new Set(result.map(n => `${n.position.x}:${n.position.y}`)).size, nodes.length);
});

test('unsaved-change detection ignores selection but includes edits, positions, ports and removed links', () => {
  const initial = canvasSignature('Channel', nodes, edges);
  assert.equal(canvasSignature('Channel', nodes.map(n => ({ ...n, selected: true, measured: { width: 200, height: 90 } })), edges), initial);
  assert.notEqual(canvasSignature('Renamed', nodes, edges), initial);
  assert.notEqual(canvasSignature('Channel', nodes.map(n => ({ ...n, position: { x: 20, y: 40 } })), edges), initial);
  assert.notEqual(canvasSignature('Channel', nodes.map(n => ({ ...n, data: { name: 'changed' } })), edges), initial);
  assert.notEqual(canvasSignature('Channel', nodes, edges.map(e => ({ ...e, sourceHandle: 'top' }))), initial);
  assert.notEqual(canvasSignature('Channel', nodes, edges.slice(1)), initial);
});

test('pathExists follows link direction, handles loops in the data, and treats a hop as reaching itself', async () => {
  const { pathExists } = await import('../frontend/src/lib/canvas-editing.js');
  const links = [{ source: 'a', target: 'b' }, { source: 'b', target: 'c' }, { source: 'c', target: 'b' }];
  assert.equal(pathExists(links, 'a', 'c'), true);
  assert.equal(pathExists(links, 'c', 'a'), false, 'direction matters');
  assert.equal(pathExists(links, 'b', 'x'), false, 'a b↔c loop does not recurse forever');
  assert.equal(pathExists(links, 'a', 'a'), true);
});

test('depths: longest path wins, unknown link ends are ignored, loops stay at 0', async () => {
  const { depths } = await import('../frontend/src/lib/canvas-editing.js');
  const d = depths(['a', 'b', 'c', 'x', 'y'], [{ source: 'a', target: 'b' }, { source: 'b', target: 'c' }, { source: 'a', target: 'c' }, { source: 'a', target: 'ghost' }, { source: 'x', target: 'y' }, { source: 'y', target: 'x' }]);
  assert.deepEqual(Object.fromEntries(d), { a: 0, b: 1, c: 2, x: 0, y: 0 });
});
