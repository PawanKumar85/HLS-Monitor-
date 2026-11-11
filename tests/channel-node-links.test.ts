import { test } from 'node:test';
import assert from 'node:assert/strict';
import { CHANNEL_NODE_ID, isChannelLink, readChannelLinks } from '../frontend/src/lib/channel-node-links.js';

test('channel label links restore their endpoints and connection sides', () => {
  const links = readChannelLinks(JSON.stringify([{ source: CHANNEL_NODE_ID, target: 'ingest', sourceHandle: 'bottom', targetHandle: 'top' }]), ['ingest']);
  assert.equal(links.length, 1);
  assert.equal(links[0]!.sourceHandle, 'bottom');
  assert.equal(links[0]!.targetHandle, 'top');
  assert.equal(links[0]!.label, 'CHANNEL');
  assert.equal(isChannelLink(links[0]!), true);
  assert.equal(isChannelLink({ source: 'ingest', target: 'cdn' }), false);
});
test('removed hops, duplicate links and invalid stored label links are ignored', () => {
  const edge = { source: CHANNEL_NODE_ID, target: 'ingest' };
  assert.equal(readChannelLinks(JSON.stringify([edge, edge, { ...edge, target: 'deleted' }, { source: CHANNEL_NODE_ID, target: CHANNEL_NODE_ID }, { source: 'ingest', target: 'cdn' }]), ['ingest', 'cdn']).length, 1);
  assert.deepEqual(readChannelLinks('invalid', []), []);
});
