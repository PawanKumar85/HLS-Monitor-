import { test } from 'node:test';
import assert from 'node:assert/strict';
import { alertEvents, unreadCount } from '../frontend/src/lib/notifications.js';

test('notifications: one alert when an incident opens, one when it resolves, newest first; unread = newer than last seen', () => {
  const events = alertEvents([
    { key: 'node:a', kind: 'CONVICT', title: 'A is down', channels: ['X'], openedAt: '2026-11-12T10:00:00Z', resolvedAt: '2026-11-12T10:05:00Z' },
    { key: 'node:b', kind: 'OFFLINE', title: 'B is down', channels: [], openedAt: '2026-11-12T10:03:00Z', resolvedAt: null },
    { key: 'node:c', kind: 'OFFLINE', title: 'bad date', channels: [], openedAt: 'nope' },
  ]);
  assert.deepEqual(events.map(e => `${e.type}:${e.incident.key}`), ['resolved:node:a', 'opened:node:b', 'opened:node:a']);
  assert.equal(new Set(events.map(e => e.id)).size, 3);
  assert.equal(unreadCount(events, 0), 3);
  assert.equal(unreadCount(events, Date.parse('2026-11-12T10:03:00Z')), 1, 'only the resolution is newer');
  assert.equal(unreadCount(events, Date.parse('2026-11-12T11:00:00Z')), 0);
});
