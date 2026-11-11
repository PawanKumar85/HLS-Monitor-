import { test } from 'node:test';
import assert from 'node:assert/strict';
import Fastify from 'fastify';
import { allNotifiers, incidentPayload, parseSubscription } from '../src/notify/web-push.js';
import { registerPushRoutes, type PushApi } from '../src/http/push-routes.js';

const keys = { p256dh: 'BNcRdreALRFXTkOOUHK1EtK2wtaz5Ry4YfYCA_0QTpQtUbVlUls0VJXg7A8u-Ts1XbjhazAkj7I99e8QcYP7DkM', auth: 'tBHItJI5svbpez7KI4CCXg' };

test('push subscriptions: only real browser push services, https, well-formed keys', () => {
  for (const endpoint of ['https://fcm.googleapis.com/fcm/send/abc', 'https://updates.push.services.mozilla.com/wpush/v2/abc', 'https://web.push.apple.com/abc', 'https://wns2-par02p.notify.windows.com/w/?token=abc']) {
    assert.equal(parseSubscription({ endpoint, keys }).endpoint, new URL(endpoint).href);
  }
  for (const bad of [
    { endpoint: 'http://fcm.googleapis.com/x', keys }, { endpoint: 'https://127.0.0.1/x', keys }, { endpoint: 'https://evil.example/x', keys },
    { endpoint: 'https://googleapis.com.evil.example/x', keys }, { endpoint: 'https://fcm.googleapis.com/x', keys: { p256dh: 'x', auth: keys.auth } }, null, {},
  ]) assert.throws(() => parseSubscription(bad), TypeError, JSON.stringify(bad));
});

test('incident alerts: opened is critical and opens its server history; resolved replaces it with the duration', () => {
  const incident = { key: 'node:xcode3.test:443', kind: 'CONVICT' as const, title: 'Xcode3 (xcode3.test:443) is down', channels: ['ABN', 'Sakshi'], openedAt: new Date('2026-11-12T10:00:00Z') };
  const opened = incidentPayload('opened', incident);
  assert.deepEqual([opened.title, opened.body, opened.tag, opened.critical, opened.url], ['Critical: Xcode3 (xcode3.test:443) is down', 'Channels: ABN, Sakshi', 'node:xcode3.test:443', true, '/reports?hours=24&server=xcode3.test']);
  const resolved = incidentPayload('resolved', { ...incident, resolvedAt: new Date('2026-11-12T10:12:00Z') });
  assert.deepEqual([resolved.title, resolved.body, resolved.tag, resolved.critical], ['Resolved: Xcode3 (xcode3.test:443) is down', 'Down for 12 min. Channels: ABN, Sakshi', 'node:xcode3.test:443', false]);
  assert.equal(incidentPayload('opened', { ...incident, key: 'monitor:checks-stalled', kind: 'OFFLINE', channels: [] }).url, '/');
});

test('allNotifiers: a failing channel (webhook down) does not stop push', async () => {
  const got: string[] = [];
  await allNotifiers(async () => { throw new Error('webhook down'); }, undefined, async e => { got.push(e.type); })({ type: 'opened', incident: { key: 'k', kind: 'OFFLINE', title: 't', channels: [], openedAt: new Date() } });
  assert.deepEqual(got, ['opened']);
});

test('push API: key, subscribe (validated), status, test and unsubscribe, with endpoints only in bodies', async () => {
  const subs = new Map<string, string>(); const sent: string[] = [];
  const push: PushApi = {
    async publicKey() { return 'PUBLIC'; },
    async subscribe(userId, s) { subs.set(s.endpoint, userId); },
    async unsubscribe(endpoint) { subs.delete(endpoint); },
    async isSubscribed(endpoint) { return subs.has(endpoint); },
    async send(payload, userId) { sent.push(`${payload.title}|${userId}`); return [...subs.values()].filter(u => u === userId).length; },
  };
  const app = Fastify(); registerPushRoutes(app, push);
  const endpoint = 'https://fcm.googleapis.com/fcm/send/device-1';
  try {
    assert.equal((await app.inject('/api/push/key')).json().publicKey, 'PUBLIC');
    assert.equal((await app.inject({ method: 'POST', url: '/api/push/subscribe', payload: { endpoint: 'https://10.0.0.1/x', keys } })).statusCode, 400);
    assert.equal((await app.inject({ method: 'POST', url: '/api/push/subscribe', payload: { endpoint, keys } })).statusCode, 201);
    assert.equal((await app.inject({ method: 'POST', url: '/api/push/status', payload: { endpoint } })).json().subscribed, true);
    assert.equal((await app.inject({ method: 'POST', url: '/api/push/test' })).json().delivered, 1);
    assert.deepEqual(sent, ['Test alert from HLS monitor|anonymous']);
    assert.equal((await app.inject({ method: 'POST', url: '/api/push/unsubscribe', payload: { endpoint } })).statusCode, 204);
    assert.equal(subs.size, 0);
  } finally { await app.close(); }
});
