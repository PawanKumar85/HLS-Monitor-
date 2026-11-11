import { test } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { MongoClient } from 'mongodb';
import { WebPushService } from '../../src/notify/web-push.js';
const enabled = process.env['RUN_INTEGRATION'] === '1';
const keys = { p256dh: 'BNcRdreALRFXTkOOUHK1EtK2wtaz5Ry4YfYCA_0QTpQtUbVlUls0VJXg7A8u-Ts1XbjhazAkj7I99e8QcYP7DkM', auth: 'tBHItJI5svbpez7KI4CCXg' };

test('web push: one VAPID pair for every instance, per-user delivery, and gone devices are removed', { skip: !enabled, timeout: 15000 }, async t => {
  assert.match(process.env['MONGO_DATABASE'] ?? '', /^gate_test_[a-zA-Z0-9_]+$/);
  delete process.env['VAPID_PUBLIC_KEY']; delete process.env['VAPID_PRIVATE_KEY'];
  const mongo = new MongoClient(process.env['MONGO_URI']!);
  const db = mongo.db(`gate_test_push_${randomUUID().replaceAll('-', '')}`);
  t.after(async () => { await db.dropDatabase(); await mongo.close(); });
  const sent: string[] = [];
  const sender = async (s: { endpoint: string }, body: string) => {
    if (s.endpoint.endsWith('/gone')) throw Object.assign(new Error('gone'), { statusCode: 410 });
    sent.push(`${s.endpoint.split('/').at(-1)}:${JSON.parse(body).title}`);
  };
  const [a, b] = [new WebPushService(() => db, { sender, log: () => {} }), new WebPushService(() => db, { sender, log: () => {} })];
  const [keyA, keyB] = await Promise.all([a.publicKey(), b.publicKey()]);
  assert.equal(keyA, keyB, 'two instances starting together share one generated key pair');
  assert.match(keyA, /^[A-Za-z0-9_-]{80,}$/);

  await a.subscribe('u1', { endpoint: 'https://fcm.googleapis.com/fcm/send/phone', keys });
  await a.subscribe('u2', { endpoint: 'https://fcm.googleapis.com/fcm/send/laptop', keys });
  await a.subscribe('u2', { endpoint: 'https://fcm.googleapis.com/fcm/send/gone', keys });
  assert.equal(await a.isSubscribed('https://fcm.googleapis.com/fcm/send/phone'), true);

  assert.equal(await a.send({ title: 'only u1', body: '', tag: 't', url: '/', critical: false }, 'u1'), 1);
  assert.equal(await a.send({ title: 'all', body: '', tag: 't', url: '/', critical: true }), 2);
  assert.deepEqual(sent.sort(), ['laptop:all', 'phone:all', 'phone:only u1']);
  assert.equal(await a.isSubscribed('https://fcm.googleapis.com/fcm/send/gone'), false, 'a 410 from the push service deletes the subscription');

  await a.unsubscribe('https://fcm.googleapis.com/fcm/send/phone');
  assert.equal(await db.collection('push_subscriptions').countDocuments(), 1);
});
