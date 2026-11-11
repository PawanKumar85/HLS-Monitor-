import { api } from '../api';

export type PushState = 'unsupported' | 'blocked' | 'off' | 'on';

/** Web Push needs a secure page (HTTPS, or localhost), a service worker and the Push API. */
export const pushSupported = () => window.isSecureContext && 'serviceWorker' in navigator && 'PushManager' in window && 'Notification' in window;

const registration = () => navigator.serviceWorker.register('/sw.js', { scope: '/' }).then(() => navigator.serviceWorker.ready);

function keyBytes(base64url: string): Uint8Array {
  const raw = atob((base64url + '='.repeat((4 - base64url.length % 4) % 4)).replace(/-/g, '+').replace(/_/g, '/'));
  return Uint8Array.from(raw, c => c.charCodeAt(0));
}

export async function pushState(): Promise<PushState> {
  if (!pushSupported()) return 'unsupported';
  if (Notification.permission === 'denied') return 'blocked';
  const subscription = await (await registration()).pushManager.getSubscription();
  if (!subscription) return 'off';
  // The browser may still hold a subscription the server dropped (expired): treat it as off.
  const { subscribed } = await api<{ subscribed: boolean }>('/api/push/status', { method: 'POST', body: JSON.stringify({ endpoint: subscription.endpoint }) });
  return subscribed ? 'on' : 'off';
}

export async function enablePush(): Promise<PushState> {
  if (await Notification.requestPermission() !== 'granted') return Notification.permission === 'denied' ? 'blocked' : 'off';
  const reg = await registration();
  const { publicKey } = await api<{ publicKey: string }>('/api/push/key');
  const subscription = await reg.pushManager.getSubscription() ?? await reg.pushManager.subscribe({ userVisibleOnly: true, applicationServerKey: keyBytes(publicKey) as BufferSource });
  await api('/api/push/subscribe', { method: 'POST', body: JSON.stringify(subscription.toJSON()) });
  return 'on';
}

export async function disablePush(): Promise<PushState> {
  const subscription = await (await registration()).pushManager.getSubscription();
  if (subscription) {
    await api('/api/push/unsubscribe', { method: 'POST', body: JSON.stringify({ endpoint: subscription.endpoint }) });
    await subscription.unsubscribe();
  }
  return 'off';
}
