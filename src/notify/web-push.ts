import { createHash } from 'node:crypto';
import webpush from 'web-push';
import type { Db } from 'mongodb';
import type { IncidentNotifier, PipelineIncident } from '../monitoring/pipeline-monitor.js';
import { incidentServer } from '../monitoring/incident-server.js';

export interface PushSubscriptionInput { endpoint: string; keys: { p256dh: string; auth: string } }
export interface PushPayload { title: string; body: string; tag: string; url: string; critical: boolean }
export type PushSender = (subscription: PushSubscriptionInput, payload: string) => Promise<void>;

/** Browsers' push services. The server POSTs to the endpoint, so arbitrary URLs (internal hosts) are refused. */
const PUSH_HOSTS = [/\.googleapis\.com$/, /\.mozilla\.com$/, /\.mozaws\.net$/, /(^|\.)push\.apple\.com$/, /\.notify\.windows\.com$/];

/** Validates a browser PushSubscription (as sent by the dashboard). Throws TypeError with a message fit for the user. */
export function parseSubscription(value: unknown): PushSubscriptionInput {
  const v = value as { endpoint?: unknown; keys?: { p256dh?: unknown; auth?: unknown } } | null;
  if (!v || typeof v.endpoint !== 'string' || v.endpoint.length > 2048) throw new TypeError('Invalid push subscription');
  let url: URL; try { url = new URL(v.endpoint); } catch { throw new TypeError('Invalid push subscription endpoint'); }
  if (url.protocol !== 'https:' || !PUSH_HOSTS.some(host => host.test(url.hostname))) throw new TypeError('Push endpoint is not a known browser push service');
  const { p256dh, auth } = v.keys ?? {};
  const b64url = /^[A-Za-z0-9_-]{16,200}={0,2}$/;
  if (typeof p256dh !== 'string' || typeof auth !== 'string' || !b64url.test(p256dh) || !b64url.test(auth)) throw new TypeError('Invalid push subscription keys');
  return { endpoint: url.href, keys: { p256dh, auth } };
}

/** What a phone or desktop shows for an incident. Opened = critical and stays until dismissed; resolved replaces it (same tag). */
export function incidentPayload(type: 'opened' | 'resolved', incident: PipelineIncident): PushPayload {
  const host = incidentServer(incident.key);
  const channels = incident.channels.length ? `Channels: ${incident.channels.join(', ')}` : '';
  const minutes = incident.resolvedAt ? Math.max(1, Math.round((new Date(incident.resolvedAt).getTime() - new Date(incident.openedAt).getTime()) / 60_000)) : null;
  return type === 'opened'
    ? { title: `${incident.kind === 'CONVICT' ? 'Critical' : 'Alert'}: ${incident.title}`, body: channels || 'Open the dashboard for details.', tag: incident.key, critical: true,
        url: host ? `/reports?hours=24&server=${encodeURIComponent(host)}` : '/' }
    : { title: `Resolved: ${incident.title}`, body: `${minutes ? `Down for ${minutes} min. ` : ''}${channels}`.trim(), tag: incident.key, critical: false, url: '/' };
}

type StoredSubscription = PushSubscriptionInput & { _id: string; userId: string; createdAt: Date };
const id = (endpoint: string) => createHash('sha256').update(endpoint).digest('hex');

/**
 * Self-hosted Web Push (VAPID, no paid provider). Subscriptions live in MongoDB `push_subscriptions`; VAPID keys come
 * from VAPID_PUBLIC_KEY / VAPID_PRIVATE_KEY, or are generated once and kept in `app_settings` so they never change.
 * Delivery is best effort: the dashboard and incident history remain the record.
 */
export class WebPushService {
  private keys: Promise<{ publicKey: string; privateKey: string }> | undefined;

  constructor(private readonly db: () => Db, private readonly options: { subject?: string; sender?: PushSender; log?: (message: string) => void } = {}) {}

  private vapid() {
    this.keys ??= (async () => {
      const fromEnv = { publicKey: process.env['VAPID_PUBLIC_KEY'], privateKey: process.env['VAPID_PRIVATE_KEY'] };
      if (fromEnv.publicKey && fromEnv.privateKey) return fromEnv as { publicKey: string; privateKey: string };
      const settings = this.db().collection<{ _id: string; publicKey: string; privateKey: string }>('app_settings');
      const generated = webpush.generateVAPIDKeys();
      // First instance to start wins; every later start (and every other instance) reads the same pair.
      await settings.updateOne({ _id: 'vapid' }, { $setOnInsert: generated }, { upsert: true });
      const stored = await settings.findOne({ _id: 'vapid' });
      return { publicKey: stored!.publicKey, privateKey: stored!.privateKey };
    })().catch(error => { this.keys = undefined; throw error; });
    return this.keys;
  }

  private collection() { return this.db().collection<StoredSubscription>('push_subscriptions'); }

  async publicKey(): Promise<string> { return (await this.vapid()).publicKey; }

  async subscribe(userId: string, subscription: PushSubscriptionInput): Promise<void> {
    await this.collection().updateOne({ _id: id(subscription.endpoint) },
      { $set: { ...subscription, userId }, $setOnInsert: { createdAt: new Date() } }, { upsert: true });
  }

  async unsubscribe(endpoint: string): Promise<void> { await this.collection().deleteOne({ _id: id(endpoint) }); }

  async isSubscribed(endpoint: string): Promise<boolean> { return (await this.collection().countDocuments({ _id: id(endpoint) }, { limit: 1 })) === 1; }

  /** Sends to every subscription (or one user's). Gone subscriptions (404/410) are deleted. Returns how many were delivered. */
  async send(payload: PushPayload, userId?: string): Promise<number> {
    const subscriptions = await this.collection().find(userId ? { userId } : {}).toArray();
    if (!subscriptions.length) return 0;
    const keys = await this.vapid();
    const send: PushSender = this.options.sender ?? (async (subscription, body) => {
      await webpush.sendNotification(subscription, body, {
        vapidDetails: { subject: this.options.subject ?? 'mailto:alerts@localhost', ...keys },
        TTL: 3600, urgency: payload.critical ? 'high' : 'normal',
      });
    });
    const body = JSON.stringify(payload);
    let delivered = 0;
    await Promise.all(subscriptions.map(async s => {
      try { await send({ endpoint: s.endpoint, keys: s.keys }, body); delivered++; }
      catch (error) {
        const status = (error as { statusCode?: number }).statusCode;
        if (status === 404 || status === 410) await this.collection().deleteOne({ _id: s._id }); // the browser unsubscribed
        else (this.options.log ?? console.error)(`Web Push delivery failed${status ? ` (HTTP ${status})` : ''}`);
      }
    }));
    return delivered;
  }

  /** Plugs into the incident alerts (pipeline monitor, check watchdog) next to the webhook. */
  notifier(): IncidentNotifier {
    return async ({ type, incident }) => {
      try { await this.send(incidentPayload(type, incident)); }
      catch { (this.options.log ?? console.error)('Web Push alerts unavailable'); }
    };
  }
}

/** Runs several notifiers for one event; one failing does not stop the others. */
export function allNotifiers(...notifiers: (IncidentNotifier | undefined)[]): IncidentNotifier {
  const list = notifiers.filter((n): n is IncidentNotifier => !!n);
  return async event => { await Promise.allSettled(list.map(n => n(event))); };
}
