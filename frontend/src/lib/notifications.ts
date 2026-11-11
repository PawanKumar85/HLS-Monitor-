import type { Incident } from '../Pipeline';

export interface AlertEvent { id: string; type: 'opened' | 'resolved'; at: number; incident: Incident }

/** Every incident is one alert when it opens and another when it resolves; newest first. */
export function alertEvents(incidents: readonly Incident[]): AlertEvent[] {
  return incidents.flatMap(incident => {
    const opened = Date.parse(incident.openedAt); const resolved = incident.resolvedAt ? Date.parse(incident.resolvedAt) : NaN;
    return [
      ...(Number.isFinite(opened) ? [{ id: `${incident.key}|${incident.openedAt}|opened`, type: 'opened' as const, at: opened, incident }] : []),
      ...(Number.isFinite(resolved) ? [{ id: `${incident.key}|${incident.openedAt}|resolved`, type: 'resolved' as const, at: resolved, incident }] : []),
    ];
  }).sort((a, b) => b.at - a.at);
}

export const unreadCount = (events: readonly AlertEvent[], seenAt: number) => events.filter(e => e.at > seenAt).length;
