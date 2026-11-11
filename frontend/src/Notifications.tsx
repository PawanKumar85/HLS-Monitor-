import { useEffect, useMemo, useRef, useState } from 'react';
import { Link } from 'react-router-dom';
import { api, stamp } from './api';
import { toast } from './toast';
import type { Incident } from './Pipeline';
import { alertEvents, unreadCount } from './lib/notifications';
import { incidentServer } from './lib/dashboard-summary';

const SEEN_KEY = 'notifications-seen-at';
const readSeen = () => { try { return Number(localStorage.getItem(SEEN_KEY)) || 0; } catch { return 0; } };

/**
 * Navbar bell: incident alerts of the last 24 hours (opened and resolved), with a count of the ones not seen yet
 * on this browser. A new alert that arrives while the site is open also shows as a toast.
 */
export function Notifications() {
  const [incidents, setIncidents] = useState<Incident[] | null>(null); // null until the first load
  const [seenAt, setSeenAt] = useState(readSeen);
  const [open, setOpen] = useState(false);
  const [newSince, setNewSince] = useState(0); // what counted as unread when the panel was opened, to mark it "New"
  const known = useRef<Set<string> | null>(null);
  const root = useRef<HTMLDivElement>(null);
  const events = useMemo(() => alertEvents(incidents ?? []), [incidents]);
  const unread = unreadCount(events, seenAt);

  useEffect(() => {
    let current = true;
    const load = () => api<{ incidents: Incident[] }>('/api/pipeline/incidents?hours=24', { signal: AbortSignal.timeout(8000) })
      .then(r => { if (current) setIncidents(r.incidents); }, () => { /* keeps the last list; retried on the next tick */ });
    void load();
    const timer = setInterval(() => { if (!document.hidden) void load(); }, 15_000);
    return () => { current = false; clearInterval(timer); };
  }, []);

  // Toast alerts that arrive after the first load (the first load only fills the list).
  useEffect(() => {
    if (!incidents) return;
    if (!known.current) { known.current = new Set(events.map(e => e.id)); return; }
    for (const e of [...events].reverse()) {
      if (known.current.has(e.id)) continue;
      known.current.add(e.id);
      toast(`${e.type === 'opened' ? 'Alert' : 'Resolved'}: ${e.incident.title}`, e.type === 'opened');
    }
  }, [events, incidents]);

  useEffect(() => {
    if (!open) return;
    const outside = (event: MouseEvent) => { if (!root.current?.contains(event.target as Node)) setOpen(false); };
    document.addEventListener('mousedown', outside);
    return () => document.removeEventListener('mousedown', outside);
  }, [open]);

  function toggle() {
    if (!open) {
      setNewSince(seenAt);
      const now = Date.now(); setSeenAt(now);
      try { localStorage.setItem(SEEN_KEY, String(now)); } catch { /* the count just returns after a reload */ }
    }
    setOpen(o => !o);
  }

  return (
    <div className="notify" ref={root} onKeyDown={e => { if (e.key === 'Escape') setOpen(false); }}>
      <button className="notify__bell" type="button" aria-label={`Notifications, ${unread} unread`} aria-expanded={open} aria-controls="notify-panel" onClick={toggle}>
        <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true"><path d="M6 9a6 6 0 1 1 12 0c0 6 2.5 7 2.5 7h-17S6 15 6 9Z" /><path d="M10 20a2 2 0 0 0 4 0" /></svg>
        {unread > 0 && <span className="notify__count" aria-hidden="true">{unread > 99 ? '99+' : unread}</span>}
      </button>
      {open && (
        <div className="notify__panel" id="notify-panel" role="region" aria-label="Notifications">
          <div className="notify__head"><h3>Notifications</h3><span className="muted">Last 24 hours</span></div>
          {!events.length ? <p className="muted notify__empty">No alerts in the last 24 hours.</p> : (
            <ul>{events.slice(0, 30).map(e => {
              const server = incidentServer(e.incident.key);
              return (
                <li key={e.id} data-new={e.at > newSince || undefined}>
                  <span className={`state state--${e.type === 'opened' ? 'offline' : 'online'}`}>{e.type === 'opened' ? 'Alert' : 'Resolved'}</span>
                  <div>
                    <b>{e.incident.title}</b>
                    {e.incident.channels.length > 0 && <p className="muted">{e.incident.channels.join(', ')}</p>}
                    <p className="muted">{stamp.format(new Date(e.at))}{e.at > newSince && <> · <span className="notify__new">New</span></>}
                      {server && <> · <Link to={`/reports?hours=168&server=${encodeURIComponent(server)}`} onClick={() => setOpen(false)}>Server history</Link></>}</p>
                  </div>
                </li>
              );
            })}</ul>
          )}
          <div className="notify__foot"><Link to="/" onClick={() => setOpen(false)}>Open Dashboard</Link><Link to="/account" onClick={() => setOpen(false)}>Background alerts</Link></div>
        </div>
      )}
    </div>
  );
}
