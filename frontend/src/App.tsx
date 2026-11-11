import { InstallApp } from './InstallApp';
import { Documentation } from './Documentation';
import { Toaster } from './toast';
import { useEffect, useRef, useState } from 'react';
import { useHlsStatus } from './lib/useHlsStatus';
import { ago, api, toLogin, type User } from './api';
import { Pipeline } from './Pipeline';
import { Workspace } from './Workspace';
import { Dlq } from './Dlq';
import { Dashboard } from './Dashboard';
import { Report } from './Report';
import { Account } from './Account';
import { FailuresProvider } from './lib/failures-context';
import { Notifications } from './Notifications';
import { Link, Navigate, NavLink, Route, Routes, useLocation } from 'react-router-dom';

export type Live = ReturnType<typeof useHlsStatus>;
const PAGE_TITLES: Record<string, string> = { '/': 'Dashboard', '/workspace': 'Workspace', '/create': 'Add Stream', '/reports': 'Reports', '/dlq': 'DLQ', '/account': 'Account', '/documentation': 'Documentation' };
// Pages that use the dark observability palette (styles.css keys it on data-page).
const PAGE_THEME: Record<string, string> = { '/': 'dashboard', '/create': 'pipeline', '/workspace': 'workspace', '/reports': 'reports', '/account': 'account', '/dlq': 'dlq', '/documentation': 'reports' };

export function App() {
  const { pathname } = useLocation();
  const firstLoad = useRef(true);
  useEffect(() => {
    document.title = `${PAGE_TITLES[pathname] ?? 'Page not found'} | HLS monitor`;
    window.scrollTo(0, 0);
    // On the first load keep the browser's focus order (skip link, sidebar); after navigating, start at the content.
    if (firstLoad.current) firstLoad.current = false;
    else document.getElementById('main-content')?.focus({ preventScroll: true });
  }, [pathname]);
  const [user, setUser] = useState<User | null>(null);
  const live = useHlsStatus('/events/hls-status', 25_000, '/api/hls/status/snapshot', status => { if (status === 401) toLogin(); });
  useEffect(() => { api<User>('/api/me').then(setUser, () => { /* 401 already redirected */ }); }, []);

  // Phone: the sidebar is a drawer opened from the navbar. It closes on navigation, Escape or a tap outside.
  const [menuOpen, setMenuOpen] = useState(false);
  useEffect(() => setMenuOpen(false), [pathname]);
  useEffect(() => {
    if (!menuOpen) return;
    (document.querySelector('.sidebar nav a.active, .sidebar nav a') as HTMLElement | null)?.focus();
    const onKey = (event: KeyboardEvent) => { if (event.key === 'Escape') { setMenuOpen(false); document.getElementById('menu-button')?.focus(); } };
    document.addEventListener('keydown', onKey);
    return () => document.removeEventListener('keydown', onKey);
  }, [menuOpen]);

  return (
    <FailuresProvider>
    <div className="app-layout" data-page={PAGE_THEME[pathname]}>
      <a className="skip-link" href="#main-content">Skip to content</a>
      <Sidebar user={user} open={menuOpen} onNavigate={() => setMenuOpen(false)} />
      {menuOpen && <button className="sidebar-backdrop" type="button" aria-label="Close menu" onClick={() => setMenuOpen(false)} />}
      <Toaster />
      <div className="workspace">
      <Navbar live={live} title={PAGE_TITLES[pathname] ?? 'Page not found'} menuOpen={menuOpen} onMenu={() => setMenuOpen(o => !o)} />
      <main className="container app-content" id="main-content" tabIndex={-1}>
        <Routes>
          <Route path="/" element={<Dashboard />} />
          <Route path="/documentation" element={<Documentation />} />
          <Route path="/workspace" element={<Workspace />} />
          <Route path="/create" element={<Pipeline />} />
          <Route path="/reports" element={<Report enabled />} />
          <Route path="/dlq" element={<Dlq />} />
          <Route path="/account" element={<Account user={user} onUpdated={setUser} />} />
          {/* Old addresses keep working (bookmarks, links in alerts). */}
          {['/dashboard', '/overview'].map(path => <Route key={path} path={path} element={<Navigate to="/" replace />} />)}
          <Route path="/pipeline" element={<Navigate to="/create" replace />} />
          <Route path="*" element={<section className="now"><h1>Page not found</h1><p className="lead">This page does not exist. <Link to="/">Go to Dashboard</Link>.</p></section>} />
        </Routes>
      </main>
      </div>
    </div>
    </FailuresProvider>
  );
}

function Navbar({ live, title, menuOpen, onMenu }: { live: Live; title: string; menuOpen: boolean; onMenu: () => void }) {
  const state = !live.loaded && live.failed ? 'lost' : live.connected ? 'live' : live.loaded ? 'lost' : 'connecting';
  const lastCheck = live.streams.reduce((max, s) => {
    const checkedAt = Date.parse(s.checkedAt);
    return Number.isFinite(checkedAt) ? Math.max(max, checkedAt) : max;
  }, 0);
  return (
    <header className="navbar">
      <button className="navbar__menu" id="menu-button" type="button" aria-label="Menu" aria-expanded={menuOpen} aria-controls="sidebar" onClick={onMenu}>
        <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" aria-hidden="true"><path d="M4 7h16M4 12h16M4 17h16" /></svg>
      </button>
      <p className="navbar__title">{title}</p>
      <div className="navbar__status">
        <p className="conn" data-state={state} role="status" aria-atomic="true">
          <span className="conn__dot" aria-hidden="true" />
          {state === 'live' ? 'Live' : state === 'lost' ? 'Live status lost, reconnecting' : 'Connecting'}
        </p>
        <p className="navbar__checked">Last check {lastCheck ? ago(lastCheck, live.now) : 'pending'}</p>
        <Notifications />
      </div>
    </header>
  );
}

const SIDEBAR_LINKS = [
  ['/', 'Dashboard'],
  ['/workspace', 'Workspace'],
  ['/create', 'Add Stream'],
  ['/reports', 'Reports'],
  ['/dlq', 'DLQ'],
  ['/account', 'Account'],
] as const;

function Sidebar({ user, open, onNavigate }: { user: User | null; open: boolean; onNavigate: () => void }) {
  return (
    <aside className={`sidebar${open ? ' sidebar--open' : ''}`} id="sidebar" aria-label="Dashboard sidebar">
      <div className="sidebar__inner">
        <Link className="brand" to="/" aria-label="HLS monitor, home"><b>HLS monitor</b></Link>
        <nav aria-label="Main navigation" onClick={onNavigate}>
          {/* end: "/" is active only on the Dashboard itself, not on every page under it. */}
          {SIDEBAR_LINKS.map(([to, label]) => <NavLink key={to} to={to} end={to === '/'}>{label}</NavLink>)}
        </nav>
        <div className="sidebar__footer">
          <InstallApp compact />
          <nav className="sidebar__docs" aria-label="Documentation" onClick={onNavigate}>
            <NavLink to="/documentation">Documentation</NavLink>
          </nav>
          {user && <p className="who">{user.username}</p>}
          <form method="post" action="/logout" className="logout"><button className="btn btn--quiet" type="submit">Sign out</button></form>
        </div>
      </div>
    </aside>
  );
}

