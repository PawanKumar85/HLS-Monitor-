import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Link } from 'react-router-dom';
import { api, AuthError, clock, stamp } from './api';
import type { Status, Incident, Topology, SavedProbe } from './Pipeline';
import { useFailures } from './lib/failures-context';
import type { SrtPing } from './lib/srt-ping';
import { StatusGauge } from './StatusGauge';
import { HealthCharts } from './HealthCharts';
import { CHANNEL_LABELS, dashboardSummary, incidentServer } from './lib/dashboard-summary';

const CHANNEL_GAUGES = [
  { metric: 'healthy', title: 'Online · primary', tone: 'online', description: 'Channels healthy on their primary path' },
  { metric: 'critical', title: 'Offline / fallback', tone: 'offline', description: 'Down, root cause detected, or VOD fallback' },
  { metric: 'backup', title: 'On backup', tone: 'backup', description: 'Live through an alternate or backup path' },
  { metric: 'unknown', title: 'Unverified', tone: 'unknown', description: 'Awaiting probes or missing verified health' },
] as const;
const SOURCE_LABELS = ['Pipeline status', 'Channel inventory', 'Incident history', 'Probe targets', 'SRT server reachability', 'DLQ counts'];

export function Dashboard() {
  const [status, setStatus] = useState<Status | null>(null);
  const [channels, setChannels] = useState<Topology[] | null>(null);
  const [srtServers, setSrtServers] = useState<SrtPing[]>([]);
  const [probes, setProbes] = useState<SavedProbe[] | null>(null);
  const [incidents, setIncidents] = useState<Incident[] | null>(null);
  const [dlqOpen, setDlqOpen] = useState<number | null>(null);
  const [errors, setErrors] = useState<string[]>([]);
  const [busy, setBusy] = useState(false);
  const [updated, setUpdated] = useState<Date | null>(null);
  const mounted = useRef(true); const running = useRef(false); const lastSlow = useRef(0);
  const failures = useFailures();
  const load = useCallback(async () => {
    if (running.current) return;
    running.current = true; setBusy(true);
    // Status and incidents change every probe round (10 s). Channels, probe targets and SRT pings (refreshed
    // server-side every 30 s) change rarely, so they are fetched every 30 s: 2 requests per tab per round, not 5.
    const slow = Date.now() - lastSlow.current >= 30_000;
    const skipped = Promise.reject(new Error('skipped'));
    skipped.catch(() => {});
    const get = <T,>(url: string, include = true) => include ? api<T>(url, { signal: AbortSignal.timeout(8000) }) : skipped as Promise<T>;
    const results = await Promise.allSettled([
      get<Status>('/api/pipeline/status'),
      get<{ channels: Topology[] }>('/api/pipeline/channels', slow),
      get<{ incidents: Incident[] }>('/api/pipeline/incidents?hours=24'),
      get<{ probes: SavedProbe[] }>('/api/pipeline/probes', slow),
      get<{ servers: SrtPing[] }>('/api/pipeline/srt-reachability', slow),
      get<{ byResolution: Record<string, number> }>('/api/failures/summary', slow),
    ]);
    if (slow && results[1].status === 'fulfilled') lastSlow.current = Date.now(); // retry next round if the slow set failed
    if (mounted.current) {
      const [health, inventory, history, targets, ping, dlq] = results;
      // Whole-history count; the live feed only holds the newest records.
      if (dlq.status === 'fulfilled') setDlqOpen((dlq.value.byResolution['OPEN'] ?? 0) + (dlq.value.byResolution['REPLAY_REQUESTED'] ?? 0));
      if (ping.status === 'fulfilled') setSrtServers(ping.value.servers);
      if (targets.status === 'fulfilled') setProbes(targets.value.probes);
      if (health.status === 'fulfilled') setStatus(health.value);
      if (inventory.status === 'fulfilled') setChannels(inventory.value.channels);
      if (history.status === 'fulfilled') setIncidents(history.value.incidents);
      setErrors(previous => results.flatMap((result, i) => {
        if (result.status === 'fulfilled') return [];
        // A skipped (not due) request keeps its last known error state.
        if ((result.reason as Error).message === 'skipped') return previous.filter(e => e.startsWith(SOURCE_LABELS[i]!));
        return result.reason instanceof AuthError ? [] : [`${SOURCE_LABELS[i]} unavailable`];
      }));
      if (health.status === 'fulfilled') setUpdated(new Date());
      setBusy(false);
    }
    running.current = false;
  }, []);
  useEffect(() => {
    mounted.current = true; void load();
    const timer = setInterval(() => { if (!document.hidden) void load(); }, 10_000);
    return () => { mounted.current = false; clearInterval(timer); };
  }, [load]);
  const summary = useMemo(() => dashboardSummary(status?.pending ? [] : status?.channels ?? [], channels), [status, channels]);
  const openIncidents = incidents?.filter(i => !i.resolvedAt).sort((a, b) => Date.parse(b.openedAt) - Date.parse(a.openedAt));
  const openFailures = Object.values(failures.failures).filter(f => f.resolution !== 'RESOLVED').sort((a, b) => Date.parse(b.lastFailedAt) - Date.parse(a.lastFailedAt));
  const stale = !!status?.evaluatedAt && Date.now() - Date.parse(status.evaluatedAt) > 60_000;
  const ready = !!status && !status.pending && !stale && !errors.includes('Pipeline status unavailable');
  const show = (value: number) => ready ? value : '—';
  return <section className="observability-dashboard" aria-labelledby="dashboard-heading">
    <div className="sec-head"><div><h2 id="dashboard-heading">Dashboard</h2><p>Live operations overview · channel availability, server reachability and processing health.</p></div><div className="tools"><p className="muted">{updated ? `Updated ${clock.format(updated)}` : 'Connecting…'}</p><button className="btn" type="button" disabled={busy} onClick={() => void load()}>{busy ? 'Refreshing…' : 'Refresh'}</button></div></div>
    {!!errors.length && <p className="help help--error" role="alert">{errors.join(' · ')}. Previously loaded information may be outdated.</p>}
    <div className="monitor-section-heading"><h3>Channel status meters</h3><span>Current snapshot · refresh 10s</span></div>
    <div className="status-gauge-grid">
      {CHANNEL_GAUGES.map(({ metric, ...gauge }) => <StatusGauge key={metric} {...gauge} count={summary[metric]} total={summary.total} available={ready} />)}
    </div>
    <p className="monitor-footnote">Meters show the current share of configured channels, not historical uptime. SRT ping indicates server reachability only.</p>
    <div className="dashboard-stats monitor-stats">
      {[['Configured channels', channels !== null ? summary.total : status ? summary.total : '—', 'stale'], ['Verified online', ready ? summary.healthy + summary.backup : '—', 'online'], ['Unprobed hops', show(summary.unprobed), 'stale'], ['Open incidents · 24h', openIncidents?.length ?? '—', 'offline'], ['Unresolved DLQ', dlqOpen ?? (failures.connected || openFailures.length ? openFailures.length : '—'), 'offline'], ['SRT hosts reachable', srtServers.length ? `${srtServers.filter(s => s.state === 'REACHABLE' && Date.now() - Date.parse(s.checkedAt) < 90000).length} / ${srtServers.length}` : '—', 'online']].map(([label, value, statTone]) => <div className="panel" key={label}><span className="muted">{label}</span><strong data-tone={statTone}>{value}</strong></div>)}
    </div>
    <HealthCharts srtServers={srtServers} channels={channels} probes={probes} verdicts={status?.channels ?? []} fresh={ready} />
    <div className="dashboard-grid">
      <div className="panel dashboard-report"><div className="incidents-head"><h3>Channel health</h3><Link to="/create">View all</Link></div>
        {!status ? <p className="muted">Waiting for pipeline status…</p> : !summary.total ? <p className="muted">Create your first channel in Add Stream.</p> : <ul className="dashboard-list">{summary.channels.slice(0, 8).map(c => <li key={c.triggerId}><div><b>{c.channelName}</b><p className="muted">{c.rootCauseSummary ?? (c.overallState === 'UNVERIFIED' ? 'Configure probe targets to verify this channel.' : 'See Pipeline for hop details.')}</p></div><span className={`state state--${!ready ? 'stale' : c.overallState === 'OPTIMAL' ? 'online' : c.overallState === 'UNVERIFIED' ? 'stale' : c.overallState === 'DEGRADED_FAILOVER' ? 'warn' : 'offline'}`}>{!ready ? 'Last known' : CHANNEL_LABELS[c.overallState]}</span></li>)}{summary.pending.slice(0, 8).map(c => <li key={c.triggerId}><b>{c.channelName}</b><span className="state state--stale">Awaiting probe</span></li>)}</ul>}
      </div>
      <div className="panel dashboard-report"><div className="incidents-head"><h3>Open incidents · last 24 hours</h3><Link to="/create">View incidents</Link></div>{!openIncidents ? <p className="muted">Incident history unavailable or loading.</p> : !openIncidents.length ? <p className="muted">No open incidents in the reporting window.</p> : <ul className="dashboard-list">{openIncidents.slice(0, 5).map(i => <li key={i.key}><div><b>{i.title}</b><p className="muted">{i.channels.join(', ')}</p><p className="muted">Opened {stamp.format(new Date(i.openedAt))}{incidentServer(i.key) && <> · <Link to={`/reports?hours=168&server=${encodeURIComponent(incidentServer(i.key)!)}`}>Server history</Link></>}</p></div><span className="state state--offline">Open</span></li>)}</ul>}</div>
      <div className="panel dashboard-report dashboard-report--wide"><div className="incidents-head"><h3>DLQ · recent unresolved failures ({openFailures.length})</h3><Link to="/dlq">Open DLQ</Link></div><p className="help">Latest feed records only; this is not the total archive count. {failures.connected ? 'Live feed connected.' : 'Failure feed connecting or reconnecting.'}</p>{!openFailures.length ? <p className="muted">{failures.connected ? 'No unresolved failures in loaded records.' : 'Waiting for failure records…'}</p> : <ul className="dashboard-list">{openFailures.slice(0, 5).map(f => <li key={f.failureId}><div><b>{f.serverIp}{f.streamId ? ` / ${f.streamId}` : ''}</b><p className="muted">{f.failureStage} · {f.errorCode}</p><p className="dashboard-error">{f.errorMessage}</p></div><span className={`state state--${f.resolution === 'OPEN' ? 'offline' : 'stale'}`}>{f.resolution === 'OPEN' ? 'Open' : 'Replay requested'}</span></li>)}</ul>}</div>
    </div>
  </section>;
}
