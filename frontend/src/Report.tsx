import { Fragment, useCallback, useEffect, useId, useRef, useState } from 'react';
import { Link } from 'react-router-dom';
import { reportSummary } from './lib/report-summary';
import { api, AuthError, clock, param, setParam, stamp, type Report as ReportData } from './api';

const WINDOWS = [{ hours: 1, label: '1 hour', text: 'the last hour' }, { hours: 24, label: '24 hours', text: 'the last 24 hours' }, { hours: 168, label: '7 days', text: 'the last 7 days' }];
const pct = (n: number) => n.toLocaleString(undefined, { maximumFractionDigits: 2 });
/** 45 s · 12 min · 3 h 5 min */
const duration = (ms: number) => {
  if (ms < 60_000) return `${Math.round(ms / 1000)} s`;
  const minutes = Math.round(ms / 60_000); // round once, then split, so 5 h 59.6 min shows as 6 h 0 min, never "5 h 60 min"
  return minutes < 60 ? `${minutes} min` : `${Math.floor(minutes / 60)} h ${minutes % 60} min`;
};
// Plain words for the stored reason codes (UNKNOWN: rows saved before reasons were recorded).
const REASON: Record<string, string> = {
  TIMEOUT: 'Timeout', DNS: 'DNS failure', REFUSED: 'Connection refused', HTTP_404: 'HTTP 404', HTTP_4XX: 'HTTP 4xx', HTTP_5XX: 'HTTP 5xx',
  BAD_PLAYLIST: 'Bad playlist', EMPTY_SEGMENT: 'Empty segment', ENDED: 'Playlist ended', FROZEN: 'Playlist frozen', NETWORK: 'Network error', UNKNOWN: 'Not recorded',
};
const reasonLabel = (reason: string) => REASON[reason] ?? reason;
const TIMELINE_STATES = [
  { tone: 'ok', label: 'No downtime', explanation: 'Green: no downtime' },
  { tone: 'warn', label: 'Some downtime', explanation: 'Amber: some downtime' },
  { tone: 'bad', label: 'Under 99 %', explanation: 'Red: under 99 % available' },
  { tone: 'none', label: 'No checks', explanation: 'Grey: no checks that hour' },
] as const;
const share = (part: number, all: { checks: number }[]) => { const total = all.reduce((sum, r) => sum + r.checks, 0); return total > 0 ? Math.round(part / total * 100) : 0; };
const Dur = ({ ms }: { ms: number | null }) => ms === null ? <span className="muted">—</span> : <span className="fig">{duration(ms)}</span>;
const Latency = ({ s }: { s: { avgLatencyMs: number | null; p95LatencyMs: number | null; p99LatencyMs: number | null } }) => s.avgLatencyMs === null
  ? <span className="muted">n/a</span>
  : <span className="num">{s.avgLatencyMs.toLocaleString()} / {s.p95LatencyMs?.toLocaleString() ?? '—'} / {s.p99LatencyMs?.toLocaleString() ?? '—'}<span className="unit">ms</span></span>;

const HOUR = 3_600_000;
interface Outage { streamId: string; startedAt: string; endedAt: string | null; durationMs: number; offlineChecks: number; reason: string }
interface Details { serverIp: string; outages: Outage[]; timeline: { streamId: string; hours: { hour: string; availabilityPercent: number; downtimeMs: number }[] }[] }
type Names = ReadonlyMap<string, string>;

/** Pipeline hops are streams named "pipeline:<channel>/<hop>"; show "Channel › Hop" with a link to where they are edited. */
const StreamName = ({ id, names }: { id: string; names: Names }) => names.has(id)
  ? <Link to="/create" title={`${id}: edit on the Add Stream page`}>{names.get(id)}</Link>
  : <span className="mono">{id}</span>;

/** One bar per hour of the window: green = fully available, amber = some downtime, red = under 99 %, grey = no checks. */
function Timeline({ rows, hours, to, names }: { rows: Details['timeline']; hours: number; to: string; names: Names }) {
  const end = Date.parse(to);
  const slots: number[] = [];
  for (let t = Math.floor((end - hours * HOUR) / HOUR) * HOUR; t <= end; t += HOUR) slots.push(t);
  return (
    <div className="report-timeline">
      {rows.map(r => {
        const byHour = new Map(r.hours.map(h => [Date.parse(h.hour), h]));
        return (
          <div className="report-timeline-row" key={r.streamId}>
            <StreamName id={r.streamId} names={names} />
            <ol aria-label={`Hourly availability of ${names.get(r.streamId) ?? r.streamId}`}>{slots.map(t => {
              const h = byHour.get(t);
              const tone = !h ? 'none' : h.downtimeMs === 0 ? 'ok' : h.availabilityPercent >= 99 ? 'warn' : 'bad';
              const text = `${stamp.format(new Date(t))}: ${h ? `${pct(h.availabilityPercent)}% available${h.downtimeMs ? `, ${duration(h.downtimeMs)} down` : ''}` : 'no checks'}`;
              return <li key={t} data-tone={tone} title={text} aria-label={text} />;
            })}</ol>
          </div>
        );
      })}
      <p className="report-timeline-legend muted">{TIMELINE_STATES.map(({ tone, label }) => <Fragment key={tone}><i data-tone={tone} />{label}</Fragment>)}</p>
    </div>
  );
}

/** An (i) button that shows a short explanation below it; Escape closes it. */
function InfoTip({ label, children }: { label: string; children: React.ReactNode }) {
  const [shown, setShown] = useState(false);
  const id = useId();
  return <>
    <button className="info-tip" type="button" aria-label={label} aria-expanded={shown} aria-controls={id}
      onClick={() => setShown(s => !s)} onKeyDown={e => { if (e.key === 'Escape') setShown(false); }}>i</button>
    {shown && <div className="info-tip-note" id={id} role="note">{children}</div>}
  </>;
}

function ServerDetail({ details, hours, to, names }: { details: Details | 'loading' | { error: string } | undefined; hours: number; to: string; names: Names }) {
  if (!details || details === 'loading') return <p className="muted">Loading outages and hourly availability…</p>;
  if ('error' in details) return <p className="help help--error" role="alert">Could not load outages: {details.error}</p>;
  return <>
    <div className="with-info"><h3>Hourly availability</h3><InfoTip label="About hourly availability">
      <p>Each stream gets one bar per hour of the window:</p>
      <ul>
        {TIMELINE_STATES.map(({ tone, explanation }) => <li key={tone}><i data-tone={tone} />{explanation}</li>)}
      </ul>
      <p>Hover a bar to see that hour’s availability and downtime. Built from the hourly totals, refreshed every minute.</p>
    </InfoTip></div>
    {details.timeline.length ? <Timeline rows={details.timeline} hours={hours} to={to} names={names} /> : <p className="muted">No hourly totals yet; they are refreshed every minute.</p>}
    <h3>Outages, newest first</h3>
    {!details.outages.length ? <p className="muted">No outages in this window.</p> : (
      <div className="scroll-x"><table className="report-table">
        <thead><tr><th scope="col">Stream</th><th scope="col">Started</th><th scope="col">Ended</th><th scope="col" className="r">Duration</th><th scope="col">Reason</th><th scope="col" className="r">Offline checks</th></tr></thead>
        <tbody>{details.outages.map(o => (
          <tr key={`${o.streamId}:${o.startedAt}`}>
            <th scope="row"><StreamName id={o.streamId} names={names} /></th>
            <td><span className="time">{stamp.format(new Date(o.startedAt))}</span></td>
            <td>{o.endedAt ? <span className="time">{stamp.format(new Date(o.endedAt))}</span> : <span className="state state--offline">Still offline</span>}</td>
            <td className="r"><Dur ms={o.durationMs} /></td>
            <td>{reasonLabel(o.reason)}</td>
            <td className="r"><span className="num">{o.offlineChecks.toLocaleString()}</span></td>
          </tr>
        ))}</tbody>
      </table></div>
    )}
    {details.outages.length >= 100 && <p className="muted">Showing the 100 most recent outages.</p>}
  </>;
}

/** Stored checks per origin server, worst first: which server goes offline most (for root-cause analysis). */
export function Report({ enabled }: { enabled: boolean }) {
  const [hours, setHours] = useState(() => WINDOWS.some(w => w.hours === Number(param('hours'))) ? Number(param('hours')) : 24);
  const [report, setReport] = useState<ReportData | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  // ?server=<host> (from a Dashboard incident) filters to that server and opens it once the report arrives.
  const [search, setSearch] = useState(() => param('server'));
  const autoOpen = useRef(param('server'));
  const request = useRef(0);
  const [open, setOpen] = useState<ReadonlySet<string>>(new Set());
  const [details, setDetails] = useState<Record<string, Details | 'loading' | { error: string }>>({});
  const [names, setNames] = useState<Names>(new Map());
  useEffect(() => {
    api<{ channels: { triggerId: string; channelName: string; nodes: { id: string; name: string }[] }[] }>('/api/pipeline/channels')
      .then(r => setNames(new Map(r.channels.flatMap(c => c.nodes.map(n => [`pipeline:${c.triggerId}/${n.id}`, `${c.channelName} › ${n.name}`] as const)))))
      .catch(() => { /* names are a convenience; raw stream IDs still show */ });
  }, []);
  const label = WINDOWS.find(w => w.hours === hours)!.text;

  const load = useCallback(async () => {
    const current = ++request.current; setLoading(true);
    try { const result = await api<ReportData>(`/api/servers/report?hours=${hours}`); if (current === request.current) { setReport(result); setError(null); } }
    catch (e) { if (current === request.current && !(e instanceof AuthError)) setError((e as Error).message); }
    finally { if (current === request.current) setLoading(false); }
  }, [hours]);
  useEffect(() => {
    setParam('hours', hours === 24 ? '' : String(hours));
    if (!enabled) return;
    void load();
    const timer = setInterval(() => { if (!document.hidden) void load(); }, 60_000);
    return () => { clearInterval(timer); request.current++; };
  }, [enabled, hours, load]);
  const toggle = (ip: string) => {
    const key = `${ip}|${hours}`;
    if (!open.has(ip) && !details[key]) {
      setDetails(d => ({ ...d, [key]: 'loading' }));
      api<Details>(`/api/servers/${encodeURIComponent(ip)}/details?hours=${hours}`)
        .then(r => setDetails(d => ({ ...d, [key]: r })), e => { if (!(e instanceof AuthError)) setDetails(d => ({ ...d, [key]: { error: (e as Error).message } })); });
    }
    setOpen(previous => { const next = new Set(previous); if (!next.delete(ip)) next.add(ip); return next; });
  };

  useEffect(() => {
    const target = autoOpen.current;
    if (!report || !target || !report.servers.some(s => s.serverIp === target)) return;
    autoOpen.current = '';
    if (!open.has(target)) toggle(target);
  }, [report]); // eslint-disable-line react-hooks/exhaustive-deps -- runs once per loaded report
  const shown = report?.servers.filter(s => s.serverIp.toLowerCase().includes(search.toLowerCase().trim())) ?? [];
  const summary = report ? reportSummary(report.servers) : null;
  let body;
  if (!enabled) body = null;
  else if (loading && !report) body = <div className="skel" aria-hidden="true"><span /><span /><span /></div>;
  else if (error) body = <div className="msg msg--error"><h3>Could not load the report</h3><p>{error}. A 7-day report scans a lot of data and can time out; try 24 hours.</p><button className="btn" type="button" onClick={() => void load()}>Try again</button></div>;
  else if (report && !report.servers.length) body = <div className="msg"><h3>No stored checks in {label}</h3><p>Checks are saved in batches every few seconds. If monitoring just started, try again in a minute.</p></div>;
  else if (report && !shown.length) body = <div className="msg"><h3>No matching servers</h3><p>Try a different hostname or IP.</p></div>;
  else if (report) body = (
    <div className="tablewrap"><table className="report-table">
      <caption className="visually-hidden">Availability by server for {label}, least available first</caption>
      <thead><tr>
        <th scope="col">Server</th><th scope="col" className="r">Availability</th><th scope="col" className="r">Downtime</th>
        <th scope="col" className="r">Outages</th><th scope="col" className="r hide-sm" title="Mean time to recover: average outage length">MTTR</th>
        <th scope="col" className="hide-sm">Top cause</th><th scope="col" className="r hide-sm">Streams affected</th><th scope="col" className="r hide-sm" title="Online checks only">Latency avg / p95 / p99</th><th scope="col" className="hide-sm">Last offline</th>
      </tr></thead>
      <tbody>{shown.map((s, index) => {
        const id = `rep-${encodeURIComponent(s.serverIp)}`; const isOpen = open.has(s.serverIp);
        return (
          <Fragment key={s.serverIp}>
            <tr>
              <th scope="row"><span className="report-server-rank">{String(index + 1).padStart(2, '0')}</span><button className="toggle mono" type="button" aria-expanded={isOpen} aria-controls={id} onClick={() => toggle(s.serverIp)}>{s.serverIp}</button></th>
              <td className="r"><span className={`report-availability ${s.downtimeMs > 0 ? 'report-availability--affected' : ''}`}><span><span className="fig">{pct(s.availabilityPercent)}</span><span className="unit">%</span></span><span className="report-availability-track" aria-hidden="true"><i style={{ width: `${Math.max(0, Math.min(100, s.availabilityPercent))}%` }} /></span></span></td>
              <td className="r" title={`Downtime out of ${duration(s.observedMs)} monitored`}><Dur ms={s.downtimeMs} /></td>
              <td className="r"><span className="fig">{s.outages.toLocaleString()}</span></td>
              <td className="r hide-sm"><Dur ms={s.mttrMs} /></td>
              <td className="hide-sm">{s.failureReasons[0] ? <>{reasonLabel(s.failureReasons[0].reason)} <span className="unit">{share(s.failureReasons[0].checks, s.failureReasons)}%</span></> : <span className="muted">—</span>}</td>
              <td className="r hide-sm"><span className="fig">{s.streamsWithOffline.toLocaleString()}</span><span className="unit">of {s.streams.toLocaleString()}</span></td>
              <td className="r hide-sm"><Latency s={s} /></td>
              <td className="hide-sm">{s.lastOfflineAt ? <span className="time">{stamp.format(new Date(s.lastOfflineAt))}</span> : <span className="muted">Never</span>}</td>
            </tr>
            {isOpen && (
              <tr className="detail" id={id}><td colSpan={9}><div className="detail-body">
                {s.failureReasons.length > 0 && <p className="muted">Why it was offline: {s.failureReasons.map(r => `${reasonLabel(r.reason)} ${share(r.checks, s.failureReasons)}%`).join(' · ')}</p>}
                {s.worstStreams.length > 0 && <><h3>Streams on {s.serverIp} with the most downtime</h3>
                <div className="scroll-x"><table className="report-table">
                  <thead><tr><th scope="col">Stream</th><th scope="col" className="r">Availability</th><th scope="col" className="r">Downtime</th><th scope="col" className="r">Outages</th><th scope="col" className="r">MTTR</th><th scope="col" className="r">Latency avg / p95 / p99</th><th scope="col">Last offline</th></tr></thead>
                  <tbody>{s.worstStreams.map(w => (
                    <tr key={w.streamId}>
                      <th scope="row"><StreamName id={w.streamId} names={names} /></th>
                      <td className="r"><span className="fig">{pct(w.availabilityPercent)}</span><span className="unit">%</span></td>
                      <td className="r"><Dur ms={w.downtimeMs} /></td>
                      <td className="r"><span className="num">{w.outages.toLocaleString()}</span></td>
                      <td className="r"><Dur ms={w.mttrMs} /></td>
                      <td className="r"><Latency s={w} /></td>
                      <td>{w.lastOfflineAt ? <span className="time">{stamp.format(new Date(w.lastOfflineAt))}</span> : 'Never'}</td>
                    </tr>
                  ))}</tbody>
                </table></div></>}
                <ServerDetail details={details[`${s.serverIp}|${hours}`]} hours={hours} to={report.to} names={names} />
              </div></td></tr>
            )}
          </Fragment>
        );
      })}</tbody>
    </table></div>
  );

  return (
    <section id="report" className="reports-page" aria-labelledby="report-h">
      <div className="reports-page-head"><div><p className="hc-eyebrow">Analytics / historical availability</p><h2 id="report-h">Reports</h2><p>Understand downtime, compare servers and trace the checks behind each outage.</p></div><span className={`hc-feed ${report && !error ? 'hc-feed--live' : ''}`}><i aria-hidden="true" />{loading ? 'Updating report…' : report ? `Updated ${clock.format(new Date(report.to))}` : 'Waiting for stored checks'}</span></div>
      <div className="reports-toolbar"><div><p className="hc-eyebrow">Reporting window</p><div className="hc-view-switch" role="group" aria-label="Report window">{WINDOWS.map(w => <button key={w.hours} type="button" aria-pressed={w.hours === hours} onClick={() => { if (hours !== w.hours) { request.current++; setReport(null); setError(null); setLoading(true); setOpen(new Set()); setHours(w.hours); } }}>{w.label}</button>)}</div></div><p className="reports-range">{report ? <>{stamp.format(new Date(report.from))}<span aria-hidden="true"> → </span>{stamp.format(new Date(report.to))}</> : `Report for ${label}`}</p>{enabled && <button className="btn" type="button" disabled={loading} onClick={() => void load()}>{loading ? 'Refreshing…' : 'Refresh report'}</button>}</div>
      <div className="report-metrics" aria-label="Report summary">
        <div><span>Observed availability</span><strong data-tone={summary?.downtimeMs ? 'warn' : 'online'}>{summary?.availability != null ? `${pct(summary.availability)}%` : '—'}</strong><small>Weighted by monitored stream time</small></div>
        <div><span>Servers in report</span><strong>{summary?.servers ?? '—'}</strong><small>{summary ? `${summary.affected} with recorded downtime` : 'Waiting for report data'}</small></div>
        <div><span>Total stream downtime</span><strong data-tone={summary?.downtimeMs ? 'offline' : 'neutral'}>{summary ? duration(summary.downtimeMs) : '—'}</strong><small>Summed across monitored streams</small></div>
        <div><span>Outages recorded</span><strong data-tone={summary?.outages ? 'offline' : 'neutral'}>{summary?.outages.toLocaleString() ?? '—'}</strong><small>Online → offline transitions</small></div>
      </div>
      <div className="panel reports-results"><div className="reports-results-head"><div><h3>Availability by server</h3><p>Least available first. Select a server to inspect its streams.</p></div><div className="hc-search"><svg width="16" height="16" viewBox="0 0 20 20" fill="none" stroke="currentColor" aria-hidden="true"><circle cx="8" cy="8" r="5" /><path d="m12 12 5 5" /></svg><label className="visually-hidden" htmlFor="report-server-search">Find server in report</label><input id="report-server-search" type="search" placeholder="Search server or IP…" value={search} onChange={e => setSearch(e.target.value)} /></div></div>{body}<div className="reports-results-footer"><span>{report ? `${shown.length} of ${report.servers.length} servers` : 'Loading server report'}</span><span>Hourly totals · automatic refresh every minute</span></div></div>
      <details className="reports-method"><summary>How to read this report</summary><p>The reporting window starts at a whole hour. Availability is weighted by observed stream time; gaps in monitoring are not counted as healthy. Total stream downtime adds time across streams and can exceed the selected wall-clock window. A stream becomes offline after two failed checks in a row. Latency uses online checks only; p95/p99 are approximate. MTTR is the average recorded outage length.</p></details>

    </section>
  );
}
