import { useMemo, useState } from 'react';
import type { Topology, SavedProbe, ChannelPipelineVerdict } from './Pipeline';
import { srtPingLabel, type SrtPing } from './lib/srt-ping';
import { healthCharts, type HealthBucket, type HealthRow } from './lib/health-charts';
const BUCKETS: { key: HealthBucket; label: string; short: string }[] = [{ key: 'healthy', label: 'Healthy', short: 'Healthy' }, { key: 'degraded', label: 'Backup / fallback active', short: 'Alternate path' }, { key: 'offline', label: 'Offline / affected', short: 'Offline / affected' }, { key: 'unknown', label: 'Unknown / not probed', short: 'Unverified' }];
type View = 'all' | 'servers' | 'channels' | 'roles';
const VIEWS: { id: View; label: string }[] = [{ id: 'all', label: 'All views' }, { id: 'servers', label: 'Servers' }, { id: 'channels', label: 'Channels' }, { id: 'roles', label: 'Hop roles' }];

function ViewIcon({ kind }: { kind: Exclude<View, 'all'> }) {
  return <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" aria-hidden="true">{kind === 'servers' ? <><rect x="3" y="3" width="18" height="7" rx="2" /><rect x="3" y="14" width="18" height="7" rx="2" /><path d="M6 6.5h.01M6 17.5h.01M10 6.5h8M10 17.5h8" /></> : kind === 'channels' ? <><rect x="3" y="5" width="18" height="13" rx="3" /><path d="m10 9 5 3-5 3V9ZM8 21h8" /></> : <><rect x="2" y="9" width="5" height="6" rx="1" /><rect x="17" y="2" width="5" height="6" rx="1" /><rect x="17" y="16" width="5" height="6" rx="1" /><path d="M7 12h5V5h5M12 12v7h5" /></>}</svg>;
}

export function HealthCharts({ channels, probes, verdicts, fresh, srtServers }: { channels: Topology[] | null; probes: SavedProbe[] | null; verdicts: ChannelPipelineVerdict[]; fresh: boolean; srtServers: SrtPing[] }) {
  const [channel, setChannel] = useState('');
  const [view, setView] = useState<View>('all');
  const [serverSearch, setServerSearch] = useState('');
  const [selected, setSelected] = useState<{ kind: Exclude<View, 'all'>; id: string } | null>(null);
  const included = useMemo(() => (channels ?? []).filter(c => !channel || c.triggerId === channel), [channels, channel]);
  const charts = useMemo(() => healthCharts(included, probes ?? [], verdicts, fresh), [included, probes, verdicts, fresh]);
  const totals = BUCKETS.map(bucket => ({ ...bucket, count: charts.roles.reduce((sum, role) => sum + role[bucket.key], 0) }));
  const pingFor = (host: string) => srtServers.find(p => p.host.replace(/^\[|\]$/g, '').replace(/\.$/, '').toLowerCase() === host.toLowerCase());
  const detail = selected ? charts[selected.kind].find(row => row.id === selected.id) : null;
  const choose = (kind: Exclude<View, 'all'>, row: HealthRow) => setSelected(selected?.kind === kind && selected.id === row.id ? null : { kind, id: row.id });
  return <section className="dashboard-charts hc-section" aria-labelledby="health-charts-heading">
    <div className="hc-section-head">
      <div><p className="hc-eyebrow">Infrastructure / live snapshot</p><h3 id="health-charts-heading">Health charts</h3><p className="hc-subtitle">Follow the signal. Find the server, channel or hop that needs attention.</p></div>
      <div className="hc-channel-filter"><label htmlFor="chart-channel">Channel scope</label><select className="select" id="chart-channel" value={channel} onChange={e => { setChannel(e.target.value); setSelected(null); }}><option value="">All channels</option>{channels?.map(c => <option key={c.triggerId} value={c.triggerId}>{c.channelName}</option>)}</select></div>
    </div>
    <div className="hc-control-bar"><div className="hc-view-switch" role="group" aria-label="Chart view">{VIEWS.map(item => <button type="button" key={item.id} aria-pressed={view === item.id} onClick={() => setView(item.id)}>{item.label}</button>)}</div><span className={`hc-feed ${fresh ? 'hc-feed--live' : ''}`}><i aria-hidden="true" />{fresh ? 'Current probe results' : 'Awaiting fresh results'}</span></div>
    <div className="hc-totals" aria-label="Hop health counts">{totals.map(bucket => <div className={`hc-total hc-total--${bucket.key}`} key={bucket.key}><span><i className={`chart-color chart-color--${bucket.key}`} aria-hidden="true" />{bucket.short}</span><strong>{channels === null ? '—' : bucket.count}</strong><small>hops</small></div>)}</div>
    {!fresh && <p className="hc-notice" role="status">Probe results are pending, stale or unavailable. Hops remain unverified until fresh data arrives.</p>}
    <div className={`hc-grid ${view !== 'all' ? 'hc-grid--focused' : ''}`}>
      {(view === 'all' || view === 'servers') && <Chart kind="servers" title="Server health" subtitle="Grouped by server IP or domain" rows={charts.servers.filter(row => row.label.toLowerCase().includes(serverSearch.toLowerCase().trim()))} loading={channels === null || probes === null} selected={selected?.kind === 'servers' ? selected.id : ''} onSelect={row => choose('servers', row)} pingFor={pingFor}><div className="hc-search"><svg width="16" height="16" viewBox="0 0 20 20" fill="none" stroke="currentColor" aria-hidden="true"><circle cx="8" cy="8" r="5" /><path d="m12 12 5 5" /></svg><label className="visually-hidden" htmlFor="chart-server">Find server IP or domain</label><input id="chart-server" type="search" placeholder="Search server or IP…" value={serverSearch} onChange={e => setServerSearch(e.target.value)} /></div></Chart>}
      {(view === 'all' || view === 'channels') && <Chart kind="channels" title="Channel health" subtitle="Every channel, from ingest to final output" rows={charts.channels} loading={channels === null} selected={selected?.kind === 'channels' ? selected.id : ''} onSelect={row => choose('channels', row)} />}
      {(view === 'all' || view === 'roles') && <Chart kind="roles" title="Processing roles" subtitle="Main, backup and every hop in between" rows={charts.roles} loading={channels === null} selected={selected?.kind === 'roles' ? selected.id : ''} onSelect={row => choose('roles', row)} />}
    </div>
    <div className="hc-footnote"><span>Click any chart row to inspect its hops.</span><span>Hop checks · not server CPU or uptime history</span></div>
    {detail && <div className="panel hc-details" id="health-chart-details"><div className="hc-details-head"><div><p className="hc-eyebrow">Hop inspection</p><h3>{detail.label}</h3><span className="muted">{detail.total} {detail.total === 1 ? 'hop' : 'hops'} in this group</span></div><button className="btn btn--quiet" type="button" onClick={() => setSelected(null)}>Close details</button></div><div className="tablewrap"><table><caption className="visually-hidden">Hop health details for {detail.label}</caption><thead><tr><th>Channel</th><th>Hop / role</th><th>Server</th><th>Stream health</th><th>SRT server ping</th></tr></thead><tbody>{detail.hops.map(hop => <tr key={hop.id}><td>{hop.channel}</td><td>{hop.name}<p className="muted">{hop.role}</p></td><td className="mono">{hop.server}</td><td><span className={`hc-status hc-status--${hop.state}`}><i aria-hidden="true" />{hop.srt && hop.state === 'unknown' ? 'SRT stream not monitored' : BUCKETS.find(b => b.key === hop.state)?.label}</span></td><td>{hop.srt ? srtPingLabel(pingFor(hop.server)) : '—'}</td></tr>)}</tbody></table></div></div>}
  </section>;
}

function Chart({ kind, title, subtitle, rows, loading, selected, onSelect, children, pingFor }: { kind: Exclude<View, 'all'>; title: string; subtitle: string; rows: HealthRow[]; loading: boolean; selected: string; onSelect: (row: HealthRow) => void; children?: React.ReactNode; pingFor?: (host: string) => SrtPing | undefined }) {
  const maximum = Math.max(1, ...rows.map(row => row.total));
  const total = rows.reduce((sum, row) => sum + row.total, 0);
  return <div className={`panel hc-panel hc-panel--${kind}`}>
    <div className="hc-panel-head"><span className="hc-panel-icon"><ViewIcon kind={kind} /></span><div><h4>{title}</h4><p>{subtitle}</p></div><span className="hc-panel-count">{loading ? '—' : rows.filter(r => r.total > 0).length}<small>{kind === 'servers' ? 'servers' : kind === 'channels' ? 'channels' : 'roles'}</small></span></div>
    {children && <div className="hc-panel-search">{children}</div>}
    {loading ? <div className="hc-empty"><span className="hc-loading" aria-hidden="true" /><p>Waiting for chart data…</p></div> : !rows.length ? <div className="hc-empty"><ViewIcon kind={kind} /><p>No matching {kind === 'roles' ? 'hops' : kind}.</p></div> : <>
      <div className="hc-axis"><span>{kind === 'channels' ? 'Healthy share / hop breakdown' : 'Hop distribution'}</span><span>0 — {maximum}</span></div>
      <div className="hc-rows">{rows.map(row => {
        const percent = row.total ? Math.round(row.healthy / row.total * 100) : 0;
        const ping = pingFor && row.hops.some(h => h.srt) ? pingFor(row.label) : undefined;
        return <button type="button" className={`hc-row ${selected === row.id ? 'hc-row--selected' : ''}`} key={row.id} onClick={() => onSelect(row)} aria-expanded={selected === row.id} aria-controls="health-chart-details" aria-label={`${row.label}: ${row.total} hops, ${row.healthy} healthy, ${row.degraded} backup or fallback active, ${row.offline} offline or affected, ${row.unknown} unverified. Show details.`} disabled={!row.total}>
          <span className="hc-row-top">{kind === 'channels' && <svg viewBox="0 0 40 40" className="hc-ring" aria-hidden="true"><circle cx="20" cy="20" r="16" className="hc-ring-track" /><circle cx="20" cy="20" r="16" className="hc-ring-value" pathLength="100" strokeDasharray={`${percent} 100`} /><text x="20" y="23" textAnchor="middle">{percent}</text></svg>}<span className="hc-row-name">{row.label}{pingFor && row.hops.some(h => h.srt) && <small className="hc-ping">{srtPingLabel(ping)}</small>}</span><span className="hc-row-number">{row.total}<small>{row.total === 1 ? 'hop' : 'hops'}</small></span><span className="hc-row-arrow" aria-hidden="true">↗</span></span>
          <span className="hc-bar" aria-hidden="true">{BUCKETS.map(bucket => <span key={bucket.key} className={`chart-color--${bucket.key}`} style={{ width: `${100 * row[bucket.key] / maximum}%` }} />)}</span>
          <span className="hc-row-breakdown">{BUCKETS.filter(b => row[b.key] > 0).map(bucket => <span key={bucket.key}><i className={`chart-color chart-color--${bucket.key}`} aria-hidden="true" />{row[bucket.key]}<span>{bucket.short}</span></span>)}{!row.total && <span>No configured hops</span>}</span>
        </button>;
      })}</div>
      <div className="hc-panel-footer"><span>{total} hop checks</span><span>{rows.filter(r => r.offline > 0).length} groups affected</span></div>
    </>}
  </div>;
}
