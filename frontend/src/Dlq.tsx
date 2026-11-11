import { Fragment, useCallback, useEffect, useMemo, useState } from 'react';
import { api, ApiError, AuthError, stamp } from './api';
import { toast } from './toast';
import { useFailures } from './lib/failures-context';
import type { HlsFailure } from './lib/useHlsFailures';

const date = (value: string) => Number.isFinite(Date.parse(value)) ? stamp.format(new Date(value)) : 'Unknown';
const RESOLUTION: Record<HlsFailure['resolution'], string> = { OPEN: 'Open', REPLAY_REQUESTED: 'Replay requested', RESOLVED: 'Resolved' };
const STAGES = ['PARSE', 'HEALTH_CHECK', 'QUEUE', 'PERSISTENCE'] as const;
const STAGE_LABELS: Record<string, string> = { PARSE: 'Parsing', HEALTH_CHECK: 'Health check', QUEUE: 'Queue', PERSISTENCE: 'Persistence' };
const tone = (r: HlsFailure['resolution']) => r === 'RESOLVED' ? 'online' : r === 'OPEN' ? 'offline' : 'stale';
interface Summary { total: number; byResolution: Record<string, number>; openByStage: Record<string, number> }
interface Group { key: string; items: HlsFailure[]; latest: HlsFailure }

/**
 * Dead-letter failures. Counts come from the whole history (/api/failures/summary). The list merges the live feed
 * with server-side search pages (/api/failures), so older failures are reachable with "Load older".
 */
export function Dlq() {
  const live = useFailures();
  const [seenConnection, setSeenConnection] = useState(false);
  const [text, setText] = useState('');
  const [q, setQ] = useState('');
  const [resolution, setResolution] = useState('OPEN');
  const [stage, setStage] = useState('');
  const [grouped, setGrouped] = useState(true);
  const [openGroup, setOpenGroup] = useState('');
  const [selected, setSelected] = useState('');
  const [summary, setSummary] = useState<Summary | null>(null);
  const [pages, setPages] = useState<HlsFailure[]>([]);
  const [next, setNext] = useState<string | undefined>();
  const [listLoading, setListLoading] = useState(true);
  const [listError, setListError] = useState('');
  const [loadingOlder, setLoadingOlder] = useState(false);
  const [confirmReplay, setConfirmReplay] = useState(false);
  const [replaying, setReplaying] = useState(false);
  useEffect(() => { if (live.connected) setSeenConnection(true); }, [live.connected]);
  useEffect(() => { const t = setTimeout(() => setQ(text.trim()), 300); return () => clearTimeout(t); }, [text]); // search as you type, one request per pause

  const loadSummary = useCallback(() => { void api<Summary>('/api/failures/summary').then(setSummary, () => { /* counts are optional; the list still works */ }); }, []);
  useEffect(() => {
    loadSummary();
    const timer = setInterval(() => { if (!document.hidden) loadSummary(); }, 30_000);
    return () => clearInterval(timer);
  }, [loadSummary]);

  const fetchPage = useCallback((before?: string) => {
    const params = new URLSearchParams({ limit: '100' });
    if (resolution) params.set('resolution', resolution);
    if (stage) params.set('stage', stage);
    if (q) params.set('q', q);
    if (before) params.set('before', before);
    return api<{ items: HlsFailure[]; next?: string }>(`/api/failures?${params}`);
  }, [resolution, stage, q]);
  useEffect(() => {
    let current = true;
    setPages([]); setNext(undefined); setListLoading(true); setListError('');
    fetchPage().then(r => { if (current) { setPages(r.items); setNext(r.next); setListLoading(false); } }, e => { if (current) { setListLoading(false); if (!(e instanceof AuthError)) { setListError((e as Error).message); toast(`Could not search failures: ${(e as Error).message}`, true); } } });
    return () => { current = false; };
  }, [fetchPage]);
  async function loadOlder() {
    if (!next) return;
    setLoadingOlder(true);
    try { const r = await fetchPage(next); setPages(p => [...p, ...r.items]); setNext(r.next); }
    catch (e) { if (!(e instanceof AuthError)) toast(`Could not load older failures: ${(e as Error).message}`, true); }
    finally { setLoadingOlder(false); }
  }

  // Live feed and searched pages, one entry per failure; the newer revision wins.
  const failures = useMemo(() => {
    const byId = new Map<string, HlsFailure>();
    for (const f of [...pages, ...Object.values(live.failures)]) { const seen = byId.get(f.failureId); if (!seen || seen.revision <= f.revision) byId.set(f.failureId, f); }
    return [...byId.values()].sort((a, b) => Date.parse(b.lastFailedAt) - Date.parse(a.lastFailedAt));
  }, [pages, live.failures]);
  const needle = q.toLowerCase();
  const shown = failures.filter(f => (!resolution || f.resolution === resolution) && (!stage || f.failureStage === stage)
    && (!needle || [f.serverIp, f.streamId, f.errorCode, f.errorMessage, f.failureId].some(v => v?.toLowerCase().includes(needle))));
  const groups = useMemo(() => {
    const map = new Map<string, Group>();
    for (const f of shown) {
      const key = JSON.stringify([f.serverIp, f.streamId ?? '', f.failureStage, f.errorCode]);
      const g = map.get(key);
      if (g) g.items.push(f); else map.set(key, { key, items: [f], latest: f });
    }
    return [...map.values()];
  }, [shown]);
  const failure = failures.find(f => f.failureId === selected);
  useEffect(() => setConfirmReplay(false), [selected]);

  async function replay(f: HlsFailure) {
    setReplaying(true);
    try {
      await api(`/api/failures/${encodeURIComponent(f.failureId)}/replay`, { method: 'POST' });
      toast(`Replay requested for ${f.streamId ?? f.failureId}. It is processed again within about a minute.`);
      setConfirmReplay(false); loadSummary();
    } catch (e) {
      if (!(e instanceof AuthError)) toast(e instanceof ApiError && e.status === 409 ? e.message : `Could not replay: ${(e as Error).message}`, true);
    } finally { setReplaying(false); }
  }

  const count = (r: string) => summary ? (summary.byResolution[r] ?? 0).toLocaleString() : '—';
  const row = (f: HlsFailure, nested = false) => <tr key={f.failureId} className={nested ? 'dlq-nested' : undefined}>
    <td><span className="mono">{f.serverIp}</span><p className="muted">{f.streamId ?? 'Unknown stream'}</p></td>
    <td><span className="dlq-stage-tag">{STAGE_LABELS[f.failureStage] ?? f.failureStage}</span><p className="dlq-error-code">{f.errorCode}</p></td>
    <td><span className="num">{f.attempts}</span><small className="dlq-volume-label">attempts</small></td>
    <td><span className={`state state--${tone(f.resolution)}`}>{RESOLUTION[f.resolution] ?? f.resolution}</span></td>
    <td>{date(f.lastFailedAt)}</td>
    <td><button className="btn btn--sm" type="button" aria-expanded={selected === f.failureId} onClick={() => setSelected(selected === f.failureId ? '' : f.failureId)}>Details</button></td>
  </tr>;

  return <section className="dlq-page" aria-labelledby="dlq-heading">
    <div className="dlq-page-head"><div><p className="hc-eyebrow">Operations / failure recovery</p><h2 id="dlq-heading">Dead-letter queue</h2><p>Inspect failed messages, trace their cause and replay them when you’re ready.</p></div><div className="dlq-feed-controls"><p className={`hc-feed ${live.connected ? 'hc-feed--live' : ''}`} role="status"><i aria-hidden="true" />{live.connected ? 'Live failure feed' : seenConnection ? 'Reconnecting to feed…' : 'Connecting to feed…'}</p><button className="btn" type="button" onClick={live.reconnect}>Reconnect</button></div></div>
    <div className="dlq-status-cards" aria-label="Failure counts, whole history">{Object.entries(RESOLUTION).map(([value, label], i) => <button key={value} className={`panel dlq-status-card dlq-status-card--${tone(value as HlsFailure['resolution'])}`} type="button" aria-pressed={resolution === value} onClick={() => setResolution(resolution === value ? '' : value)}><span className="dlq-card-heading"><span className="dlq-card-dot" aria-hidden="true" />{label}<span className="dlq-card-index">0{i + 1}</span></span><strong>{count(value)}</strong><span className="dlq-card-description">{value === 'OPEN' ? 'Failures awaiting investigation' : value === 'REPLAY_REQUESTED' ? 'Queued for another processing attempt' : 'Recovered messages · retained 30 days'}</span><span className="dlq-card-action">{resolution === value ? 'Showing this status' : 'View failures'} <span aria-hidden="true">↗</span></span></button>)}</div>
    <div className="dlq-stage-overview"><span>Open by stage</span><div>{STAGES.map(value => <button type="button" key={value} aria-pressed={stage === value} onClick={() => setStage(stage === value ? '' : value)}>{STAGE_LABELS[value]}<b>{summary ? summary.openByStage[value] ?? 0 : '—'}</b></button>)}</div><span>Whole history · sync 30s</span></div>
    <div className="dlq-filters">
      <div className="field"><label htmlFor="dlq-search">Search failures</label><input className="input" id="dlq-search" type="search" placeholder="Server, stream, error or failure ID" value={text} onChange={e => setText(e.target.value)} maxLength={200} /></div>
      <div className="field"><label htmlFor="dlq-resolution">Resolution</label><select className="select" id="dlq-resolution" value={resolution} onChange={e => setResolution(e.target.value)}><option value="">All resolutions</option>{Object.entries(RESOLUTION).map(([id, label]) => <option key={id} value={id}>{label}</option>)}</select></div>
      <div className="field"><label htmlFor="dlq-stage">Failure stage</label><select className="select" id="dlq-stage" value={stage} onChange={e => setStage(e.target.value)}><option value="">All stages</option>{STAGES.map(value => <option key={value} value={value}>{STAGE_LABELS[value]}</option>)}</select></div>
      <div className="field field--check"><label className="check" htmlFor="dlq-grouped"><input id="dlq-grouped" type="checkbox" checked={grouped} onChange={e => setGrouped(e.target.checked)} /> Group repeated errors</label></div>
    </div>
    {listError && <p className="hc-notice" role="alert">Archive search unavailable: {listError}. Live records may still appear.</p>}
    <div className="panel dlq-explorer"><div className="dlq-explorer-head"><div><h3>Failure explorer</h3><p>{grouped ? 'Repeated errors are grouped by server, stream, stage and error code.' : 'Individual failures, newest first.'}</p></div><span>{shown.length} failures{grouped ? ` · ${groups.length} groups` : ''}</span></div>
      {listLoading && !shown.length ? <div className="msg"><h3>Loading failure records</h3><p>Reading the archive and listening for live updates.</p></div>
        : !shown.length ? <div className="msg"><h3>{listError ? 'Archive unavailable' : resolution || stage || q ? 'No matching failures' : 'No failures'}</h3><p>{listError ? 'The archive could not be loaded. Live-feed records will appear when available.' : resolution || stage || q ? 'No loaded records match this scope. Try another resolution, stage or search.' : 'No dead-letter records were returned by the archive or live feed.'}</p></div>
        : <div className="tablewrap"><table><caption className="visually-hidden">Dead-letter failures, most recent failure first</caption>
          <thead><tr><th>Server / stream</th><th>Stage / error</th><th>Volume</th><th>Resolution</th><th>Last failure</th><th><span className="visually-hidden">Details</span></th></tr></thead>
          <tbody>{grouped ? groups.map(g => g.items.length === 1 ? row(g.latest) : <Fragment key={g.key}>
            <tr>
              <td><span className="mono">{g.latest.serverIp}</span><p className="muted">{g.latest.streamId ?? 'Unknown stream'}</p></td>
              <td><span className="dlq-stage-tag">{STAGE_LABELS[g.latest.failureStage] ?? g.latest.failureStage}</span><p className="dlq-error-code">{g.latest.errorCode}</p></td>
              <td><span className="num">{g.items.length}</span><small className="dlq-volume-label">failures</small></td>
              <td><span className={`state state--${tone(g.latest.resolution)}`}>{RESOLUTION[g.latest.resolution] ?? g.latest.resolution}</span></td>
              <td>{date(g.latest.lastFailedAt)}</td>
              <td><button className="btn btn--sm" type="button" aria-expanded={openGroup === g.key} onClick={() => setOpenGroup(openGroup === g.key ? '' : g.key)}>{openGroup === g.key ? 'Hide' : `Show ${g.items.length}`}</button></td>
            </tr>
            {openGroup === g.key && g.items.map(f => row(f, true))}
          </Fragment>) : shown.map(f => row(f))}</tbody></table></div>}
      <div className="dlq-explorer-footer"><span>{shown.length} loaded failures in the current scope</span><span>Resolved records expire after 30 days</span></div>
      {next && <div className="dlq-more"><button className="btn" type="button" disabled={loadingOlder} onClick={() => void loadOlder()}>{loadingOlder ? 'Loading…' : 'Load older failures'}</button></div>}
    </div>
    {failure && <div className="panel dlq-details"><div className="incidents-head"><div><p className="hc-eyebrow">Message inspector</p><h3>Failure details</h3><span className={`state state--${tone(failure.resolution)}`}>{RESOLUTION[failure.resolution]}</span></div><button className="btn btn--quiet" type="button" onClick={() => setSelected('')}>Close</button></div><div className="dlq-failure-reason"><span>Failure reason · {failure.errorCode}</span><p className="dlq-error">{failure.errorMessage}</p></div><dl>
      <dt>Failure ID</dt><dd>{failure.failureId}</dd><dt>Original message</dt><dd>{failure.originalMessageId}</dd><dt>Server</dt><dd>{failure.serverIp}</dd><dt>Stream</dt><dd>{failure.streamId ?? 'Unknown'}</dd><dt>Stream URL</dt><dd>{failure.streamUrl ?? 'Unavailable'}</dd><dt>Stage / code</dt><dd>{failure.failureStage} / {failure.errorCode}</dd><dt>Retryable</dt><dd>{failure.retryable ? 'Yes' : 'No'}</dd><dt>Attempts</dt><dd>{failure.attempts}</dd><dt>First failure</dt><dd>{date(failure.firstFailedAt)}</dd><dt>Last failure</dt><dd>{date(failure.lastFailedAt)}</dd><dt>Updated</dt><dd>{date(failure.updatedAt)}</dd><dt>Resolution</dt><dd>{RESOLUTION[failure.resolution]}</dd>
    </dl>
      {failure.resolution === 'OPEN' && (confirmReplay
        ? <div className="row-actions" role="group" aria-label="Confirm replay"><span className="ask">Process this message again? If the cause is fixed it is saved; if not, it fails and returns here.</span><button className="btn btn--primary btn--sm" type="button" autoFocus disabled={replaying} onClick={() => void replay(failure)}>{replaying ? 'Requesting…' : 'Replay message'}</button><button className="btn btn--quiet btn--sm" type="button" onClick={() => setConfirmReplay(false)}>Cancel</button></div>
        : <div className="row-actions"><button className="btn btn--sm" type="button" onClick={() => setConfirmReplay(true)}>Replay…</button></div>)}
    </div>}
  </section>;
}
