import { useCallback, useEffect, useState } from 'react';
import { api, ApiError, AuthError, clock } from './api';
import { toast } from './toast';
import { PipelineCanvas } from './PipelineCanvas';
import { Link } from 'react-router-dom';
import { signalPaths, type SignalHop } from './lib/signal-paths';
import { serverDomain } from './lib/workspace-graph';

type HopStatus = 'HEALTHY' | 'CONVICT' | 'CASCADED' | 'FAILOVER_ACTIVE' | 'FALLBACK_ACTIVE' | 'STANDBY' | 'UNPROBED';
type ChannelState = 'OPTIMAL' | 'DEGRADED_FAILOVER' | 'ON_FALLBACK' | 'CRITICAL_CONVICT' | 'OFFLINE' | 'UNVERIFIED';
export interface HopEvaluation { nodeId: string; nodeName: string; nodeType: string; isLive: boolean | null; status: HopStatus; convictNodeId?: string; details?: string }
export interface ChannelPipelineVerdict {
  triggerId: string; channelName: string; overallState: ChannelState; rootCauseSummary: string | null;
  convictNodeIds: string[]; hopEvaluations: HopEvaluation[]; alertSuppression: { suppressedCount: number; alertRecommended: boolean };
}
export interface Status { evaluatedAt: string | null; intervalMs: number; pending: boolean; channels: ChannelPipelineVerdict[] }
export interface Incident { key: string; kind: 'CONVICT' | 'OFFLINE' | 'ON_FALLBACK'; title: string; channels: string[]; openedAt: string; resolvedAt?: string | null }

// Every state is a word as well as a colour.
const CHANNEL: Record<ChannelState, { text: string; tone: string }> = {
  OPTIMAL: { text: 'OK', tone: 'online' },
  DEGRADED_FAILOVER: { text: 'On backup', tone: 'warn' },
  ON_FALLBACK: { text: 'On VOD fallback', tone: 'offline' },
  CRITICAL_CONVICT: { text: 'Down', tone: 'offline' },
  OFFLINE: { text: 'Down, cause unknown', tone: 'offline' },
  UNVERIFIED: { text: 'Not probed', tone: 'stale' },
};
const HOP: Record<HopStatus, { text: string; tone: string }> = {
  HEALTHY: { text: 'Live', tone: 'online' },
  CONVICT: { text: 'Root cause', tone: 'offline' },
  CASCADED: { text: 'Affected', tone: 'stale' },
  FAILOVER_ACTIVE: { text: 'On backup', tone: 'warn' },
  FALLBACK_ACTIVE: { text: 'On fallback', tone: 'warn' },
  STANDBY: { text: 'Backup down', tone: 'stale' },
  UNPROBED: { text: 'Not probed', tone: 'stale' },
};
const FILTERS = [
  { id: 'attention', label: 'Needs attention', match: (c: ChannelPipelineVerdict) => c.alertSuppression.alertRecommended },
  { id: 'backup', label: 'On backup', match: (c: ChannelPipelineVerdict) => c.overallState === 'DEGRADED_FAILOVER' },
  { id: 'all', label: 'All', match: () => true },
] as const;

/** Each channel's path from ingest to CDN. The first hop down is the root cause; hops below it are "affected", not alerted. */
export function Pipeline() {
  const [status, setStatus] = useState<Status | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [filter, setFilter] = useState<(typeof FILTERS)[number]['id']>('all');
  const [topology, setTopology] = useState<Topology[] | null>(null);

  const load = useCallback(async () => {
    const [health, definitions] = await Promise.allSettled([api<Status>('/api/pipeline/status'), api<{ channels: Topology[] }>('/api/pipeline/channels')]);
    if (health.status === 'fulfilled') { setStatus(health.value); setError(null); }
    else if (!(health.reason instanceof AuthError)) setError((health.reason as Error).message);
    if (definitions.status === 'fulfilled') setTopology(definitions.value.channels);
  }, []);
  useEffect(() => {
    void load();
    const timer = setInterval(() => { if (!document.hidden) void load(); }, 10_000);
    return () => clearInterval(timer);
  }, [load]);

  const channels = signalPaths(status?.pending ? [] : status?.channels ?? [], topology);
  const shown = channels.filter(FILTERS.find(f => f.id === filter)!.match);

  let body;
  if (error && !status && !channels.length) body = <div className="msg msg--error"><h3>Could not load the pipeline</h3><p>{error}. It retries every 10 seconds.</p></div>;
  else if ((!status || status.pending) && !channels.length) body = <div className="msg"><h3>First probe round is running</h3><p>Results appear within {Math.round((status?.intervalMs ?? 10_000) / 1000)} seconds.</p></div>;
  else if (!channels.length) body = <div className="msg"><h3>No channels yet</h3><p>Add a channel and its hops in Channels and probe targets below. It is checked from the next round.</p></div>;
  else if (!shown.length) body = filter === 'attention' ? null : <div className="msg"><h3>No channels here</h3><p>Choose another filter.</p></div>;
  else body = (
    <ul className="pipes">{shown.map(c => (
      <li key={c.triggerId}>
        <div className="pipe-head">
          <h3>{c.channelName}</h3>
          <span className={`state state--${CHANNEL[c.overallState].tone}`}>{CHANNEL[c.overallState].text}</span>
          {c.alertSuppression.suppressedCount > 0 && <span className="muted">{c.alertSuppression.suppressedCount} downstream hop{c.alertSuppression.suppressedCount === 1 ? '' : 's'} not alerted</span>}
        </div>
        {c.rootCauseSummary && <p className="pipe-cause">{c.rootCauseSummary}</p>}
        <div className="signal-nodes-summary"><span>{c.hopEvaluations.length} nodes</span><span>{c.hopEvaluations.filter(h => h.backup).length} backup</span><span>{c.hopEvaluations.filter(h => h.fallback).length} fallback</span></div>
        <div className="signal-lane"><p className="signal-lane-label">Main / processing nodes</p><ul className="hops signal-all-hops" aria-label={`${c.channelName} main and processing nodes`}>{c.hopEvaluations.filter(h => !h.backup && !h.fallback).map(h => <SignalNode key={h.nodeId} hop={h} />)}</ul></div>
        {c.hopEvaluations.some(h => h.backup || h.fallback) && <div className="signal-lane signal-lane--alternate"><p className="signal-lane-label">Backup / fallback nodes</p><ul className="hops signal-all-hops" aria-label={`${c.channelName} backup and fallback nodes`}>{c.hopEvaluations.filter(h => h.backup || h.fallback).map(h => <SignalNode key={h.nodeId} hop={h} />)}</ul></div>}

      </li>
    ))}</ul>
  );

  return (
    <section id="pipeline" className="pipeline-page" aria-labelledby="pipeline-h">
      <div className="pipeline-page-head">
        <div><p className="hc-eyebrow">Operations / signal management</p><h2 id="pipeline-h">Add Stream</h2><p className="pipeline-intro">Trace every hop. Configure your channels. Find the first point of failure.</p></div>
        <Link className="btn" to="/workspace">Open Workspace <span aria-hidden="true">↗</span></Link>
      </div>
      <div className="pipeline-overview" aria-label="Latest pipeline summary">
        {[['Channels evaluated', status && !status.pending ? channels.length : '—', 'neutral'], ['Healthy primary', status && !status.pending ? channels.filter(c => c.overallState === 'OPTIMAL').length : '—', 'online'], ['Need attention', status && !status.pending ? channels.filter(c => c.alertSuppression.alertRecommended).length : '—', 'offline'], ['On backup', status && !status.pending ? channels.filter(c => c.overallState === 'DEGRADED_FAILOVER').length : '—', 'warn']].map(([label, value, tone]) => <div key={label}><span>{label}</span><strong data-tone={tone}>{value}</strong></div>)}
      </div>
      <div className="pipeline-live-toolbar">
        <div className="hc-view-switch" role="group" aria-label="Show channels">{FILTERS.map(f => <button key={f.id} type="button" aria-pressed={filter === f.id} onClick={() => setFilter(f.id)}>{f.label}<span className="pipeline-filter-count">{channels.filter(f.match).length}</span></button>)}</div>
        <p className={`hc-feed ${status?.evaluatedAt && !error && Date.now() - Date.parse(status.evaluatedAt) < 60_000 ? 'hc-feed--live' : ''}`}><i aria-hidden="true" />{status?.evaluatedAt ? `Last probe ${clock.format(new Date(status.evaluatedAt))}` : 'Waiting for first probe'} · 10s refresh</p>
      </div>
      {error && status && <p className="hc-notice" role="alert">Live status refresh failed. Showing the last loaded results.</p>}
      {body && <div className="panel pipeline-live-panel"><div className="pipeline-panel-label"><span>Signal paths · all configured nodes</span><span>Root cause first · downstream alerts suppressed</span></div>{body}</div>}

      <Channels onChanged={() => void load()} />


    </section>
  );
}

function SignalNode({ hop }: { hop: SignalHop }) {
  return <li title={hop.details} className={hop.backup || hop.fallback ? 'signal-node--alternate' : ''}>
    <span className="signal-node-role">{hop.fallback ? 'VOD fallback' : hop.backup ? 'Backup' : hop.nodeType === 'INGEST' ? 'Main' : hop.nodeType.replaceAll('_', ' ')}</span>
    <span className="hop-name">{hop.nodeName}</span>
    {hop.host && <span className="signal-node-host">{hop.host}</span>}
    <span className={`state state--${HOP[hop.status].tone}`}>{HOP[hop.status].text}</span>
    {hop.feeds.length > 0 && <span className="signal-node-links">{hop.feeds.map((feed, i) => <span key={i}>→ {feed.name}<small>{feed.type}</small></span>)}</span>}
  </li>;
}

export type NodeType = 'INGEST' | 'SCTE_RELAY' | 'TRANSCODER' | 'EGRESS_CDN' | 'FALLBACK_SOURCE';
export type EdgeType = 'PRIMARY' | 'FAILOVER' | 'BYPASS' | 'FALLBACK';
export interface TopologyNode { id: string; name: string; type: NodeType; host?: string; port?: number; isBackup?: boolean; transport?: 'srt'; probe?: 'hls' | 'tcp' | 'none' }
export interface Topology { triggerId: string; channelName: string; nodes: TopologyNode[]; edges: { fromNodeId: string; toNodeId: string; type: EdgeType }[] }
export interface SavedProbe { triggerId: string; nodeId: string; kind: 'hls' | 'tcp'; hls?: string; tcp?: string }
type Kind = 'none' | 'hls' | 'tcp';

/** Channels (each one's hops from ingest to CDN) and how each hop is checked. Changes apply from the next round. */
function Channels({ onChanged }: { onChanged: () => void }) {
  const [channels, setChannels] = useState<Topology[] | null>(null);
  const [saved, setSaved] = useState<Map<string, SavedProbe>>(new Map());
  const [error, setError] = useState<string | null>(null);
  const [channelId, setChannelId] = useState('');
  const [editing, setEditing] = useState<Topology | 'new' | null>(null);
  const [confirming, setConfirming] = useState(false);

  const load = useCallback(async () => {
    try {
      const [topology, probes] = await Promise.all([api<{ channels: Topology[] }>('/api/pipeline/channels'), api<{ probes: SavedProbe[] }>('/api/pipeline/probes')]);
      setChannels(topology.channels);
      setSaved(new Map(probes.probes.map(p => [`${p.triggerId}/${p.nodeId}`, p])));
      setError(null);
    } catch (e) { if (!(e instanceof AuthError)) setError((e as Error).message); }
  }, []);
  useEffect(() => { void load(); }, [load]);

  const channel = channels?.find(c => c.triggerId === channelId) ?? channels?.[0];
  const count = (c: Topology) => c.nodes.filter(n => saved.has(`${c.triggerId}/${n.id}`)).length;

  async function remove(c: Topology) {
    try {
      await api(`/api/pipeline/channels/${encodeURIComponent(c.triggerId)}`, { method: 'DELETE' });
      setConfirming(false); setChannelId(''); toast(`Deleted ${c.channelName}.`);
      await load(); onChanged();
    } catch (e) { if (!(e instanceof AuthError)) toast(`Could not delete: ${(e as Error).message}`, true); }
  }

  if (editing) return (
    <PipelineCanvas probes={saved} channel={editing === 'new' ? null : editing} onCancel={() => { setEditing(null); void load(); onChanged(); }}
      onSaved={async saved => { setEditing(null); setChannelId(saved.triggerId); toast(`Saved ${saved.channelName}. It is checked from the next round.`); await load(); onChanged(); }} />
  );

  return (
    <div className="panel probes pipeline-config">
      <div className="incidents-head">
        <div><p className="hc-eyebrow">Configuration / channel library</p><h3>Channels &amp; probe targets</h3><p className="pipeline-config-intro">Build the signal path on canvas, then choose how each hop is checked.</p></div>
        <button className="btn btn--primary" type="button" onClick={() => setEditing('new')}><span aria-hidden="true">＋ </span>Add channel</button>
      </div>
      {error && !channels ? <p className="help help--error probes-pad" role="alert">Could not load channels: {error}</p>
        : !channels ? <p className="muted probes-pad">Loading…</p>
        : !channel ? <div className="msg"><h3>No channels yet</h3><p>Add a channel with its hops in order, from ingest to the CDN viewers watch. Then say how each hop is checked.</p></div>
        : (
        <>
          <div className="pipeline-channel-summary"><div><span className="pipeline-channel-mark" aria-hidden="true">◈</span><div><strong>{channel.channelName}</strong><p>{channel.nodes.length} hops <span aria-hidden="true">·</span> {count(channel)} probe targets configured</p></div></div><span className="pipeline-config-count">{channels.length} {channels.length === 1 ? 'channel' : 'channels'} in library</span></div>
          <div className="channel-bar probes-pad">
            <div className="field">
              <label htmlFor="probe-channel">Channel</label>
              <select className="select" id="probe-channel" value={channel.triggerId} onChange={e => { setChannelId(e.target.value); setConfirming(false); }}>
                {channels.map(c => <option key={c.triggerId} value={c.triggerId}>{c.channelName} ({count(c)} of {c.nodes.length} probed)</option>)}
              </select>
            </div>
            {confirming ? (
              <div className="row-actions">
                <span className="ask">Delete {channel.channelName} and its probe targets?</span>
                <button className="btn btn--danger btn--sm" type="button" autoFocus onClick={e => { e.currentTarget.disabled = true; void remove(channel); }}>Delete channel</button>
                <button className="btn btn--quiet btn--sm" type="button" onClick={() => setConfirming(false)}>Cancel</button>
              </div>
            ) : (
              <div className="row-actions">
                <button className="btn btn--sm" type="button" onClick={() => setEditing(channel)}>Edit on canvas</button>
                <button className="btn btn--quiet btn--sm" type="button" onClick={() => setConfirming(true)}>Delete</button>
              </div>
            )}
          </div>
          <div className="tablewrap pipeline-probe-table"><table>
            <caption className="visually-hidden">Probe targets for {channel.channelName}, from ingest to CDN</caption>
            <thead><tr><th scope="col">Hop</th><th scope="col">Check with</th><th scope="col">Target</th><th scope="col"><span className="visually-hidden">Actions</span></th></tr></thead>
            <tbody>{channel.nodes.map(n => (
              <ProbeRow key={`${channel.triggerId}/${n.id}`}
                triggerId={channel.triggerId} node={n} saved={saved.get(`${channel.triggerId}/${n.id}`)} onChange={load} />
            ))}</tbody>
          </table></div>
        </>
      )}
    </div>
  );
}

function ProbeRow({ triggerId, node, saved, onChange }: { triggerId: string; node: TopologyNode; saved?: SavedProbe; onChange: () => Promise<void> }) {
  const [kind, setKind] = useState<Kind>(saved?.kind ?? 'none');
  const [value, setValue] = useState(saved?.hls ?? saved?.tcp ?? '');
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState<{ text: string; error?: boolean } | null>(null);
  const id = `probe-${triggerId}-${node.id}`;
  const url = `/api/pipeline/probes/${encodeURIComponent(triggerId)}/${encodeURIComponent(node.id)}`;
  const dirty = kind !== (saved?.kind ?? 'none') || (kind !== 'none' && value.trim() !== (saved?.hls ?? saved?.tcp ?? ''));

  async function save() {
    setBusy(true); setMessage(null);
    try {
      if (kind === 'none') { if (saved) await api(url, { method: 'DELETE' }); }
      else await api(url, { method: 'PUT', body: JSON.stringify({ [kind]: value.trim() }) });
      await onChange();
      setMessage(null); toast(`${node.name}: ${kind === 'none' ? 'probe target removed' : 'probe target saved'}. Used from the next round.`);
    } catch (e) {
      if (e instanceof AuthError) return;
      setMessage({ text: e instanceof ApiError && e.status === 400 ? e.message : `Could not save: ${(e as Error).message}`, error: true });
    } finally { setBusy(false); }
  }

  return (
    <tr>
      <th scope="row"><span className="pipeline-hop-title"><span className={`pipeline-probe-indicator ${saved ? 'pipeline-probe-indicator--configured' : ''}`} aria-hidden="true" /><span className="hop-name">{node.name}</span></span><span className="pipeline-role-label">{node.isBackup ? 'Backup · ' : ''}{node.type.replaceAll('_', ' ')}{node.transport === 'srt' ? ' · SRT' : ''}</span>{serverDomain(node, saved) && <span className="muted probe-host">{serverDomain(node, saved)}{node.port ? `:${node.port}` : ''}</span>}</th>
      <td data-label="Check with">
        <label className="visually-hidden" htmlFor={`${id}-kind`}>How to check {node.name}</label>
        <select className="select" id={`${id}-kind`} value={kind} onChange={e => { setKind(e.target.value as Kind); setMessage(null); }}>
          <option value="none">Not probed</option><option value="hls">HLS playlist</option><option value="tcp">TCP port</option>
        </select>
      </td>
      <td className="probe-value" data-label="Target">
        {kind !== 'none' && <>
          <label className="visually-hidden" htmlFor={`${id}-value`}>{kind === 'hls' ? 'Playlist URL' : 'Host and port'} for {node.name}</label>
          <input className="input" id={`${id}-value`} type={kind === 'hls' ? 'url' : 'text'} inputMode={kind === 'hls' ? 'url' : 'text'} autoComplete="off" spellCheck={false} maxLength={8192}
            placeholder={kind === 'hls' ? 'https://…/index.m3u8' : node.host && node.port ? `${node.host}:${node.port}` : 'host:port'}
            aria-invalid={!!message?.error} value={value} onChange={e => { setValue(e.target.value); setMessage(null); }}
            onKeyDown={e => { if (e.key === 'Enter' && dirty && !busy) void save(); }} />
        </>}
        <p className={`help${message?.error ? ' help--error' : ''}`} role="status">{message?.text ?? (kind === 'none' ? node.transport === 'srt' ? 'SRT server ping is separate; media health is not probed.' : 'Choose a probe to monitor this hop.' : '')}</p>
      </td>
      <td className="r"><button className="btn" type="button" disabled={!dirty || busy || (kind !== 'none' && !value.trim())} onClick={() => void save()}>{kind === 'none' && saved ? 'Remove' : 'Save'}</button></td>
    </tr>
  );
}
