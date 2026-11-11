import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Link } from 'react-router-dom';
import { ReactFlow, Handle, Position, ConnectionMode, Background, Controls, MiniMap, type Connection, type NodeProps, type ReactFlowInstance, type Edge } from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import { api, AuthError, clock } from './api';
import type { Topology, SavedProbe } from './Pipeline';
import { workspaceGraph, type ServerNode } from './lib/workspace-graph';
import { ARROW_SIDES, arrowLayout, readArrowLayouts, reconnectArrow, type ArrowLayout } from './lib/workspace-arrows';
const ARROW_STORAGE = 'workspace-arrow-layout:v1';
const CONNECTION_POINTS = [['top', Position.Top], ['right', Position.Right], ['bottom', Position.Bottom], ['left', Position.Left]] as const;
const ARROW_FIELDS = [
  ['sourceSide', 'arrow-source', 'Start side'],
  ['targetSide', 'arrow-target', 'End side'],
] as const;

function ServerCard({ data }: NodeProps<ServerNode>) {
  const channels = new Set(data.members.map(m => m.channelId)).size;
  return <div className="workspace-server">
    {CONNECTION_POINTS.map(([side, position]) => <Handle key={side} type="source" position={position} id={side} isConnectable isConnectableStart={false} isConnectableEnd title={`Attach arrow to ${side}`} />)}
    <div className="workspace-server__header"><span className="workspace-server__icon" aria-hidden="true"><svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5"><rect x="3" y="3" width="18" height="7" rx="2" /><rect x="3" y="14" width="18" height="7" rx="2" /><path d="M6 6.5h2M6 17.5h2M12 6.5h6M12 17.5h6" /></svg></span><span className="workspace-server__kind">{data.domain ? 'Server / domain' : 'Identity pending'}</span>{channels > 1 && <span className="workspace-shared-badge">Shared · {channels}</span>}</div>
    <b>{data.label}</b><p className="workspace-server__meta">{data.members.length} {data.members.length === 1 ? 'hop' : 'hops'} across {channels} {channels === 1 ? 'channel' : 'channels'}</p>
    <ul>{data.members.slice(0, 4).map(m => <li key={`${m.channelId}/${m.nodeId}`}><span>{m.channelName}</span><span className="workspace-hop-chip">{m.name}</span></li>)}</ul>
    {data.members.length > 4 && <p className="muted">+{data.members.length - 4} more hops · select to view</p>}
  </div>;
}
const nodeTypes = { server: ServerCard };

export function Workspace() {
  const [data, setData] = useState<{ channels: Topology[]; probes: SavedProbe[] } | null>(null);
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);
  const [updated, setUpdated] = useState<Date | null>(null);
  const [channelId, setChannelId] = useState('');
  const [selected, setSelected] = useState('');
  const [selectedArrow, setSelectedArrow] = useState('');
  const [arrowMessage, setArrowMessage] = useState('');
  const [layouts, setLayouts] = useState(() => {
    try { return readArrowLayouts(localStorage.getItem(ARROW_STORAGE)); } catch { return {}; }
  });
  function updateArrow(edge: Edge, layout: ArrowLayout) {
    const next = { ...layouts, [edge.id]: layout };
    setLayouts(next);
    try { localStorage.setItem(ARROW_STORAGE, JSON.stringify(next)); setArrowMessage('Arrow layout saved in this browser.'); }
    catch { setArrowMessage('Arrow adjusted for this visit. Browser storage is unavailable.'); }
  }
  function reconnect(edge: Edge, connection: Connection) {
    const result = reconnectArrow(edge, connection, layouts[edge.id]);
    if (!result) { setArrowMessage('Drop the endpoint on another side of the same server. Change channel connections in Add Stream.'); return; }
    updateArrow(edge, result);
  }
  const flow = useRef<ReactFlowInstance<ServerNode, Edge> | null>(null);
  const loading = useRef(false);
  const mounted = useRef(true);
  const load = useCallback(async () => {
    if (loading.current) return;
    loading.current = true; setBusy(true);
    try {
      const [channels, probes] = await Promise.all([api<{ channels: Topology[] }>('/api/pipeline/channels'), api<{ probes: SavedProbe[] }>('/api/pipeline/probes')]);
      if (mounted.current) { setData({ channels: channels.channels, probes: probes.probes }); setError(''); setUpdated(new Date()); }
    } catch (e) { if (mounted.current && !(e instanceof AuthError)) setError((e as Error).message); }
    finally { loading.current = false; if (mounted.current) setBusy(false); }
  }, []);
  useEffect(() => {
    mounted.current = true; void load();
    const timer = setInterval(() => { if (!document.hidden) void load(); }, 30_000);
    return () => { mounted.current = false; clearInterval(timer); };
  }, [load]);
  const shown = useMemo(() => data?.channels.filter(c => !channelId || c.triggerId === channelId) ?? [], [data, channelId]);
  const graph = useMemo(() => workspaceGraph(shown, data?.probes ?? []), [shown, data]);
  const arrows = useMemo(() => graph.edges.map(edge => {
    const layout = arrowLayout(edge, layouts[edge.id]);
    return { ...edge, selected: edge.id === selectedArrow, sourceHandle: layout.sourceSide, targetHandle: layout.targetSide, pathOptions: { curvature: layout.curvature } };
  }), [graph, layouts, selectedArrow]);
  const activeArrow = arrows.find(edge => edge.id === selectedArrow);
  const activeLayout = activeArrow ? arrowLayout(activeArrow, layouts[activeArrow.id]) : null;
  const server = graph.nodes.find(n => n.id === selected);
  const structure = JSON.stringify(graph.nodes.map(n => [n.id, n.position]));
  useEffect(() => { const timer = setTimeout(() => { void flow.current?.fitView({ padding: 0.25, maxZoom: 1 }); }, 100); return () => clearTimeout(timer); }, [structure]);
  return <section className="workspace-page" aria-labelledby="workspace-heading">
    <div className="workspace-page-head"><div><p className="hc-eyebrow">Infrastructure / topology explorer</p><h2 id="workspace-heading">Workspace</h2><p className="workspace-page-intro">Every channel. Every shared server. One connected view.</p></div><span className={`hc-feed ${updated && !error ? 'hc-feed--live' : ''}`}><i aria-hidden="true" />{busy ? 'Updating topology…' : updated ? `Synced ${clock.format(updated)}` : 'Connecting…'}</span></div>
    <div className="workspace-scope-bar"><div className="field"><label htmlFor="workspace-channel">Channel scope</label><select id="workspace-channel" className="select" value={channelId} onChange={e => { setChannelId(e.target.value); setSelected(''); setSelectedArrow(''); }}><option value="">All channels</option>{data?.channels.map(c => <option key={c.triggerId} value={c.triggerId}>{c.channelName}</option>)}</select></div><p>Automatic sync <span>every 30 seconds</span></p></div>

    {error && <p className="help help--error" role="alert">Could not refresh workspace: {error}{data ? '. Showing the last loaded topology.' : ''}</p>}
    {!data ? <div className="panel msg"><p>{error ? 'Connection unavailable. Retrying automatically every 30 seconds.' : 'Loading channels and servers…'}</p></div>
      : !data.channels.length ? <div className="panel msg"><h3>No channels yet</h3><p><Link to="/create">Build a channel in Add Stream</Link> to see its servers here.</p></div>
      : <>
        <div className="workspace-metrics" aria-label="Topology counts">{[['Channels in view', shown.length], ['Signal hops', graph.hops], ['Server nodes', graph.nodes.length], ['Shared servers', graph.shared]].map(([label, value]) => <div key={label}><span>{label}</span><strong>{value}</strong></div>)}</div>
        <div className="panel workspace-board">
          <div className="workspace-board-head"><div><h3>Network topology</h3><p>Pan to explore · select a server for details · select a link to adjust its curve</p></div><span className="workspace-view-badge">Topology read-only</span></div>
          <div className="workspace-legend" aria-label="Channel colors">{graph.legend.map(c => <span key={c.id}><i style={{ background: c.color }} aria-hidden="true" />{c.name}</span>)}</div>
          <div className="pipeline-canvas workspace-canvas">
            <ReactFlow<ServerNode, Edge> nodes={graph.nodes} edges={arrows} nodeTypes={nodeTypes} onInit={instance => { flow.current = instance; }} nodesDraggable={false} nodesConnectable={false} connectionMode={ConnectionMode.Loose} connectOnClick={false} edgesReconnectable onReconnect={reconnect} reconnectRadius={18} deleteKeyCode={null} nodesFocusable edgesFocusable elementsSelectable onNodeClick={(_, n) => { setSelected(n.id); setSelectedArrow(''); }} onEdgeClick={(_, e) => { setSelectedArrow(e.id); setSelected(''); setArrowMessage(''); }} onPaneClick={() => { setSelected(''); setSelectedArrow(''); }} fitView fitViewOptions={{ padding: 0.25, maxZoom: 1 }} minZoom={0.05}>
              <Background gap={20} /><Controls showInteractive={false} /><MiniMap pannable zoomable nodeColor="var(--color-rule-2)" bgColor="var(--color-paper-2)" maskColor="rgba(0,0,0,0.25)" />
            </ReactFlow>
          </div>
        </div>
        <div className="workspace-canvas-footnote"><span>One node per server/domain. Shared hosts are grouped automatically.</span><span>{graph.edges.length} server links · {graph.links.filter(link => link.internal).length} internal connections</span></div>
        {arrowMessage && <p className="help" role="status">{arrowMessage}</p>}
        {activeArrow && activeLayout && <div className="panel workspace-details">
          <div className="incidents-head"><div><p className="hc-eyebrow">Connection inspector</p><h3>{String(activeArrow.label)}</h3></div><button className="btn btn--quiet" type="button" onClick={() => setSelectedArrow('')}>Close</button></div>
          <p className="muted">Drag either end onto another point of the same server, or choose sides below.</p>
          <div className="workspace-arrow-tools">
            {ARROW_FIELDS.map(([field, id, label]) => <div className="field" key={field}><label htmlFor={id}>{label}</label><select className="select" id={id} value={activeLayout[field]} onChange={e => updateArrow(activeArrow, { ...activeLayout, [field]: e.target.value as ArrowLayout[typeof field] })}>{ARROW_SIDES.map(side => <option key={side}>{side}</option>)}</select></div>)}
            <div className="field"><label htmlFor="arrow-curve">Curve ({activeLayout.curvature.toFixed(2)})</label><input id="arrow-curve" type="range" min="0" max="1.5" step="0.05" value={activeLayout.curvature} onChange={e => updateArrow(activeArrow, { ...activeLayout, curvature: Number(e.target.value) })} /></div>
            <button className="btn" type="button" onClick={() => updateArrow(activeArrow, arrowLayout(activeArrow))}>Reset arrow</button>
          </div>
        </div>}
        {server && <div className="panel workspace-details"><div className="incidents-head"><div><p className="hc-eyebrow">Server inspector</p><h3>{server.data.label}</h3></div><button className="btn btn--quiet" type="button" onClick={() => setSelected('')}>Close details</button></div><p className="muted">{server.data.domain ? 'Hops grouped by hostname. Ports and playlist paths do not create duplicate server nodes.' : 'Configure a host or probe target in Add Stream to identify this server.'}</p><div className="tablewrap"><table><thead><tr><th>Channel</th><th>Hop</th><th>Type</th><th>Probe</th></tr></thead><tbody>{server.data.members.map(m => <tr key={`${m.channelId}/${m.nodeId}`}><td>{m.channelName}</td><td>{m.name}</td><td>{m.type}</td><td><span className={`workspace-probe-badge ${m.probe === 'none' ? '' : 'workspace-probe-badge--active'}`}>{m.probe === 'none' ? 'Not probed' : m.probe.toUpperCase()}</span></td></tr>)}</tbody></table></div></div>}
        <details className="panel workspace-details"><summary><span>All hop connections</span><span className="workspace-connection-count">{graph.links.length}</span></summary><div className="tablewrap"><table><thead><tr><th>Channel</th><th>From</th><th>To</th><th>Link type</th></tr></thead><tbody>{graph.links.map((link, i) => <tr key={i}><td>{link.channelName}</td><td>{link.from}</td><td>{link.to}{link.internal ? ' (same server)' : ''}</td><td>{link.kind}</td></tr>)}</tbody></table></div></details>
      </>}
  </section>;
}
