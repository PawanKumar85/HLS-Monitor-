import { useEffect, useRef, useState, type KeyboardEvent } from 'react';
import { ReactFlow, Background, Controls, MiniMap, ConnectionMode, Handle, Position, addEdge, useNodesState, useEdgesState, type ReactFlowInstance, type NodeProps, type Node, type Edge, type Connection } from '@xyflow/react';
import { useBlocker } from 'react-router-dom';
import '@xyflow/react/dist/style.css';
import { api, AuthError } from './api';
import { flowEdge } from './lib/pipeline-canvas-edge';
import { arrangeNodes, canvasSignature, pathExists } from './lib/canvas-editing';
import type { Topology, TopologyNode, SavedProbe, NodeType, EdgeType } from './Pipeline';
import { CHANNEL_NODE_ID, isChannelLink, readChannelLinks } from './lib/channel-node-links';
type Hop = Node<{ renameChannel?: (name: string) => void; disabled?: boolean; label: string; hop: TopologyNode; kind: 'none' | 'hls' | 'tcp'; target: string }>;
const TYPES: Record<NodeType, string> = { INGEST: 'Ingest', SCTE_RELAY: 'SCTE relay', TRANSCODER: 'Transcoder', EGRESS_CDN: 'CDN egress', FALLBACK_SOURCE: 'VOD fallback' };
const MAX_HOPS = 30;
const CONNECTION_POINTS = [['top', Position.Top], ['right', Position.Right], ['bottom', Position.Bottom], ['left', Position.Left]] as const;
const newHopId = () => `hop-${crypto.randomUUID().slice(0, 12)}`;
const EDGE_TYPES: EdgeType[] = ['PRIMARY', 'FAILOVER', 'BYPASS', 'FALLBACK'];
const layoutKey = (id: string) => `pipeline-canvas-layout:${id}`;
function makeNodes(channel: Topology | null, probes: Map<string, SavedProbe>): Hop[] {
  let positions: Record<string, { x: number; y: number }> = {};
  try { positions = JSON.parse(localStorage.getItem(layoutKey(channel?.triggerId ?? 'new')) ?? '{}') ?? {}; } catch { /* storage is optional */ }
  return (channel?.nodes ?? []).map((hop, i) => {
    const probe = probes.get(`${channel?.triggerId}/${hop.id}`);
    const pos = positions[hop.id];
    return { id: hop.id, type: 'hls', position: pos && Number.isFinite(pos.x) && Number.isFinite(pos.y) ? pos : { x: (i % 3) * 280, y: Math.floor(i / 3) * 160 }, sourcePosition: Position.Right, targetPosition: Position.Left,
      data: { label: `${hop.name} · ${TYPES[hop.type]}`, hop, kind: probe?.kind ?? 'hls', target: probe?.hls ?? probe?.tcp ?? '' } };
  });
}
function savedEdges(channel: Topology): Edge[] {
  let ports: Record<string, { source?: string; target?: string }> = {};
  try { ports = JSON.parse(localStorage.getItem(`pipeline-canvas-ports:${channel.triggerId}`) ?? '{}') ?? {}; } catch { /* optional browser layout */ }
  const hopEdges = channel.edges.map(e => {
    const pair = ports[`${e.fromNodeId}-${e.toNodeId}`];
    return flowEdge(e.fromNodeId, e.toNodeId, e.type, pair?.source, pair?.target);
  });
  try { return [...hopEdges, ...readChannelLinks(localStorage.getItem(`pipeline-channel-links:${channel.triggerId}`), channel.nodes.map(n => n.id))]; } catch { return hopEdges; }
}
function NodeHandles({ label, isConnectable }: { label: string; isConnectable: boolean }) {
  return <>{CONNECTION_POINTS.map(([side, position]) => <Handle key={side} id={side} type="source" position={position} isConnectable={isConnectable} title={`Drag from ${side} to connect a hop`} aria-label={`${side} connection on ${label}`} />)}</>;
}
function HlsNode({ data, isConnectable }: NodeProps<Hop>) {
  return <div className="hls-canvas-node">
    <NodeHandles label={data.hop.name} isConnectable={isConnectable} />
    <b>{data.hop.name}</b><span className="muted">{TYPES[data.hop.type]}</span>
  </div>;
}
function ChannelNameNode({ data, isConnectable }: NodeProps<Hop>) {
  return <div className="channel-name-node"><NodeHandles label="channel name" isConnectable={isConnectable} /><span className="muted">Channel</span><label className="visually-hidden" htmlFor="canvas-channel-title">Channel name</label><input id="canvas-channel-title" className="nodrag nopan nowheel" aria-label="Channel name" placeholder="Enter channel name" maxLength={100} value={data.label} disabled={data.disabled} onChange={e => data.renameChannel?.(e.target.value)} /><span className="muted">Drag this title card to position it</span></div>;
}
function channelPosition(id: string) {
  try { const p = JSON.parse(localStorage.getItem(layoutKey(id)) ?? '{}')[CHANNEL_NODE_ID]; if (p && Number.isFinite(p.x) && Number.isFinite(p.y)) return { x: p.x, y: p.y }; } catch { /* optional layout */ }
  return { x: 0, y: -180 };
}
type Snapshot = { name: string; channelVisible: boolean; channelPos: { x: number; y: number }; nodes: Hop[]; edges: Edge[] };
const nodeTypes = { hls: HlsNode, channel: ChannelNameNode };

export function PipelineCanvas({ channel, probes, onCancel, onSaved }: { channel: Topology | null; probes: Map<string, SavedProbe>; onCancel: () => void; onSaved: (channel: Topology) => void }) {
  const [nodes, setNodes, onNodesChange] = useNodesState<Hop>(makeNodes(channel, probes));
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>(channel ? savedEdges(channel) : []);
  const [name, setName] = useState(channel?.channelName ?? '');
  const [channelDimensions, setChannelDimensions] = useState<{ width: number; height: number }>();
  const [channelVisible, setChannelVisible] = useState(!!channel);
  const [channelPos, setChannelPos] = useState(() => channelPosition(channel?.triggerId ?? 'new'));
  const [selected, setSelected] = useState(channel ? CHANNEL_NODE_ID : '');
  const [edgeId, setEdgeId] = useState('');
  const [edgeType, setEdgeType] = useState<EdgeType>('PRIMARY');
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState('');
  const [savedId, setSavedId] = useState(channel?.triggerId);
  const node = nodes.find(n => n.id === selected);
  const edge = edges.find(e => e.id === edgeId);

  const [past, setPast] = useState<Snapshot[]>([]);
  const [future, setFuture] = useState<Snapshot[]>([]);
  const [discarding, setDiscarding] = useState(false);
  const [snap, setSnap] = useState(true);
  const flow = useRef<ReactFlowInstance<Hop, Edge> | null>(null);
  const signature = canvasSignature(name, [...nodes, ...(channelVisible ? [{ id: CHANNEL_NODE_ID, position: channelPos, data: { label: name } }] : [])], edges);
  const baseline = useRef(signature);
  const dirty = signature !== baseline.current;
  useEffect(() => {
    if (!dirty) return;
    const warn = (event: BeforeUnloadEvent) => { event.preventDefault(); event.returnValue = ''; };
    window.addEventListener('beforeunload', warn);
    return () => window.removeEventListener('beforeunload', warn);
  }, [dirty]);
  // In-app navigation (sidebar links) never fires beforeunload; ask with the same discard prompt instead.
  const blocker = useBlocker(dirty);
  const snapshot = (): Snapshot => structuredClone({ name, channelVisible, channelPos, nodes, edges });
  function checkpoint() { setPast(p => [...p.slice(-49), snapshot()]); setFuture([]); setDiscarding(false); }
  function restore(state: Snapshot) { setName(state.name); setChannelVisible(state.channelVisible); setChannelPos(state.channelPos); setNodes(state.nodes); setEdges(state.edges); setSelected(''); setEdgeId(''); setMessage(''); }
  function undo() {
    if (busy || !past.length) return;
    setFuture(f => [...f, snapshot()]); restore(past[past.length - 1]!); setPast(p => p.slice(0, -1));
  }
  function redo() {
    if (busy || !future.length) return;
    setPast(p => [...p, snapshot()]); restore(future[future.length - 1]!); setFuture(f => f.slice(0, -1));
  }
  const channelNode: Hop = { id: CHANNEL_NODE_ID, selected: selected === CHANNEL_NODE_ID, type: 'channel', position: channelPos, measured: channelDimensions, data: { label: name, hop: { id: CHANNEL_NODE_ID, name, type: 'INGEST' }, kind: 'none', target: '', disabled: busy, renameChannel: value => { checkpoint(); setName(value); } } };
  function showChannelNode() {
    const instance = flow.current;
    if (!instance) return;
    let position = channelPos;
    if (!channelVisible) {
      checkpoint();
      const canvas = document.querySelector('.canvas-editor .pipeline-canvas')?.getBoundingClientRect();
      if (canvas) position = instance.screenToFlowPosition({ x: canvas.left + canvas.width / 2, y: canvas.top + canvas.height / 2 });
      setChannelPos(position);
      setChannelVisible(true);
    }
    setSelected(CHANNEL_NODE_ID); setEdgeId('');
    // Center on the known card dimensions; fitting before a new node is measured can do nothing.
    void instance.setCenter(position.x + 170, position.y + 65, { zoom: 1 });
    requestAnimationFrame(() => {
      const input = document.getElementById('canvas-channel-title') as HTMLInputElement | null;
      input?.focus({ preventScroll: true }); input?.select();
    });
  }
  function fit() { void flow.current?.fitView({ padding: 0.25, maxZoom: 1 }); }
  function arrange() {
    checkpoint(); setNodes(ns => arrangeNodes(ns, edges));
    requestAnimationFrame(() => { requestAnimationFrame(fit); });
  }
  function duplicate() {
    if (!node || nodes.length >= MAX_HOPS || busy) return;
    checkpoint();
    const id = newHopId();
    const copy = structuredClone(node);
    copy.id = id; copy.selected = false;
    copy.position = { x: node.position.x + 40, y: node.position.y + 140 };
    copy.data.hop = { ...copy.data.hop, id, name: `${copy.data.hop.name.slice(0, 94)} copy` };
    copy.data.label = `${copy.data.hop.name} · ${TYPES[copy.data.hop.type]}`;
    setNodes(ns => [...ns, copy]); setSelected(id); setEdgeId('');
  }
  function removeSelection() {
    if (busy) return;
    if (node) removeNode();
    else if (edge) { checkpoint(); setEdges(es => es.filter(e => e.id !== edge.id)); setEdgeId(''); }
  }
  function keys(event: KeyboardEvent<HTMLDivElement>) {
    const target = event.target as HTMLElement;
    const typing = !!target.closest('input, textarea, select, [contenteditable="true"]');
    const mod = event.metaKey || event.ctrlKey;
    if (mod && event.key.toLowerCase() === 's') { event.preventDefault(); if (!busy) void save(); return; }
    if (typing || busy) return;
    if (mod && event.key.toLowerCase() === 'z') { event.preventDefault(); event.shiftKey ? redo() : undo(); }
    else if (mod && event.key.toLowerCase() === 'y') { event.preventDefault(); redo(); }
    else if (mod && event.key.toLowerCase() === 'd') { event.preventDefault(); duplicate(); }
    else if (['Delete', 'Backspace'].includes(event.key)) { event.preventDefault(); removeSelection(); }
    else if (event.key.toLowerCase() === 'f' && !mod) { event.preventDefault(); fit(); }
  }

  function patch(data: Partial<Hop['data']>) {
    checkpoint();
    setNodes(ns => ns.map(n => n.id === selected ? { ...n, data: { ...n.data, ...data, label: `${data.hop?.name ?? n.data.hop.name} · ${TYPES[data.hop?.type ?? n.data.hop.type]}` } } : n));
    setMessage('');
  }
  function connect(c: Connection) {
    if (c.source === c.target || edges.some(e => e.source === c.source && e.target === c.target)) { setMessage('Choose two different hops that are not already linked.'); return; }
    if (pathExists(edges, c.target, c.source)) { setMessage('This link would create a loop. Signal must flow toward the CDN.'); return; }
    const channelLink = isChannelLink(c);
    const kind = edgeType === 'PRIMARY' && pathExists(edges, c.source, c.target) ? 'BYPASS' : edgeType;
    checkpoint();
    setEdges(es => addEdge({ ...flowEdge(c.source, c.target, kind, c.sourceHandle, c.targetHandle), ...(channelLink ? { label: 'CHANNEL' } : {}) }, es)); setMessage('');
  }
  function addNode(type: NodeType) {
    checkpoint();
    const id = newHopId();
    const hop: TopologyNode = { id, name: TYPES[type], type, ...(type === 'FALLBACK_SOURCE' ? { isBackup: true } : {}) };
    setNodes(ns => [...ns, { id, type: 'hls', position: { x: (ns.length % 3) * 280, y: Math.floor(ns.length / 3) * 160 }, sourcePosition: Position.Right, targetPosition: Position.Left, data: { label: `${hop.name} · ${TYPES[type]}`, hop, kind: 'hls', target: '' } }]);
    setSelected(id); setEdgeId('');
  }
  function removeNode() {
    checkpoint();
    setNodes(ns => ns.filter(n => n.id !== selected));
    setEdges(es => es.filter(e => e.source !== selected && e.target !== selected)); setSelected('');
  }
  async function save() {
    setMessage('');
    if (!channelVisible || !name.trim() || !nodes.length) { setMessage('Add the Channel name node, enter its name, and add at least one hop.'); return; }
    if (nodes.filter(n => n.data.hop.type === 'EGRESS_CDN' && !n.data.hop.isBackup).length !== 1) { setMessage('Choose exactly one main CDN egress node.'); return; }
    const signalEdges = edges.filter(e => !isChannelLink(e));
    if (nodes.length > 1 && nodes.some(n => !signalEdges.some(e => e.source === n.id || e.target === n.id))) { setMessage('Connect every node before saving.'); return; }
    const cdn = nodes.find(n => n.data.hop.type === 'EGRESS_CDN' && !n.data.hop.isBackup)!;
    if (nodes.some(n => !pathExists(signalEdges, n.id, cdn.id))) { setMessage('Every node must have a path to the main CDN egress. Check link directions.'); return; }
    for (const n of nodes) {
      if (n.data.hop.host?.trim() && !/^[A-Za-z0-9.:[\]\-]{1,253}$/.test(n.data.hop.host.trim())) { setMessage(`${n.data.hop.name}: enter a hostname or IP only, without srt://, http://, port or path.`); return; }
      if (n.data.hop.port !== undefined && (!Number.isInteger(n.data.hop.port) || n.data.hop.port < 1 || n.data.hop.port > 65535)) { setMessage(`${n.data.hop.name}: port must be an integer from 1 to 65535.`); return; }
      if (!n.data.hop.name.trim()) { setMessage('Every node needs a name.'); return; }
      if (n.data.kind === 'hls' && n.data.target.trim()) {
        try { const url = new URL(n.data.target.trim()); if (!['http:', 'https:'].includes(url.protocol)) throw Error(); }
        catch { setMessage(`${n.data.hop.name}: enter an HTTP or HTTPS playlist URL.`); return; }
      }
      if (n.data.kind === 'tcp' && !n.data.target.trim()) { setMessage(`${n.data.hop.name}: enter a host:port target.`); return; }
    }
    setBusy(true);
    let persisted = false;
    try {
      const body = JSON.stringify({ channelName: name.trim(), nodes: nodes.map(n => n.data.hop), edges: signalEdges.map(e => ({ fromNodeId: e.source, toNodeId: e.target, type: e.data?.kind ?? 'PRIMARY' })) });
      const result = await api<Topology>(savedId ? `/api/pipeline/channels/${encodeURIComponent(savedId)}` : '/api/pipeline/channels', { method: savedId ? 'PUT' : 'POST', body });
      setSavedId(result.triggerId); persisted = true;
      try { localStorage.setItem(`pipeline-channel-links:${result.triggerId}`, JSON.stringify(edges.filter(isChannelLink).map(({ source, target, sourceHandle, targetHandle }) => ({ source, target, sourceHandle, targetHandle })))); localStorage.setItem(`pipeline-canvas-ports:${result.triggerId}`, JSON.stringify(Object.fromEntries(edges.map(e => [e.id, { source: e.sourceHandle, target: e.targetHandle }])))); localStorage.setItem(layoutKey(result.triggerId), JSON.stringify(Object.fromEntries([...nodes.map(n => [n.id, n.position]), [CHANNEL_NODE_ID, channelPos]]))); } catch { /* layout storage is optional */ }
      const currentProbes = (await api<{ probes: SavedProbe[] }>('/api/pipeline/probes')).probes;
      for (const n of nodes) {
        const url = `/api/pipeline/probes/${encodeURIComponent(result.triggerId)}/${encodeURIComponent(n.id)}`;
        if (n.data.kind !== 'none' && n.data.target.trim()) await api(url, { method: 'PUT', body: JSON.stringify({ [n.data.kind]: n.data.target.trim() }) });
        else if (currentProbes.some(p => p.triggerId === result.triggerId && p.nodeId === n.id)) await api(url, { method: 'DELETE' });
      }
      baseline.current = signature;
      onSaved(result);
    } catch (error) {
      if (!(error instanceof AuthError)) setMessage(`${persisted ? 'Channel saved, but some probe targets could not be saved. Retry Save to finish. ' : ''}${(error as Error).message}`);
    } finally { setBusy(false); }
  }

  return (
    <div className="panel canvas-editor" onKeyDown={keys} tabIndex={-1}>
      <div className="canvas-toolbar">
        <div><h3>{channel ? `Edit ${channel.channelName}` : 'Build a channel'}</h3><p className="muted">Add HLS nodes, drag them into place, then connect any of the four points to a point on another node. Links that skip an existing path become bypasses automatically.</p></div>

      </div>
      <fieldset className="canvas-fields" disabled={busy}>
        <legend className="visually-hidden">Pipeline canvas editor</legend>
        <div className="canvas-toolbar canvas-tools">
          <div className="row-actions"><button className="btn btn--sm" type="button" onClick={showChannelNode}>{channelVisible ? 'Show channel name' : '+ Channel name node'}</button>{Object.entries(TYPES).map(([type, label]) => <button className="btn btn--sm" key={type} type="button" disabled={nodes.length >= MAX_HOPS} onClick={() => addNode(type as NodeType)}>+ {label}</button>)}</div>
          <div className="field"><label htmlFor="canvas-edge-type">New link type</label><select className="select" id="canvas-edge-type" value={edgeType} onChange={e => setEdgeType(e.target.value as EdgeType)}>{EDGE_TYPES.map(t => <option key={t}>{t}</option>)}</select></div>
        </div>
        <div className="canvas-toolbar canvas-edit-tools">
          <div className="row-actions">
            <button className="btn btn--sm" type="button" disabled={!past.length} onClick={undo} title="⌘/Ctrl+Z">Undo</button>
            <button className="btn btn--sm" type="button" disabled={!future.length} onClick={redo} title="⌘/Ctrl+Shift+Z">Redo</button>
            <button className="btn btn--sm" type="button" disabled={!node || nodes.length >= MAX_HOPS} onClick={duplicate} title="⌘/Ctrl+D">Duplicate node</button>
            <button className="btn btn--sm" type="button" disabled={!node && !edge} onClick={removeSelection}>Delete selected</button>
            <button className="btn btn--sm" type="button" disabled={!nodes.length} onClick={arrange}>Auto-arrange</button>
            <button className="btn btn--sm" type="button" onClick={fit} title="F">Fit view</button>
          </div>
          <label><input type="checkbox" checked={snap} onChange={e => setSnap(e.target.checked)} /> Snap to grid</label>
        </div>
        <div className="canvas-layout">
          <div className="pipeline-canvas" aria-label="HLS pipeline canvas">
            <ReactFlow onInit={instance => { flow.current = instance; }} onNodeDragStart={checkpoint} snapToGrid={snap} snapGrid={[20, 20]} fitViewOptions={{ padding: 0.25, maxZoom: 1 }} onPaneClick={() => { setSelected(''); setEdgeId(''); }} connectionMode={ConnectionMode.Loose} nodeTypes={nodeTypes} nodes={[...(channelVisible ? [channelNode] : []), ...nodes]} edges={edges} onNodesChange={changes => { for (const change of changes) { if (change.type === 'position' && change.id === CHANNEL_NODE_ID && change.position) setChannelPos(change.position); if (change.type === 'dimensions' && change.id === CHANNEL_NODE_ID && change.dimensions) { const dimensions = change.dimensions; setChannelDimensions(previous => previous?.width === dimensions.width && previous?.height === dimensions.height ? previous : dimensions); } } onNodesChange(changes.filter(change => !('id' in change) || change.id !== CHANNEL_NODE_ID)); }} onEdgesChange={onEdgesChange} onConnect={connect} onNodeClick={(_, n) => { setSelected(n.id); setEdgeId(''); }} onEdgeClick={(_, e) => { setEdgeId(e.id); setSelected(''); }} fitView nodesDraggable={!busy} nodesConnectable={!busy} elementsSelectable={!busy} deleteKeyCode={null}>
              <Background gap={20} /><Controls showInteractive={false} /><MiniMap pannable zoomable nodeColor="var(--color-rule-2)" bgColor="var(--color-paper-2)" maskColor="rgba(0,0,0,0.25)" />
            </ReactFlow>
          </div>
          <div className="canvas-inspector">
            {selected === CHANNEL_NODE_ID ? <><h3>Channel name</h3><p className="muted">Edit the name directly inside the Channel node on the canvas. Drag any of its four connection points to link the channel name to a hop. These visual links are saved in this browser.</p><button className="btn" type="button" onClick={showChannelNode}>Edit channel name</button></> : node ? <>
              <h3>Node settings</h3>
              <div className="field"><label htmlFor="hop-name">Name</label><input className="input" id="hop-name" maxLength={100} value={node.data.hop.name} onChange={e => patch({ hop: { ...node.data.hop, name: e.target.value } })} /></div>
              <div className="field"><label htmlFor="hop-type">Hop type</label><select className="select" id="hop-type" value={node.data.hop.type} onChange={e => patch({ hop: { ...node.data.hop, type: e.target.value as NodeType } })}>{Object.entries(TYPES).map(([t, label]) => <option key={t} value={t}>{label}</option>)}</select></div>
              <label><input type="checkbox" checked={node.data.hop.transport === 'srt'} onChange={e => { const hop = { ...node.data.hop }; if (e.target.checked) hop.transport = 'srt'; else delete hop.transport; patch({ hop }); }} /> SRT server · enable ping reachability</label>
              <div className="field"><label htmlFor="hop-host">Server host / IP</label><input className="input" id="hop-host" autoComplete="off" spellCheck={false} maxLength={253} placeholder="148.113.16.234 or multi.ottlive.co.in" value={node.data.hop.host ?? ''} onChange={e => patch({ hop: { ...node.data.hop, host: e.target.value } })} /><p className="help">Hostname or IP only. Identifies this server in Dashboard and Workspace; does not enable a health check.</p></div>
              <div className="field"><label htmlFor="hop-port">Server port</label><input className="input" id="hop-port" type="number" min={1} max={65535} step={1} placeholder="63004" value={node.data.hop.port ?? ''} onChange={e => patch({ hop: { ...node.data.hop, port: e.target.value === '' ? undefined : Number(e.target.value) } })} /><p className="help">Optional. For SRT endpoints, keep Check with set to Not probed.</p></div>
              <label><input type="checkbox" checked={!!node.data.hop.isBackup} onChange={e => patch({ hop: { ...node.data.hop, isBackup: e.target.checked } })} /> Backup node</label>
              <div className="field"><label htmlFor="hop-kind">Check with</label><select className="select" id="hop-kind" value={node.data.kind} onChange={e => patch({ kind: e.target.value as Hop['data']['kind'], target: '' })}><option value="hls">HLS playlist</option><option value="tcp">TCP port</option><option value="none">Not probed</option></select></div>
              {node.data.kind !== 'none' && <div className="field"><label htmlFor="hop-target">{node.data.kind === 'hls' ? 'Playlist URL' : 'Host:port'}</label><input className="input" id="hop-target" value={node.data.target} maxLength={8192} placeholder={node.data.kind === 'hls' ? 'https://host/live/index.m3u8' : 'host:port'} onChange={e => patch({ target: e.target.value })} /><p className="help">{node.data.kind === 'hls' ? 'Leave empty to configure monitoring later.' : 'Enter the TCP endpoint for this hop.'}</p></div>}
              <button className="btn" type="button" onClick={removeNode}>Remove node</button>
            </> : edge ? <>
              <h3>Hop connection</h3><p className="muted">{edge.source === CHANNEL_NODE_ID ? name || 'Channel' : nodes.find(n => n.id === edge.source)?.data.hop.name} → {edge.target === CHANNEL_NODE_ID ? name || 'Channel' : nodes.find(n => n.id === edge.target)?.data.hop.name}</p>
              {isChannelLink(edge) ? <p className="muted">Channel label connection · saved in this browser.</p> : <><label htmlFor="selected-link">Link type</label><select className="select" id="selected-link" value={String(edge.data?.kind ?? 'PRIMARY')} onChange={e => { checkpoint(); setEdges(es => es.map(x => x.id === edge.id ? { ...x, ...flowEdge(x.source, x.target, e.target.value as EdgeType, x.sourceHandle, x.targetHandle) } : x)); }}>{EDGE_TYPES.map(t => <option key={t}>{t}</option>)}</select></>}
              <button className="btn" type="button" onClick={removeSelection}>Remove link</button>
            </> : <p className="muted">Select a node to set its playlist URL, or a connection to edit the hop type.</p>}
            <details><summary>Connect without dragging</summary><LinkForm nodes={[...(channelVisible ? [channelNode] : []), ...nodes]} onConnect={connect} /></details>
          </div>
        </div>
      </fieldset>
      <div className="canvas-toolbar"><div><p className="help help--error" role="alert">{message}</p><p className="muted canvas-hint">{nodes.length} nodes · {edges.length} links · {dirty ? 'Unsaved changes' : 'No unsaved changes'}</p><p className="muted canvas-hint">⌘/Ctrl+Z undo · ⌘/Ctrl+D duplicate · Delete remove · F fit · ⌘/Ctrl+S save</p></div><div className="row-actions"><button className="btn btn--primary" type="button" disabled={busy} onClick={() => void save()}>{busy ? 'Saving…' : 'Save channel'}</button><button className="btn" type="button" disabled={busy} onClick={() => dirty ? setDiscarding(true) : onCancel()}>Cancel</button></div></div>
      {(discarding || blocker.state === 'blocked') && <div className="canvas-toolbar" role="alert"><p>Discard your unsaved canvas changes?</p><div className="row-actions"><button className="btn" type="button" autoFocus onClick={() => { setDiscarding(false); blocker.reset?.(); }}>Keep editing</button><button className="btn btn--danger" type="button" onClick={() => blocker.state === 'blocked' ? blocker.proceed() : onCancel()}>Discard changes</button></div></div>}
    </div>
  );
}
function LinkForm({ nodes, onConnect }: { nodes: Hop[]; onConnect: (c: Connection) => void }) {
  const [from, setFrom] = useState(''); const [to, setTo] = useState('');
  return <div className="canvas-link-form"><label htmlFor="link-from">From</label><select className="select" id="link-from" value={from} onChange={e => setFrom(e.target.value)}><option value="">Choose node</option>{nodes.map(n => <option key={n.id} value={n.id}>{n.data.hop.name}</option>)}</select><label htmlFor="link-to">To</label><select className="select" id="link-to" value={to} onChange={e => setTo(e.target.value)}><option value="">Choose node</option>{nodes.map(n => <option key={n.id} value={n.id}>{n.data.hop.name}</option>)}</select><button className="btn" type="button" disabled={!nodes.some(n => n.id === from) || !nodes.some(n => n.id === to)} onClick={() => onConnect({ source: from, target: to, sourceHandle: null, targetHandle: null })}>Connect hops</button></div>;
}
