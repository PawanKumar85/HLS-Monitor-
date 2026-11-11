import { gaugeValue } from './lib/gauge';
const point = (angle: number, radius: number) => ({ x: 120 + radius * Math.cos(angle * Math.PI / 180), y: 112 + radius * Math.sin(angle * Math.PI / 180) });
const start = point(135, 82), end = point(405, 82);
const ARC = `M ${start.x} ${start.y} A 82 82 0 1 1 ${end.x} ${end.y}`;
export function StatusGauge({ title, count, total, available, tone, description }: { title: string; count: number; total: number; available: boolean; tone: 'online' | 'offline' | 'backup' | 'unknown'; description: string }) {
  const value = gaugeValue(count, total, available);
  const needle = value ? point(value.angle, 64) : null;
  return <article className={`panel status-gauge status-gauge--${tone}`}>
    <div className="status-gauge__head"><h3>{title}</h3><span className="status-gauge__led" aria-hidden="true" /></div>
    <svg viewBox="0 0 240 215" role="img" aria-label={value ? `${title}: ${count} of ${total} channels, ${value.percent} percent` : `${title}: ${available ? 'No configured channels' : 'Current status unavailable'}`}>
      <path d={ARC} className="status-gauge__track" pathLength={100} />
      {value && <path d={ARC} className="status-gauge__arc" pathLength={100} strokeDasharray={`${value.ratio * 100} 100`} />}
      {Array.from({ length: 11 }, (_, i) => { const a = point(135 + i * 27, 94), b = point(135 + i * 27, 100); return <line key={i} x1={a.x} y1={a.y} x2={b.x} y2={b.y} className="status-gauge__tick" />; })}
      {needle && <><line x1="120" y1="112" x2={needle.x} y2={needle.y} className="status-gauge__needle" /><circle cx="120" cy="112" r="6" className="status-gauge__pivot" /></>}
      <text x="120" y="161" textAnchor="middle" className="status-gauge__value">{value ? `${value.percent}%` : '—'}</text>
      <text x="27" y="201" className="status-gauge__scale">0</text><text x="211" y="201" textAnchor="end" className="status-gauge__scale">100%</text>
    </svg>
    <p className="status-gauge__count">{value ? <><b>{count}</b><span> / {total} channels</span></> : available ? 'No channels configured' : 'Awaiting fresh data'}</p>
    <p className="status-gauge__description">{description}</p>
  </article>;
}
