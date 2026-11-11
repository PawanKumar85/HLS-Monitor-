import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { isIP } from 'node:net';
import type { ChannelTopology } from '../types/pipeline.types.js';
const run = promisify(execFile);
export interface SrtPing { host: string; state: 'REACHABLE' | 'NO_REPLY' | 'UNAVAILABLE'; latencyMs: number | null; checkedAt: string }
export async function pingSrtHost(host: string): Promise<SrtPing> {
  const checkedAt = new Date().toISOString();
  const result = (state: SrtPing['state'], latencyMs: number | null = null): SrtPing => ({ host, state, latencyMs, checkedAt });
  // Spawn without a shell and reject option-like hosts. Never treat missing ping permissions as a server outage.
  const address = host.replace(/^\[|\]$/g, '');
  if (!(isIP(address) || /^[a-zA-Z0-9][a-zA-Z0-9.-]*$/.test(address)) || host.length > 253) return result('UNAVAILABLE');
  const ipv6 = isIP(address) === 6;
  const command = process.platform === 'darwin' && ipv6 ? 'ping6' : 'ping';
  const args = [...(process.platform !== 'darwin' && ipv6 ? ['-6'] : []), '-n', '-c', '1', address];
  try {
    const { stdout } = await run(command, args, { timeout: 3000, maxBuffer: 64 * 1024, encoding: 'utf8', env: { ...process.env, LC_ALL: 'C' } });
    const match = /time[=<]\s*([\d.]+)\s*ms/i.exec(stdout);
    return match ? result('REACHABLE', Number(match[1])) : result('UNAVAILABLE');
  } catch (error) {
    const e = error as { code?: string | number; killed?: boolean; stderr?: string; stdout?: string };
    if (e.killed || /100(?:\.0)?% packet loss|0 (?:packets )?received/i.test(e.stdout ?? '')) return result('NO_REPLY');
    return result('UNAVAILABLE');
  }
}
/** Ping is only server reachability; these results never enter the stream RCA engine. */
export class SrtReachabilityMonitor {
  private timer: NodeJS.Timeout | undefined;
  private active: Promise<void> | undefined;
  private results: SrtPing[] = [];
  private evaluatedAt: string | null = null;
  constructor(private readonly channels: () => Promise<readonly ChannelTopology[]>, private readonly ping: (host: string) => Promise<SrtPing> = pingSrtHost) {}
  reset() { this.results = []; this.evaluatedAt = null; }
  status() { return { evaluatedAt: this.evaluatedAt, intervalMs: 30_000, servers: this.results }; }
  start() {
    if (this.timer) return;
    const tick = () => { void this.round().catch(() => console.warn('SRT server reachability refresh unavailable')); };
    this.timer = setInterval(tick, 30_000); this.timer.unref(); tick();
  }
  async stop() { clearInterval(this.timer); this.timer = undefined; await this.active?.catch(() => {}); }
  round(): Promise<void> {
    this.active ??= this.execute().finally(() => { this.active = undefined; });
    return this.active;
  }
  private async execute() {
    const channels = await this.channels();
    const hosts = [...new Set(channels.flatMap(c => c.nodes.filter(n => n.transport === 'srt' && n.host?.trim()).map(n => n.host!.trim().toLowerCase())))];
    const results: SrtPing[] = [];
    for (let i = 0; i < hosts.length; i += 4) {
      results.push(...await Promise.all(hosts.slice(i, i + 4).map(async host => {
        try { return await this.ping(host); } catch { return { host, state: 'UNAVAILABLE' as const, latencyMs: null, checkedAt: new Date().toISOString() }; }
      })));
    }
    this.results = results; this.evaluatedAt = new Date().toISOString();
  }
}
