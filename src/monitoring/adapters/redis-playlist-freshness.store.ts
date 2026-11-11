import { createHash } from 'node:crypto';
import type { Redis } from 'ioredis';
import { withOperationTimeout } from '../../realtime/operation-timeout.js';
import type { PlaylistFreshnessStore, PlaylistProgress } from '../probe-contracts.js';
/** A confirmed-online stream turns offline only after this many failed fetches in a row. */
export const FAILURES_TO_OFFLINE = 2;
const HEADER = `
local t = redis.call('TYPE', KEYS[1]).ok
if t ~= 'none' and t ~= 'hash' then error('WRONGTYPE freshness dependency') end
local previous = redis.call('HGET', KEYS[1], ARGV[1])
`;
// Per stream: {source, fingerprint, changedAt, last (last shown status), streak (failed fetches in a row)}.
const OBSERVE = HEADER + `
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
local state
if previous then state = cjson.decode(previous)
elseif redis.call('HLEN', KEYS[1]) >= 10000 then error('Freshness capacity exceeded') end
if not state or state.source ~= ARGV[2] or state.fingerprint ~= ARGV[3] then
 state = {source=ARGV[2], fingerprint=ARGV[3], changedAt=now}
end
local result = 1
if now - tonumber(state.changedAt) >= tonumber(ARGV[4]) then result = 0 end
state.last = result; state.streak = 0
redis.call('HSET', KEYS[1], ARGV[1], cjson.encode(state))
return result`;
const FAIL = HEADER + `
if not previous then return 0 end
local state = cjson.decode(previous)
if state.source ~= ARGV[2] then return 0 end
state.streak = (state.streak or 0) + 1
if state.last ~= 1 or state.streak >= tonumber(ARGV[3]) then state.last = 0 end
redis.call('HSET', KEYS[1], ARGV[1], cjson.encode(state))
return state.last`;
export class RedisPlaylistFreshnessStore implements PlaylistFreshnessStore {
  readonly key: string;
  private active = 0;
  constructor(private readonly redis: Redis, namespace = 'default') {
    if (!/^[\w-]{1,80}$/.test(namespace)) throw new Error('Invalid freshness namespace');
    this.key = `hls:probes:${namespace}:freshness`;
  }
  private async run(script: string, serverIp: string, streamId: string, streamUrl: string, ...args: (string | number)[]): Promise<boolean> {
    if (this.active >= 100) throw new Error('Freshness command capacity exceeded');
    const hash = (value: string) => createHash('sha256').update(value).digest('hex');
    this.active++;
    const work = Promise.resolve().then(() => this.redis.eval(script, 1, this.key, hash(JSON.stringify([serverIp, streamId])), hash(streamUrl), ...args));
    void work.then(() => { this.active--; }, () => { this.active--; });
    return Number(await withOperationTimeout(work, 5000, 'Playlist freshness command')) === 1;
  }
  async observe(serverIp: string, streamId: string, streamUrl: string, progress: PlaylistProgress, staleAfterMs: number): Promise<boolean> {
    const threshold = Math.max(staleAfterMs, progress.targetDurationMs * 3);
    if (!Number.isFinite(threshold) || threshold < 1 || threshold > 2_147_483_647) throw new RangeError('Invalid freshness threshold');
    return this.run(OBSERVE, serverIp, streamId, streamUrl, progress.fingerprint, threshold);
  }
  fail(serverIp: string, streamId: string, streamUrl: string): Promise<boolean> {
    return this.run(FAIL, serverIp, streamId, streamUrl, FAILURES_TO_OFFLINE);
  }
}
