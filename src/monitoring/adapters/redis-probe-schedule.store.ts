import type { StreamRegistration, StreamScheduleProjection } from '../../inventory/stream-inventory.js';
import { createHash } from 'node:crypto';
import type { Redis } from 'ioredis';
import { withOperationTimeout } from '../../realtime/operation-timeout.js';
import { normalizeProbeConfig, type DueProbe, type ProbeJobProducer, type StreamInventory, type StreamProbeConfig, type StreamProbeRegistration } from '../probe-contracts.js';

const TYPES = `
local function check(key, wanted)
 local t = redis.call('TYPE', key).ok
 if t ~= 'none' and t ~= wanted then error('WRONGTYPE probe dependency') end
end
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
`;
const UPSERT = TYPES + `
check(KEYS[1], 'hash'); check(KEYS[2], 'zset'); check(KEYS[3], 'hash')
if redis.call('HGET', KEYS[3], ARGV[1]) then error('Managed inventory must be changed in the database') end
local previous = redis.call('HGET', KEYS[1], ARGV[1])
if not previous and redis.call('HLEN', KEYS[1]) >= 10000 then error('Probe inventory capacity exceeded') end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
if ARGV[3] == '0' then redis.call('ZREM', KEYS[2], ARGV[1])
elseif previous ~= ARGV[2] or not redis.call('ZSCORE', KEYS[2], ARGV[1]) then
 redis.call('ZADD', KEYS[2], now + tonumber(ARGV[4]), ARGV[1])
end
return 1`;
const DUE = TYPES + `
check(KEYS[1], 'hash'); check(KEYS[2], 'zset')
local ids = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now, 'LIMIT', 0, ARGV[1])
local result = {}
for _, id in ipairs(ids) do
 local config = redis.call('HGET', KEYS[1], id)
 if config then table.insert(result, {config, redis.call('ZSCORE', KEYS[2], id)}) end
end
return result`;
const ENQUEUE = TYPES + `
check(KEYS[1], 'hash'); check(KEYS[2], 'zset'); check(KEYS[3], 'hash'); check(KEYS[4], 'stream'); check(KEYS[5], 'hash')
local version = redis.call('HGET', KEYS[5], ARGV[1])
if version and cjson.decode(version).config ~= ARGV[2] then return 'changed' end
if redis.call('HGET', KEYS[1], ARGV[1]) ~= ARGV[2] then return 'changed' end
local config = cjson.decode(ARGV[2])
local due = redis.call('ZSCORE', KEYS[2], ARGV[1])
if not config.enabled or not due or tonumber(due) > now then return 'not-due' end
local group = redis.pcall('XGROUP', 'CREATE', KEYS[4], ARGV[3], '0', 'MKSTREAM')
if group.err and not string.find(group.err, 'BUSYGROUP', 1, true) then return redis.error_reply(group.err) end
local function compareNumber(a, b)
 if string.len(a) ~= string.len(b) then return string.len(a) < string.len(b) end
 return a < b
end
local function idLess(a, b)
 local am, as = string.match(a, '(%d+)%-(%d+)')
 local bm, bs = string.match(b, '(%d+)%-(%d+)')
 if am == bm then return compareNumber(as, bs) end
 return compareNumber(am, bm)
end
local previous = redis.call('HGET', KEYS[3], ARGV[1])
if previous then
 local lease = cjson.decode(previous)
 local exists = #redis.call('XRANGE', KEYS[4], lease.jobId, lease.jobId) > 0
 local outstanding = false
 if exists then
  local pending = redis.call('XPENDING', KEYS[4], ARGV[3], lease.jobId, lease.jobId, 1)
  outstanding = #pending > 0
  if not outstanding then
   for _, info in ipairs(redis.call('XINFO', 'GROUPS', KEYS[4])) do
    local name; local delivered
    for i = 1, #info, 2 do
     if info[i] == 'name' then name = info[i+1] end
     if info[i] == 'last-delivered-id' then delivered = info[i+1] end
    end
    if name == ARGV[3] then outstanding = idLess(delivered, lease.jobId) end
   end
  end
 elseif tonumber(lease.expiresAt) > now then outstanding = true end
 if outstanding then
  redis.call('ZADD', KEYS[2], now + math.min(config.intervalMs, 1000), ARGV[1])
  return 'busy'
 end
end
-- Reserve an explicit stream ID before XADD. A Lua runtime failure after XADD
-- cannot lose the link to an already queued job and enqueue a duplicate on retry.
local last
local info = redis.call('XINFO', 'STREAM', KEYS[4])
for i = 1, #info, 2 do
 if info[i] == 'last-generated-id' then last = info[i+1]; break end
end
local millis = string.format('%.0f', now)
local lm, ls = string.match(last, '(%d+)%-(%d+)')
local function increment(s)
 local carry = 1; local result = ''
 for i = #s, 1, -1 do
  local digit = tonumber(string.sub(s, i, i)) + carry
  if digit == 10 then digit = 0; carry = 1 else carry = 0 end
  result = tostring(digit) .. result
 end
 if carry == 1 then result = '1' .. result end
 return result
end
local sequence = '0'
if not compareNumber(lm, millis) then millis = lm; sequence = increment(ls) end
local id = millis .. '-' .. sequence
-- Acked jobs are never deleted elsewhere; keep only ARGV[7] ms of history so Redis memory stays flat.
redis.call('XTRIM', KEYS[4], 'MINID', string.format('%.0f', now - tonumber(ARGV[7])))
redis.call('HSET', KEYS[3], ARGV[1], cjson.encode({jobId=id, expiresAt=now+tonumber(ARGV[6])}))
redis.call('XADD', KEYS[4], id, 'streamId', config.streamId, 'streamUrl', config.streamUrl,
 'serverIp', config.serverIp, 'createdAt', ARGV[4])
redis.call('ZADD', KEYS[2], now + config.intervalMs + tonumber(ARGV[5]), ARGV[1])
return 'queued'`;

const APPLY = TYPES + `
check(KEYS[1], 'hash'); check(KEYS[2], 'zset'); check(KEYS[3], 'hash')
local previous = redis.call('HGET', KEYS[3], ARGV[1])
local incoming = ARGV[2]
local metadata
if previous then
 metadata = cjson.decode(previous)
 local version = metadata.version
 if #version > #incoming or (#version == #incoming and version > incoming) then return 'stale' end
 if version == incoming and (metadata.id ~= ARGV[3] or metadata.config ~= ARGV[4] or metadata.deleted ~= (ARGV[5] == '1')) then
  error('Conflicting inventory projection at the same version')
 end
end
local deleted = ARGV[5] == '1'
local config = cjson.decode(ARGV[4])
local current = redis.call('HGET', KEYS[1], ARGV[1])
local due = redis.call('ZSCORE', KEYS[2], ARGV[1])
if metadata and metadata.version == incoming then
 if deleted and not current and not due then return 'applied' end
 if not deleted and current == ARGV[4] and ((config.enabled and due) or (not config.enabled and not due)) then return 'applied' end
end
if not deleted and not current and redis.call('HLEN', KEYS[1]) >= 10000 then error('Probe inventory capacity exceeded') end
-- Fence older workers before changing the projection. Equal-version retries repair
-- incomplete applications; ENQUEUE checks this metadata against its configuration.
redis.call('HSET', KEYS[3], ARGV[1], cjson.encode({version=incoming, id=ARGV[3], config=ARGV[4], deleted=deleted}))
if deleted then
 redis.call('HDEL', KEYS[1], ARGV[1]); redis.call('ZREM', KEYS[2], ARGV[1])
else
 redis.call('HSET', KEYS[1], ARGV[1], ARGV[4])
 if config.enabled then redis.call('ZADD', KEYS[2], now + tonumber(ARGV[6]), ARGV[1])
 else redis.call('ZREM', KEYS[2], ARGV[1]) end
end
return 'applied'`;

// shortcut: jobs older than 1 h are trimmed even if never acked (the consumer logs and skips them).
// ~20 jobs/s × 250 B × 3600 s ≈ 18 MB per 200 streams; must stay longer than the lease so a live job is never trimmed.
export const JOB_RETENTION_MS = 60 * 60 * 1000;
export class RedisProbeScheduleStore implements StreamInventory, ProbeJobProducer, StreamScheduleProjection {
  readonly keys: string[];
  private active = 0;
  constructor(private readonly redis: Redis, namespace = 'default', readonly groupName = 'hls-workers', private readonly leaseMs = 120_000) {
    if (!/^[\w-]{1,80}$/.test(namespace) || !groupName || !Number.isSafeInteger(leaseMs) || leaseMs < 1000 || leaseMs >= JOB_RETENTION_MS) throw new RangeError('Invalid probe store configuration');
    this.keys = ['inventory', 'due', 'leases', 'versions'].map(name => `hls:probes:${namespace}:${name}`);
  }
  identity(serverIp: string, streamId: string): string { return createHash('sha256').update(JSON.stringify([serverIp, streamId])).digest('hex'); }
  private async evaluate(...args: Parameters<Redis['eval']>): Promise<unknown> {
    if (this.active >= 100) throw new Error('Probe command capacity exceeded');
    this.active++;
    const work = Promise.resolve().then(() => this.redis.eval(...args));
    void work.then(() => { this.active--; }, () => { this.active--; });
    return withOperationTimeout(work, 5000, 'Probe schedule command');
  }
  async upsert(value: StreamProbeRegistration, initialJitterMs = 0): Promise<void> {
    const config = normalizeProbeConfig(value);
    if (!Number.isSafeInteger(initialJitterMs) || initialJitterMs < 0 || initialJitterMs > config.intervalMs) throw new RangeError('Invalid initial jitter');
    await this.evaluate(UPSERT, 3, ...this.keys.slice(0, 2), this.keys[3]!, this.identity(config.serverIp, config.streamId), JSON.stringify(config), config.enabled ? '1' : '0', initialJitterMs);
  }
  async get(serverIp: string, streamId: string): Promise<StreamProbeConfig | null> {
    const value = await this.evaluate("return redis.call('HGET', KEYS[1], ARGV[1])", 1, this.keys[0]!, this.identity(serverIp, streamId)) as string | null;
    return value ? normalizeProbeConfig(JSON.parse(value) as StreamProbeRegistration) : null;
  }
  async list(): Promise<StreamProbeConfig[]> {
    const values = await this.evaluate("return redis.call('HVALS', KEYS[1])", 1, this.keys[0]!) as string[];
    return values.map(value => normalizeProbeConfig(JSON.parse(value) as StreamProbeRegistration));
  }
  /** Pre-DB registrations only: entries without projection version metadata. */
  async legacy(): Promise<StreamProbeConfig[]> {
    const values = await this.evaluate(`local result = {}
for _, id in ipairs(redis.call('HKEYS', KEYS[1])) do
 if redis.call('HEXISTS', KEYS[2], id) == 0 then table.insert(result, redis.call('HGET', KEYS[1], id)) end
end
return result`, 2, this.keys[0]!, this.keys[3]!) as string[];
    return values.map(value => normalizeProbeConfig(JSON.parse(value) as StreamProbeRegistration));
  }
  async due(limit: number): Promise<DueProbe[]> {
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 500) throw new RangeError('Invalid due probe limit');
    const values = await this.evaluate(DUE, 2, ...this.keys.slice(0, 2), limit) as [string, string][];
    return values.map(([value, time]) => ({ config: normalizeProbeConfig(JSON.parse(value) as StreamProbeRegistration), nextCheckAt: Number(time) }));
  }
  async apply(record: Pick<StreamRegistration, 'id' | 'config' | 'version' | 'deleted'>): Promise<'applied' | 'stale'> {
    if (!Number.isSafeInteger(record.version) || record.version < 1) throw new RangeError('Invalid projection version');
    const config = normalizeProbeConfig({ ...record.config, enabled: record.deleted ? false : record.config.enabled });
    const identity = this.identity(config.serverIp, config.streamId);
    // Deterministic staggering spreads admission without random startup bursts.
    const offset = Number.parseInt(identity.slice(0, 12), 16) % config.intervalMs;
    return await this.evaluate(APPLY, 3, ...this.keys.slice(0, 2), this.keys[3]!, identity, String(record.version), record.id,
      JSON.stringify(config), record.deleted ? '1' : '0', offset) as 'applied' | 'stale';
  }
  async enqueueDue(config: StreamProbeConfig, jitterMs: number): Promise<'queued' | 'busy' | 'not-due' | 'changed'> {
    if (!Number.isSafeInteger(jitterMs) || jitterMs < 0 || jitterMs > config.intervalMs) throw new RangeError('Invalid probe jitter');
    return await this.evaluate(ENQUEUE, 5, ...this.keys.slice(0, 3), `hls:streams:${config.serverIp}`, this.keys[3]!, this.identity(config.serverIp, config.streamId), JSON.stringify(config), this.groupName, new Date().toISOString(), jitterMs, this.leaseMs, JOB_RETENTION_MS) as 'queued' | 'busy' | 'not-due' | 'changed';
  }
}
