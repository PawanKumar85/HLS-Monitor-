import { createHash } from "node:crypto";
import type { Redis } from "ioredis";
import type { DeadLetterArchiveSource, FailureLedger, HlsDeadLetter, MessageContext, ProcessingFailure, RetrySettings, RetryState } from "../dead-letter.types.js";
import { redactError, redactFields, redactUrl } from "../redact.js";

// TYPE preflights avoid an XACK after a failed XADD. Lua/Redis do not roll back
// commands on runtime errors; source ACK is deliberately AFTER successful XADD.
const types = `
local function check(key, wanted)
  local t = redis.call('TYPE', key).ok
  if t ~= 'none' and t ~= wanted then error('WRONGTYPE DLQ dependency') end
end
`;

const recordFailure = types + `
check(KEYS[1], 'hash')
local state = redis.call('HGET', KEYS[1], 'state')
if state == 'dead_lettered' then return redis.call('HGET', KEYS[1], 'record') end
local r = cjson.decode(ARGV[1])
local count = redis.call('HINCRBY', KEYS[1], 'attempts', 1)
local first = redis.call('HGET', KEYS[1], 'firstFailedAt') or r.firstFailedAt
local generation = tonumber(redis.call('HGET', KEYS[1], 'generation') or '0')
r.attempts = count; r.firstFailedAt = first; r.generation = generation
r.revision = redis.call('HINCRBY', KEYS[1], 'revision', 1)
r.failureId = r.failureId .. ':' .. generation
local delay = math.min(tonumber(ARGV[3]), tonumber(ARGV[2]) * 2 ^ math.min(30, count - 1))
redis.call('HSET', KEYS[1], 'state', 'retry', 'firstFailedAt', first, 'record', cjson.encode(r), 'nextRetryAt', tonumber(ARGV[4]) + delay)
return cjson.encode(r)
`;

const transfer = types + `
check(KEYS[1], 'stream'); check(KEYS[2], 'stream'); check(KEYS[3], 'hash'); check(KEYS[4], 'set')
local record = cjson.decode(ARGV[3])
local generation = tonumber(redis.call('HGET', KEYS[3], 'generation') or '0')
if record.generation ~= generation then error('Stale DLQ transfer generation') end
if redis.call('HGET', KEYS[3], 'state') == 'dead_lettered' then
  redis.call('XACK', KEYS[1], ARGV[1], ARGV[2]); return 'already'
end
local pending = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1)
if #pending == 0 then return 'not-pending' end
redis.call('SADD', KEYS[4], ARGV[4])
local id = redis.call('XADD', KEYS[2], '*', 'failure', ARGV[3])
redis.call('HSET', KEYS[3], 'state', 'dead_lettered', 'dlqId', id, 'record', ARGV[3])
redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
return id
`;

const clear = types + `
check(KEYS[1], 'stream')
for i = 2, #KEYS do check(KEYS[i], 'hash') end
for i = 2, #KEYS do
  local key = KEYS[i]
  local state = redis.call('HGET', key, 'state')
  if state ~= 'dead_lettered' then
    local record = redis.call('HGET', key, 'record')
    if record and tonumber(redis.call('HGET', key, 'generation') or '0') > 0 then
      local r = cjson.decode(record); r.resolution = 'RESOLVED'; r.updatedAt = ARGV[1]
      r.revision = redis.call('HINCRBY', key, 'revision', 1)
      redis.call('XADD', KEYS[1], '*', 'failure', cjson.encode(r))
    end
    redis.call('DEL', key)
  end
end
return 1
`;

const replay = types + `
check(KEYS[1], 'stream'); check(KEYS[2], 'stream'); check(KEYS[3], 'hash')
local state = redis.call('HGET', KEYS[3], 'state')
if state == 'replaying' then return 0 end
if state ~= 'dead_lettered' and state ~= 'replay_requested' then error('Message is not dead-lettered') end
local original = redis.call('XRANGE', KEYS[1], ARGV[2], ARGV[2])
if #original == 0 then error('Original message no longer exists; explicit repair is required') end
if state == 'dead_lettered' then
  local r = cjson.decode(redis.call('HGET', KEYS[3], 'record'))
  r.generation = tonumber(redis.call('HGET', KEYS[3], 'generation') or '0') + 1
  r.failureId = KEYS[3] .. ':' .. r.generation
  r.attempts = 0; r.resolution = 'REPLAY_REQUESTED'; r.updatedAt = ARGV[3]
  r.revision = tonumber(redis.call('HGET', KEYS[3], 'revision') or '0') + 1
  local encoded = cjson.encode(r)
  -- Record the intent BEFORE touching the source PEL. If XADD fails, no replay
  -- was scheduled and the existing dead-lettered state remains safe to retry.
  redis.call('XADD', KEYS[2], '*', 'failure', encoded)
  redis.call('HSET', KEYS[3], 'state', 'replay_requested', 'generation', r.generation,
    'revision', r.revision, 'attempts', 0, 'nextRetryAt', 0, 'record', encoded)
end
redis.call('HDEL', KEYS[3], 'firstFailedAt')
local pending = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1)
if #pending == 0 then
  local claimed = redis.call('XCLAIM', KEYS[1], ARGV[1], 'dlq-replay', 0, ARGV[2], 'FORCE', 'JUSTID')
  if #claimed == 0 then error('Could not schedule replay') end
end
-- A failure after XCLAIM leaves replay_requested, never dead_lettered. Recovery
-- can process that pending entry; an operator retry resumes without a new audit.
redis.call('HSET', KEYS[3], 'state', 'replaying')
return 1
`;

export class RedisDeadLetterService implements FailureLedger, DeadLetterArchiveSource {
  readonly registryKey = "hls:dlq:servers";
  readonly archiveGroup = "hls-dlq-archive";

  private readonly archiveCursors = new Map<string, string>();

  constructor(
    private readonly redis: Redis,
    private readonly now = () => new Date()
  ) { }

  metadataKey(stream: string, group: string, id: string): string {
    return 'hls:retry:' + createHash('sha256').update(JSON.stringify([stream, group, id])).digest('hex');
  }

  async get(context: MessageContext): Promise<RetryState | null> {
    const data = await this.redis.hgetall(this.metadataKey(context.originalStreamKey, context.groupName, context.originalMessageId));
    if (!data['record']) return null;
    return { state: data['state'] as RetryState['state'], nextRetryAt: Number(data['nextRetryAt'] ?? 0), record: JSON.parse(data['record']) as HlsDeadLetter };
  }

  async recordFailure(context: MessageContext, failure: ProcessingFailure, settings: RetrySettings): Promise<RetryState> {
    const time = this.now();
    const rawFields = redactFields(context.rawFields);
    const fields = Object.fromEntries(Array.from({ length: Math.floor(rawFields.length / 2) }, (_, i) => [rawFields[i * 2]!, rawFields[i * 2 + 1]!]));
    const key = this.metadataKey(context.originalStreamKey, context.groupName, context.originalMessageId);
    const record: HlsDeadLetter = {
      ...context, rawFields, failureId: key, failureStage: failure.stage,
      errorCode: failure.code, errorMessage: redactError(failure.message), attempts: 0,
      firstFailedAt: time.toISOString(), lastFailedAt: time.toISOString(), updatedAt: time.toISOString(),
      retryable: failure.retryable, bounded: failure.bounded, generation: 0, revision: 0, resolution: 'OPEN',
      ...(fields['streamId'] ? { streamId: fields['streamId'] } : {}),
      ...(fields['streamUrl'] ? { streamUrl: redactUrl(fields['streamUrl']) } : {}),
    };
    await this.redis.eval(recordFailure, 1, key, JSON.stringify(record), settings.baseDelayMs, settings.maxDelayMs, time.getTime());
    return (await this.get(context))!;
  }

  async moveToDeadLetter(context: MessageContext, record: HlsDeadLetter): Promise<void> {
    await this.redis.eval(transfer, 4, context.originalStreamKey, `hls:dlq:${context.serverIp}`,
      this.metadataKey(context.originalStreamKey, context.groupName, context.originalMessageId), this.registryKey,
      context.groupName, context.originalMessageId, JSON.stringify(record), context.serverIp);
  }

  async clearSucceeded(stream: string, group: string, ids: readonly string[]): Promise<void> {
    if (!ids.length) return;
    const server = stream.slice('hls:streams:'.length);
    const keys = [`hls:dlq:${server}`, ...ids.map((id) => this.metadataKey(stream, group, id))];
    await this.redis.eval(clear, keys.length, ...keys, this.now().toISOString());
  }

  /** Operator-only. Same Redis entry re-enters the PEL; recovery picks it up after minIdle. */
  async replay(context: Pick<MessageContext, 'originalStreamKey' | 'originalMessageId' | 'groupName' | 'serverIp'>): Promise<boolean> {
    if (context.originalStreamKey !== `hls:streams:${context.serverIp}`) throw new Error('Replay server does not match original stream');
    const result = await this.redis.eval(replay, 3, context.originalStreamKey, `hls:dlq:${context.serverIp}`,
      this.metadataKey(context.originalStreamKey, context.groupName, context.originalMessageId),
      context.groupName, context.originalMessageId, this.now().toISOString());
    return Number(result) === 1;
  }
  async servers(): Promise<string[]> { return this.redis.smembers(this.registryKey); }
  async readArchiveBatch(serverIp: string, consumer: string, count: number) {
    if (!Number.isSafeInteger(count) || count < 1) throw new RangeError('Archive count must be a positive safe integer');
    const key = `hls:dlq:${serverIp}`;
    try { await this.redis.xgroup('CREATE', key, this.archiveGroup, '0', 'MKSTREAM'); }
    catch (error) { if (!(error instanceof Error && error.message.includes('BUSYGROUP'))) throw error; }
    const claimed = await this.redis.xautoclaim(key, this.archiveGroup, consumer, 60_000, this.archiveCursors.get(key) ?? '0-0', 'COUNT', count) as [string, [string, string[]][]];
    this.archiveCursors.set(key, claimed[0]);
    const entries = new Map<string, string[]>(claimed[1]);
    for (const cursor of ['0', '>']) {
      const remaining = count - entries.size;
      if (remaining <= 0) break;
      const result = await this.redis.xreadgroup('GROUP', this.archiveGroup, consumer, 'COUNT', remaining, 'STREAMS', key, cursor) as [string, [string, string[]][]][] | null;
      for (const [id, fields] of result?.[0]?.[1] ?? []) entries.set(id, fields);
    }
    return [...entries].map(([id, fields]) => ({ id, record: JSON.parse(fields[fields.indexOf('failure') + 1]!) as HlsDeadLetter }));
  }

  async archiveCompleted(serverIp: string, id: string): Promise<void> {
    // MongoDB already holds the event. Trim only this confirmed archive entry.
    await this.redis.eval("redis.call('XACK', KEYS[1], ARGV[1], ARGV[2]); return redis.call('XDEL', KEYS[1], ARGV[2])", 1, `hls:dlq:${serverIp}`, this.archiveGroup, id);
  }
}
