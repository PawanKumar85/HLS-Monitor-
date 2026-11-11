import { withOperationTimeout } from "../operation-timeout.js";
import { randomUUID } from 'node:crypto';
import type { Redis } from 'ioredis';
import type { HlsStatusEvent } from '../../types/hls-status.types.js';
import { parseCursor, type RecordedStatus, type StatusEventStore, type StatusReplay, type StatusSnapshot } from '../status-event-store.js';

// One atomic operation establishes the sequence and its corresponding replay record.
const APPEND = `
local epoch = redis.call('GET', KEYS[1])
if not epoch then
 epoch = ARGV[1]
 redis.call('SET', KEYS[1], epoch)
 redis.call('DEL', KEYS[2], KEYS[3], KEYS[4])
end
local n = tonumber(redis.call('GET', KEYS[2]) or '0')
if n >= 9007199254740991 then return redis.error_reply('Status sequence exhausted') end
local previous = redis.call('HGET', KEYS[4], ARGV[4])
if not previous and redis.call('HLEN', KEYS[4]) >= tonumber(ARGV[5]) then
 return redis.error_reply('Status channel capacity exceeded')
end
n = n + 1
local event = cjson.decode(ARGV[2])
event.epoch = epoch
event.sequence = n
event.cursor = epoch .. ':' .. string.format('%.0f', n)
redis.call('XADD', KEYS[3], 'MAXLEN', ARGV[3], string.format('%.0f', n)..'-0', 'event', cjson.encode(event))
redis.call('SET', KEYS[2], string.format('%.0f', n))
if not previous or cjson.decode(previous).checkedAt <= event.data.checkedAt then
 redis.call('HSET', KEYS[4], ARGV[4], cjson.encode(event.data))
end
return 1`;

const SNAPSHOT = `
local epoch = redis.call('GET', KEYS[1])
if not epoch then
 epoch = ARGV[1]
 redis.call('DEL', KEYS[2], KEYS[3], KEYS[4])
 redis.call('SET', KEYS[1], epoch)
end
return {epoch, redis.call('GET', KEYS[2]) or '0', redis.call('HVALS', KEYS[4])}`;

const READ = `
local epoch = redis.call('GET', KEYS[1])
local n = tonumber(redis.call('GET', KEYS[2]) or '0')
if not epoch or epoch ~= ARGV[1] or tonumber(ARGV[2]) > n then return {'reset'} end
return {epoch, string.format('%.0f', n), redis.call('XRANGE', KEYS[3], '('..ARGV[2]..'-0', '+', 'COUNT', ARGV[3])}`;

export class RedisStatusEventStore implements StatusEventStore {
  readonly keys: string[];
  private activeCommands = 0;

  constructor(private readonly redis: Redis, namespace = process.env['STATUS_FEED_NAMESPACE'] ?? 'default', private readonly maxEvents = 10_000, private readonly maxChannels = 10_000, private readonly commandTimeoutMs = 5000) {
    if (!/^[\w-]{1,80}$/.test(namespace)) throw new Error('Invalid status feed namespace');
    if (![maxEvents, maxChannels].every(n => Number.isSafeInteger(n) && n > 0)) throw new RangeError('Status capacities must be positive integers');
    if (!Number.isSafeInteger(commandTimeoutMs) || commandTimeoutMs < 1 || commandTimeoutMs > 2_147_483_647) throw new RangeError('Invalid status command timeout');
    this.keys = ['epoch', 'sequence', 'events', 'latest'].map(name => `hls:status:{${namespace}}:${name}`);
  }

  private async evaluate(...args: Parameters<Redis['eval']>): Promise<unknown> {
    if (this.activeCommands >= 100) throw new Error('Status command capacity exceeded');
    this.activeCommands++;
    const operation = Promise.resolve().then(() => this.redis.eval(...args));
    // Timed-out commands can remain in the driver. Keep their slots occupied until
    // they really settle so repeated timeouts cannot grow the queue indefinitely.
    void operation.then(() => { this.activeCommands--; }, () => { this.activeCommands--; });
    return withOperationTimeout(operation, this.commandTimeoutMs, 'Redis status command');
  }

  async append(event: HlsStatusEvent): Promise<void> {
    const data = event.data;
    if (event.type !== 'hls:status' || !data.streamId || !data.serverIp || data.streamId.length > 1024 || data.serverIp.length > 256 ||
      ![0, 1].includes(data.status) || !Number.isFinite(data.latencyMs) || data.latencyMs < 0 || !Number.isFinite(Date.parse(data.checkedAt))) throw new Error('Invalid status event');
    const normalized = { ...event, data: { ...data, checkedAt: new Date(data.checkedAt).toISOString() } };
    await this.evaluate(APPEND, 4, ...this.keys, randomUUID(), JSON.stringify(normalized), this.maxEvents, JSON.stringify([data.serverIp, data.streamId]), this.maxChannels);
  }

  async snapshot(): Promise<StatusSnapshot> {
    const [epoch, rawSequence, values] = await this.evaluate(SNAPSHOT, 4, ...this.keys, randomUUID()) as [string, string, string[]];
    const sequence = Number(rawSequence);
    return { epoch, sequence, cursor: `${epoch}:${sequence}`, streams: values.map(value => JSON.parse(value) as HlsStatusEvent['data']) };
  }

  async readAfter(cursor: string, limit: number): Promise<StatusReplay> {
    const parsed = parseCursor(cursor);
    if (!parsed) return { kind: 'reset' };
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 500) throw new RangeError('Invalid replay limit');
    const result = await this.evaluate(READ, 3, ...this.keys.slice(0, 3), parsed.epoch, parsed.sequence, limit) as [string, string, [string, string[]][]];
    if (result[0] === 'reset') return { kind: 'reset' };
    const events = result[2].map(([, fields]) => JSON.parse(fields[1]!) as RecordedStatus);
    let expected = parsed.sequence;
    for (const event of events) if (event.epoch !== parsed.epoch || event.sequence !== ++expected || event.cursor !== `${parsed.epoch}:${expected}`) return { kind: 'reset' };
    if (expected < Number(result[1]) && events.length === 0) return { kind: 'reset' };
    return { kind: 'events', events, head: `${result[0]}:${result[1]}` };
  }
}
