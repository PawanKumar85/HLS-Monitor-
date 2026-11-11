import type { Redis } from 'ioredis';

type Callback = (error?: unknown) => void;
type GetCallback = (error: unknown, session?: unknown) => void;
type Session = { cookie?: { expires?: Date | string | null; originalMaxAge?: number | null } };

// Write only if the stored session is still the generation this request read; atomic in Redis.
// @fastify/session keeps the generation in __fastifySessionMetadata (falls back to the session id).
const COMPARE_AND_SET = `
local current = redis.call('GET', KEYS[1])
if not current then return 0 end
local generation = ARGV[3]
local ok, session = pcall(cjson.decode, current)
if ok and type(session) == 'table' and type(session['__fastifySessionMetadata']) == 'table' and session['__fastifySessionMetadata']['generation'] then
  generation = session['__fastifySessionMetadata']['generation']
end
if generation ~= ARGV[1] then return 0 end
redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[4])
return 1`;

/** @fastify/session store in Redis: sessions survive restarts, are shared across instances and end on logout. */
export class RedisSessionStore {
  constructor(private readonly redis: Pick<Redis, 'get' | 'set' | 'del' | 'eval'>, private readonly prefix = 'gate:session:', private readonly fallbackTtlMs = 12 * 3600_000) {}
  private key(id: string) { return `${this.prefix}${id}`; }
  private ttl(session: Session) {
    const expires = session.cookie?.expires ? new Date(session.cookie.expires).getTime() - Date.now() : session.cookie?.originalMaxAge ?? this.fallbackTtlMs;
    return Math.max(1000, Math.min(Number.isFinite(expires) ? expires : this.fallbackTtlMs, 30 * 86_400_000));
  }
  get(id: string, callback: GetCallback): void {
    this.redis.get(this.key(id)).then(value => {
      let session: unknown = null;
      try { session = value ? JSON.parse(value) : null; } catch { session = null; }
      callback(null, session);
    }, callback);
  }
  set(id: string, session: Session, callback: Callback): void {
    this.redis.set(this.key(id), JSON.stringify(session), 'PX', this.ttl(session)).then(() => callback(), callback);
  }
  compareAndSet(id: string, expectedGeneration: string, session: Session, callback: Callback): void {
    this.redis.eval(COMPARE_AND_SET, 1, this.key(id), expectedGeneration, JSON.stringify(session), id, this.ttl(session)).then(result => {
      if (Number(result) === 1) return callback();
      callback(Object.assign(new Error('The session does not exist or has changed'), { code: 'ENOENT' }));
    }, callback);
  }
  destroy(id: string, callback: Callback): void {
    this.redis.del(this.key(id)).then(() => callback(), callback);
  }
}
