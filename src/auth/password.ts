import { randomBytes, scrypt as scryptCallback, timingSafeEqual, type ScryptOptions } from 'node:crypto';

const scrypt = (password: string, salt: Buffer, length: number, options: ScryptOptions) =>
  new Promise<Buffer>((resolve, reject) => scryptCallback(password, salt, length, options, (error, key) => error ? reject(error) : resolve(key)));
const N = 16384, R = 8, P = 1, LENGTH = 32;
const options = (n: number, r: number, p: number): ScryptOptions => ({ N: n, r, p, maxmem: 128 * n * r * 2 });

/** Stored as `scrypt$N$r$p$salt$hash` (base64), so cost can rise later without breaking old hashes. */
export async function hashPassword(password: string): Promise<string> {
  if (typeof password !== 'string' || password.length < 12 || password.length > 1024) throw new RangeError('Password must be 12 to 1024 characters');
  const salt = randomBytes(16);
  const hash = await scrypt(password.normalize('NFKC'), salt, LENGTH, options(N, R, P));
  return `scrypt$${N}$${R}$${P}$${salt.toString('base64')}$${hash.toString('base64')}`;
}

export async function verifyPassword(password: string, stored: string): Promise<boolean> {
  if (typeof stored !== 'string' || stored.length > 256) return false;
  const parts = /^scrypt\$(\d+)\$(\d+)\$(\d+)\$([A-Za-z0-9+/=]+)\$([A-Za-z0-9+/=]+)$/.exec(stored);
  if (!parts || typeof password !== 'string' || password.length > 1024) return false;
  const [n, r, p] = [Number(parts[1]), Number(parts[2]), Number(parts[3])];
  if (![n, r, p].every(Number.isSafeInteger) || n < 1024 || n > (1 << 20) || (n & (n - 1)) !== 0 || r < 1 || r > 32 || p < 1 || p > 16 || 128 * n * r > 64 * 1024 * 1024) return false;
  const expected = Buffer.from(parts[5]!, 'base64');
  const salt = Buffer.from(parts[4]!, 'base64');
  if (salt.length !== 16 || expected.length !== LENGTH || salt.toString('base64') !== parts[4] || expected.toString('base64') !== parts[5]) return false;
  try {
    const actual = await scrypt(password.normalize('NFKC'), salt, LENGTH, options(n, r, p));
    return timingSafeEqual(actual, expected);
  } catch { return false; }
}

/** A real hash of a random password: checking unknown usernames against it takes as long as a real check. */
export const DUMMY_HASH = hashPassword(randomBytes(24).toString('base64'));
