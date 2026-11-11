import { MongoServerError } from 'mongodb';
import { randomUUID } from 'node:crypto';
import type { MongoConfig } from '../config/mongo.config.js';

/** Every signed-in user can see and change everything; there are no roles. */
export interface User { id: string; username: string; disabled: boolean; passwordHash: string; createdAt: Date; updatedAt: Date; authVersion?: number }
export type SessionUser = Pick<User, 'id' | 'username' | 'authVersion'>;
export interface AccountPatch { username?: string; passwordHash?: string }

export interface UserStore {
  findByUsername(username: string): Promise<User | null>;
  findById(id: string): Promise<User | null>;
  updateAccount?(id: string, expectedPasswordHash: string, patch: AccountPatch): Promise<'updated' | 'conflict' | 'duplicate'>;
}

export function normalizeUsername(value: string): string {
  const username = String(value ?? '').trim().toLowerCase();
  if (!/^[a-z0-9][a-z0-9._-]{2,63}$/.test(username)) throw new RangeError('Username must be 3 to 64 letters, digits, dots, dashes or underscores');
  return username;
}

export class MongoUserStore implements UserStore {
  private indexed: Promise<unknown> | undefined;
  constructor(private readonly connection: Pick<MongoConfig, 'getDb'>) {}
  private async collection() {
    const collection = this.connection.getDb().collection<User>('users');
    this.indexed ??= collection.createIndexes([{ key: { username: 1 }, unique: true }, { key: { id: 1 }, unique: true }]).catch(error => { this.indexed = undefined; throw error; });
    await this.indexed;
    return collection;
  }
  async findByUsername(username: string) {
    let name: string; try { name = normalizeUsername(username); } catch { return null; }
    return (await this.collection()).findOne({ username: name }, { projection: { _id: 0 }, timeoutMS: 5000 });
  }
  async findById(id: string) {
    return typeof id === 'string' ? (await this.collection()).findOne({ id }, { projection: { _id: 0 }, timeoutMS: 5000 }) : null;
  }
  async updateAccount(id: string, expectedPasswordHash: string, patch: AccountPatch): Promise<'updated' | 'conflict' | 'duplicate'> {
    try {
      const result = await (await this.collection()).updateOne({ id, passwordHash: expectedPasswordHash, disabled: false }, {
        $set: { ...(patch.username !== undefined ? { username: normalizeUsername(patch.username) } : {}), ...(patch.passwordHash !== undefined ? { passwordHash: patch.passwordHash } : {}), updatedAt: new Date() },
        ...(patch.passwordHash !== undefined ? { $inc: { authVersion: 1 } } : {}),
      });
      return result.matchedCount === 1 ? 'updated' : 'conflict';
    } catch (error) {
      if (error instanceof MongoServerError && error.code === 11000) return 'duplicate';
      throw error;
    }
  }
  /** Creates the user, or resets its password and re-enables it. Drops the old `role` field, if any. */
  async upsert(username: string, passwordHash: string): Promise<'created' | 'updated'> {
    const now = new Date();
    const result = await (await this.collection()).updateOne({ username: normalizeUsername(username) },
      { $set: { passwordHash, disabled: false, updatedAt: now }, $inc: { authVersion: 1 }, $unset: { role: '' }, $setOnInsert: { id: randomUUID(), createdAt: now } }, { upsert: true });
    return result.upsertedCount ? 'created' : 'updated';
  }
  async setDisabled(username: string, disabled: boolean): Promise<boolean> {
    const result = await (await this.collection()).updateOne({ username: normalizeUsername(username) }, { $set: { disabled, updatedAt: new Date() } });
    return result.matchedCount === 1;
  }
  async list() {
    return (await this.collection()).find({}, { projection: { _id: 0, passwordHash: 0 } }).sort({ username: 1 }).toArray();
  }
}
