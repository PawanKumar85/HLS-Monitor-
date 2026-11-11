import { randomUUID } from 'node:crypto';
import { MongoServerError, type Db, type Filter } from 'mongodb';
import type { MongoConfig } from '../../config/mongo.config.js';
import type { StreamProbeConfig } from '../../monitoring/probe-contracts.js';
import { InventoryConflictError, InventoryVersionError, type StreamRegistration, type StreamInventoryRepository } from '../stream-inventory.js';
interface Document extends StreamRegistration { namespace: string }
const initialized = new WeakMap<Db, Promise<unknown>>();
export class MongoStreamInventoryRepository implements StreamInventoryRepository {
  constructor(private readonly connection: Pick<MongoConfig, 'getDb'>, private readonly namespace = 'default', private readonly now = () => new Date()) {
    if (!/^[\w-]{1,80}$/.test(namespace)) throw new Error('Invalid inventory namespace');
  }
  async initialize(): Promise<void> {
    const db = this.connection.getDb();
    let pending = initialized.get(db);
    if (!pending) {
      pending = db.collection<Document>('hls_stream_inventory').createIndexes([
        { key: { namespace: 1, 'config.serverIp': 1, 'config.streamId': 1 }, unique: true },
        { key: { namespace: 1, id: 1 }, unique: true },
        { key: { namespace: 1, deleted: 1, id: 1 } },
        { key: { namespace: 1, pendingSync: 1, nextSyncAt: 1, id: 1 } },
      ]);
      initialized.set(db, pending);
    }
    try { await pending; } catch (error) { initialized.delete(db); throw error; }
  }
  private async collection() { await this.initialize(); return this.connection.getDb().collection<Document>('hls_stream_inventory'); }
  async get(id: string) { return (await this.collection()).findOne({ namespace: this.namespace, id }, { timeoutMS: 5000 }); }
  async findIdentity(serverIp: string, streamId: string) {
    return (await this.collection()).findOne({ namespace: this.namespace, 'config.serverIp': serverIp, 'config.streamId': streamId }, { timeoutMS: 5000 });
  }
  async create(config: StreamProbeConfig): Promise<StreamRegistration> {
    const collection = await this.collection();
    const previous = await this.findIdentity(config.serverIp, config.streamId);
    const time = this.now();
    if (previous) {
      if (!previous.deleted) throw new InventoryConflictError('Stream is already registered');
      if (previous.version >= Number.MAX_SAFE_INTEGER) throw new InventoryVersionError('Stream version exhausted');
      const updated = await collection.findOneAndUpdate({ namespace: this.namespace, id: previous.id, version: previous.version, deleted: true }, {
        $set: { config, deleted: false, pendingSync: true, syncAttempts: 0, nextSyncAt: time, updatedAt: time },
        $inc: { version: 1 }, $unset: { syncedAt: '' },
      }, { returnDocument: 'after', includeResultMetadata: false, writeConcern: { w: 'majority' }, timeoutMS: 5000 });
      if (!updated) throw new InventoryConflictError('Stream registration changed; retry');
      return updated;
    }
    const record: Document = { namespace: this.namespace, id: randomUUID(), config, version: 1, deleted: false, pendingSync: true,
      syncAttempts: 0, nextSyncAt: time, createdAt: time, updatedAt: time };
    try { await collection.insertOne(record, { writeConcern: { w: 'majority' }, timeoutMS: 5000 }); }
    catch (error) { if (error instanceof MongoServerError && error.code === 11000) throw new InventoryConflictError('Stream is already registered'); throw error; }
    return record;
  }
  async update(id: string, version: number, config: StreamProbeConfig, deleted: boolean) {
    if (!Number.isSafeInteger(version) || version < 1 || version >= Number.MAX_SAFE_INTEGER) throw new InventoryVersionError('Invalid stream version');
    const time = this.now();
    return (await this.collection()).findOneAndUpdate({ namespace: this.namespace, id, version, deleted: false }, {
      $set: { config, deleted, pendingSync: true, syncAttempts: 0, nextSyncAt: time, updatedAt: time }, $inc: { version: 1 }, $unset: { syncedAt: '' },
    }, { returnDocument: 'after', includeResultMetadata: false, writeConcern: { w: 'majority' }, timeoutMS: 5000 });
  }
  async list(limit: number, cursor?: string) {
    const filter: Filter<Document> = { namespace: this.namespace, deleted: false, ...(cursor ? { id: { $gt: cursor } } : {}) };
    const items = await (await this.collection()).find(filter, { timeoutMS: 5000 }).sort({ id: 1 }).limit(limit + 1).toArray();
    const hasMore = items.length > limit;
    if (hasMore) items.pop();
    return { items, ...(hasMore ? { nextCursor: items.at(-1)!.id } : {}) };
  }
  async pending(limit: number, now: Date) {
    return (await this.collection()).find({ namespace: this.namespace, pendingSync: true, nextSyncAt: { $lte: now } }, { timeoutMS: 5000 })
      .sort({ nextSyncAt: 1, id: 1 }).limit(limit).toArray();
  }
  async scan(limit: number, cursor?: string) {
    return (await this.collection()).find({ namespace: this.namespace, ...(cursor ? { id: { $gt: cursor } } : {}) }, { timeoutMS: 5000 }).sort({ id: 1 }).limit(limit).toArray();
  }
  async markSynced(id: string, version: number, now: Date) {
    const result = await (await this.collection()).updateOne({ namespace: this.namespace, id, version, pendingSync: true }, {
      $set: { pendingSync: false, syncedAt: now, syncAttempts: 0 },
    }, { writeConcern: { w: 'majority' }, timeoutMS: 5000 });
    return result.matchedCount === 1;
  }
  async deferSync(id: string, version: number, next: Date) {
    await (await this.collection()).updateOne({ namespace: this.namespace, id, version }, {
      $set: { pendingSync: true, nextSyncAt: next }, $inc: { syncAttempts: 1 },
    }, { writeConcern: { w: 'majority' }, timeoutMS: 5000 });
  }
}
