import { ObjectId } from "mongodb";
import type { MongoConfig } from "../../config/mongo.config.js";
import type { FailureHistoryWriter, HlsDeadLetter } from "../dead-letter.types.js";
import { failureView, parseFailureCursor, type FailureEvent, type FailureFeed, type FailureQuery, type FailureSummary, type FailureView } from "../failure-feed.js";
type Stored = HlsDeadLetter & { projectedAt?: Date; resolvedAt?: Date };
/** Resolved failures are deleted this long after they were resolved (TTL on resolvedAt). Open ones are kept. */
export const RESOLVED_FAILURE_RETENTION_SECONDS = 30 * 24 * 60 * 60;
const escapeRegex = (text: string) => text.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
// shortcut: the feed skips writes younger than this so a write whose $$NOW is older than an already-read one
// still gets read. A write slower than 5s to commit can be missed live (it stays in Mongo); upgrade to change streams on a replica set.
const SETTLE_MS = 5000;
const indexes = new WeakMap<object, Promise<unknown>>();
export class MongoFailureHistoryStore implements FailureHistoryWriter, FailureFeed {
  constructor(private readonly connection: Pick<MongoConfig, 'getDb'>, private readonly now = () => Date.now()) {}
  private async collection() {
    const db = this.connection.getDb();
    const collection = db.collection<Stored>('hls_processing_failures');
    let index = indexes.get(db);
    if (!index) {
      index = collection.createIndexes([
        { key: { originalStreamKey: 1, originalMessageId: 1, groupName: 1 }, unique: true },
        { key: { projectedAt: 1, _id: 1 } },
        { key: { resolvedAt: 1 }, name: 'resolved_ttl', expireAfterSeconds: RESOLVED_FAILURE_RETENTION_SECONDS },
        { key: { resolution: 1, failureStage: 1 } },
        { key: { failureId: 1 } },
      ]);
      indexes.set(db, index);
    }
    try { await index; }
    catch (error) { indexes.delete(db); throw error; }
    return collection;
  }
  async write(record: HlsDeadLetter): Promise<void> { await this.writeMany([record]); }

  /** One round trip for a whole archive batch. */
  async writeMany(records: readonly HlsDeadLetter[]): Promise<void> {
    if (!records.length) return;
    const collection = await this.collection();
    await collection.bulkWrite(records.map(record => {
      // An older redelivery must not overwrite a newer replay/resolution event.
      // projectedAt moves only on a new revision, so a re-archived duplicate is not re-sent on the SSE feed.
      const merged = { $mergeObjects: ['$$ROOT', { $literal: record }, {
        projectedAt: { $cond: [{ $lt: [{ $ifNull: ['$revision', -1] }, record.revision] }, '$$NOW', '$projectedAt'] },
      }] };
      // resolvedAt starts the 30-day TTL; a failure that is reopened (replayed, failed again) loses it.
      const next = record.resolution === 'RESOLVED'
        ? { $mergeObjects: [merged, { resolvedAt: { $ifNull: ['$resolvedAt', '$$NOW'] } }] }
        : { $unsetField: { field: 'resolvedAt', input: merged } };
      return { updateOne: {
        filter: { originalStreamKey: record.originalStreamKey, originalMessageId: record.originalMessageId, groupName: record.groupName },
        update: [{ $replaceWith: { $cond: [{ $lte: [{ $ifNull: ['$revision', 0] }, record.revision] }, next, '$$ROOT'] } }],
        upsert: true,
      } };
    }), { ordered: true });
  }

  async summary(): Promise<FailureSummary> {
    const collection = await this.collection();
    const [byResolution, openByStage] = await Promise.all([
      collection.aggregate<{ _id: string; n: number }>([{ $group: { _id: '$resolution', n: { $sum: 1 } } }], { maxTimeMS: 5000 }).toArray(),
      collection.aggregate<{ _id: string; n: number }>([{ $match: { resolution: 'OPEN' } }, { $group: { _id: '$failureStage', n: { $sum: 1 } } }], { maxTimeMS: 5000 }).toArray(),
    ]);
    const toRecord = (rows: { _id: string; n: number }[]) => Object.fromEntries(rows.map(r => [r._id, r.n]));
    return { total: byResolution.reduce((sum, r) => sum + r.n, 0), byResolution: toRecord(byResolution), openByStage: toRecord(openByStage) };
  }

  async search(query: FailureQuery): Promise<{ items: FailureView[]; next?: string }> {
    const collection = await this.collection();
    const limit = Math.min(Math.max(query.limit ?? 50, 1), 200);
    const filter: Record<string, unknown> = { projectedAt: { $exists: true } };
    if (query.resolution) filter['resolution'] = query.resolution;
    if (query.stage) filter['failureStage'] = query.stage;
    const q = query.q?.trim();
    if (q) {
      const pattern = { $regex: escapeRegex(q.slice(0, 200)), $options: 'i' }; // user text, matched literally
      filter['$or'] = ['serverIp', 'streamId', 'errorCode', 'errorMessage', 'failureId'].map(field => ({ [field]: pattern }));
    }
    if (query.before) {
      const before = parseFailureCursor(query.before);
      if (!before) throw new RangeError('Invalid failure cursor');
      const id = new ObjectId(before.id);
      filter['$and'] = [{ $or: [{ projectedAt: { $lt: before.at } }, { projectedAt: before.at, _id: { $lt: id } }] }];
    }
    const docs = await collection.find(filter, { timeoutMS: 5000 }).sort({ projectedAt: -1, _id: -1 }).limit(limit + 1).toArray() as (Stored & { _id: ObjectId })[];
    const page = docs.slice(0, limit);
    const last = page.at(-1);
    return { items: page.map(failureView), ...(docs.length > limit && last ? { next: `${last.projectedAt!.getTime()}-${last._id.toHexString()}` } : {}) };
  }

  async replayTarget(failureId: string) {
    const doc = await (await this.collection()).findOne({ failureId }, { projection: { originalStreamKey: 1, originalMessageId: 1, groupName: 1, serverIp: 1 }, timeoutMS: 5000 });
    return doc ? { originalStreamKey: doc.originalStreamKey, originalMessageId: doc.originalMessageId, groupName: doc.groupName, serverIp: doc.serverIp } : null;
  }
  async readAfter(cursor: string | undefined, limit: number): Promise<FailureEvent[]> {
    const collection = await this.collection();
    const settled = new Date(this.now() - SETTLE_MS);
    const toEvent = (doc: Stored & { _id: ObjectId }): FailureEvent => ({ cursor: `${doc.projectedAt!.getTime()}-${doc._id.toHexString()}`, failure: failureView(doc) });
    if (!cursor) {
      const newest = await collection.find({ projectedAt: { $lte: settled } }, { timeoutMS: 5000 }).sort({ projectedAt: -1, _id: -1 }).limit(limit).toArray();
      return newest.reverse().map(toEvent);
    }
    const after = parseFailureCursor(cursor);
    if (!after) throw new RangeError('Invalid failure cursor');
    const id = new ObjectId(after.id);
    const docs = await collection.find({ projectedAt: { $lte: settled }, $or: [{ projectedAt: { $gt: after.at } }, { projectedAt: after.at, _id: { $gt: id } }] },
      { timeoutMS: 5000 }).sort({ projectedAt: 1, _id: 1 }).limit(limit).toArray();
    return docs.map(toEvent);
  }
}
