import { MongoServerError, type Db } from 'mongodb';
import type { IncidentStore, PipelineIncident } from '../pipeline-monitor.js';

type Doc = PipelineIncident & { open?: true };
const RESOLVED_RETENTION_SECONDS = 7 * 24 * 60 * 60;

/**
 * Pipeline incidents in MongoDB. A unique index over open incidents makes "open" race-safe across
 * instances, so each incident is opened and announced once. Resolved incidents expire after 7 days.
 */
export class MongoPipelineIncidentStore implements IncidentStore {
  private indexed: Promise<unknown> | undefined;
  constructor(private readonly connection: { getDb(): Db }) {}

  private async collection() {
    const collection = this.connection.getDb().collection<Doc>('pipeline_incidents');
    this.indexed ??= collection.createIndexes([
      { key: { key: 1 }, name: 'one_open_per_key', unique: true, partialFilterExpression: { open: true } },
      { key: { resolvedAt: 1 }, name: 'resolved_ttl', expireAfterSeconds: RESOLVED_RETENTION_SECONDS },
      { key: { openedAt: -1 } },
    ]).catch(error => { this.indexed = undefined; throw error; });
    await this.indexed;
    return collection;
  }

  async open(incident: Omit<PipelineIncident, 'openedAt' | 'resolvedAt'>, at: Date): Promise<boolean> {
    try {
      const result = await (await this.collection()).updateOne({ key: incident.key, open: true },
        { $set: { title: incident.title, channels: incident.channels }, $setOnInsert: { kind: incident.kind, openedAt: at, resolvedAt: null } },
        { upsert: true, timeoutMS: 5000 });
      return result.upsertedCount === 1;
    } catch (error) {
      if (error instanceof MongoServerError && error.code === 11000) return false; // another instance opened it first
      throw error;
    }
  }

  async resolve(key: string, at: Date): Promise<PipelineIncident | null> {
    const doc = await (await this.collection()).findOneAndUpdate({ key, open: true }, { $unset: { open: '' }, $set: { resolvedAt: at } },
      { returnDocument: 'after', projection: { _id: 0, open: 0 }, timeoutMS: 5000 });
    return doc;
  }

  async openKeys(): Promise<string[]> {
    return (await (await this.collection()).find({ open: true }, { projection: { key: 1 }, timeoutMS: 5000 }).toArray()).map(d => d.key);
  }

  async list(since: Date): Promise<PipelineIncident[]> {
    return (await this.collection()).find({ $or: [{ open: true }, { openedAt: { $gte: since } }] }, { projection: { _id: 0, open: 0 }, timeoutMS: 5000 })
      .sort({ openedAt: -1 }).limit(200).toArray();
  }
}
