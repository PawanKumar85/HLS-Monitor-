import type { Db } from 'mongodb';
import { targetKey, type ProbeTarget, type ProbeTargetStore } from '../../config/pipeline-probes.js';

type Doc = { _id: string; triggerId: string; nodeId: string; target: ProbeTarget; updatedAt: Date; updatedBy: string | null };

/** Pipeline probe targets entered in the dashboard. One document per hop, _id = "<triggerId>/<nodeId>". */
export class MongoProbeTargetStore implements ProbeTargetStore {
  constructor(private readonly connection: { getDb(): Db }) {}
  private collection() { return this.connection.getDb().collection<Doc>('pipeline_probe_targets'); }

  async all(): Promise<Map<string, ProbeTarget>> {
    const docs = await this.collection().find({}, { projection: { target: 1 }, timeoutMS: 5000 }).toArray();
    return new Map(docs.map(d => [d._id, d.target]));
  }

  async set(triggerId: string, nodeId: string, target: ProbeTarget, by: string | null): Promise<void> {
    await this.collection().replaceOne({ _id: targetKey(triggerId, nodeId) },
      { triggerId, nodeId, target, updatedAt: new Date(), updatedBy: by }, { upsert: true, timeoutMS: 5000 });
  }

  async remove(triggerId: string, nodeId: string): Promise<boolean> {
    return (await this.collection().deleteOne({ _id: targetKey(triggerId, nodeId) }, { timeoutMS: 5000 })).deletedCount === 1;
  }
}
