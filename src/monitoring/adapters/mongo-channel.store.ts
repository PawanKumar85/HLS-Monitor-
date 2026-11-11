import { MongoServerError, type Db } from 'mongodb';
import type { ChannelStore } from '../../config/pipeline-channels.js';
import type { ChannelTopology } from '../../types/pipeline.types.js';

type Doc = Omit<ChannelTopology, 'triggerId'> & { _id: string; updatedAt: Date };
const toChannel = ({ _id, channelName, nodes, edges }: Doc): ChannelTopology => ({ triggerId: _id, channelName, nodes, edges });

/**
 * Pipeline channels entered in the dashboard. One document per channel, _id = triggerId.
 * shortcut: edits are last-write-wins; add a version check if several people edit one channel at once.
 */
export class MongoChannelStore implements ChannelStore {
  constructor(private readonly connection: { getDb(): Db }) {}
  private collection() { return this.connection.getDb().collection<Doc>('pipeline_channels'); }
  private doc(c: ChannelTopology): Doc { return { _id: c.triggerId, channelName: c.channelName, nodes: c.nodes, edges: c.edges, updatedAt: new Date() }; }

  async list() { return (await this.collection().find({}, { timeoutMS: 5000 }).sort({ channelName: 1 }).toArray()).map(toChannel); }
  async get(triggerId: string) { const d = await this.collection().findOne({ _id: triggerId }, { timeoutMS: 5000 }); return d ? toChannel(d) : null; }
  async create(channel: ChannelTopology) {
    try { await this.collection().insertOne(this.doc(channel), { timeoutMS: 5000 }); return true; }
    catch (error) { if (error instanceof MongoServerError && error.code === 11000) return false; throw error; }
  }
  async replace(channel: ChannelTopology) { return (await this.collection().replaceOne({ _id: channel.triggerId }, this.doc(channel), { timeoutMS: 5000 })).matchedCount === 1; }
  async remove(triggerId: string) { return (await this.collection().deleteOne({ _id: triggerId }, { timeoutMS: 5000 })).deletedCount === 1; }
}
