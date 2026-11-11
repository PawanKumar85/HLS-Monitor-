import { withOperationTimeout } from "../operation-timeout.js";
import type { FastifyInstance } from 'fastify';
import type { HlsStatusEvent } from '../../types/hls-status.types.js';
import type { StatusTransport } from '../status-transport.js';
import type { StatusEventStore } from '../status-event-store.js';
import { SseClientWriter } from '../sse-client-writer.js';
interface Client {
  writer: SseClientWriter;
  cursor: string;
  ready: boolean;
  busy: boolean;
  replayStartedAt: number;
}

interface Options {
  pollMs?: number;
  heartbeatMs?: number;
  drainTimeoutMs?: number;
  storeTimeoutMs?: number;
  shutdownTimeoutMs?: number;
  maxReplayMs?: number
}

/** Durable replay is scoped to the shared, unfiltered status feed. */
export class SseStatusTransport implements StatusTransport {
  readonly name = 'sse';
  private readonly wakes = new Set<() => void>();
  private pendingPublishes = 0;
  private readonly timing: Required<Options>;
  constructor(private readonly store: StatusEventStore, options: Options = {}) {
    this.timing = { pollMs: 500, heartbeatMs: 15_000, drainTimeoutMs: 5000, maxReplayMs: 30_000, storeTimeoutMs: 5000, shutdownTimeoutMs: 5000, ...options };
    if (!Object.values(this.timing).every(n => Number.isSafeInteger(n) && n > 0 && n <= 2_147_483_647)) throw new RangeError('Invalid SSE timing');
  }

  async publish(event: HlsStatusEvent): Promise<void> {
    if (this.pendingPublishes >= 100) throw new Error('Status publisher busy');
    this.pendingPublishes++;
    const operation = Promise.resolve().then(() => this.store.append(event)).then(() => {
      for (const wake of this.wakes) wake();
    }).finally(() => { this.pendingPublishes--; });
    // A timeout does not free the slot of an underlying append still in progress.
    await withOperationTimeout(operation, this.timing.storeTimeoutMs, 'Status publication');
  }

  async register(app: FastifyInstance): Promise<void> {
    const clients = new Set<Client>();
    const connections = new Set<SseClientWriter>();
    const jobs = new Set<Promise<void>>();
    let closing = false;
    const shutdown = new AbortController();
    let polling: ReturnType<typeof setInterval> | undefined;
    let heartbeat: ReturnType<typeof setInterval> | undefined;
    const stopTimers = () => {
      if (polling) clearInterval(polling);
      if (heartbeat) clearInterval(heartbeat);
      polling = undefined; heartbeat = undefined;
    };
    const resync = async (client: Client, reason: string) => {
      // Remove before awaiting so a heartbeat cannot get queued after this control.
      clients.delete(client);
      if (!clients.size) stopTimers();
      await client.writer.send(`event: hls:resync\ndata: ${JSON.stringify({ reason })}\n\n`);
      await client.writer.end();
    };
    const pump = () => {
      if (closing || !clients.size) return;
      const reads = new Map<string, ReturnType<StatusEventStore['readAfter']>>();
      for (const client of clients) {
        if (client.busy) continue;
        client.busy = true;
        let catchUp = false;
        const job = (async () => {
          try {
            let read = reads.get(client.cursor);
            if (!read) { read = withOperationTimeout(this.store.readAfter(client.cursor, 100), this.timing.storeTimeoutMs, 'Status replay', shutdown.signal); reads.set(client.cursor, read); }
            const result = await read;
            if (!clients.has(client)) return;
            if (result.kind === 'reset') { await resync(client, 'cursor_unavailable'); return; }
            for (const event of result.events) {
              if (!await client.writer.send(`id: ${event.cursor}\nevent: hls:status\ndata: ${JSON.stringify(event)}\n\n`)) return;
              client.cursor = event.cursor;
            }
            catchUp = client.cursor !== result.head;
            if (catchUp) {
              if (!client.replayStartedAt) client.replayStartedAt = Date.now();
              if (Date.now() - client.replayStartedAt >= this.timing.maxReplayMs) {
                await resync(client, 'replay_timeout'); catchUp = false;
              }
            } else {
              client.replayStartedAt = 0;
              if (!client.ready) {
                if (await client.writer.send(`event: hls:ready\ndata: ${JSON.stringify({ cursor: client.cursor })}\n\n`)) client.ready = true;
              }
            }
          } catch { client.writer.close(); }
        })().finally(() => {
          client.busy = false;
          jobs.delete(job);
          // Yield between batches; slow clients do not gate other clients.
          if (catchUp && !closing && clients.has(client)) setImmediate(pump);
        });
        jobs.add(job);
      }
    };
    const startTimers = () => {
      if (polling) return;
      polling = setInterval(pump, this.timing.pollMs);
      heartbeat = setInterval(() => {
        for (const client of clients) {
          if (!client.busy) void client.writer.send(`event: hls:heartbeat\ndata: ${JSON.stringify({ sentAt: Date.now() })}\n\n`);
        }
      }, this.timing.heartbeatMs);
      polling.unref(); heartbeat.unref();
    };
    this.wakes.add(pump);
    app.get('/api/hls/status/snapshot', async (_request, reply) => {
      reply.header('Cache-Control', 'no-store');
      if (closing) return reply.code(503).send({ error: 'Server shutting down' });
      try { return await withOperationTimeout(this.store.snapshot(), this.timing.storeTimeoutMs, 'Status snapshot', shutdown.signal); }
      catch { return reply.code(503).send({ error: 'Status snapshot unavailable' }); }
    });
    app.get<{ Querystring: { after?: string } }>('/events/hls-status', (request, reply) => {
      if (closing) return reply.code(503).send({ error: 'Server shutting down' });
      const header = request.headers['last-event-id'];
      const cursor = typeof header === 'string' ? header : request.query.after ?? '';
      reply.hijack();
      const response = reply.raw;
      for (const [name, value] of Object.entries(reply.getHeaders())) if (value !== undefined) response.setHeader(name, value);
      response.writeHead(200, { 'Content-Type': 'text/event-stream; charset=utf-8', 'Cache-Control': 'no-cache, no-transform', 'X-Accel-Buffering': 'no' });
      const writer = new SseClientWriter(response, () => {
        clients.delete(client);
        connections.delete(writer);
        if (!clients.size) stopTimers();
      }, this.timing.drainTimeoutMs);
      const client: Client = { writer, cursor, ready: false, busy: false, replayStartedAt: Date.now() };
      clients.add(client);
      connections.add(writer);
      response.flushHeaders();
      void writer.send('retry: 3000\n: connected\n\n');
      startTimers(); pump();
      return reply;
    });
    app.addHook('preClose', async () => {
      closing = true; stopTimers(); this.wakes.delete(pump);
      shutdown.abort();
      for (const writer of connections) writer.close();
      try {
        await withOperationTimeout(Promise.all(jobs), this.timing.shutdownTimeoutMs, 'SSE shutdown');
      } catch {
        app.log?.warn('SSE shutdown wait exceeded its deadline');
      }
    });
  }
}
