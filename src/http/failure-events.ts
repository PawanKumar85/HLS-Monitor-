import type { FastifyInstance } from 'fastify';
import { parseFailureCursor, type FailureEvent, type FailureFeed } from '../dlq/failure-feed.js';
import { withOperationTimeout } from '../realtime/operation-timeout.js';
import { SseClientWriter } from '../realtime/sse-client-writer.js';

const BATCH = 100;
const MAX_CLIENTS = 100;
interface Client { cursor: string | undefined; busy: boolean }

/** Failure feed. Register inside an authenticated scope; clients read it with fetch(), not EventSource. */
export function registerFailureEvents(app: FastifyInstance, feed: FailureFeed, timing = { pollMs: 2000, heartbeatMs: 15_000 }): void {
  const clients = new Map<SseClientWriter, Client>();
  let closing = false;
  let poll: NodeJS.Timeout | undefined;
  let heartbeat: NodeJS.Timeout | undefined;
  const stopTimers = () => { clearInterval(poll); clearInterval(heartbeat); poll = heartbeat = undefined; };
  const pump = () => {
    // Clients at the same cursor (normally everyone caught up) share one query per poll.
    const reads = new Map<string, Promise<FailureEvent[]>>();
    for (const [writer, client] of clients) {
      if (client.busy) continue;
      client.busy = true;
      const key = client.cursor ?? '';
      let read = reads.get(key);
      if (!read) { read = withOperationTimeout(feed.readAfter(client.cursor, BATCH), 5000, 'Failure feed'); reads.set(key, read); }
      void read.then(async events => {
        for (const event of events) {
          if (!await writer.send(`id: ${event.cursor}\nevent: hls:failure\ndata: ${JSON.stringify(event.failure)}\n\n`)) return;
          client.cursor = event.cursor;
        }
        if (events.length === BATCH && client.cursor) setImmediate(pump); // still catching up
      }).catch(() => writer.close()).finally(() => { client.busy = false; });
    }
  };
  app.get<{ Querystring: { after?: string } }>('/events/hls-failures', (request, reply) => {
    const header = request.headers['last-event-id'];
    const cursor = (typeof header === 'string' && header) || request.query.after || undefined;
    if (cursor !== undefined && !parseFailureCursor(cursor)) return reply.code(400).send({ error: 'Invalid failure cursor' });
    if (closing || clients.size >= MAX_CLIENTS) return reply.code(503).send({ error: 'Failure feed unavailable' });
    reply.hijack();
    const response = reply.raw;
    for (const [name, value] of Object.entries(reply.getHeaders())) if (value !== undefined) response.setHeader(name, value);
    response.writeHead(200, { 'Content-Type': 'text/event-stream; charset=utf-8', 'Cache-Control': 'no-store, no-transform', 'X-Accel-Buffering': 'no' });
    const writer = new SseClientWriter(response, () => { clients.delete(writer); if (!clients.size) stopTimers(); });
    clients.set(writer, { cursor, busy: false });
    response.flushHeaders();
    void writer.send('retry: 3000\n: connected\n\n');
    if (!poll) {
      poll = setInterval(pump, timing.pollMs);
      heartbeat = setInterval(() => { for (const w of clients.keys()) void w.send(`event: hls:heartbeat\ndata: ${Date.now()}\n\n`); }, timing.heartbeatMs);
      poll.unref(); heartbeat.unref();
    }
    pump();
    return reply;
  });
  app.addHook('preClose', async () => { closing = true; stopTimers(); for (const writer of clients.keys()) writer.close(); });
}
