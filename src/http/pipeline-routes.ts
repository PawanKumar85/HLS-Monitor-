import type { FastifyInstance } from 'fastify';
import { parseProbeTarget, targetKey, type ProbeTarget, type ProbeTargetStore } from '../config/pipeline-probes.js';
import { channelId, parseChannel, type ChannelStore } from '../config/pipeline-channels.js';
import type { SessionUser } from '../auth/users.js';
import type { PipelineMonitor, IncidentStore } from '../monitoring/pipeline-monitor.js';
import type { SrtReachabilityMonitor } from '../monitoring/srt-reachability.js';
import type { ChannelTopology } from '../types/pipeline.types.js';

export interface PipelineApiOptions {
  channels: ChannelStore;
  probes: ProbeTargetStore;
  monitor: Pick<PipelineMonitor, 'status'>;
  srtReachability?: Pick<SrtReachabilityMonitor, 'status'>;
  incidents?: Pick<IncidentStore, 'list'>;
}

const describe = (t: ProbeTarget) => t.kind === 'hls' ? { kind: t.kind, hls: t.url } : { kind: t.kind, tcp: `${t.host.includes(':') ? `[${t.host}]` : t.host}:${t.port}` };
type Hop = { Params: { triggerId: string; nodeId: string } };

/**
 * Pipeline API. Status only reads the monitor's latest round; requests never trigger probes.
 * Channels and probe targets are set from the dashboard by any signed-in user. A channel's ID comes from its
 * name when it is created and never changes, so renaming keeps its probe targets.
 */
export function registerPipelineRoutes(app: FastifyInstance, options: PipelineApiOptions): void {
  const { channels, probes, monitor, incidents } = options;
  const withProbeKind = (channel: ChannelTopology, targets: ReadonlyMap<string, ProbeTarget>) => ({
    ...channel,
    nodes: channel.nodes.map(n => ({ ...n, probe: targets.get(targetKey(channel.triggerId, n.id))?.kind ?? 'none' })),
  });
  const hop = async (params: Hop['Params']) => (await channels.get(params.triggerId))?.nodes.some(n => n.id === params.nodeId);
  /** Probe targets (and their linked streams) of hops that no longer exist. */
  const dropTargets = async (triggerId: string, keep: ReadonlySet<string> = new Set()) => {
    for (const key of (await probes.all()).keys()) {
      const nodeId = key.slice(triggerId.length + 1);
      if (key.startsWith(`${triggerId}/`) && !keep.has(nodeId)) await probes.remove(triggerId, nodeId);
    }
  };
  const parse = (body: unknown, triggerId: string) => { try { return parseChannel(body, triggerId); } catch (error) { return error as Error; } };

  app.register(async routes => {
    routes.addHook('onRequest', async (_request, reply) => { reply.header('Cache-Control', 'no-store'); });

    routes.get('/api/pipeline/channels', async () => {
      const [list, targets] = await Promise.all([channels.list(), probes.all()]);
      return { total: list.length, channels: list.map(c => withProbeKind(c, targets)) };
    });

    routes.get<{ Params: { triggerId: string } }>('/api/pipeline/channels/:triggerId', async (request, reply) => {
      const channel = await channels.get(request.params.triggerId);
      return channel ? withProbeKind(channel, await probes.all()) : reply.code(404).send({ error: 'Channel not found' });
    });

    routes.post('/api/pipeline/channels', async (request, reply) => {
      const name = (request.body as { channelName?: unknown } | null)?.channelName;
      const channel = parse(request.body, channelId(typeof name === 'string' ? name : ''));
      if (channel instanceof Error) return reply.code(400).send({ error: channel.message });
      if (!await channels.create(channel)) return reply.code(409).send({ error: `A channel named like "${channel.channelName}" already exists` });
      return reply.code(201).send(channel);
    });

    routes.put<{ Params: { triggerId: string } }>('/api/pipeline/channels/:triggerId', async (request, reply) => {
      const channel = parse(request.body, request.params.triggerId);
      if (channel instanceof Error) return reply.code(400).send({ error: channel.message });
      if (!await channels.replace(channel)) return reply.code(404).send({ error: 'Channel not found' });
      await dropTargets(channel.triggerId, new Set(channel.nodes.map(n => n.id)));
      return channel;
    });

    routes.delete<{ Params: { triggerId: string } }>('/api/pipeline/channels/:triggerId', async (request, reply) => {
      if (!await channels.remove(request.params.triggerId)) return reply.code(404).send({ error: 'Channel not found' });
      await dropTargets(request.params.triggerId);
      return reply.code(204).send();
    });

    routes.get('/api/pipeline/probes', async () => {
      const targets = await probes.all();
      return { probes: [...targets].map(([key, t]) => { const [triggerId, nodeId] = key.split('/'); return { triggerId, nodeId, ...describe(t) }; }) };
    });

    routes.put<Hop>('/api/pipeline/probes/:triggerId/:nodeId', async (request, reply) => {
      if (!await hop(request.params)) return reply.code(404).send({ error: 'No such hop in this channel' });
      let target: ProbeTarget;
      try { target = parseProbeTarget(request.body); } catch (error) { return reply.code(400).send({ error: (error as Error).message }); }
      await probes.set(request.params.triggerId, request.params.nodeId, target, (request as { user?: SessionUser }).user?.username ?? null);
      return describe(target);
    });

    routes.delete<Hop>('/api/pipeline/probes/:triggerId/:nodeId', async (request, reply) => {
      if (!await hop(request.params)) return reply.code(404).send({ error: 'No such hop in this channel' });
      await probes.remove(request.params.triggerId, request.params.nodeId);
      return reply.code(204).send();
    });

    routes.get('/api/pipeline/srt-reachability', async () => options.srtReachability?.status() ?? { evaluatedAt: null, intervalMs: 30_000, servers: [] });

    routes.get('/api/pipeline/status', async () => {
      const status = monitor.status();
      return { ...status, total: status.channels.length, pending: status.evaluatedAt === null };
    });

    routes.get<{ Params: { triggerId: string } }>('/api/pipeline/status/:triggerId', async (request, reply) => {
      const verdict = monitor.status().channels.find(c => c.triggerId === request.params.triggerId);
      return verdict ?? reply.code(404).send({ error: 'No status for this channel yet' });
    });

    routes.get<{ Querystring: { hours?: number } }>('/api/pipeline/incidents', {
      schema: { querystring: { type: 'object', additionalProperties: false, properties: { hours: { type: 'integer', minimum: 1, maximum: 168 } } } },
    }, async (request, reply) => {
      if (!incidents) return reply.code(404).send({ error: 'Incident history is not configured' });
      const since = new Date(Date.now() - (request.query.hours ?? 24) * 3_600_000);
      return { incidents: await incidents.list(since) };
    });
  });
}
