import { test } from 'node:test';
import assert from 'node:assert/strict';
import { PipelineProberService, type HopProbeResult } from '../src/services/pipeline-prober.service.js';
import { PipelineRcaEngine } from '../src/services/pipeline-rca.service.js';
import { targetKey, type ProbeTarget } from '../src/config/pipeline-probes.js';
import type { ChannelTopology } from '../src/types/pipeline.types.js';

function createMockHlsFetch(options: {
  status?: number;
  segmentStatus?: number;
  delayMs?: number;
  playlist?: string;
} = {}) {
  const status = options.status ?? 200;
  const segmentStatus = options.segmentStatus ?? 200;
  const playlistBody = options.playlist ?? `#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:6
#EXT-X-MEDIA-SEQUENCE:100
#EXTINF:6.0,
segment100.ts
`;

  return (async (input: string | URL | Request, _init?: RequestInit): Promise<Response> => {
    if (options.delayMs) {
      await new Promise(resolve => setTimeout(resolve, options.delayMs));
    }
    const urlStr = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url;

    if (urlStr.endsWith('.ts') || urlStr.includes('segment')) {
      if (segmentStatus !== 200) {
        return new Response('Not Found', { status: segmentStatus });
      }
      return new Response(new Uint8Array([0x47, 0x00, 0x00, 0x10, 0x00]), {
        status: 200,
        headers: { 'Content-Type': 'video/mp2t' },
      });
    }

    if (status !== 200) {
      return new Response('Playlist Error', { status });
    }

    return new Response(playlistBody, {
      status: 200,
      headers: { 'Content-Type': 'application/vnd.apple.mpegurl' },
    });
  }) as typeof fetch;
}

const sampleChannel: ChannelTopology = {
  triggerId: 'ch1',
  channelName: 'Channel One',
  nodes: [
    { id: 'ingest', name: 'Ingest 1', type: 'INGEST', host: '10.0.0.1', port: 1935 },
    { id: 'scte', name: 'SCTE Relay', type: 'SCTE_RELAY', host: '10.0.0.2', port: 63004 },
    { id: 'xcode', name: 'Transcoder', type: 'TRANSCODER', host: 'xcode.test' },
    { id: 'cdn', name: 'CDN Egress', type: 'EGRESS_CDN', host: 'cdn.test' },
    { id: 'vod', name: 'VOD Fallback', type: 'FALLBACK_SOURCE', isBackup: true },
  ],
  edges: [
    { fromNodeId: 'ingest', toNodeId: 'scte', type: 'PRIMARY' },
    { fromNodeId: 'scte', toNodeId: 'xcode', type: 'PRIMARY' },
    { fromNodeId: 'vod', toNodeId: 'xcode', type: 'FALLBACK' },
    { fromNodeId: 'xcode', toNodeId: 'cdn', type: 'PRIMARY' },
  ],
};

test('PipelineProberService: unconfigured hop returns UNPROBED', async () => {
  const prober = new PipelineProberService();
  const result = await prober.probeHop(sampleChannel.nodes[4]!, undefined);

  assert.equal(result.nodeId, 'vod');
  assert.equal(result.isLive, null);
  assert.equal(result.latencyMs, null);
  assert.equal(result.reason, 'UNPROBED');
});

test('PipelineProberService: HLS hop probe succeeds with live media bytes', async () => {
  const fetcher = createMockHlsFetch({ status: 200 });
  const prober = new PipelineProberService({ fetcher });

  const target: ProbeTarget = { kind: 'hls', url: 'https://cdn.test/live/ch1/master.m3u8' };
  const result = await prober.probeHop(sampleChannel.nodes[3]!, target);

  assert.equal(result.nodeId, 'cdn');
  assert.equal(result.isLive, true);
  assert.equal(typeof result.latencyMs, 'number');
  assert.equal(result.target?.kind, 'hls');
  assert.equal(result.target?.endpoint, target.url);
});

test('PipelineProberService: HLS hop probe detects HTTP 404 playlist failure', async () => {
  const fetcher = createMockHlsFetch({ status: 404 });
  const prober = new PipelineProberService({ fetcher });

  const target: ProbeTarget = { kind: 'hls', url: 'https://cdn.test/live/ch1/missing.m3u8' };
  const result = await prober.probeHop(sampleChannel.nodes[3]!, target);

  assert.equal(result.nodeId, 'cdn');
  assert.equal(result.isLive, false);
  assert.equal(result.reason, 'HTTP_404');
});

test('PipelineProberService: HLS hop probe detects HTTP 503 upstream error', async () => {
  const fetcher = createMockHlsFetch({ status: 503 });
  const prober = new PipelineProberService({ fetcher });

  const target: ProbeTarget = { kind: 'hls', url: 'https://cdn.test/live/ch1/outage.m3u8' };
  const result = await prober.probeHop(sampleChannel.nodes[3]!, target);

  assert.equal(result.nodeId, 'cdn');
  assert.equal(result.isLive, false);
  assert.equal(result.reason, 'HTTP_5XX');
});

test('PipelineProberService: TCP hop probe succeeds when reachable', async () => {
  const checkTcp = async (_host: string, _port: number) => true;
  const prober = new PipelineProberService({ checkTcp });

  const target: ProbeTarget = { kind: 'tcp', host: '10.0.0.2', port: 63004 };
  const result = await prober.probeHop(sampleChannel.nodes[1]!, target);

  assert.equal(result.nodeId, 'scte');
  assert.equal(result.isLive, true);
  assert.equal(typeof result.latencyMs, 'number');
  assert.equal(result.reason, undefined);
  assert.equal(result.target?.endpoint, '10.0.0.2:63004');
});

test('PipelineProberService: TCP hop probe fails when refused', async () => {
  const checkTcp = async (_host: string, _port: number) => false;
  const prober = new PipelineProberService({ checkTcp });

  const target: ProbeTarget = { kind: 'tcp', host: '10.0.0.2', port: 63004 };
  const result = await prober.probeHop(sampleChannel.nodes[1]!, target);

  assert.equal(result.nodeId, 'scte');
  assert.equal(result.isLive, false);
  assert.equal(result.reason, 'REFUSED');
});

test('PipelineProberService: probeChannel runs all hops in parallel and toObservations integrates with RCA engine', async () => {
  const fetcher = createMockHlsFetch({ status: 200 });
  // Ingest UP, SCTE DOWN
  const checkTcp = async (_host: string, port: number) => port !== 63004;
  const prober = new PipelineProberService({ fetcher, checkTcp });

  const targets = new Map<string, ProbeTarget>([
    [targetKey('ch1', 'ingest'), { kind: 'tcp', host: '10.0.0.1', port: 1935 }],
    [targetKey('ch1', 'scte'), { kind: 'tcp', host: '10.0.0.2', port: 63004 }], // Will fail (DOWN)
    [targetKey('ch1', 'xcode'), { kind: 'tcp', host: 'xcode.test', port: 8080 }],
    [targetKey('ch1', 'cdn'), { kind: 'hls', url: 'https://cdn.test/live/ch1/master.m3u8' }],
    // 'vod' is intentionally left unconfigured -> UNPROBED
  ]);

  const results = await prober.probeChannel(sampleChannel, targets);

  assert.equal(results.size, 5);
  assert.equal(results.get('ingest')?.isLive, true);
  assert.equal(results.get('scte')?.isLive, false); // SCTE is down
  assert.equal(results.get('xcode')?.isLive, true);
  assert.equal(results.get('cdn')?.isLive, true);
  assert.equal(results.get('vod')?.isLive, null); // Unprobed

  // Convert to observations for RCA
  const observations = prober.toObservations(results);
  assert.equal(observations.get('ingest'), true);
  assert.equal(observations.get('scte'), false);
  assert.equal(observations.get('xcode'), true);
  assert.equal(observations.get('cdn'), true);
  assert.equal(observations.get('vod'), null);

  // Evaluate with RCA engine
  const rca = new PipelineRcaEngine();
  const verdict = rca.evaluate(sampleChannel, observations);
  assert.equal(verdict.triggerId, 'ch1');
  const scteEval = verdict.hopEvaluations.find(h => h.nodeId === 'scte');
  assert.equal(scteEval?.status, 'CONVICT');
});

test('PipelineProberService: probeChannels evaluates multiple channels', async () => {
  const prober = new PipelineProberService({
    fetcher: createMockHlsFetch({ status: 200 }),
    checkTcp: async () => true,
  });

  const channel2: ChannelTopology = {
    ...sampleChannel,
    triggerId: 'ch2',
    channelName: 'Channel Two',
  };

  const targets = new Map<string, ProbeTarget>([
    [targetKey('ch1', 'cdn'), { kind: 'hls', url: 'https://cdn.test/ch1.m3u8' }],
    [targetKey('ch2', 'cdn'), { kind: 'hls', url: 'https://cdn.test/ch2.m3u8' }],
  ]);

  const allResults = await prober.probeChannels([sampleChannel, channel2], targets);

  assert.equal(allResults.size, 2);
  assert.ok(allResults.has('ch1'));
  assert.ok(allResults.has('ch2'));
  assert.equal(allResults.get('ch1')?.get('cdn')?.isLive, true);
  assert.equal(allResults.get('ch2')?.get('cdn')?.isLive, true);
});

