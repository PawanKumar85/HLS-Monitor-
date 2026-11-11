import { test } from 'node:test';
import assert from 'node:assert/strict';
import { hashPassword, verifyPassword } from '../src/auth/password.js';
import { RedisLoginLimiter } from '../src/http/auth.js';
import { PipelineMonitor } from '../src/monitoring/pipeline-monitor.js';
import { PipelineRcaEngine } from '../src/services/pipeline-rca.service.js';
import { targetKey, type ProbeTarget } from '../src/config/pipeline-probes.js';
import type { ChannelTopology } from '../src/types/pipeline.types.js';

const channel = (id: string): ChannelTopology => ({ triggerId: id, channelName: id,
  nodes: [{ id: 'source', name: 'Source', type: 'INGEST' }, { id: 'end', name: 'End', type: 'EGRESS_CDN' }],
  edges: [{ fromNodeId: 'source', toNodeId: 'end', type: 'PRIMARY' }] });

test('malformed stored credentials fail closed while generated hashes still verify', async () => {
  const password = 'a valid test password';
  const hash = await hashPassword(password);
  assert.equal(await verifyPassword(password, hash), true);
  assert.equal(await verifyPassword('wrong password', hash), false);
  for (const stored of ['scrypt$16384$8$1$=$=', hash.replace('$16384$', '$1$'), hash.replace('$8$', '$0$'), hash.replace('$16384$', '$16385$'), hash.replace('$1$', '$0$'), hash + '=', 'x'.repeat(10000)]) {
    assert.equal(await verifyPassword(password, stored), false, stored.slice(0, 80));
  }
});

test('login failure uses one atomic Redis operation with the expiry window', async () => {
  const calls: unknown[][] = [];
  const limiter = new RedisLoginLimiter({ get: async () => '10', del: async () => 1,
    eval: async (...args) => { calls.push(args); return 1; } });
  await limiter.fail('client:user');
  assert.equal(calls.length, 1);
  assert.deepEqual(calls[0]!.slice(1), [1, 'gate:login-fail:client:user', 900000]);
  assert.match(String(calls[0]![0]), /PTTL/);
  assert.equal(await limiter.blocked('client:user'), true);
});

test('target edits, removal and re-addition clear TCP history and hold-down state', async () => {
  let rows = [channel('a')];
  const targets = new Map<string, ProbeTarget>(rows[0]!.nodes.map(node => [targetKey('a', node.id), { kind: 'tcp', host: 'old.test', port: 1234 }]));
  let reachable = true;
  const monitor = new PipelineMonitor({ channels: async () => rows, targets: async () => targets,
    liveStatus: async () => [], rca: new PipelineRcaEngine(), checkTcp: async () => reachable });
  await monitor.round();
  reachable = false;
  for (const key of targets.keys()) targets.set(key, { kind: 'tcp', host: 'new.test', port: 1234 });
  await monitor.round();
  assert.notEqual(monitor.status().channels[0]!.overallState, 'OPTIMAL');
  rows = [];
  await monitor.round();
  const state = monitor as unknown as { tcpState: Map<string, unknown>; lastDownAt: Map<string, unknown>; targetIdentities: Map<string, unknown> };
  assert.equal(state.tcpState.size, 0);
  assert.equal(state.lastDownAt.size, 0);
  assert.equal(state.targetIdentities.size, 0);
  rows = [channel('a')]; reachable = true;
  await monitor.round();
  assert.equal(monitor.status().channels[0]!.overallState, 'OPTIMAL');
});

test('probe and incident workers run concurrently within limits; rounds share active work', async () => {
  const rows = Array.from({ length: 8 }, (_, i) => channel(String(i)));
  const targets = new Map<string, ProbeTarget>(rows.flatMap(c => c.nodes.map(n => [targetKey(c.triggerId, n.id), { kind: 'tcp' as const, host: `${c.triggerId}-${n.id}.test`, port: 1234 }] as const)));
  let tcpActive = 0, tcpPeak = 0, alertActive = 0, alertPeak = 0, probes = 0, notifications = 0;
  const pause = () => new Promise<void>(resolve => setImmediate(resolve));
  const monitor = new PipelineMonitor({ channels: async () => rows, targets: async () => targets, liveStatus: async () => [], rca: new PipelineRcaEngine(),
    tcpConcurrency: 3, incidentConcurrency: 2,
    checkTcp: async () => { probes++; tcpPeak = Math.max(tcpPeak, ++tcpActive); await pause(); tcpActive--; return false; },
    incidents: { openKeys: async () => [], open: async () => true, resolve: async () => null, list: async () => [] },
    notify: async () => { notifications++; alertPeak = Math.max(alertPeak, ++alertActive); await pause(); alertActive--; },
  });
  const round = monitor.round();
  assert.equal(monitor.round(), round);
  await round;
  assert.equal(probes, 16);
  assert.equal(tcpPeak, 3);
  assert.equal(alertPeak, 2);
  assert.ok(notifications >= 8);
  assert.equal(alertActive, 0);
});

test('failed incident tasks do not leave background workers running after a round rejects', async () => {
  let active = 0, completed = 0;
  const rows = Array.from({ length: 6 }, (_, i) => channel(String(i)));
  const targets = new Map<string, ProbeTarget>(rows.flatMap(c => c.nodes.map(n => [targetKey(c.triggerId, n.id), { kind: 'tcp' as const, host: `${c.triggerId}.test`, port: 1234 }] as const)));
  const monitor = new PipelineMonitor({ channels: async () => rows, targets: async () => targets, liveStatus: async () => [],
    rca: new PipelineRcaEngine(), checkTcp: async () => false, incidentConcurrency: 2,
    incidents: { openKeys: async () => [], resolve: async () => null, list: async () => [], open: async () => true },
    notify: async () => {
      active++;
      await new Promise<void>(resolve => setImmediate(resolve));
      active--; completed++;
      if (completed === 1) throw new Error('notification failed');
    },
  });
  await assert.rejects(monitor.round(), /notification failed/);
  assert.equal(active, 0);
  assert.ok(completed >= 6, 'other incident tasks finish despite one failure');
});
