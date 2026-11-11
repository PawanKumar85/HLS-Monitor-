import { test } from 'node:test';
import assert from 'node:assert/strict';
import type { Redis } from 'ioredis';
import { RedisDeadLetterService } from '../src/dlq/adapters/redis-dead-letter.service.js';
const entry = (id: string): [string, string[]] => [id, ['failure', JSON.stringify({ failureId: id })]];
test('archive reads share one count budget across reclaimed, own and fresh entries', async () => {
  const counts: number[] = [];
  const redis = {
    xgroup: async () => 'OK',
    xautoclaim: async () => ['0-0', [entry('1-0'), entry('2-0')]],
    xreadgroup: async (...args: (string | number)[]) => {
      counts.push(Number(args[args.indexOf('COUNT') + 1]));
      return [['stream', args.at(-1) === '0' ? [entry('1-0'), entry('3-0'), entry('4-0')] : [entry('5-0')]]];
    },
  } as unknown as Redis;
  const entries = await new RedisDeadLetterService(redis).readArchiveBatch('s', 'consumer', 5);
  assert.deepEqual(entries.map(value => value.id), ['1-0', '2-0', '3-0', '4-0', '5-0']);
  assert.deepEqual(counts, [3, 1]);
});
test('full reclaim budget skips further reads; invalid counts never reach Redis', async () => {
  let calls = 0;
  const redis = {
    xgroup: async () => { calls++; return 'OK'; },
    xautoclaim: async () => ['0-0', [entry('1-0')]],
    xreadgroup: async () => { assert.fail('Budget already exhausted'); },
  } as unknown as Redis;
  const ledger = new RedisDeadLetterService(redis);
  for (const count of [0, -1, 1.5, Number.NaN, Number.MAX_SAFE_INTEGER + 1]) await assert.rejects(ledger.readArchiveBatch('s', 'consumer', count), RangeError);
  assert.equal(calls, 0);
  assert.equal((await ledger.readArchiveBatch('s', 'consumer', 1)).length, 1);
});
