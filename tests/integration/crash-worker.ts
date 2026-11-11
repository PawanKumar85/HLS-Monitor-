import { createApplicationRuntime } from '../../src/application/create-runtime.js';
const runtime = createApplicationRuntime();
await runtime.dbManager.connectAll();
const [server, group] = process.argv.slice(2);
if (!server || !group) throw new Error('server and group required');
const consumer = runtime.hlsConsumer;
const task = consumer.consume(server, undefined, { groupName: group });
const poll = setInterval(() => {
  if (Object.values(runtime.batchQueueManager.getAllStats()).some((stats) => stats.itemCount > 0)) {
    clearInterval(poll);
    process.send?.('buffered'); // Parent kills this process before the 5s flush timer.
  }
}, 10);
await task;
