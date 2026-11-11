import { readFile } from 'node:fs/promises';
import { normalizeProbeConfig, type StreamInventory, type StreamProbeRegistration } from './probe-contracts.js';

/** Configuration loading is separate from the scheduler's polling lifecycle. */
export async function loadProbeInventory(file: string, inventory: Pick<StreamInventory, 'upsert'>): Promise<void> {
  const values: unknown = JSON.parse(await readFile(file, 'utf8'));
  if (!Array.isArray(values) || values.length > 10_000) throw new TypeError('Probe config must be an array of at most 10000 streams');
  const configs = values.map(value => normalizeProbeConfig(value as StreamProbeRegistration));
  const identities = new Set(configs.map(config => JSON.stringify([config.serverIp, config.streamId])));
  if (identities.size !== configs.length) throw new TypeError('Duplicate stream identity in probe config');
  // Validate the whole file before mutating inventory or starting consumers.
  for (const config of configs) await inventory.upsert(config);
}
