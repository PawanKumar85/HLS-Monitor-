import { ProcessingFailure } from '../dlq/dead-letter.types.js';
import type { HlsHealthChecker, HlsProbeIdentity } from '../ports/hls-processing.js';
import { inspectHls, type HlsInspection } from '../services/hls-health.service.js';
import type { PlaylistFreshnessStore, StreamInventory } from './probe-contracts.js';
export class LiveHlsHealthChecker implements HlsHealthChecker {
  constructor(private readonly inventory: Pick<StreamInventory, 'get'>, private readonly freshness: PlaylistFreshnessStore,
    private readonly inspect: (url: string) => Promise<HlsInspection> = inspectHls) {}
  async check(url: string, identity?: HlsProbeIdentity) {
    const { health, progress, failure } = await this.inspect(url);
    if (failure) console.warn(JSON.stringify({ event: 'hls:probe-failed', ...failure }));
    if (!identity || (health.status && !progress)) return health;
    try {
      if (!health.status) {
        // One failed fetch keeps a confirmed-online stream online; the next one in a row marks it offline.
        const stillOnline = await this.freshness.fail(identity.serverIp, identity.streamId, url);
        if (stillOnline) { const { reason: _blip, ...online } = health; return { ...online, status: 1 as const }; }
        return health;
      }
      const config = await this.inventory.get(identity.serverIp, identity.streamId);
      const fresh = await this.freshness.observe(identity.serverIp, identity.streamId, url, progress!, config?.staleAfterMs ?? 30_000);
      return fresh ? health : { ...health, status: 0 as const, reason: 'FROZEN' as const };
    } catch {
      // Monitoring infrastructure failure is not evidence that the HLS stream is offline.
      throw new ProcessingFailure('HEALTH_CHECK', 'FRESHNESS_UNAVAILABLE', 'Playlist freshness state unavailable', true, false);
    }
  }
}
