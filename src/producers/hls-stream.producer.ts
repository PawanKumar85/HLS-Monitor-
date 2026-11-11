import { redisClient } from "../config/redis.config.js";
import { EnrichStream, Decorder } from "../utils/index.js";
import type { HlsStream, EnrichedHlsStream } from "../types/stream.types.js";

export type { HlsStream, EnrichedHlsStream };
export { EnrichStream, Decorder };

export class HlsStreamProducer {
  @EnrichStream
  public async addHlsStream(stream: HlsStream): Promise<void> {
    const enriched = stream as EnrichedHlsStream;

    try {
      const streamKey = `hls:streams:${enriched.serverIp}`;

      await redisClient.xadd(
        streamKey,
        "*",
        "streamId",
        enriched.streamId,
        "streamUrl",
        enriched.streamUrl,
        "serverIp",
        enriched.serverIp,
        "createdAt",
        enriched.createdAt
      );
      console.log(`✅ HLS stream added to [${streamKey}]: ${enriched.streamId} (IP: ${enriched.serverIp})`);
    } catch (error) {
      console.error(`❌ Failed to add HLS stream ${stream.streamId}:`, error);
      throw error;
    }
  }
}

// Backward-compatible alias
export const StreamProducer = HlsStreamProducer;

// Singleton instance and functional export
export const hlsStreamProducer = new HlsStreamProducer();
export const streamProducer = hlsStreamProducer;
export const addHlsStream = (stream: HlsStream): Promise<void> =>
  hlsStreamProducer.addHlsStream(stream);

