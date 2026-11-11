import type { HlsConsumedStream } from "../types/stream.types.js";

export function parseHlsStreamMessage(
  messageId: string,
  rawFields: string[]
): HlsConsumedStream {
  if (!messageId || rawFields.length % 2 !== 0) throw new Error("Invalid Redis message fields");
  const data: Record<string, string> = Object.create(null);
  for (let i = 0; i < rawFields.length; i += 2) data[rawFields[i]!] = rawFields[i + 1]!;
  if (!data['streamId'] || !data['streamUrl'] || !data['createdAt']) throw new Error("Missing required HLS message fields");
  const url = new URL(data['streamUrl']);
  if (url.protocol !== 'http:' && url.protocol !== 'https:') throw new Error("Invalid HLS URL protocol");

  const now = Date.now();
  const receivedAt = new Date(now).toISOString();

  const createdMs = Date.parse(data['createdAt']!);

  if (!Number.isFinite(createdMs)) {
    throw new Error(`Invalid createdAt: ${messageId}`);
  }

  return {
    messageId,
    streamId: data['streamId']!,
    streamUrl: data['streamUrl']!,
    serverIp: data['serverIp'] ?? 'unknown',
    createdAt: data['createdAt']!,
    receivedAt,
    delay: Math.max(0, now - createdMs),
  };
}
