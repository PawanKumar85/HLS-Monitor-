/**
 * Deterministically maps a key to a partition using Murmur-like 32-bit hash
 */
export function hashKey(key: string, numPartitions: number): number {
  if (numPartitions <= 1) return 0;

  let hash = 0;
  for (let i = 0; i < key.length; i++) {
    const char = key.charCodeAt(i);
    hash = (hash << 5) - hash + char;
    hash |= 0; // Convert to 32bit integer
  }
  return Math.abs(hash) % numPartitions;
}

export class RoundRobinPartitioner {
  private counter: number = 0;

  public getNextPartition(numPartitions: number): number {
    if (numPartitions <= 1) return 0;
    const partition = this.counter % numPartitions;
    this.counter = (this.counter + 1) % 1000000;
    return partition;
  }
}

/**
 * Returns the appropriate partitioning / routing key for a stream.
 * If serverIp is valid, routes server-wise; if "unknown", falls back to streamId
 * to prevent all unresolved streams from hotspotting a single partition.
 */
export function getStreamRoutingKey(stream: { streamId: string; serverIp?: string }): string {
  if (stream.serverIp && stream.serverIp !== "unknown") {
    return stream.serverIp;
  }
  return stream.streamId;
}


