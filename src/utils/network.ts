import { lookup } from "node:dns/promises";
import { isIP } from "node:net";

/**
 * Resolves the server IP address from a validated stream URL.
 *
 * Caveats & Architectural Notes:
 * 1. IP Family: Returns either IPv4 or IPv6 depending on system DNS resolution.
 * 2. CDN / Proxy / Load Balancer: The resolved IP is frequently an edge node (Cloudflare,
 *    CloudFront, CDN POP) or reverse proxy, NOT the actual origin media server.
 * 3. Fallback: If DNS resolution fails, returns "unknown" so producer ingestion does not fail.
 *    Downstream routing/partitioning must handle "unknown" (e.g. by falling back to streamId
 *    hash) to avoid hotspotting a single partition.
 */
export async function resolveServerIp(streamUrl: string): Promise<string> {
    try {
        const url = new URL(streamUrl);
        const host = url.hostname;

        // If host is already an IPv4 or IPv6 address
        if (isIP(host) !== 0) {
            return host;
        }

        const { address } = await lookup(host);
        return address;
    } catch {
        return "unknown";
    }
}

