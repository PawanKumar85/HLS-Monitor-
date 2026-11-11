/** The host of a "node:<host>[:<port>]" incident, to open its history in Reports. Channel-level and host-less incidents have none. */
export function incidentServer(key: string): string | null {
  if (!key.startsWith('node:')) return null;
  const rest = key.slice(5);
  if (rest.includes('/')) return null; // node:<channel>/<hop>: the hop has no host
  const parts = rest.split(':');
  return parts.length === 2 && /^\d{1,5}$/.test(parts[1]!) ? parts[0]! : rest; // drop a port; bare IPv6 stays whole
}
