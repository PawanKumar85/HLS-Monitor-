import "dotenv/config";
import { SseStatusTransport } from "./adapters/sse-status.transport.js";
import type { StatusTransport } from "./status-transport.js";

/** Shared application composition point for SSE and future adapters. */
export function createStatusTransport(store: import("./status-event-store.js").StatusEventStore, provider = process.env["STATUS_TRANSPORT"] ?? "sse"): StatusTransport {
  switch (provider) {
    case "sse": return new SseStatusTransport(store);
    default: throw new Error(`Unsupported STATUS_TRANSPORT: ${provider}`);
  }
}

