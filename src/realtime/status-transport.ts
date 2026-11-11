import type { FastifyInstance } from "fastify";
import type { HlsStatusEvent } from "../types/hls-status.types.js";

export interface StatusPublisher {
  publish(event: HlsStatusEvent): void | Promise<void>;
}

export interface StatusTransport extends StatusPublisher {
  readonly name: string;
  register(app: FastifyInstance): Promise<void>;
}
