import type { MessageAcknowledger } from "../ports/messaging.js";
import type { FailureLedger } from "./dead-letter.types.js";
export class TrackingAcknowledger implements MessageAcknowledger {
  constructor(
    private readonly delegate: MessageAcknowledger,
    private readonly ledger: FailureLedger
  ) { }
  async acknowledge(stream: string, group: string, ids: readonly string[]): Promise<number> {
    const count = await this.delegate.acknowledge(stream, group, ids);
    await this.ledger.clearSucceeded(stream, group, ids);
    return count;
  }
}
