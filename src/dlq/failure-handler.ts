import { BufferedMessageError, ProcessingFailure, type FailureLedger, type MessageContext, type MessageFailureHandler } from "./dead-letter.types.js";
import { RetryPolicy } from "./retry-policy.js";

export class FailureHandler implements MessageFailureHandler {
  constructor(private readonly ledger: FailureLedger, private readonly policy = new RetryPolicy(), private readonly now = () => Date.now()) { }
  async prepare(context: MessageContext): Promise<boolean> {
    const saved = await this.ledger.get(context);
    if (!saved) return true;
    if (saved.state === "dead_lettered") {
      // Retry a possibly interrupted ACK using the existing DLQ identity.
      await this.ledger.moveToDeadLetter(context, saved.record);
      return false;
    }
    if (saved.state === "retry" && this.policy.terminal(saved.record)) {
      await this.ledger.moveToDeadLetter(context, saved.record);
      return false;
    }
    return saved.nextRetryAt <= this.now();
  }
  async failed(context: MessageContext, error: unknown): Promise<boolean> {
    if (error instanceof BufferedMessageError) return false;
    const failure = error instanceof ProcessingFailure ? error : new ProcessingFailure("QUEUE", "WORKER_EXCEPTION", error instanceof Error ? error.message : String(error));
    const saved = await this.ledger.recordFailure(context, failure, this.policy.settings);
    if (saved.state === "dead_lettered" || this.policy.terminal(saved.record)) {
      await this.ledger.moveToDeadLetter(context, saved.record);
      return true;
    }
    return false;
  }
}
