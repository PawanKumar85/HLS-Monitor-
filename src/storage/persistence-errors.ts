export interface MetricsWriteFailure {
  index: number;
  failure: {
    code: string;
    message: string;
    retryable: boolean;
  };
}

export class PartialMetricsWriteError extends Error {
  override readonly name = "PartialMetricsWriteError";

  constructor(readonly failures: readonly MetricsWriteFailure[]) {
    super("Some observations failed persistence");
  }
}
