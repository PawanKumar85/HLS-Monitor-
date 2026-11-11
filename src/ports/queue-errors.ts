/** A single record cannot fit even in an empty queue; retrying cannot admit it. */
export class QueueRecordTooLargeError extends RangeError {
  override readonly name = 'QueueRecordTooLargeError';

  constructor() {
    super('Record exceeds batch queue capacity; message remains unacknowledged');
  }
}
