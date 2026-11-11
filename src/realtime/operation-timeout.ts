/** Bound the caller's wait. The underlying operation may still complete later. */
export function withOperationTimeout<T>(
  operation: Promise<T>,
  timeoutMs: number,
  label: string,
  signal?: AbortSignal,
): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    let settled = false;
    const cleanup = () => { clearTimeout(timer); signal?.removeEventListener('abort', abort); };
    const fail = (error: unknown) => {
      if (settled) return;
      settled = true; cleanup(); reject(error);
    };
    const abort = () => fail(new Error(`${label} cancelled`));
    const timer = setTimeout(() => fail(new Error(`${label} timed out`)), timeoutMs);
    // Keep handlers attached after timeout/abort to consume late rejections.
    operation.then(value => {
      if (settled) return;
      settled = true; cleanup(); resolve(value);
    }, fail);
    if (signal?.aborted) abort();
    else signal?.addEventListener('abort', abort, { once: true });
  });
}
