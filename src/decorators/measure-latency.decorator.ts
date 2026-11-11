export function MeasureLatency<This, Args extends unknown[], Result>(
  originalMethod: (this: This, ...args: Args) => Promise<Result>,
  context: ClassMethodDecoratorContext<
    This,
    (this: This, ...args: Args) => Promise<Result>
  >
) {
  return async function (this: This, ...args: Args): Promise<Result> {
    const start = performance.now();
    try {
      return await originalMethod.apply(this, args);
    } finally {
      const duration = performance.now() - start;
      console.log(`${String(context.name)}: ${duration.toFixed(2)}ms`);
    }
  };
}
