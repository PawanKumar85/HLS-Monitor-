/** Gauges describe the current snapshot, not historical uptime percentages. */
export function gaugeValue(count: number, total: number, available: boolean) {
  if (!available || !Number.isFinite(count) || !Number.isFinite(total) || total <= 0) return null;
  const ratio = Math.max(0, Math.min(1, count / total));
  return { ratio, percent: Math.round(ratio * 100), angle: 135 + ratio * 270 };
}
