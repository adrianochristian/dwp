import type { ShutdownOptions } from './sqs.js';

export function normalizeShutdownOptions(input: number | ShutdownOptions = {}): { timeoutMs: number; cleanupMs: number; signal?: AbortSignal | undefined } {
  const options = typeof input === 'number' ? { timeoutMs: input } : input;
  const timeoutMs = options.timeoutMs ?? 10_000;
  const cleanupMs = options.cleanupMs ?? 3_000;
  for (const value of [timeoutMs, cleanupMs]) {
    if (!Number.isSafeInteger(value) || value < 0 || value > 2_147_483_647) {
      throw new Error('Shutdown budgets must be nonnegative timer-safe integers');
    }
  }
  return { timeoutMs, cleanupMs, signal: options.signal };
}

/** Joins tasks without letting a stuck SQL query hold shutdown indefinitely. */
export async function waitForSettlement(tasks: Iterable<Promise<unknown>>, timeoutMs: number, signal?: AbortSignal): Promise<boolean> {
  const settled = Promise.allSettled(tasks).then(() => true);
  if (signal?.aborted) return false;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let cancel!: () => void;
  const stopped = new Promise<boolean>(resolve => {
    cancel = () => resolve(false);
    timer = setTimeout(cancel, timeoutMs);
    signal?.addEventListener('abort', cancel, { once: true });
  });
  try {
    return await Promise.race([settled, stopped]);
  } finally {
    clearTimeout(timer);
    signal?.removeEventListener('abort', cancel);
  }
}
