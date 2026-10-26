export interface PublisherConfiguration {
  batchSize: number;
  leaseMs: number;
  idleMs: number;
  errorMs: number;
}

export function publisherConfiguration(environment: Record<string, string | undefined> = process.env): PublisherConfiguration {
  const raw = environment.OUTBOX_BATCH_SIZE ?? '10';
  if (!/^\d+$/.test(raw) || Number(raw) < 1 || Number(raw) > 100) {
    throw new Error('OUTBOX_BATCH_SIZE must be an integer between 1 and 100');
  }
  return { batchSize: Number(raw), leaseMs: 30_000, idleMs: 250, errorMs: 1_000 };
}

export async function publisherDelay(milliseconds: number, signal: AbortSignal): Promise<void> {
  if (signal.aborted) return;
  await new Promise<void>(resolve => {
    const finish = () => { clearTimeout(timer); signal.removeEventListener('abort', finish); resolve(); };
    const timer = setTimeout(finish, milliseconds);
    signal.addEventListener('abort', finish, { once: true });
  });
}

export interface PublisherLoopOptions {
  signal: AbortSignal;
  onPublished?: (count: number) => void;
  onError?: (error: unknown) => void;
  wait?: (milliseconds: number, signal: AbortSignal) => Promise<void>;
}

export async function runPublisherLoop(
  publisher: { runOnce(): Promise<number> },
  configuration: PublisherConfiguration,
  options: PublisherLoopOptions,
): Promise<void> {
  const wait = options.wait ?? publisherDelay;
  while (!options.signal.aborted) {
    let count: number;
    try {
      count = await publisher.runOnce();
    } catch (error) {
      try { options.onError?.(error); } catch { /* Diagnostics cannot stop recovery. */ }
      await wait(configuration.errorMs, options.signal);
      continue;
    }
    try { if (count > 0) options.onPublished?.(count); } catch { /* Publication already committed. */ }
    if (count === 0) await wait(configuration.idleMs, options.signal);
  }
}
