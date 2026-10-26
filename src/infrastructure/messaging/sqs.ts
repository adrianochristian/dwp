import { GetQueueUrlCommand, SQSClient } from '@aws-sdk/client-sqs';

export const queueNames = Object.freeze({
  commands: 'wager-transactions.fifo',
  deadLetters: 'wager-transactions-dlq.fifo',
  events: 'wager-events',
});

export interface SqsCallOptions {
  signal?: AbortSignal | undefined;
  timeoutMs?: number | undefined;
}

export interface ShutdownOptions {
  /** Time allowed for accepted work to finish before cancellation. */
  timeoutMs?: number | undefined;
  /** Best-effort release budget after draining. */
  cleanupMs?: number | undefined;
  signal?: AbortSignal | undefined;
}

export class SqsCallTimeoutError extends Error {
  readonly code = 'SQS_CALL_TIMEOUT';

  constructor(readonly timeoutMs: number) {
    super(`SQS call exceeded its ${timeoutMs}ms deadline`);
    this.name = 'SqsCallTimeoutError';
  }
}

/** Classifies transport diagnostics without exposing an exception's raw text. */
export function sqsDiagnosticCode(error: unknown, fallback = 'INFRASTRUCTURE_UNAVAILABLE'): string {
  try {
    if (error === null || typeof error !== 'object') return fallback;
    const value = error as { code?: unknown; name?: unknown; $metadata?: unknown };
    if (value.code === 'SQS_CALL_TIMEOUT' || value.name === 'SqsCallTimeoutError'
      || (value.name === 'TimeoutError' && (value.code === 'ETIMEDOUT' || value.$metadata !== undefined))) {
      return 'SQS_CALL_TIMEOUT';
    }
    if (value.code === 'OPERATION_CANCELLED' || value.code === 'ABORT_ERR' || value.name === 'AbortError') {
      return 'OPERATION_CANCELLED';
    }
  } catch { /* Diagnostic inspection must not change recovery behavior. */ }
  return fallback;
}

/** Bounds the entire SDK call, including its retries and backoff. */
export async function sqsCall<T>(operation: (signal: AbortSignal) => Promise<T>, options: SqsCallOptions = {}): Promise<T> {
  const timeoutMs = options.timeoutMs ?? 30_000;
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 2_147_483_647) {
    throw new Error('SQS call timeout must be a positive timer-safe integer');
  }
  if (options.signal?.aborted) {
    throw options.signal.reason ?? new DOMException('SQS call aborted', 'AbortError');
  }
  const controller = new AbortController();
  let cancel!: (reason: unknown) => void;
  const cancelled = new Promise<never>((_, reject) => { cancel = reject; });
  const abort = (reason: unknown): void => {
    controller.abort(reason);
    cancel(reason);
  };
  const callerAborted = (): void => abort(options.signal?.reason ?? new DOMException('SQS call aborted', 'AbortError'));
  options.signal?.addEventListener('abort', callerAborted, { once: true });
  const timer = setTimeout(() => abort(new SqsCallTimeoutError(timeoutMs)), timeoutMs);
  try {
    return await Promise.race([Promise.resolve().then(() => operation(controller.signal)), cancelled]);
  } finally {
    clearTimeout(timer);
    options.signal?.removeEventListener('abort', callerAborted);
  }
}

export function createSqsClient(): SQSClient {
  return new SQSClient({
    region: process.env.AWS_REGION ?? 'us-east-1',
    endpoint: process.env.SQS_ENDPOINT ?? 'http://127.0.0.1:4566',
    credentials: {
      accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? '000000000000',
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? 'local',
    },
    maxAttempts: 3,
    requestHandler: { connectionTimeout: 3_000, requestTimeout: 25_000, throwOnRequestTimeout: true },
  });
}

export async function getQueueUrl(client: SQSClient, name: string, abortSignal?: AbortSignal): Promise<string> {
  const result = await sqsCall(signal => client.send(new GetQueueUrlCommand({ QueueName: name }), { abortSignal: signal }), { signal: abortSignal });
  if (!result.QueueUrl) throw new Error(`Missing URL for queue ${name}`);
  return result.QueueUrl;
}
