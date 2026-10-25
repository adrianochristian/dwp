import {
  ChangeMessageVisibilityCommand,
  DeleteMessageCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
  type Message,
  type SQSClient,
} from '@aws-sdk/client-sqs';
import { createHash } from 'node:crypto';

export interface WagerCommandContext {
  consumerName: string;
  messageId: string;
}

export interface WagerCommandResult {
  status: string;
  transactionId?: string;
}

export interface WagerCommandHandler {
  submit(data: unknown, key: unknown, context: WagerCommandContext): Promise<WagerCommandResult>;
  failSubmission?(data: unknown, key: unknown, code: string, context: WagerCommandContext): Promise<WagerCommandResult>;
}

export interface WagerCommandEnvelope {
  messageId: string;
  type: 'WagerTransactionRequested';
  occurredAt: string;
  data: Record<string, unknown> & { idempotencyKey: string };
}

export interface SqsConsumerOptions {
  consumerName?: string;
  visibilitySeconds?: number;
  maxAttempts?: number;
  afterCommitBeforeAck?: (result: WagerCommandResult, message: WagerCommandEnvelope) => Promise<void> | void;
  onError?: (error: unknown) => void;
  onRetry?: (observation: SqsConsumerObservation) => void;
  onDeadLetter?: (observation: SqsConsumerObservation) => void;
  onProcessed?: (observation: SqsConsumerObservation) => void;
}

export interface SqsConsumerObservation {
  consumerName: string;
  messageId: string;
  transactionId?: string;
  status?: string;
  failureCode?: string;
  attempt?: number;
  delaySeconds?: number;
}

interface InFlight {
  message: Message;
  receipt: string;
  released: boolean;
  heartbeatTimer?: ReturnType<typeof setInterval>;
  heartbeatTask?: Promise<void>;
  task?: Promise<void>;
}

class InvalidCommandEnvelope extends Error {
  readonly code = 'INVALID_COMMAND_ENVELOPE';
}

function isObject(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function validText(value: unknown, maximum: number): value is string {
  return typeof value === 'string' && value.length > 0 && value.length <= maximum
    && value.trim() === value && !/[\u0000-\u001f]/.test(value);
}

function validTimestamp(value: unknown): value is string {
  if (typeof value !== 'string' || value.length > 64) return false;
  const match = /^(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?(?:Z|[+-]\d{2}:\d{2})$/.exec(value);
  if (!match || !Number.isFinite(Date.parse(value))) return false;
  const normalized = `${match[1]}T${match[2]}.${(match[3] ?? '').padEnd(3, '0').slice(0, 3)}Z`;
  return new Date(normalized).toISOString() === normalized;
}

export function parseWagerCommand(body: string | undefined): WagerCommandEnvelope {
  let value: unknown;
  try { value = JSON.parse(body ?? ''); } catch { throw new InvalidCommandEnvelope('The command body must be JSON'); }
  if (!isObject(value) || Object.keys(value).some(key => !['messageId', 'type', 'occurredAt', 'data'].includes(key))
    || !validText(value.messageId, 200) || value.type !== 'WagerTransactionRequested'
    || !validTimestamp(value.occurredAt)
    || !isObject(value.data) || !validText(value.data.idempotencyKey, 500)) {
    throw new InvalidCommandEnvelope('The command envelope is invalid');
  }
  return value as unknown as WagerCommandEnvelope;
}

function statusCode(error: unknown): number | undefined {
  if (error === null || typeof error !== 'object') return undefined;
  const value = error as { getStatus?: () => unknown; status?: unknown; statusCode?: unknown };
  const candidate = typeof value.getStatus === 'function' ? value.getStatus() : value.status ?? value.statusCode;
  return typeof candidate === 'number' ? candidate : undefined;
}

function failureCode(error: unknown, fallback: string): string {
  if (error === null || typeof error !== 'object') return fallback;
  const value = error as { code?: unknown; response?: unknown; getResponse?: () => unknown };
  const response = typeof value.getResponse === 'function' ? value.getResponse() : value.response;
  const code = isObject(response) ? response.code : value.code;
  return typeof code === 'string' && /^[A-Z][A-Z0-9_]{0,99}$/.test(code) ? code : fallback;
}

function isPermanent(error: unknown): boolean {
  if (error instanceof InvalidCommandEnvelope) return true;
  const status = statusCode(error);
  return status !== undefined && status >= 400 && status < 500 && ![408, 422, 425, 429].includes(status);
}

export class SqsConsumer {
  private readonly consumerName: string;
  private readonly visibilitySeconds: number;
  private readonly maxAttempts: number;
  private readonly inFlight = new Map<string, InFlight>();
  private readonly receivers = new Set<AbortController>();
  private readonly runs = new Set<Promise<number>>();
  private stopping = false;
  private stopTask: Promise<void> | undefined;

  constructor(
    private readonly handler: WagerCommandHandler,
    private readonly sqs: SQSClient,
    private readonly commandsUrl: string,
    private readonly dlqUrl: string,
    private readonly options: SqsConsumerOptions = {},
  ) {
    this.consumerName = options.consumerName ?? 'wager-transactions';
    this.visibilitySeconds = options.visibilitySeconds ?? 30;
    this.maxAttempts = options.maxAttempts ?? 5;
    if (!Number.isInteger(this.visibilitySeconds) || this.visibilitySeconds < 1 || this.visibilitySeconds > 43_200
      || !Number.isInteger(this.maxAttempts) || this.maxAttempts < 1) {
      throw new Error('Invalid SQS visibility timeout or attempt limit');
    }
  }

  runOnce(options: { waitTimeSeconds?: number; maxMessages?: number } = {}): Promise<number> {
    const task = this.receiveOnce(options);
    this.runs.add(task);
    task.then(() => this.runs.delete(task), () => this.runs.delete(task));
    return task;
  }

  private async receiveOnce(options: { waitTimeSeconds?: number; maxMessages?: number }): Promise<number> {
    if (this.stopping) return 0;
    const waitTimeSeconds = options.waitTimeSeconds ?? 0;
    const maxMessages = options.maxMessages ?? 10;
    if (!Number.isInteger(waitTimeSeconds) || waitTimeSeconds < 0 || waitTimeSeconds > 20
      || !Number.isInteger(maxMessages) || maxMessages < 1 || maxMessages > 10) {
      throw new Error('Invalid SQS receive options');
    }
    const receiver = new AbortController();
    this.receivers.add(receiver);
    let messages: Message[];
    try {
      const response = await this.sqs.send(new ReceiveMessageCommand({
        QueueUrl: this.commandsUrl,
        MaxNumberOfMessages: maxMessages,
        WaitTimeSeconds: waitTimeSeconds,
        VisibilityTimeout: this.visibilitySeconds,
        MessageSystemAttributeNames: ['ApproximateReceiveCount', 'MessageGroupId'],
      }), { abortSignal: receiver.signal });
      messages = response.Messages ?? [];
    } catch (error) {
      if (this.stopping && receiver.signal.aborted) return 0;
      throw error;
    } finally {
      this.receivers.delete(receiver);
    }
    const tasks = messages.map(message => {
      if (!message.ReceiptHandle) return Promise.reject(new Error('SQS returned a message without a receipt handle'));
      const entry: InFlight = { message, receipt: message.ReceiptHandle, released: false };
      this.inFlight.set(entry.receipt, entry);
      entry.task = this.stopping ? this.release(entry) : this.process(entry);
      return entry.task;
    });
    const results = await Promise.allSettled(tasks);
    const failed = results.find((result): result is PromiseRejectedResult => result.status === 'rejected');
    if (failed) throw failed.reason;
    return messages.length;
  }

  async run(): Promise<void> {
    while (!this.stopping) {
      try { await this.runOnce({ waitTimeSeconds: 20 }); } catch (error) {
        if (this.stopping) break;
        this.reportError(error);
        await new Promise<void>(resolve => { setTimeout(resolve, 1_000); });
      }
    }
  }

  stop(timeoutMs = 10_000): Promise<void> {
    if (!this.stopTask) this.stopTask = this.drain(timeoutMs);
    return this.stopTask;
  }

  private async drain(timeoutMs: number): Promise<void> {
    this.stopping = true;
    for (const receiver of this.receivers) receiver.abort();
    let timer: ReturnType<typeof setTimeout> | undefined;
    await Promise.race([
      Promise.allSettled(this.runs),
      new Promise<void>(resolve => { timer = setTimeout(resolve, Math.max(0, timeoutMs)); }),
    ]).finally(() => clearTimeout(timer));
    const entries = [...this.inFlight.values()];
    for (const entry of entries) entry.released = true;
    await Promise.allSettled(entries.map(entry => this.release(entry)));
  }

  private async process(entry: InFlight): Promise<void> {
    this.startHeartbeat(entry);
    let envelope: WagerCommandEnvelope | undefined;
    try {
      envelope = parseWagerCommand(entry.message.Body);
      const result = await this.handler.submit(envelope.data, envelope.data.idempotencyKey, {
        consumerName: this.consumerName,
        messageId: envelope.messageId,
      });
      if (entry.released) return;
      await this.options.afterCommitBeforeAck?.(result, envelope);
      if (entry.released) return;
      await this.stopHeartbeat(entry);
      await this.ack(entry);
      this.observe(this.options.onProcessed, {
        consumerName: this.consumerName, messageId: envelope.messageId, status: result.status,
        ...(result.transactionId === undefined ? {} : { transactionId: result.transactionId }),
      });
    } catch (error) {
      if (entry.released) return;
      await this.stopHeartbeat(entry);
      if (statusCode(error) === 422) {
        await this.ack(entry);
        this.observe(this.options.onProcessed, {
          consumerName: this.consumerName,
          messageId: envelope?.messageId ?? entry.message.MessageId ?? 'unknown',
          status: 'REJECTED', failureCode: failureCode(error, 'BUSINESS_REJECTION'),
        });
        return;
      }
      const attempt = Math.max(1, Number.parseInt(entry.message.Attributes?.ApproximateReceiveCount ?? '1', 10) || 1);
      if (isPermanent(error) || attempt >= this.maxAttempts) {
        const code = failureCode(error, isPermanent(error) ? 'INVALID_COMMAND' : 'RETRY_EXHAUSTED');
        let audited: WagerCommandResult | undefined;
        if (envelope) {
          try {
            audited = await this.handler.failSubmission?.(envelope.data, envelope.data.idempotencyKey, code, {
              consumerName: this.consumerName, messageId: envelope.messageId,
            });
          } catch (auditError) {
            // The original command remains recoverable in the DLQ if the database is unavailable.
            this.reportError(auditError);
          }
        }
        await this.deadLetter(entry, code);
        this.observe(this.options.onDeadLetter, {
          consumerName: this.consumerName,
          messageId: envelope?.messageId ?? entry.message.MessageId ?? 'unknown',
          failureCode: code, attempt,
          ...(audited?.status === undefined ? {} : { status: audited.status }),
          ...(audited?.transactionId === undefined ? {} : { transactionId: audited.transactionId }),
        });
      } else {
        const delaySeconds = Math.min(60, 2 ** Math.min(attempt - 1, 6));
        await this.sqs.send(new ChangeMessageVisibilityCommand({
          QueueUrl: this.commandsUrl,
          ReceiptHandle: entry.receipt,
          VisibilityTimeout: delaySeconds,
        }));
        this.observe(this.options.onRetry, {
          consumerName: this.consumerName,
          messageId: envelope?.messageId ?? entry.message.MessageId ?? 'unknown',
          failureCode: failureCode(error, 'TRANSIENT_FAILURE'), attempt, delaySeconds,
        });
      }
    } finally {
      await this.stopHeartbeat(entry);
      this.inFlight.delete(entry.receipt);
    }
  }

  private startHeartbeat(entry: InFlight): void {
    entry.heartbeatTimer = setInterval(() => {
      if (entry.released || entry.heartbeatTask) return;
      entry.heartbeatTask = this.sqs.send(new ChangeMessageVisibilityCommand({
        QueueUrl: this.commandsUrl,
        ReceiptHandle: entry.receipt,
        VisibilityTimeout: this.visibilitySeconds,
      }), { abortSignal: AbortSignal.timeout(Math.max(500, Math.min(3_000, this.visibilitySeconds * 500))) })
        .then(() => undefined).catch(error => { this.reportError(error); }).finally(() => {
        delete entry.heartbeatTask;
      });
    }, Math.max(250, Math.floor(this.visibilitySeconds * 1_000 / 3)));
    entry.heartbeatTimer.unref();
  }

  private async stopHeartbeat(entry: InFlight): Promise<void> {
    clearInterval(entry.heartbeatTimer);
    await entry.heartbeatTask;
  }

  private async ack(entry: InFlight): Promise<void> {
    if (!entry.released) {
      await this.sqs.send(new DeleteMessageCommand({ QueueUrl: this.commandsUrl, ReceiptHandle: entry.receipt }));
    }
  }

  private async deadLetter(entry: InFlight, code: string): Promise<void> {
    const sourceId = entry.message.MessageId ?? createHash('sha256').update(entry.message.Body ?? '').digest('hex');
    const deduplicationId = createHash('sha256').update(`${this.commandsUrl}\0${sourceId}\0${code}`).digest('hex');
    await this.sqs.send(new SendMessageCommand({
      QueueUrl: this.dlqUrl,
      MessageBody: entry.message.Body ?? '{}',
      MessageGroupId: entry.message.Attributes?.MessageGroupId ?? 'invalid-command',
      MessageDeduplicationId: deduplicationId,
      MessageAttributes: {
        failureCode: { DataType: 'String', StringValue: code },
        sourceMessageId: { DataType: 'String', StringValue: sourceId },
      },
    }));
    await this.ack(entry);
  }

  private async release(entry: InFlight): Promise<void> {
    entry.released = true;
    await this.stopHeartbeat(entry);
    try {
      await this.sqs.send(new ChangeMessageVisibilityCommand({
        QueueUrl: this.commandsUrl,
        ReceiptHandle: entry.receipt,
        VisibilityTimeout: 0,
      }), { abortSignal: AbortSignal.timeout(3_000) });
    } catch (error) {
      this.reportError(error);
      throw error;
    } finally {
      this.inFlight.delete(entry.receipt);
    }
  }

  private reportError(error: unknown): void {
    try { this.options.onError?.(error); } catch { /* Diagnostics must not interrupt acknowledgement or recovery. */ }
  }

  private observe(callback: ((value: SqsConsumerObservation) => void) | undefined, value: SqsConsumerObservation): void {
    try { callback?.(value); } catch (error) { this.reportError(error); }
  }
}
