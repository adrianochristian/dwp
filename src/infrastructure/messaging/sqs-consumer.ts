import { ApplicationError } from '../../application/application-error.js';
import {
  ChangeMessageVisibilityCommand,
  DeleteMessageCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
  type Message,
  type SQSClient,
} from '@aws-sdk/client-sqs';
import { createHash } from 'node:crypto';
import { sqsCall, sqsDiagnosticCode, type ShutdownOptions } from './sqs.js';
import { normalizeShutdownOptions, waitForSettlement } from './shutdown.js';

export interface WagerCommandContext {
  consumerName: string;
  messageId: string;
}

export interface WagerCommandResult {
  status: string;
  transactionId?: string;
  failureCode?: string;
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
  operationTimeoutMs?: number;
  afterCommitBeforeAck?: (result: WagerCommandResult, message: WagerCommandEnvelope) => Promise<void> | void;
  onError?: (error: unknown, observation?: SqsConsumerObservation) => void;
  onRetry?: (observation: SqsConsumerObservation) => void;
  onDeadLetter?: (observation: SqsConsumerObservation) => void;
  onProcessed?: (observation: SqsConsumerObservation) => void;
}

export interface SqsConsumerObservation {
  consumerName: string;
  messageId: string;
  transactionId?: string;
  correlationId?: string;
  walletId?: string;
  providerId?: string;
  status?: string;
  failureCode?: string;
  failureStage?: 'processing' | 'ack' | 'receive' | 'visibility' | 'diagnostic';
  causeCode?: string;
  attempt?: number;
  delaySeconds?: number;
}

export interface SqsReceiveOptions {
  waitTimeSeconds?: number;
  maxMessages?: number;
  signal?: AbortSignal;
}

interface InFlight {
  message: Message;
  receipt: string;
  released: boolean;
  controller: AbortController;
  observation: SqsConsumerObservation;
  terminalFailureCode?: string;
  acknowledged?: boolean;
  heartbeatTimer?: ReturnType<typeof setInterval>;
  heartbeatStopped?: boolean;
  heartbeatController?: AbortController;
  heartbeatTask?: Promise<void>;
  releaseTask?: Promise<void>;
  removeCallerListener?: () => void;
  task?: Promise<void>;
}

class InvalidCommandEnvelope extends Error {
  readonly code = 'INVALID_COMMAND_ENVELOPE';
}

class AcknowledgementFailed extends Error {
  readonly code = 'ACK_FAILED';
  readonly causeCode: string;

  constructor(error: unknown) {
    super('The committed command could not be acknowledged');
    this.name = 'AcknowledgementFailed';
    this.causeCode = sqsDiagnosticCode(error, 'SQS_TRANSPORT_FAILED');
  }
}

export class SqsConsumerDiagnosticError extends Error {
  constructor(readonly code: string) {
    super(`SQS consumer diagnostic: ${code}`);
    this.name = 'SqsConsumerDiagnosticError';
  }
}

function isObject(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function validText(value: unknown, maximum: number): value is string {
  return typeof value === 'string' && value.length > 0 && value.length <= maximum
    && value.trim() === value && !/[\u0000-\u001f]/.test(value) && !/[\uD800-\uDFFF]/u.test(value);
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
  const transportCode = sqsDiagnosticCode(error, '');
  if (transportCode) return transportCode;
  if (error === null || typeof error !== 'object') return fallback;
  const value = error as { code?: unknown; response?: unknown; getResponse?: () => unknown };
  const response = typeof value.getResponse === 'function' ? value.getResponse() : value.response;
  const code = isObject(response) ? response.code : value.code;
  return typeof code === 'string' && /^[A-Z][A-Z0-9_]{0,99}$/.test(code) ? code : fallback;
}

function isPermanent(error: unknown): boolean {
  if (error instanceof ApplicationError) return error.kind !== 'unavailable';
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
  private readonly retryWaits = new Set<() => void>();
  private readonly errorContexts = new WeakMap<object, SqsConsumerObservation>();
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
    if (!validText(this.consumerName, 200)
      || !Number.isInteger(this.visibilitySeconds) || this.visibilitySeconds < 1 || this.visibilitySeconds > 43_200
      || !Number.isInteger(this.maxAttempts) || this.maxAttempts < 1) {
      throw new Error('Invalid SQS visibility timeout or attempt limit');
    }
    if (options.operationTimeoutMs !== undefined
      && (!Number.isSafeInteger(options.operationTimeoutMs) || options.operationTimeoutMs < 1
        || options.operationTimeoutMs > 2_147_483_647)) {
      throw new Error('Invalid SQS operation timeout');
    }
  }

  runOnce(options: SqsReceiveOptions = {}): Promise<number> {
    const task = this.receiveOnce(options);
    this.runs.add(task);
    task.then(() => this.runs.delete(task), () => this.runs.delete(task));
    if (!options.signal) return task;
    const signal = options.signal;
    return new Promise<number>((resolve, reject) => {
      const aborted = () => {
        signal.removeEventListener('abort', aborted);
        reject(signal.reason ?? new DOMException('SQS receive cancelled', 'AbortError'));
      };
      signal.addEventListener('abort', aborted, { once: true });
      task.then(value => {
        signal.removeEventListener('abort', aborted);
        resolve(value);
      }, error => {
        signal.removeEventListener('abort', aborted);
        reject(error);
      });
      if (signal.aborted) aborted();
    });
  }

  private async receiveOnce(options: SqsReceiveOptions): Promise<number> {
    if (this.stopping) return 0;
    if (options.signal?.aborted) throw options.signal.reason ?? new DOMException('SQS receive cancelled', 'AbortError');
    const waitTimeSeconds = options.waitTimeSeconds ?? 0;
    const maxMessages = options.maxMessages ?? 10;
    if (!Number.isInteger(waitTimeSeconds) || waitTimeSeconds < 0 || waitTimeSeconds > 20
      || !Number.isInteger(maxMessages) || maxMessages < 1 || maxMessages > 10) {
      throw new Error('Invalid SQS receive options');
    }
    const receiver = new AbortController();
    const cancelReceive = () => receiver.abort(options.signal?.reason);
    options.signal?.addEventListener('abort', cancelReceive, { once: true });
    this.receivers.add(receiver);
    let messages: Message[];
    try {
      const response = await this.transport(signal => this.sqs.send(new ReceiveMessageCommand({
        QueueUrl: this.commandsUrl,
        MaxNumberOfMessages: maxMessages,
        WaitTimeSeconds: waitTimeSeconds,
        VisibilityTimeout: this.visibilitySeconds,
        MessageSystemAttributeNames: ['ApproximateReceiveCount', 'MessageGroupId'],
      }), { abortSignal: signal }), receiver.signal);
      messages = response.Messages ?? [];
    } catch (error) {
      if (this.stopping && receiver.signal.aborted) return 0;
      throw error;
    } finally {
      options.signal?.removeEventListener('abort', cancelReceive);
      this.receivers.delete(receiver);
    }
    const tasks = messages.map(message => {
      if (!message.ReceiptHandle) return Promise.reject(new Error('SQS returned a message without a receipt handle'));
      const entry: InFlight = {
        message, receipt: message.ReceiptHandle, released: false, controller: new AbortController(),
        observation: {
          consumerName: this.consumerName,
          messageId: validText(message.MessageId, 200) ? message.MessageId : 'unknown',
          correlationId: validText(message.MessageId, 200) ? message.MessageId : crypto.randomUUID(),
        },
      };
      this.inFlight.set(entry.receipt, entry);
      if (options.signal) {
        const signal = options.signal;
        const cancelled = () => { void this.release(entry, 3_000, signal.reason); };
        signal.addEventListener('abort', cancelled, { once: true });
        entry.removeCallerListener = () => signal.removeEventListener('abort', cancelled);
        if (signal.aborted) cancelled();
      }
      entry.task = (this.stopping ? this.release(entry) : this.process(entry)).catch(error => {
        if (error !== null && typeof error === 'object') this.errorContexts.set(error, this.observation(entry,
          error instanceof AcknowledgementFailed ? { failureStage: 'ack', causeCode: error.causeCode } : {}));
        throw error;
      });
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
        await this.waitForRetry();
      }
    }
  }

  quiesce(): void {
    this.stopping = true;
    for (const receiver of this.receivers) receiver.abort(new DOMException('SQS consumer stopping', 'AbortError'));
    for (const wake of this.retryWaits) wake();
  }

  stop(options: number | ShutdownOptions = 10_000): Promise<void> {
    if (!this.stopTask) this.stopTask = this.drain(options);
    return this.stopTask;
  }

  private async drain(options: number | ShutdownOptions): Promise<void> {
    const { timeoutMs, cleanupMs, signal } = normalizeShutdownOptions(options);
    this.quiesce();
    await waitForSettlement(this.runs, timeoutMs, signal);
    const entries = [...this.inFlight.values()];
    for (const entry of entries) entry.released = true;
    await waitForSettlement(entries.map(entry => this.release(entry, cleanupMs, signal?.reason)), cleanupMs);
  }

  private async process(entry: InFlight): Promise<void> {
    if (entry.released) return;
    this.startHeartbeat(entry);
    let envelope: WagerCommandEnvelope | undefined;
    try {
      envelope = parseWagerCommand(entry.message.Body);
      this.identify(entry, envelope);
      const result = await this.handler.submit(envelope.data, envelope.data.idempotencyKey, {
        consumerName: this.consumerName,
        messageId: envelope.messageId,
      });
      this.identify(entry, envelope, result);
      if (entry.released) return;
      await this.options.afterCommitBeforeAck?.(result, envelope);
      if (entry.released) return;
      if (result.status === 'FAILED') {
        const code = result.failureCode ?? 'PROCESSING_FAILED';
        await this.deadLetter(entry, code);
        this.observe(this.options.onDeadLetter, this.observation(entry, { failureCode: code, failureStage: 'processing' }));
        return;
      }
      await this.ack(entry);
      this.observe(this.options.onProcessed, this.observation(entry));
    } catch (error) {
      if (entry.released) return;
      if (statusCode(error) === 422) {
        await this.ack(entry);
        this.observe(this.options.onProcessed, this.observation(entry, {
          status: 'REJECTED', failureCode: failureCode(error, 'BUSINESS_REJECTION'), failureStage: 'processing',
        }));
        return;
      }
      const attempt = Math.max(1, Number.parseInt(entry.message.Attributes?.ApproximateReceiveCount ?? '1', 10) || 1);
      const acknowledgementFailed = error instanceof AcknowledgementFailed;
      const failureStage = acknowledgementFailed ? 'ack' : 'processing';
      const causeCode = acknowledgementFailed ? error.causeCode : undefined;
      if (isPermanent(error) || attempt >= this.maxAttempts) {
        const code = acknowledgementFailed ? 'ACK_FAILED'
          : entry.terminalFailureCode ?? failureCode(error, isPermanent(error) ? 'INVALID_COMMAND' : 'RETRY_EXHAUSTED');
        let audited: WagerCommandResult | undefined;
        if (envelope && !acknowledgementFailed && entry.observation.status !== 'FAILED') {
          try {
            audited = await this.handler.failSubmission?.(envelope.data, envelope.data.idempotencyKey, code, {
              consumerName: this.consumerName, messageId: envelope.messageId,
            });
            if (audited) this.identify(entry, envelope, audited);
          } catch (auditError) {
            if (entry.released) return;
            // The original command remains recoverable in the DLQ if the database is unavailable.
            this.reportError(auditError, this.observation(entry));
          }
        }
        if (entry.released) return;
        await this.deadLetter(entry, code, failureStage, causeCode);
        this.observe(this.options.onDeadLetter, this.observation(entry, {
          failureCode: code, failureStage, attempt, ...(causeCode ? { causeCode } : {}),
        }));
      } else {
        const delaySeconds = Math.min(60, 2 ** Math.min(attempt - 1, 6));
        await this.stopHeartbeat(entry);
        if (entry.released) return;
        await this.transport(signal => this.sqs.send(new ChangeMessageVisibilityCommand({
          QueueUrl: this.commandsUrl,
          ReceiptHandle: entry.receipt,
          VisibilityTimeout: delaySeconds,
        }), { abortSignal: signal }), entry.controller.signal);
        this.observe(this.options.onRetry, this.observation(entry, {
          failureCode: failureCode(error, 'TRANSIENT_FAILURE'), failureStage, attempt, delaySeconds,
          ...(causeCode ? { causeCode } : {}),
        }));
      }
    } finally {
      await this.stopHeartbeat(entry);
      entry.removeCallerListener?.();
      if (!entry.releaseTask) this.inFlight.delete(entry.receipt);
    }
  }

  private startHeartbeat(entry: InFlight): void {
    entry.heartbeatStopped = false;
    entry.heartbeatTimer = setInterval(() => {
      if (entry.released || entry.heartbeatStopped || entry.heartbeatTask) return;
      const controller = new AbortController();
      entry.heartbeatController = controller;
      const signal = AbortSignal.any([entry.controller.signal, controller.signal]);
      entry.heartbeatTask = this.transport(abortSignal => this.sqs.send(new ChangeMessageVisibilityCommand({
        QueueUrl: this.commandsUrl,
        ReceiptHandle: entry.receipt,
        VisibilityTimeout: this.visibilitySeconds,
      }), { abortSignal }), signal, Math.max(500, Math.min(3_000, this.visibilitySeconds * 500)))
        .then(() => undefined).catch(error => {
          if (!controller.signal.aborted && !entry.released) this.reportError(error, this.observation(entry, { failureStage: 'visibility' }));
        }).finally(() => {
        delete entry.heartbeatTask;
        if (entry.heartbeatController === controller) delete entry.heartbeatController;
      });
    }, Math.max(250, Math.floor(this.visibilitySeconds * 1_000 / 3)));
    entry.heartbeatTimer.unref();
  }

  private async stopHeartbeat(entry: InFlight): Promise<void> {
    entry.heartbeatStopped = true;
    clearInterval(entry.heartbeatTimer);
    entry.heartbeatController?.abort(new DOMException('SQS heartbeat stopped', 'AbortError'));
    await entry.heartbeatTask;
  }

  private async ack(entry: InFlight): Promise<void> {
    if (!entry.released) {
      try {
        await this.transport(signal => this.sqs.send(new DeleteMessageCommand({
          QueueUrl: this.commandsUrl, ReceiptHandle: entry.receipt,
        }), { abortSignal: signal }), entry.controller.signal);
        entry.acknowledged = true;
      } catch (error) {
        if (entry.released) throw error;
        throw new AcknowledgementFailed(error);
      }
    }
  }

  private async deadLetter(entry: InFlight, code: string, failureStage: 'processing' | 'ack' = 'processing', causeCode?: string): Promise<void> {
    if (entry.released) return;
    const sourceId = entry.message.MessageId ?? createHash('sha256').update(entry.message.Body ?? '').digest('hex');
    const deduplicationId = createHash('sha256').update(`${this.commandsUrl}\0${sourceId}\0${code}`).digest('hex');
    await this.transport(signal => this.sqs.send(new SendMessageCommand({
      QueueUrl: this.dlqUrl,
      MessageBody: entry.message.Body ?? '{}',
      MessageGroupId: entry.message.Attributes?.MessageGroupId ?? 'invalid-command',
      MessageDeduplicationId: deduplicationId,
      MessageAttributes: {
        failureCode: { DataType: 'String', StringValue: code },
        sourceMessageId: { DataType: 'String', StringValue: sourceId },
        failureStage: { DataType: 'String', StringValue: failureStage },
        ...Object.fromEntries(Object.entries({
          transactionStatus: entry.observation.status,
          transactionId: entry.observation.transactionId,
          correlationId: entry.observation.correlationId,
          walletId: entry.observation.walletId,
          providerId: entry.observation.providerId,
          messageId: entry.observation.messageId,
          causeCode,
        }).filter((field): field is [string, string] => typeof field[1] === 'string')
          .map(([key, value]) => [key, { DataType: 'String', StringValue: value }])),
      },
    }), { abortSignal: signal }), entry.controller.signal);
    await this.ack(entry);
  }

  private release(entry: InFlight, cleanupMs = 3_000, reason?: unknown): Promise<void> {
    if (entry.releaseTask) return entry.releaseTask;
    entry.released = true;
    entry.removeCallerListener?.();
    entry.controller.abort(reason ?? new DOMException('SQS delivery released', 'AbortError'));
    entry.releaseTask = this.releaseVisibility(entry, cleanupMs);
    return entry.releaseTask;
  }

  private async releaseVisibility(entry: InFlight, cleanupMs: number): Promise<void> {
    await this.stopHeartbeat(entry);
    try {
      if (entry.acknowledged || cleanupMs === 0) return;
      await this.transport(signal => this.sqs.send(new ChangeMessageVisibilityCommand({
        QueueUrl: this.commandsUrl,
        ReceiptHandle: entry.receipt,
        VisibilityTimeout: 0,
      }), { abortSignal: signal }), undefined, cleanupMs);
    } catch (error) {
      this.reportError(error, this.observation(entry, { failureStage: 'visibility' }));
    } finally {
      this.inFlight.delete(entry.receipt);
    }
  }

  private transport<T>(operation: (signal: AbortSignal) => Promise<T>, signal?: AbortSignal, timeoutMs = this.options.operationTimeoutMs): Promise<T> {
    return sqsCall(operation, {
      ...(signal ? { signal } : {}),
      ...(timeoutMs === undefined ? {} : { timeoutMs }),
    });
  }

  private waitForRetry(): Promise<void> {
    return new Promise(resolve => {
      const wake = () => {
        clearTimeout(timer);
        this.retryWaits.delete(wake);
        resolve();
      };
      const timer = setTimeout(wake, 1_000);
      this.retryWaits.add(wake);
      if (this.stopping) wake();
    });
  }

  private identify(entry: InFlight, envelope: WagerCommandEnvelope, result?: WagerCommandResult): void {
    try {
      const data = envelope.data;
      const uuid = (value: unknown): value is string => typeof value === 'string'
        && /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/i.test(value);
      entry.observation.messageId = envelope.messageId;
      entry.observation.correlationId = envelope.messageId;
      if (validText(data.providerId, 200)) entry.observation.providerId = data.providerId;
      if (uuid(data.walletId)) entry.observation.walletId = data.walletId.toLowerCase();
      if (result) {
        if (uuid(result.transactionId)) {
          entry.observation.transactionId = result.transactionId.toLowerCase();
          entry.observation.correlationId = result.transactionId.toLowerCase();
        }
        if (['PENDING', 'PENDING_REFERENCE', 'PROCESSED', 'REJECTED', 'FAILED'].includes(result.status)) {
          entry.observation.status = result.status;
        }
        if (result.status === 'FAILED') {
          entry.terminalFailureCode = typeof result.failureCode === 'string' && /^[A-Z][A-Z0-9_]{0,99}$/.test(result.failureCode)
            ? result.failureCode : 'PROCESSING_FAILED';
        }
      }
    } catch { /* Diagnostic identifiers cannot change a confirmed financial result. */ }
  }

  private observation(entry: InFlight, details: Partial<SqsConsumerObservation> = {}): SqsConsumerObservation {
    return { ...entry.observation, ...details };
  }

  private reportError(error: unknown, observation?: SqsConsumerObservation, fallback = 'INFRASTRUCTURE_UNAVAILABLE'): void {
    try {
      const context = observation ?? (error !== null && typeof error === 'object' ? this.errorContexts.get(error) : undefined);
      const code = failureCode(error, fallback);
      const safe = Object.freeze({
        ...(context ?? { consumerName: this.consumerName, messageId: 'unknown', correlationId: crypto.randomUUID(), failureStage: 'receive' as const }),
        failureCode: code,
      });
      this.options.onError?.(new SqsConsumerDiagnosticError(code), safe);
    } catch { /* Diagnostics must not interrupt acknowledgement or recovery. */ }
  }

  private observe(callback: ((value: SqsConsumerObservation) => void) | undefined, value: SqsConsumerObservation): void {
    try { callback?.(Object.freeze({ ...value })); } catch {
      this.reportError(new SqsConsumerDiagnosticError('DIAGNOSTIC_CALLBACK_FAILED'), value);
    }
  }
}
