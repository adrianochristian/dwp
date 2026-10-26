import { SendMessageCommand, type SQSClient } from '@aws-sdk/client-sqs';
import type { MikroORM } from '@mikro-orm/postgresql';
import { OutboxMessage } from '../../domain/outbox-message.js';
import { sqsCall, type ShutdownOptions } from './sqs.js';
import { normalizeShutdownOptions, waitForSettlement } from './shutdown.js';

export interface OutboxDelivery {
  readonly id: string;
  readonly aggregateId: string;
  readonly eventType: string;
  readonly payload: Readonly<Record<string, unknown>>;
  readonly occurredAt: Date;
  readonly attempts: number;
}

export interface OutboxPublisherOptions {
  leaseMs?: number;
  batchSize?: number;
  operationTimeoutMs?: number;
  beforeSend?: (message: OutboxDelivery) => Promise<void> | void;
  afterSend?: (message: OutboxDelivery) => Promise<void> | void;
  onRetry?: (message: OutboxDelivery, error: unknown) => void;
  onLeaseLost?: (message: OutboxDelivery) => void;
}

interface ClaimedRow {
  id: string;
  aggregate_id: string;
  event_type: string;
  payload: Record<string, unknown>;
  occurred_at: Date;
  attempts: number;
  lease_token: string;
}

interface ActiveDelivery {
  id: string;
  token: string;
  abort: AbortController;
  stopHeartbeat(): void;
}

export class OutboxPublisher {
  private readonly leaseMs: number;
  private readonly batchSize: number;
  private readonly active = new Set<Promise<number>>();
  private readonly deliveries = new Set<ActiveDelivery>();
  private stopped = false;
  private cancelling = false;
  private stopTask: Promise<void> | undefined;

  constructor(
    private readonly orm: MikroORM,
    private readonly sqs: SQSClient,
    private readonly queueUrl: string,
    private readonly options: OutboxPublisherOptions = {},
  ) {
    this.leaseMs = options.leaseMs ?? 30_000;
    this.batchSize = options.batchSize ?? 10;
    if (!Number.isSafeInteger(this.leaseMs) || this.leaseMs < 60) {
      throw new Error('Outbox leaseMs must be an integer of at least 60 milliseconds');
    }
    if (!Number.isInteger(this.batchSize) || this.batchSize < 1 || this.batchSize > 100) {
      throw new Error('Outbox batchSize must be an integer between 1 and 100');
    }
  }

  async runOnce(): Promise<number> {
    if (this.stopped) return 0;
    const batch = this.publishBatch();
    this.active.add(batch);
    try {
      return await batch;
    } finally {
      this.active.delete(batch);
    }
  }

  quiesce(): void {
    this.stopped = true;
  }

  stop(input: number | ShutdownOptions = {}): Promise<void> {
    if (!this.stopTask) this.stopTask = this.drain(input);
    return this.stopTask;
  }

  private async drain(input: number | ShutdownOptions): Promise<void> {
    const options = normalizeShutdownOptions(input);
    this.quiesce();
    if (await waitForSettlement(this.active, options.timeoutMs, options.signal)) return;
    this.cancelling = true;
    const abandoned = [...this.deliveries];
    for (const delivery of abandoned) {
      delivery.stopHeartbeat();
      delivery.abort.abort(new DOMException('Publisher shutting down', 'AbortError'));
    }
    if (options.cleanupMs === 0 || options.signal?.aborted || abandoned.length === 0) return;
    // A blocked pool or row must not prevent shutdown. Failed cleanup leaves the
    // finite lease to expire; it never changes published state or the event id.
    await waitForSettlement([this.releaseOwnedLeases(abandoned)], options.cleanupMs, options.signal);
  }

  private async releaseOwnedLeases(deliveries: ActiveDelivery[]): Promise<void> {
    await this.orm.em.fork().transactional(async em => {
      await em.execute("select set_config('lock_timeout', '1000ms', true), set_config('statement_timeout', '2000ms', true)");
      const rows = deliveries.map(() => '(?::uuid, ?::uuid)').join(', ');
      const parameters = deliveries.flatMap(delivery => [delivery.id, delivery.token]);
      await em.execute(`update dwp.outbox_messages as message
        set lease_token = null, lease_until = null
        from (values ${rows}) as owned(id, token)
        where message.id = owned.id and message.lease_token = owned.token and message.published_at is null`, parameters);
    });
  }

  private async publishBatch(): Promise<number> {
    const token = crypto.randomUUID();
    const claimed = await this.orm.em.fork().transactional((em) => em.execute<ClaimedRow[]>(`
      with due as (
        select id from dwp.outbox_messages
        where published_at is null and next_attempt_at <= clock_timestamp()
          and (lease_until is null or lease_until <= clock_timestamp())
        order by next_attempt_at, occurred_at, id
        for update skip locked limit ?
      )
      update dwp.outbox_messages as message
      set attempts = message.attempts + 1,
          lease_token = ?, lease_until = clock_timestamp() + (? * interval '1 millisecond')
      from due where message.id = due.id
      returning message.id, message.aggregate_id, message.event_type, message.payload,
                message.occurred_at, message.attempts, message.lease_token
    `, [this.batchSize, token, this.leaseMs]));

    if (this.cancelling) return 0;
    const results = await Promise.allSettled(claimed.map((row) => this.publish(row)));
    const failure = results.find((result) => result.status === 'rejected');
    if (failure?.status === 'rejected') throw failure.reason;
    return results.filter((result) => result.status === 'fulfilled' && result.value).length;
  }

  private async publish(row: ClaimedRow): Promise<boolean> {
    const outbox = OutboxMessage.rehydrate({
      id: row.id,
      aggregateId: row.aggregate_id,
      eventType: row.event_type,
      payload: row.payload,
      occurredAt: new Date(row.occurred_at),
      attempts: row.attempts,
    });
    const message: OutboxDelivery = Object.freeze({
      id: outbox.id,
      aggregateId: outbox.aggregateId,
      eventType: outbox.eventType,
      payload: outbox.payload,
      occurredAt: outbox.occurredAt,
      attempts: outbox.attempts,
    });
    const abort = new AbortController();
    let owned = true;
    let renewal: Promise<boolean> | undefined;

    const renew = (): Promise<boolean> => {
      if (renewal) return renewal;
      if (!owned) return Promise.resolve(false);
      const query = this.orm.em.fork().execute<{ id: string }[]>(`
        update dwp.outbox_messages
        set lease_until = clock_timestamp() + (? * interval '1 millisecond')
        where id = ? and lease_token = ? and published_at is null
          and lease_until > clock_timestamp()
        returning id
      `, [this.leaseMs, row.id, row.lease_token]);
      const pending = (async () => {
        try {
          const finished = await waitForSettlement([query], this.leaseMs, abort.signal);
          if (!finished) {
            owned = false;
          } else {
            owned = (await query).length === 1 && !this.cancelling;
          }
        } catch {
          owned = false;
        }
        if (!owned) abort.abort();
        return owned;
      })();
      renewal = pending;
      void pending.then(() => { if (renewal === pending) renewal = undefined; });
      return pending;
    };

    const heartbeat = setInterval(() => { void renew(); }, Math.floor(this.leaseMs / 3));
    const delivery: ActiveDelivery = {
      id: row.id, token: row.lease_token, abort, stopHeartbeat: () => clearInterval(heartbeat),
    };
    this.deliveries.add(delivery);
    const finishHeartbeat = async (): Promise<void> => {
      clearInterval(heartbeat);
      if (renewal) await renewal;
    };

    try {
      await this.options.beforeSend?.(message);
      if (this.cancelling || !await renew()) {
        if (!this.cancelling) this.observe(() => this.options.onLeaseLost?.(message));
        return false;
      }
      await sqsCall(abortSignal => this.sqs.send(new SendMessageCommand({
        QueueUrl: this.queueUrl,
        MessageBody: JSON.stringify(message.payload),
        MessageAttributes: {
          eventId: { DataType: 'String', StringValue: message.id },
          eventType: { DataType: 'String', StringValue: message.eventType },
        },
      }), { abortSignal }), { signal: abort.signal, timeoutMs: this.options.operationTimeoutMs });
      await this.options.afterSend?.(message);
      await finishHeartbeat();
      if (this.cancelling || !owned) {
        if (!this.cancelling) this.observe(() => this.options.onLeaseLost?.(message));
        return false;
      }
      const updated = await this.orm.em.fork().execute<{ id: string }[]>(`
        update dwp.outbox_messages
        set published_at = clock_timestamp(), lease_until = null, lease_token = null
        where id = ? and lease_token = ? and published_at is null
          and lease_until > clock_timestamp()
        returning id
      `, [message.id, row.lease_token]);
      if (updated.length !== 1) this.observe(() => this.options.onLeaseLost?.(message));
      return updated.length === 1;
    } catch (error) {
      await finishHeartbeat();
      if (this.cancelling || !owned) {
        if (!this.cancelling) this.observe(() => this.options.onLeaseLost?.(message));
        return false;
      }
      const retry = OutboxMessage.rehydrate({
        id: message.id,
        aggregateId: message.aggregateId,
        eventType: message.eventType,
        payload: message.payload,
        occurredAt: message.occurredAt,
        attempts: message.attempts - 1,
      });
      retry.scheduleRetry(new Date());
      const updated = await this.orm.em.fork().execute<{ id: string }[]>(`
        update dwp.outbox_messages
        set next_attempt_at = ?, lease_until = null, lease_token = null
        where id = ? and lease_token = ? and published_at is null
        returning id
      `, [retry.nextAttemptAt!, message.id, row.lease_token]);
      if (updated.length === 1) this.observe(() => this.options.onRetry?.(message, error));
      else this.observe(() => this.options.onLeaseLost?.(message));
      return false;
    } finally {
      await finishHeartbeat();
      this.deliveries.delete(delivery);
    }
  }

  private observe(callback: () => void): void {
    try { callback(); } catch { /* Diagnostic callbacks cannot change durable delivery state. */ }
  }
}
