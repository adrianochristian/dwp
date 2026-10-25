import { DomainError } from './domain-error.js';
import type { IntegrationEvent } from './integration-event.js';

export interface OutboxMessageState {
  id: string;
  aggregateId: string;
  eventType: string;
  payload: Readonly<Record<string, unknown>>;
  occurredAt: Date;
  attempts: number;
  nextAttemptAt?: Date | undefined;
  publishedAt?: Date | undefined;
}

function freezeRecursively(value: unknown): void {
  if (value !== null && typeof value === 'object') {
    for (const child of Object.values(value)) freezeRecursively(child);
    Object.freeze(value);
  }
}

function snapshot(payload: Readonly<Record<string, unknown>>): Readonly<Record<string, unknown>> {
  const copy = JSON.parse(JSON.stringify(payload)) as Record<string, unknown>;
  freezeRecursively(copy);
  return copy;
}

export class OutboxMessage {
  private readonly identity: Pick<OutboxMessageState, 'id' | 'aggregateId' | 'eventType'>;
  private readonly serializedPayload: Readonly<Record<string, unknown>>;
  private readonly occurredTimestamp: number;
  private retryAttempts: number;
  private nextAttemptTimestamp: number | undefined;
  private publishedTimestamp: number | undefined;

  private constructor(state: OutboxMessageState) {
    this.identity = Object.freeze({ id: state.id, aggregateId: state.aggregateId, eventType: state.eventType });
    this.serializedPayload = snapshot(state.payload);
    this.occurredTimestamp = state.occurredAt.getTime();
    this.retryAttempts = state.attempts;
    this.nextAttemptTimestamp = state.nextAttemptAt?.getTime();
    this.publishedTimestamp = state.publishedAt?.getTime();
  }

  static enqueue(event: IntegrationEvent<unknown>): OutboxMessage {
    return new OutboxMessage({
      id: event.eventId,
      aggregateId: event.aggregateId,
      eventType: event.eventType,
      payload: event.toJSON(),
      occurredAt: event.occurredAt,
      attempts: 0,
    });
  }

  static rehydrate(state: OutboxMessageState): OutboxMessage {
    return new OutboxMessage(state);
  }

  get id(): string { return this.identity.id; }
  get aggregateId(): string { return this.identity.aggregateId; }
  get eventType(): string { return this.identity.eventType; }
  get payload(): Readonly<Record<string, unknown>> { return this.serializedPayload; }
  get occurredAt(): Date { return new Date(this.occurredTimestamp); }
  get attempts(): number { return this.retryAttempts; }
  get nextAttemptAt(): Date | undefined {
    return this.nextAttemptTimestamp === undefined ? undefined : new Date(this.nextAttemptTimestamp);
  }
  get publishedAt(): Date | undefined {
    return this.publishedTimestamp === undefined ? undefined : new Date(this.publishedTimestamp);
  }

  isPending(): boolean { return this.publishedTimestamp === undefined; }
  isDue(now: Date): boolean {
    return this.isPending() && (this.nextAttemptTimestamp === undefined || this.nextAttemptTimestamp <= now.getTime());
  }

  markPublished(at: Date): void {
    this.assertPending();
    this.assertValidDate(at);
    this.publishedTimestamp = at.getTime();
    this.nextAttemptTimestamp = undefined;
  }

  scheduleRetry(now: Date): void {
    this.assertPending();
    this.assertValidDate(now);
    this.retryAttempts += 1;
    const backoffSeconds = Math.min(60, 2 ** Math.min(this.retryAttempts - 1, 6));
    this.nextAttemptTimestamp = now.getTime() + backoffSeconds * 1000;
  }

  private assertPending(): void {
    if (!this.isPending()) {
      throw new DomainError('INVALID_OUTBOX_STATE', 'A published outbox message cannot transition');
    }
  }

  private assertValidDate(at: Date): void {
    if (!Number.isFinite(at.getTime())) {
      throw new DomainError('INVALID_MESSAGE_DATE', 'Outbox transition date must be valid');
    }
  }
}
