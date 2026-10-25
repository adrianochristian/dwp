import type { LedgerDirection, WalletLedgerEntry } from './ledger-entry.js';
import type { MoneyProps } from './money.js';
import type { WagerTransaction, WagerTransactionKind, WagerTransactionStatus } from './wager-transaction.js';
import type { Wallet } from './wallet.js';

export interface EventContext {
  eventId?: string;
  aggregateId?: string;
  correlationId: string;
  causationId?: string;
  occurredAt?: Date;
}

export interface IntegrationEventProps<T> {
  eventId: string;
  aggregateId: string;
  correlationId: string;
  causationId?: string | undefined;
  occurredAt: Date;
  data: T;
}

export interface IntegrationEventEnvelope<T> extends Readonly<Record<string, unknown>> {
  eventId: string;
  eventType: string;
  aggregateId: string;
  correlationId: string;
  causationId?: string;
  occurredAt: string;
  version: number;
  data: T;
}

function deepFreeze<T>(value: T): Readonly<T> {
  if (value !== null && typeof value === 'object') {
    for (const child of Object.values(value)) deepFreeze(child);
    Object.freeze(value);
  }
  return value;
}

function props<T>(data: T, ctx: EventContext, aggregateId: string): IntegrationEventProps<T> {
  return {
    eventId: ctx.eventId ?? crypto.randomUUID(),
    aggregateId: ctx.aggregateId ?? aggregateId,
    correlationId: ctx.correlationId,
    causationId: ctx.causationId,
    occurredAt: ctx.occurredAt ?? new Date(),
    data,
  };
}

export abstract class IntegrationEvent<T> {
  abstract readonly eventType: string;
  abstract readonly version: number;
  readonly eventId: string;
  readonly aggregateId: string;
  readonly correlationId: string;
  readonly causationId: string | undefined;
  readonly data: Readonly<T>;
  private readonly occurredTimestamp: number;

  protected constructor(value: IntegrationEventProps<T>) {
    this.eventId = value.eventId;
    this.aggregateId = value.aggregateId;
    this.correlationId = value.correlationId;
    this.causationId = value.causationId;
    this.occurredTimestamp = value.occurredAt.getTime();
    this.data = deepFreeze(JSON.parse(JSON.stringify(value.data)) as T);
  }

  get occurredAt(): Date { return new Date(this.occurredTimestamp); }

  toJSON(): IntegrationEventEnvelope<T> {
    return {
      eventId: this.eventId,
      eventType: this.eventType,
      aggregateId: this.aggregateId,
      correlationId: this.correlationId,
      ...(this.causationId === undefined ? {} : { causationId: this.causationId }),
      occurredAt: this.occurredAt.toISOString(),
      version: this.version,
      data: this.data as T,
    };
  }
}

export interface WagerTransactionEventData {
  transactionId: string;
  walletId: string;
  providerId: string;
  externalTransactionId: string;
  kind: WagerTransactionKind;
  status: WagerTransactionStatus;
  money: MoneyProps;
  failureCode?: string;
  referenceExternalTransactionId?: string;
}

function transactionData(transaction: WagerTransaction): WagerTransactionEventData {
  return {
    transactionId: transaction.id,
    walletId: transaction.walletId,
    providerId: transaction.providerId,
    externalTransactionId: transaction.externalTransactionId,
    kind: transaction.kind,
    status: transaction.status,
    money: transaction.money.toJSON(),
    ...(transaction.failureCode === undefined ? {} : { failureCode: transaction.failureCode }),
    ...(transaction.referenceExternalTransactionId === undefined ? {} : {
      referenceExternalTransactionId: transaction.referenceExternalTransactionId,
    }),
  };
}

export class WagerTransactionProcessed extends IntegrationEvent<WagerTransactionEventData> {
  readonly eventType = 'WagerTransactionProcessed';
  readonly version = 1;

  private constructor(value: IntegrationEventProps<WagerTransactionEventData>) {
    super(value);
    Object.freeze(this);
  }

  static create(data: WagerTransactionEventData, ctx: EventContext): WagerTransactionProcessed {
    return new WagerTransactionProcessed(props(data, ctx, data.transactionId));
  }

  static from(transaction: WagerTransaction, ctx: EventContext): WagerTransactionProcessed {
    return this.create(transactionData(transaction), ctx);
  }
}

export class WagerTransactionRejected extends IntegrationEvent<WagerTransactionEventData> {
  readonly eventType = 'WagerTransactionRejected';
  readonly version = 1;

  private constructor(value: IntegrationEventProps<WagerTransactionEventData>) {
    super(value);
    Object.freeze(this);
  }

  static create(data: WagerTransactionEventData, ctx: EventContext): WagerTransactionRejected {
    return new WagerTransactionRejected(props(data, ctx, data.transactionId));
  }

  static from(transaction: WagerTransaction, ctx: EventContext): WagerTransactionRejected {
    return this.create(transactionData(transaction), ctx);
  }
}

export class WagerTransactionFailed extends IntegrationEvent<WagerTransactionEventData> {
  readonly eventType = 'WagerTransactionFailed';
  readonly version = 1;

  private constructor(value: IntegrationEventProps<WagerTransactionEventData>) {
    super(value);
    Object.freeze(this);
  }

  static from(transaction: WagerTransaction, ctx: EventContext): WagerTransactionFailed {
    return new WagerTransactionFailed(props(transactionData(transaction), ctx, transaction.id));
  }
}

export class WagerTransactionPendingReference extends IntegrationEvent<WagerTransactionEventData> {
  readonly eventType = 'WagerTransactionPendingReference';
  readonly version = 1;

  private constructor(value: IntegrationEventProps<WagerTransactionEventData>) {
    super(value);
    Object.freeze(this);
  }

  static create(data: WagerTransactionEventData, ctx: EventContext): WagerTransactionPendingReference {
    return new WagerTransactionPendingReference(props(data, ctx, data.transactionId));
  }

  static from(transaction: WagerTransaction, ctx: EventContext): WagerTransactionPendingReference {
    return this.create(transactionData(transaction), ctx);
  }
}

export interface WalletBalanceChangedData {
  walletId: string;
  transactionId: string;
  direction: LedgerDirection;
  money: MoneyProps;
  balanceBefore: MoneyProps;
  balanceAfter: MoneyProps;
  walletVersion: number;
}

export class WalletBalanceChanged extends IntegrationEvent<WalletBalanceChangedData> {
  readonly eventType = 'WalletBalanceChanged';
  readonly version = 1;

  private constructor(value: IntegrationEventProps<WalletBalanceChangedData>) {
    super(value);
    Object.freeze(this);
  }

  static create(data: WalletBalanceChangedData, ctx: EventContext): WalletBalanceChanged {
    return new WalletBalanceChanged(props(data, ctx, data.walletId));
  }

  static from(wallet: Wallet, entry: WalletLedgerEntry, ctx: EventContext): WalletBalanceChanged {
    return this.create({
      walletId: wallet.id,
      transactionId: entry.transactionId,
      direction: entry.direction,
      money: entry.money.toJSON(),
      balanceBefore: entry.balanceBefore.toJSON(),
      balanceAfter: entry.balanceAfter.toJSON(),
      walletVersion: entry.walletVersion,
    }, ctx);
  }
}
