import { describe, expect, test } from 'bun:test';
import { DomainError } from '../../src/domain/domain-error.js';
import { InboxMessage } from '../../src/domain/inbox-message.js';
import {
  WalletBalanceChanged, WagerTransactionPendingReference, WagerTransactionProcessed, WagerTransactionRejected,
} from '../../src/domain/integration-event.js';
import { LedgerDirection, WalletLedgerEntry, type LedgerEntryState } from '../../src/domain/ledger-entry.js';
import { Money } from '../../src/domain/money.js';
import { OutboxMessage } from '../../src/domain/outbox-message.js';
import {
  WagerTransaction, WagerTransactionKind as Kind, WagerTransactionStatus as Status,
  type CreateWagerTransactionProps,
} from '../../src/domain/wager-transaction.js';
import { Wallet } from '../../src/domain/wallet.js';

const money = (amount: string, currency = 'BRL'): Money => Money.from({ amount, currency });
const at = new Date('2026-10-25T12:00:00.000Z');

function transaction(overrides: Partial<CreateWagerTransactionProps> = {}): WagerTransaction {
  return WagerTransaction.create({
    id: 'transaction-id', providerId: 'provider-a', externalTransactionId: 'external-id',
    idempotencyKey: 'provider-a:external-id', payloadHash: 'hash', walletId: 'wallet-id',
    playerId: 'player-id', roundId: 'round-id', gameId: 'game-id', kind: Kind.Bet,
    money: money('25.00'), createdAt: at, ...overrides,
  });
}

function processed(overrides: Partial<CreateWagerTransactionProps> = {}): WagerTransaction {
  const value = transaction(overrides);
  value.markProcessed(value.requiresReference() ? 'original-reference' : undefined, at);
  return value;
}

function expectCode(action: () => unknown, code: string): void {
  try {
    action();
    throw new Error(`Expected ${code}`);
  } catch (error: unknown) {
    expect(error).toBeInstanceOf(DomainError);
    expect((error as DomainError).code).toBe(code);
  }
}

describe('Wallet', () => {
  test('opens at version one for a positive or zero balance', () => {
    for (const amount of ['0.00', '100.00']) {
      const value = Wallet.open({ id: 'wallet-id', playerId: 'player-id', initialBalance: money(amount) });
      expect(value.version).toBe(1);
      expect(value.balance.toString()).toBe(amount);
    }
    expectCode(() => Wallet.open({
      id: 'wallet-id', playerId: 'player-id', initialBalance: money('1.00').negate(),
    }), 'NEGATIVE_BALANCE');
  });

  test('returns exact balance snapshots and advances version only on movements', () => {
    const value = Wallet.open({ id: 'wallet-id', playerId: 'player-id', initialBalance: money('100.00') });
    const movement = value.debit(money('80.00'), at);
    expect(movement.balanceBefore.toString()).toBe('100.00');
    expect(movement.balanceAfter.toString()).toBe('20.00');
    expect(movement.walletVersion).toBe(2);
    value.credit(money('2.01'), at);
    expect(value.balance.toString()).toBe('22.01');
    expect(value.version).toBe(3);
    expect(Object.isFrozen(movement)).toBe(true);
  });

  test('failed debit, currency mismatch, zero movement and overflow leave state intact', () => {
    const value = Wallet.open({ id: 'wallet-id', playerId: 'player-id', initialBalance: money('100.00') });
    expectCode(() => value.debit(money('100.01')), 'INSUFFICIENT_FUNDS');
    expectCode(() => value.debit(money('1.00', 'USD')), 'CURRENCY_MISMATCH');
    expectCode(() => value.credit(Money.zero('BRL')), 'NON_POSITIVE_AMOUNT');
    expect(value.balance.toString()).toBe('100.00');
    expect(value.version).toBe(1);
    const largest = Money.fromMinorUnits(Money.MAX_MINOR_UNITS, 'BRL');
    const full = Wallet.open({ id: 'full', playerId: 'full', initialBalance: largest });
    expectCode(() => full.credit(money('0.01')), 'MONEY_OVERFLOW');
    expect(full.balance).toBe(largest);
    expect(full.version).toBe(1);
  });

  test('rehydrates persisted version and isolates mutable Date objects', () => {
    const created = new Date(at);
    const value = Wallet.rehydrate({
      id: 'wallet-id', playerId: 'player-id', currency: 'BRL', balance: money('70.00'),
      version: 18, createdAt: created, updatedAt: created,
    });
    created.setFullYear(2000);
    value.updatedAt.setFullYear(2001);
    expect(value.createdAt.toISOString()).toBe(at.toISOString());
    expect(value.updatedAt.toISOString()).toBe(at.toISOString());
    expect(value.version).toBe(18);
  });
});

describe('WalletLedgerEntry', () => {
  const state = (): LedgerEntryState => ({
    id: 'entry-id', walletId: 'wallet-id', transactionId: 'transaction-id',
    direction: LedgerDirection.Debit, money: money('25.00'), balanceBefore: money('100.00'),
    balanceAfter: money('75.00'), walletVersion: 2, createdAt: new Date(at),
  });

  test('is frozen and protects the audit timestamp', () => {
    const source = state();
    const entry = WalletLedgerEntry.create(source);
    source.createdAt.setFullYear(2000);
    entry.createdAt.setFullYear(2000);
    expect(Object.isFrozen(entry)).toBe(true);
    expect(Reflect.set(entry, 'money', money('5.00'))).toBe(false);
    expect(entry.createdAt.toISOString()).toBe(at.toISOString());
    expect(entry.isBalanced()).toBe(true);
  });

  test('rejects incorrect arithmetic, negative balance, zero amount and mixed currencies', () => {
    expectCode(() => WalletLedgerEntry.create({ ...state(), balanceAfter: money('74.99') }), 'UNBALANCED_LEDGER');
    expectCode(() => WalletLedgerEntry.create({ ...state(), balanceAfter: money('1.00').negate() }), 'NEGATIVE_BALANCE');
    expectCode(() => WalletLedgerEntry.create({ ...state(), money: money('0.00') }), 'NON_POSITIVE_AMOUNT');
    expectCode(() => WalletLedgerEntry.create({ ...state(), money: money('25.00', 'USD') }), 'CURRENCY_MISMATCH');
  });
});

describe('WagerTransaction', () => {
  test('validates required references and permits zero only for LOSS', () => {
    for (const kind of [Kind.Refund, Kind.Rollback]) {
      expectCode(() => transaction({ kind }), 'REFERENCE_REQUIRED');
    }
    for (const kind of [Kind.Bet, Kind.Win, Kind.Opening]) {
      expectCode(() => transaction({ kind, money: money('0.00') }), 'INVALID_TRANSACTION_AMOUNT');
    }
    const loss = transaction({ kind: Kind.Loss, money: money('0.00') });
    expect(loss.affectsBalance()).toBe(false);
    expectCode(() => loss.ledgerDirectionFor(), 'NO_LEDGER_EFFECT');
    expectCode(() => transaction({ kind: Kind.Loss, money: money('1.00').negate() }), 'INVALID_TRANSACTION_AMOUNT');
  });

  test.each([Status.Processed, Status.Rejected, Status.Failed])('%s is terminal for every transition', (terminal) => {
    const value = transaction();
    if (terminal === Status.Processed) value.markProcessed(undefined, at);
    else if (terminal === Status.Rejected) value.reject('INSUFFICIENT_FUNDS');
    else value.fail('PERMANENT_FAILURE');
    expect(value.isTerminal()).toBe(true);
    for (const transition of [
      () => value.markProcessed(undefined, at), () => value.markPendingReference(),
      () => value.reject('OTHER'), () => value.fail('OTHER'),
    ]) expectCode(transition, 'INVALID_TRANSACTION_STATE');
    expect(value.status).toBe(terminal);
  });

  test('pending reference can resolve and replay identity compares business payload hashes', () => {
    const value = transaction({ kind: Kind.Refund, referenceExternalTransactionId: 'bet-id' });
    expect(value.status).toBe(Status.Pending);
    value.markPendingReference();
    expect(value.status).toBe(Status.PendingReference);
    expectCode(() => value.markProcessed(undefined, at), 'REFERENCE_REQUIRED');
    value.markProcessed('bet-internal-id', at);
    expect(value.referenceTransactionId).toBe('bet-internal-id');
    expect(value.matchesPayload('hash')).toBe(true);
    expect(value.matchesPayload('changed-hash')).toBe(false);
    value.processedAt?.setFullYear(2000);
    expect(value.processedAt?.toISOString()).toBe(at.toISOString());
  });

  test('reference validation checks all identity boundaries and full reversal amount', () => {
    const refund = transaction({ kind: Kind.Refund, referenceExternalTransactionId: 'external-id' });
    const bet = processed();
    expect(() => refund.validateReference(bet)).not.toThrow();
    for (const patch of [
      { providerId: 'other' }, { playerId: 'other' }, { walletId: 'other' },
      { roundId: 'other' }, { externalTransactionId: 'other' }, { money: money('25.00', 'USD') },
    ]) expectCode(() => refund.validateReference(processed(patch)), 'REFERENCE_MISMATCH');
    expectCode(() => refund.validateReference(processed({ money: money('24.99') })), 'REFERENCE_AMOUNT_MISMATCH');
    expectCode(() => refund.validateReference(processed({ kind: Kind.Win })), 'INVALID_REFERENCE_KIND');
    expectCode(() => refund.validateReference(transaction()), 'REFERENCE_PENDING');
    const rejected = transaction();
    rejected.reject('INSUFFICIENT_FUNDS');
    expectCode(() => refund.validateReference(rejected), 'REFERENCE_REJECTED');
    const failed = transaction();
    failed.fail('PERMANENT_FAILURE');
    expectCode(() => refund.validateReference(failed), 'REFERENCE_FAILED');
  });

  test('WIN may reference a BET with a different payout and rollback inverts permitted kinds', () => {
    const win = transaction({ kind: Kind.Win, money: money('50.00'), referenceExternalTransactionId: 'external-id' });
    expect(() => win.validateReference(processed())).not.toThrow();
    expect(win.ledgerDirectionFor()).toBe(LedgerDirection.Credit);
    const rollback = transaction({ kind: Kind.Rollback, referenceExternalTransactionId: 'external-id' });
    expect(rollback.ledgerDirectionFor(processed())).toBe(LedgerDirection.Credit);
    expect(rollback.ledgerDirectionFor(processed({ kind: Kind.Win }))).toBe(LedgerDirection.Debit);
    expect(rollback.ledgerDirectionFor(processed({ kind: Kind.Refund, referenceExternalTransactionId: 'bet' })))
      .toBe(LedgerDirection.Debit);
    expectCode(() => rollback.ledgerDirectionFor(processed({ kind: Kind.Loss })), 'INVALID_REFERENCE_KIND');
  });
});

describe('integration events, inbox and outbox', () => {
  test('concrete event types produce stable envelopes with immutable money snapshots', () => {
    const value = processed();
    const ctx = { eventId: 'event-id', correlationId: 'correlation-id', causationId: 'message-id', occurredAt: new Date(at) };
    const event = WagerTransactionProcessed.from(value, ctx);
    ctx.occurredAt.setFullYear(2000);
    event.occurredAt.setFullYear(2000);
    expect(event.toJSON()).toMatchObject({
      eventId: 'event-id', eventType: 'WagerTransactionProcessed', version: 1,
      aggregateId: 'transaction-id', correlationId: 'correlation-id', causationId: 'message-id',
      occurredAt: at.toISOString(), data: { money: { amount: '25.00', currency: 'BRL' } },
    });
    expect(Object.isFrozen(event)).toBe(true);
    expect(Object.isFrozen(event.data.money)).toBe(true);
    expect(WagerTransactionRejected.from(value, ctx).eventType).toBe('WagerTransactionRejected');
    expect(WagerTransactionPendingReference.from(value, ctx).eventType).toBe('WagerTransactionPendingReference');
    const wallet = Wallet.open({ id: 'wallet-id', playerId: 'player-id', initialBalance: money('100.00') });
    const movement = wallet.debit(money('25.00'), at);
    const entry = WalletLedgerEntry.create({
      id: 'entry-id', walletId: wallet.id, transactionId: value.id, direction: LedgerDirection.Debit,
      money: value.money, ...movement, createdAt: at,
    });
    const changed = WalletBalanceChanged.from(wallet, entry, { correlationId: 'correlation-id', occurredAt: at });
    expect(changed.data.walletVersion).toBe(2);
    expect(changed.data.balanceAfter.amount).toBe('75.00');
    expect(changed.eventType).toBe('WalletBalanceChanged');
  });

  test('inbox transitions once and snapshots timestamps', () => {
    const receivedAt = new Date(at);
    const message = InboxMessage.receive({ messageId: 'message-id', consumerName: 'wagering', payloadHash: 'hash', receivedAt });
    receivedAt.setFullYear(2000);
    expect(message.receivedAt.toISOString()).toBe(at.toISOString());
    expect(message.isProcessed()).toBe(false);
    message.markProcessed(at);
    expect(message.isProcessed()).toBe(true);
    expectCode(() => message.markProcessed(at), 'INVALID_INBOX_STATE');
  });

  test('outbox retries with capped exponential backoff and preserves event identity', () => {
    const event = WagerTransactionProcessed.from(processed(), { eventId: 'event-id', correlationId: 'correlation-id', occurredAt: at });
    const message = OutboxMessage.enqueue(event);
    expect(message.id).toBe(event.eventId);
    expect(message.isDue(at)).toBe(true);
    for (const seconds of [1, 2, 4, 8, 16, 32, 60, 60]) {
      message.scheduleRetry(at);
      expect(message.nextAttemptAt?.getTime()).toBe(at.getTime() + seconds * 1000);
      expect(message.isDue(at)).toBe(false);
      expect(message.isDue(new Date(at.getTime() + seconds * 1000))).toBe(true);
      expect(message.id).toBe('event-id');
    }
    expect(message.attempts).toBe(8);
    message.markPublished(at);
    expect(message.isPending()).toBe(false);
    expect(message.isDue(new Date('2027-01-01'))).toBe(false);
    expectCode(() => message.scheduleRetry(at), 'INVALID_OUTBOX_STATE');
  });
});
