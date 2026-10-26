import { describe, expect, test } from 'bun:test';
import { DomainError } from '../../src/domain/domain-error.js';
import { LedgerDirection, WalletLedgerEntry } from '../../src/domain/ledger-entry.js';
import { Money } from '../../src/domain/money.js';
import { Wallet } from '../../src/domain/wallet.js';
import { MAX_WALLET_VERSION } from '../../src/domain/wallet-version.js';

const createdAt = new Date('2026-01-01T00:00:00.000Z');
const changedAt = new Date('2026-01-02T00:00:00.000Z');
const real = (amount: string) => Money.from({ amount, currency: 'BRL' });

function walletAt(version: number): Wallet {
  return Wallet.rehydrate({
    id: 'wallet', playerId: 'player', currency: 'BRL', balance: real('10.00'),
    version, createdAt, updatedAt: createdAt,
  });
}

function expectCode(operation: () => unknown, code: string): void {
  let caught: unknown;
  try { operation(); } catch (error) { caught = error; }
  expect(caught).toBeInstanceOf(DomainError);
  expect((caught as DomainError).code).toBe(code);
}

describe('PostgreSQL INTEGER wallet version boundary', () => {
  test('allows the last credit and debit transition into the maximum persisted version', () => {
    const credited = walletAt(MAX_WALLET_VERSION - 1);
    const credit = credited.credit(real('1.00'), changedAt);
    expect(credit.walletVersion).toBe(MAX_WALLET_VERSION);
    expect(credited.version).toBe(MAX_WALLET_VERSION);
    expect(credited.balance.toString()).toBe('11.00');
    expect(credited.updatedAt).toEqual(changedAt);

    const debited = walletAt(MAX_WALLET_VERSION - 1);
    const debit = debited.debit(real('1.00'), changedAt);
    expect(debit.walletVersion).toBe(MAX_WALLET_VERSION);
    expect(debited.version).toBe(MAX_WALLET_VERSION);
    expect(debited.balance.toString()).toBe('9.00');
    expect(debited.updatedAt).toEqual(changedAt);
  });

  test('rejects further movements before changing balance, version or timestamps', () => {
    const wallet = walletAt(MAX_WALLET_VERSION);
    const originalBalance = wallet.balance;
    for (const operation of [() => wallet.credit(real('1.00'), changedAt), () => wallet.debit(real('1.00'), changedAt)]) {
      expectCode(operation, 'WALLET_VERSION_OVERFLOW');
      expect(wallet.balance).toBe(originalBalance);
      expect(wallet.version).toBe(MAX_WALLET_VERSION);
      expect(wallet.createdAt).toEqual(createdAt);
      expect(wallet.updatedAt).toEqual(createdAt);
    }
  });

  test('allows a ledger version at the INTEGER limit and rejects the next value', () => {
    const state = {
      id: 'entry', walletId: 'wallet', transactionId: 'transaction', direction: LedgerDirection.Credit,
      money: real('1.00'), balanceBefore: real('0.00'), balanceAfter: real('1.00'),
      walletVersion: MAX_WALLET_VERSION, createdAt,
    };
    expect(WalletLedgerEntry.create(state).walletVersion).toBe(MAX_WALLET_VERSION);
    expectCode(() => WalletLedgerEntry.create({ ...state, walletVersion: MAX_WALLET_VERSION + 1 }), 'INVALID_WALLET_VERSION');
  });
});
