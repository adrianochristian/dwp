import { DomainError } from './domain-error.js';
import type { Money } from './money.js';
import { MAX_WALLET_VERSION } from './wallet-version.js';

export enum LedgerDirection {
  Debit = 'DEBIT',
  Credit = 'CREDIT',
}

export interface LedgerEntryState {
  id: string;
  walletId: string;
  transactionId: string;
  direction: LedgerDirection;
  money: Money;
  balanceBefore: Money;
  balanceAfter: Money;
  walletVersion: number;
  createdAt: Date;
}

export class WalletLedgerEntry {
  private readonly _createdAt: number;

  private constructor(
    public readonly id: string,
    public readonly walletId: string,
    public readonly transactionId: string,
    public readonly direction: LedgerDirection,
    public readonly money: Money,
    public readonly balanceBefore: Money,
    public readonly balanceAfter: Money,
    public readonly walletVersion: number,
    at: Date,
  ) {
    this._createdAt = at.getTime();
    Object.freeze(this);
  }

  static create(props: LedgerEntryState): WalletLedgerEntry {
    if (!props.money.isPositive()) {
      throw new DomainError('NON_POSITIVE_AMOUNT', 'A ledger entry requires a positive amount');
    }
    if (props.balanceBefore.isNegative() || props.balanceAfter.isNegative()) {
      throw new DomainError('NEGATIVE_BALANCE', 'Ledger balances cannot be negative');
    }
    if (!Number.isSafeInteger(props.walletVersion) || props.walletVersion < 1 || props.walletVersion > MAX_WALLET_VERSION) {
      throw new DomainError('INVALID_WALLET_VERSION', 'A ledger entry requires a positive wallet version');
    }
    if (!Object.values(LedgerDirection).includes(props.direction)) {
      throw new DomainError('INVALID_LEDGER_DIRECTION', 'The ledger direction is invalid');
    }
    const entry = this.rehydrate(props);
    if (!entry.isBalanced()) {
      throw new DomainError('UNBALANCED_LEDGER', 'The ledger entry arithmetic does not balance');
    }
    return entry;
  }

  static rehydrate(props: LedgerEntryState): WalletLedgerEntry {
    return new WalletLedgerEntry(
      props.id, props.walletId, props.transactionId, props.direction, props.money,
      props.balanceBefore, props.balanceAfter, props.walletVersion, props.createdAt,
    );
  }

  get createdAt(): Date { return new Date(this._createdAt); }

  isBalanced(): boolean {
    const after = this.direction === LedgerDirection.Credit
      ? this.balanceBefore.add(this.money)
      : this.balanceBefore.subtract(this.money);
    return after.equals(this.balanceAfter);
  }
}
