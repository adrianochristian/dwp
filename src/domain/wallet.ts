import { DomainError } from './domain-error.js';
import { Money } from './money.js';

export interface WalletState {
  id: string;
  playerId: string;
  currency: string;
  balance: Money;
  version: number;
  createdAt: Date;
  updatedAt: Date;
}

export interface WalletMovement {
  readonly balanceBefore: Money;
  readonly balanceAfter: Money;
  readonly walletVersion: number;
}

export class Wallet {
  private _balance: Money;
  private _version: number;
  private _updatedAt: Date;
  private readonly _createdAt: Date;

  private constructor(
    public readonly id: string,
    public readonly playerId: string,
    public readonly currency: string,
    state: Pick<WalletState, 'balance' | 'version' | 'createdAt' | 'updatedAt'>,
  ) {
    this._balance = state.balance;
    this._version = state.version;
    this._createdAt = new Date(state.createdAt);
    this._updatedAt = new Date(state.updatedAt);
  }

  static open(props: { id: string; playerId: string; initialBalance: Money; createdAt?: Date }): Wallet {
    if (props.initialBalance.isNegative()) {
      throw new DomainError('NEGATIVE_BALANCE', 'A wallet cannot open with a negative balance');
    }
    const at = props.createdAt ?? new Date();
    return new Wallet(props.id, props.playerId, props.initialBalance.currency, {
      balance: props.initialBalance,
      version: 1,
      createdAt: at,
      updatedAt: at,
    });
  }

  static rehydrate(state: WalletState): Wallet {
    return new Wallet(state.id, state.playerId, state.currency, state);
  }

  get balance(): Money { return this._balance; }
  get version(): number { return this._version; }
  get createdAt(): Date { return new Date(this._createdAt); }
  get updatedAt(): Date { return new Date(this._updatedAt); }

  debit(money: Money, at: Date = new Date()): WalletMovement {
    this.assertMovement(money);
    if (this._balance.isLessThan(money)) {
      throw new DomainError('INSUFFICIENT_FUNDS', 'The wallet balance is insufficient');
    }
    return this.apply(this._balance.subtract(money), at);
  }

  credit(money: Money, at: Date = new Date()): WalletMovement {
    this.assertMovement(money);
    return this.apply(this._balance.add(money), at);
  }

  private assertMovement(money: Money): void {
    if (money.currency !== this.currency) {
      throw new DomainError('CURRENCY_MISMATCH', 'The operation currency must match the wallet currency');
    }
    if (!money.isPositive()) {
      throw new DomainError('NON_POSITIVE_AMOUNT', 'A balance movement requires a positive amount');
    }
    if (!Number.isSafeInteger(this._version + 1)) {
      throw new DomainError('WALLET_VERSION_OVERFLOW', 'The wallet version exceeds the supported range');
    }
  }

  private apply(balanceAfter: Money, at: Date): WalletMovement {
    const balanceBefore = this._balance;
    this._balance = balanceAfter;
    this._version += 1;
    this._updatedAt = new Date(at);
    return Object.freeze({ balanceBefore, balanceAfter, walletVersion: this._version });
  }
}
