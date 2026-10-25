import { DomainError } from './domain-error.js';

export interface MoneyProps {
  readonly amount: string;
  readonly currency: string;
}

export class Money {
  static readonly MAX_MINOR_UNITS = 9223372036854775807n;

  private constructor(
    private readonly minorUnits: bigint,
    public readonly currency: string,
  ) {
    Object.freeze(this);
  }

  static from(props: MoneyProps): Money {
    return Money.parse(props, false);
  }

  /** Reconstructs exact persisted values, including signed internal amounts. */
  static rehydrate(props: MoneyProps): Money {
    return Money.parse(props, true);
  }

  static fromMinorUnits(cents: bigint, currency: string): Money {
    Money.assertCurrency(currency);
    if (typeof cents !== 'bigint') {
      throw new DomainError('INVALID_MONEY', 'Minor units must be a bigint');
    }
    if (cents > Money.MAX_MINOR_UNITS || cents < -Money.MAX_MINOR_UNITS) {
      throw new DomainError('MONEY_OVERFLOW', 'Amount exceeds the supported financial range');
    }
    return new Money(cents, currency);
  }

  static zero(currency: string): Money {
    return Money.fromMinorUnits(0n, currency);
  }

  get cents(): bigint {
    return this.minorUnits;
  }

  add(other: Money): Money {
    this.assertSameCurrency(other);
    return Money.fromMinorUnits(this.minorUnits + other.minorUnits, this.currency);
  }

  subtract(other: Money): Money {
    this.assertSameCurrency(other);
    return Money.fromMinorUnits(this.minorUnits - other.minorUnits, this.currency);
  }

  negate(): Money {
    return Money.fromMinorUnits(-this.minorUnits, this.currency);
  }

  isZero(): boolean {
    return this.minorUnits === 0n;
  }

  isPositive(): boolean {
    return this.minorUnits > 0n;
  }

  isNegative(): boolean {
    return this.minorUnits < 0n;
  }

  isLessThan(other: Money): boolean {
    this.assertSameCurrency(other);
    return this.minorUnits < other.minorUnits;
  }

  equals(other: Money): boolean {
    this.assertSameCurrency(other);
    return this.minorUnits === other.minorUnits;
  }

  toJSON(): MoneyProps {
    return { amount: this.toString(), currency: this.currency };
  }

  toString(): string {
    const absolute = this.minorUnits < 0n ? -this.minorUnits : this.minorUnits;
    const whole = absolute / 100n;
    const fractional = (absolute % 100n).toString().padStart(2, '0');
    return `${this.minorUnits < 0n ? '-' : ''}${whole}.${fractional}`;
  }

  private static parse(props: MoneyProps, signed: boolean): Money {
    if (!props || typeof props.amount !== 'string') {
      throw new DomainError('INVALID_MONEY', 'Amount must be a decimal string with two fractional digits');
    }
    const pattern = signed ? /^-?\d+\.\d{2}$/ : /^\d+\.\d{2}$/;
    if (!pattern.test(props.amount) || props.amount.trim() !== props.amount) {
      throw new DomainError('INVALID_MONEY', 'Amount must be a decimal string with two fractional digits');
    }
    const cents = BigInt(props.amount.replace('.', ''));
    return Money.fromMinorUnits(cents, props.currency);
  }

  private static assertCurrency(currency: string): void {
    if (typeof currency !== 'string' || currency.length !== 3 || !/^[A-Z]{3}$/.test(currency)) {
      throw new DomainError('INVALID_CURRENCY', 'Currency must be a three-letter uppercase ISO code');
    }
  }

  private assertSameCurrency(other: Money): void {
    if (this.currency !== other.currency) {
      throw new DomainError('CURRENCY_MISMATCH', 'Amounts must have the same currency');
    }
  }
}
