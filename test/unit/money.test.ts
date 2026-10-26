import { describe, expect, test } from 'bun:test';
import { DomainError } from '../../src/domain/domain-error.js';
import { Money, type MoneyProps } from '../../src/domain/money.js';

function expectCode(operation: () => unknown, code: string): void {
  try {
    operation();
    throw new Error('Expected a domain error');
  } catch (error) {
    expect(error).toBeInstanceOf(DomainError);
    expect((error as DomainError).code).toBe(code);
  }
}

describe('Money', () => {
  test('parses and serializes exact cents beyond number precision', () => {
    const money = Money.from({ amount: '9007199254740993.17', currency: 'BRL' });
    expect(money.cents).toBe(900719925474099317n);
    expect(money.toString()).toBe('9007199254740993.17');
    expect(JSON.parse(JSON.stringify(money))).toEqual({ amount: '9007199254740993.17', currency: 'BRL' });
    expect(Money.from({ amount: '00001.02', currency: 'BRL' }).toString()).toBe('1.02');
  });

  test('keeps exact decimal arithmetic immutable', () => {
    const first = Money.from({ amount: '0.10', currency: 'BRL' });
    const second = Money.from({ amount: '0.20', currency: 'BRL' });
    expect(first.add(second).toString()).toBe('0.30');
    expect(first.subtract(second).toString()).toBe('-0.10');
    expect(first.toString()).toBe('0.10');
    expect(second.toString()).toBe('0.20');
    expect(first.add(Money.zero('BRL'))).not.toBe(first);
    expect(Object.isFrozen(first)).toBe(true);
    expect(() => Object.assign(first, { currency: 'USD' })).toThrow();
  });

  test('handles signed internal values and canonical zero', () => {
    const signed = Money.fromMinorUnits(-1n, 'USD');
    expect(signed.isNegative()).toBe(true);
    expect(signed.toJSON()).toEqual({ amount: '-0.01', currency: 'USD' });
    expect(signed.negate().isPositive()).toBe(true);
    expect(Money.rehydrate(signed.toJSON()).equals(signed)).toBe(true);
    expect(Money.rehydrate({ amount: '-0.00', currency: 'USD' }).toString()).toBe('0.00');
    expect(Money.zero('BRL').isZero()).toBe(true);
    expect(Money.zero('BRL').isPositive()).toBe(false);
    expect(Money.zero('BRL').isNegative()).toBe(false);
    expect(Money.zero('USD').isLessThan(signed.negate())).toBe(true);
    expect(signed.equals(signed.negate())).toBe(false);
  });

  test.each([
    '', ' ', 'NaN', 'Infinity', '-Infinity', '1e2', '1e2.00',
    '1', '1.0', '1.001', '-1.00', '-0.00', '+1.00', '.01',
    '1.', '1,00', ' 1.00', '1.00 ', '1.00\n', '1_000.00',
  ])('rejects invalid input %j without rounding', (amount) => {
    expectCode(() => Money.from({ amount, currency: 'BRL' }), 'INVALID_MONEY');
  });

  test('rejects non-string amounts', () => {
    for (const amount of [1, 1.01, NaN, Infinity, null, undefined, 100n, {}, []]) {
      expectCode(() => Money.from({ amount, currency: 'BRL' } as MoneyProps), 'INVALID_MONEY');
    }
  });

  test.each(['', 'brl', 'BrL', 'BR', 'BRLL', '123', ' BRL', 'BRL\n'])('rejects invalid currency %j', (currency) => {
    expectCode(() => Money.from({ amount: '1.00', currency }), 'INVALID_CURRENCY');
    expectCode(() => Money.zero(currency), 'INVALID_CURRENCY');
  });

  test('rejects currency conflicts in arithmetic and comparisons', () => {
    const brl = Money.from({ amount: '1.00', currency: 'BRL' });
    const usd = Money.from({ amount: '1.00', currency: 'USD' });
    for (const operation of [() => brl.add(usd), () => brl.subtract(usd), () => brl.isLessThan(usd), () => brl.equals(usd)]) {
      expectCode(operation, 'CURRENCY_MISMATCH');
    }
  });

  test('accepts the BIGINT financial boundary and rejects parser overflow', () => {
    const maximum = Money.from({ amount: '92233720368547758.07', currency: 'BRL' });
    expect(maximum.cents).toBe(Money.MAX_MINOR_UNITS);
    expect(maximum.negate().toString()).toBe('-92233720368547758.07');
    expectCode(() => Money.from({ amount: '92233720368547758.08', currency: 'BRL' }), 'MONEY_OVERFLOW');
    expectCode(() => Money.fromMinorUnits(-9223372036854775808n, 'BRL'), 'MONEY_OVERFLOW');
    expectCode(() => Money.fromMinorUnits(1 as unknown as bigint, 'BRL'), 'INVALID_MONEY');
  });

  test('rejects positive and negative arithmetic overflow before persistence', () => {
    const maximum = Money.fromMinorUnits(Money.MAX_MINOR_UNITS, 'BRL');
    const cent = Money.from({ amount: '0.01', currency: 'BRL' });
    expectCode(() => maximum.add(cent), 'MONEY_OVERFLOW');
    expectCode(() => maximum.negate().subtract(cent), 'MONEY_OVERFLOW');
    expect(maximum.subtract(cent).cents).toBe(Money.MAX_MINOR_UNITS - 1n);
  });
});
