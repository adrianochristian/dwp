import { HttpException } from '@nestjs/common';
import { DomainError } from '../domain/domain-error.js';
import { Money } from '../domain/money.js';

export function invalidRequest(message: string, code = 'INVALID_REQUEST'): never {
  throw new HttpException({ code, message }, 400);
}

export function object(value: unknown, field = 'body'): Record<string, unknown> {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    invalidRequest(`${field} must be an object`);
  }
  return value as Record<string, unknown>;
}

export function text(value: unknown, field: string, maximum = 200): string {
  if (typeof value !== 'string' || value.length === 0 || value.length > maximum
    || value.trim() !== value || /[\u0000-\u001f]/.test(value)) {
    invalidRequest(`${field} must be a nonempty string of at most ${maximum} characters`);
  }
  return value;
}

export function uuid(value: unknown, field: string): string {
  const result = text(value, field, 36);
  if (!/^[\da-f]{8}-[\da-f]{4}-[\da-f]{4}-[\da-f]{4}-[\da-f]{12}$/i.test(result)) {
    invalidRequest(`${field} must be a UUID`);
  }
  return result.toLowerCase();
}

export function money(value: unknown, field: string): Money {
  const props = object(value, field);
  try {
    const parsed = Money.from({
      amount: text(props.amount, `${field}.amount`, 32),
      currency: text(props.currency, `${field}.currency`, 3),
    });
    if (parsed.currency !== 'BRL') invalidRequest('Only BRL is supported', 'UNSUPPORTED_CURRENCY');
    return parsed;
  } catch (error) {
    if (error instanceof DomainError) invalidRequest(error.message, error.code);
    throw error;
  }
}

export function walletRequest(value: unknown): { playerId: string; initialBalance: Money } {
  const props = object(value);
  return {
    playerId: text(props.playerId, 'playerId'),
    initialBalance: money(props.initialBalance, 'initialBalance'),
  };
}
