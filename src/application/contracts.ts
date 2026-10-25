import { HttpException } from '@nestjs/common';
import { DomainError } from '../domain/domain-error.js';
import { Money } from '../domain/money.js';
import { createHash } from 'node:crypto';
import { WagerTransactionKind } from '../domain/wager-transaction.js';

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

export function money(value: unknown, field: string, anyCurrency = false): Money {
  const props = object(value, field);
  try {
    const parsed = Money.from({
      amount: text(props.amount, `${field}.amount`, 32),
      currency: text(props.currency, `${field}.currency`, 3),
    });
    if (!anyCurrency && parsed.currency !== 'BRL') invalidRequest('Only BRL is supported', 'UNSUPPORTED_CURRENCY');
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

export interface WagerInput {
  providerId: string;
  externalTransactionId: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: Money;
  referenceExternalTransactionId?: string;
}

export function wagerRequest(value: unknown): WagerInput {
  const props = object(value);
  const kind = text(props.kind, 'kind');
  if (kind !== WagerTransactionKind.Bet && kind !== WagerTransactionKind.Win
    && kind !== WagerTransactionKind.Loss && kind !== WagerTransactionKind.Refund
    && kind !== WagerTransactionKind.Rollback) {
    invalidRequest('The transaction kind cannot be submitted', 'INVALID_TRANSACTION_KIND');
  }
  if (kind === WagerTransactionKind.Bet && props.referenceExternalTransactionId !== undefined) {
    invalidRequest('BET cannot specify a reference', 'INVALID_REFERENCE');
  }
  const input: WagerInput = {
    providerId: text(props.providerId, 'providerId'),
    externalTransactionId: text(props.externalTransactionId, 'externalTransactionId'),
    playerId: text(props.playerId, 'playerId'),
    walletId: uuid(props.walletId, 'walletId'),
    roundId: text(props.roundId, 'roundId'),
    gameId: text(props.gameId, 'gameId'),
    kind,
    money: money(props.money, 'money', true),
    ...(props.referenceExternalTransactionId === undefined ? {} : {
      referenceExternalTransactionId: text(props.referenceExternalTransactionId, 'referenceExternalTransactionId'),
    }),
  };
  if (input.providerId === '__internal__') invalidRequest('The internal provider identity is reserved', 'INVALID_PROVIDER');
  if (input.kind !== WagerTransactionKind.Loss && !input.money.isPositive()) {
    invalidRequest('The transaction requires a positive amount', 'INVALID_TRANSACTION_AMOUNT');
  }
  if (input.referenceExternalTransactionId === input.externalTransactionId) {
    invalidRequest('A transaction cannot reference itself', 'INVALID_REFERENCE');
  }
  if ((input.kind === WagerTransactionKind.Refund || input.kind === WagerTransactionKind.Rollback)
    && input.referenceExternalTransactionId === undefined) {
    invalidRequest(`${input.kind} requires referenceExternalTransactionId`, 'REFERENCE_REQUIRED');
  }
  return input;
}

/** Canonical JSON sorts object keys recursively and preserves array order. */
export function canonicalJson(value: unknown): string {
  if (value === null || typeof value !== 'object') {
    const result = JSON.stringify(value);
    if (result === undefined) throw new TypeError('Canonical JSON requires JSON values');
    return result;
  }
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`;
  const fields = value as Record<string, unknown>;
  return `{${Object.keys(fields).filter(key => fields[key] !== undefined).sort()
    .map(key => `${JSON.stringify(key)}:${canonicalJson(fields[key])}`).join(',')}}`;
}

export function canonicalHash(value: unknown): string {
  return createHash('sha256').update(canonicalJson(value)).digest('hex');
}

/** Transport metadata and the idempotency header are deliberately absent. */
export function wagerPayload(input: WagerInput): Record<string, unknown> {
  return {
    providerId: input.providerId, externalTransactionId: input.externalTransactionId,
    playerId: input.playerId, walletId: input.walletId, roundId: input.roundId, gameId: input.gameId,
    kind: input.kind, money: input.money.toJSON(),
    ...(input.referenceExternalTransactionId === undefined ? {} : { referenceExternalTransactionId: input.referenceExternalTransactionId }),
  };
}
