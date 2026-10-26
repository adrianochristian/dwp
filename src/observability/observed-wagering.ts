import { HttpException } from '@nestjs/common';
import { wagerRequest } from '../application/contracts.js';
import type { WageringReadUseCases } from '../application/wagering-queries.js';
import type { WageringUseCases, InboxContext, SubmissionResult } from '../application/wagering-service.js';
import { ApplicationError } from '../application/application-error.js';
import { DomainError } from '../domain/domain-error.js';
import { log, type LogContext } from './logger.js';
import { metrics } from './metrics.js';

type ProcessingRole = 'http' | 'sqs' | 'reference';

function fields(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === 'object' ? value as Record<string, unknown> : undefined;
}

function failure(error: unknown): { code: string; lockConflict: boolean } {
  let current = error;
  for (let depth = 0; depth < 5; depth += 1) {
    const detail = fields(current);
    if (!detail) break;
    if (['40P01', '40001', '55P03'].includes(String(detail.code))) {
      return { code: 'LOCK_CONFLICT', lockConflict: true };
    }
    current = detail.cause;
  }
  let code: unknown;
  if (error instanceof ApplicationError) {
    code = error.code;
  } else if (error instanceof HttpException) {
    code = fields(error.getResponse())?.code;
    if (code === undefined) {
      code = ({ 400: 'INVALID_REQUEST', 404: 'NOT_FOUND', 409: 'CONFLICT', 422: 'BUSINESS_RULE_VIOLATION' } as Record<number, string>)[error.getStatus()];
    }
  } else if (error instanceof DomainError) {
    code = error.code;
  }
  return {
    code: typeof code === 'string' && /^[A-Z][A-Z0-9_]{0,99}$/.test(code) ? code : 'INFRASTRUCTURE_UNAVAILABLE',
    lockConflict: false,
  };
}

function identifiers(body: unknown, context?: InboxContext): LogContext {
  // Validation has already succeeded before the financial commit. A malformed
  // or subsequently changed caller object must never turn logging into a failure.
  try {
    const input = wagerRequest(body);
    return {
      walletId: input.walletId,
      providerId: input.providerId,
      ...(typeof context?.messageId === 'string' ? { messageId: context.messageId } : {}),
    };
  } catch {
    return {};
  }
}

export class ObservedWageringService implements WageringUseCases {
  constructor(private readonly inner: WageringUseCases, private readonly role: ProcessingRole) {}

  createWallet(body: unknown): ReturnType<WageringUseCases['createWallet']> { return this.inner.createWallet(body); }
  getWallet(id: string): ReturnType<WageringUseCases['getWallet']> { return this.inner.getWallet(id); }

  async submit(body: unknown, key: unknown, context?: InboxContext): Promise<SubmissionResult> {
    return this.observed(() => this.inner.submit(body, key, context), () => identifiers(body, context));
  }

  async failSubmission(body: unknown, key: unknown, code: string, context?: InboxContext): Promise<SubmissionResult> {
    return this.observed(() => this.inner.failSubmission(body, key, code, context), () => identifiers(body, context));
  }

  async reprocess(id: string): Promise<SubmissionResult | undefined> {
    return this.observed(() => this.inner.reprocess(id));
  }

  private async observed<T extends SubmissionResult | undefined>(work: () => Promise<T>, context: () => LogContext = () => ({})): Promise<T> {
    const started = performance.now();
    const correlationId = crypto.randomUUID();
    try {
      const result = await work();
      if (result === undefined) return result;
      const durationMs = performance.now() - started;
      metrics.observeDuration(durationMs / 1_000, { role: this.role, status: result.status });
      metrics.increment(result.idempotentReplay ? 'duplicates' : 'transactions', { role: this.role, status: result.status });
      log(result.idempotentReplay ? 'wager_transaction_replayed' : 'wager_transaction_completed', {
        ...context(), correlationId: result.transactionId, transactionId: result.transactionId,
        status: result.status, durationMs,
        ...(result.failureCode ? { code: result.failureCode } : {}),
      });
      return result;
    } catch (error) {
      const durationMs = performance.now() - started;
      const detail = failure(error);
      metrics.observeDuration(durationMs / 1_000, { role: this.role });
      if (detail.lockConflict) metrics.increment('lock_conflicts', { role: this.role });
      log('wager_transaction_error', { correlationId, code: detail.code, durationMs });
      throw error;
    }
  }
}

export class ObservedWageringQueries implements WageringReadUseCases {
  constructor(private readonly inner: WageringReadUseCases) {}

  getTransaction(id: string): ReturnType<WageringReadUseCases['getTransaction']> { return this.inner.getTransaction(id); }
  getExternalTransaction(providerId: string, externalId: string): ReturnType<WageringReadUseCases['getExternalTransaction']> {
    return this.inner.getExternalTransaction(providerId, externalId);
  }
  ledger(id: string, cursor?: unknown, limit?: unknown): ReturnType<WageringReadUseCases['ledger']> {
    return this.inner.ledger(id, cursor, limit);
  }

  async reconcile(id: string) {
    const started = performance.now();
    const correlationId = crypto.randomUUID();
    try {
      const result = await this.inner.reconcile(id);
      const durationMs = performance.now() - started;
      metrics.observeDuration(durationMs / 1_000, { role: 'http' });
      if (!result.consistent) metrics.increment('reconciliation_inconsistencies', { role: 'http' });
      log('wallet_reconciliation_completed', {
        correlationId, walletId: result.walletId, durationMs, status: result.consistent ? 'CONSISTENT' : 'INCONSISTENT',
      });
      return result;
    } catch (error) {
      const durationMs = performance.now() - started;
      const detail = failure(error);
      metrics.observeDuration(durationMs / 1_000, { role: 'http' });
      if (detail.lockConflict) metrics.increment('lock_conflicts', { role: 'http' });
      log('wallet_reconciliation_error', { correlationId, code: detail.code, durationMs });
      throw error;
    }
  }
}
