import { ApplicationError } from './application-error.js';
import type { MoneyProps } from '../domain/money.js';
import type { TransactionView, WageringReadPort } from './ports/wagering-store.js';
import { invalidRequest, object, text, uuid } from './contracts.js';

interface LedgerCursor {
  v: 1;
  walletId: string;
  lastVersion: number;
  ceilingVersion: number;
}

export interface LedgerEntryResponse {
  id: string;
  walletId: string;
  transactionId: string;
  direction: string;
  money: MoneyProps;
  balanceBefore: MoneyProps;
  balanceAfter: MoneyProps;
  walletVersion: number;
  createdAt: string;
}

function encodeCursor(cursor: LedgerCursor): string {
  return Buffer.from(JSON.stringify(cursor)).toString('base64url');
}

function decodeCursor(value: unknown, walletId: string): LedgerCursor | undefined {
  if (value === undefined) return undefined;
  const encoded = text(value, 'cursor', 1024);
  try {
    if (!/^[A-Za-z0-9_-]+$/.test(encoded)) throw new Error('Invalid cursor encoding');
    const decoded = object(JSON.parse(Buffer.from(encoded, 'base64url').toString('utf8')), 'cursor');
    if (Object.keys(decoded).length !== 4
      || decoded.v !== 1 || decoded.walletId !== walletId
      || !Number.isSafeInteger(decoded.lastVersion) || !Number.isSafeInteger(decoded.ceilingVersion)
      || typeof decoded.lastVersion !== 'number' || typeof decoded.ceilingVersion !== 'number'
      || decoded.lastVersion < 1 || decoded.ceilingVersion < decoded.lastVersion) {
      throw new Error('Invalid cursor fields');
    }
    const result: LedgerCursor = {
      v: 1, walletId, lastVersion: decoded.lastVersion, ceilingVersion: decoded.ceilingVersion,
    };
    if (encodeCursor(result) !== encoded) throw new Error('Invalid cursor serialization');
    return result;
  } catch {
    invalidRequest('The ledger cursor is invalid or belongs to another wallet', 'INVALID_CURSOR');
  }
}

function pageLimit(value: unknown): number {
  if (value === undefined) return 50;
  const parsed = typeof value === 'string' && /^[1-9]\d{0,2}$/.test(value) ? Number(value) : value;
  if (typeof parsed !== 'number' || !Number.isInteger(parsed) || parsed < 1 || parsed > 100) {
    invalidRequest('limit must be an integer between 1 and 100', 'INVALID_PAGE_LIMIT');
  }
  return parsed;
}

export class WageringQueries {
  constructor(private readonly store: WageringReadPort) {}

  async getTransaction(id: string) {
    return this.transactionResponse(await this.store.findTransaction(uuid(id, 'transactionId')));
  }

  async getExternalTransaction(providerId: string, externalTransactionId: string) {
    return this.transactionResponse(await this.store.findExternalTransaction(
      text(providerId, 'providerId'), text(externalTransactionId, 'externalTransactionId'),
    ));
  }

  async ledger(id: string, cursorValue: unknown = undefined, limitValue: unknown = undefined) {
    const walletId = uuid(id, 'walletId');
    const limit = pageLimit(limitValue);
    const cursor = decodeCursor(cursorValue, walletId);
    const currentCeiling = await this.store.ledgerCeiling(walletId);
    if (currentCeiling === undefined) {
      throw new ApplicationError('not_found', 'WALLET_NOT_FOUND', 'The wallet does not exist');
    }
    const ceilingVersion = cursor?.ceilingVersion ?? currentCeiling;
    if (ceilingVersion > currentCeiling) {
      invalidRequest('The ledger cursor exceeds the wallet history', 'INVALID_CURSOR');
    }
    const rows = await this.store.ledgerPage(walletId, cursor?.lastVersion ?? 0, ceilingVersion, limit + 1);
    const page = rows.slice(0, limit);
    const entries: LedgerEntryResponse[] = page.map(entry => ({
      id: entry.id, walletId: entry.walletId, transactionId: entry.transactionId, direction: entry.direction,
      money: entry.money.toJSON(), balanceBefore: entry.balanceBefore.toJSON(), balanceAfter: entry.balanceAfter.toJSON(),
      walletVersion: entry.walletVersion, createdAt: entry.createdAt.toISOString(),
    }));
    const last = page.at(-1);
    const nextCursor = rows.length > limit && last
      ? encodeCursor({ v: 1, walletId, lastVersion: last.walletVersion, ceilingVersion })
      : null;
    return { walletId, entries, nextCursor };
  }

  async reconcile(id: string) {
    const walletId = uuid(id, 'walletId');
    const snapshot = await this.store.reconciliationSnapshot(walletId);
    if (!snapshot) throw new ApplicationError('not_found', 'WALLET_NOT_FOUND', 'The wallet does not exist');
    const difference = snapshot.storedBalance.subtract(snapshot.calculatedBalance);
    const consistent = difference.isZero();
    if (!consistent) console.error(JSON.stringify({ event: 'wallet_reconciliation_inconsistent', walletId }));
    return {
      walletId, storedBalance: snapshot.storedBalance.toJSON(), calculatedBalance: snapshot.calculatedBalance.toJSON(),
      difference: difference.toJSON(), consistent, checkedEntries: snapshot.checkedEntries,
    };
  }

  private transactionResponse(transaction: TransactionView | undefined) {
    if (!transaction) throw new ApplicationError('not_found', 'TRANSACTION_NOT_FOUND', 'The transaction does not exist');
    return {
      ...transaction.result,
      transactionId: transaction.id, providerId: transaction.providerId,
      externalTransactionId: transaction.externalTransactionId, walletId: transaction.walletId,
      playerId: transaction.playerId, roundId: transaction.roundId, gameId: transaction.gameId,
      kind: transaction.kind, money: transaction.money.toJSON(), status: transaction.status,
      ...(transaction.referenceExternalTransactionId === undefined ? {} : {
        referenceExternalTransactionId: transaction.referenceExternalTransactionId,
      }),
      ...(transaction.referenceTransactionId === undefined ? {} : { referenceTransactionId: transaction.referenceTransactionId }),
      ...(transaction.failureCode === undefined ? {} : { failureCode: transaction.failureCode }),
      createdAt: transaction.createdAt.toISOString(),
      ...(transaction.processedAt === undefined ? {} : { processedAt: transaction.processedAt.toISOString() }),
    };
  }
}

export type WageringReadUseCases = Pick<WageringQueries, 'getTransaction' | 'getExternalTransaction' | 'ledger' | 'reconcile'>;
