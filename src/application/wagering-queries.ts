import { HttpException } from '@nestjs/common';
import type { MikroORM } from '@mikro-orm/postgresql';
import { Money, type MoneyProps } from '../domain/money.js';
import { invalidRequest, object, text, uuid } from './contracts.js';

interface TransactionRow {
  id: string;
  provider_id: string;
  external_transaction_id: string;
  wallet_id: string;
  player_id: string;
  round_id: string;
  game_id: string;
  kind: string;
  amount_cents: string;
  currency: string;
  reference_external_transaction_id: string | null;
  reference_transaction_id: string | null;
  status: string;
  failure_code: string | null;
  result: Record<string, unknown> | null;
  created_at: string | Date;
  processed_at: string | Date | null;
}

interface LedgerRow {
  id: string;
  wallet_id: string;
  transaction_id: string;
  direction: string;
  amount_cents: string;
  currency: string;
  balance_before_cents: string;
  balance_after_cents: string;
  wallet_version: number;
  created_at: string | Date;
}

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

const transactionColumns = `id, provider_id, external_transaction_id, wallet_id, player_id,
  round_id, game_id, kind, amount_cents::text, currency, reference_external_transaction_id,
  reference_transaction_id, status, failure_code, result, created_at, processed_at`;

function serializedMoney(cents: string, currency: string): MoneyProps {
  return Money.fromMinorUnits(BigInt(cents), currency).toJSON();
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
  constructor(private readonly orm: MikroORM) {}

  async getTransaction(id: string) {
    const transactionId = uuid(id, 'transactionId');
    const rows = await this.orm.em.fork().execute<TransactionRow[]>(
      `select ${transactionColumns} from dwp.wager_transactions where id = ?`, [transactionId],
    );
    return this.transactionResponse(rows[0]);
  }

  async getExternalTransaction(providerId: string, externalTransactionId: string) {
    const provider = text(providerId, 'providerId');
    const external = text(externalTransactionId, 'externalTransactionId');
    const rows = await this.orm.em.fork().execute<TransactionRow[]>(
      `select ${transactionColumns} from dwp.wager_transactions
       where provider_id = ? and external_transaction_id = ?`, [provider, external],
    );
    return this.transactionResponse(rows[0]);
  }

  async ledger(id: string, cursorValue: unknown = undefined, limitValue: unknown = undefined) {
    const walletId = uuid(id, 'walletId');
    const limit = pageLimit(limitValue);
    const cursor = decodeCursor(cursorValue, walletId);
    const em = this.orm.em.fork();
    const wallets = await em.execute<{ id: string; ceiling_version: number }[]>(`
      select w.id, coalesce((select max(l.wallet_version)
        from dwp.wallet_ledger_entries l where l.wallet_id = w.id), 0)::integer as ceiling_version
      from dwp.wallets w where w.id = ?`, [walletId]);
    if (!wallets[0]) {
      throw new HttpException({ code: 'WALLET_NOT_FOUND', message: 'The wallet does not exist' }, 404);
    }
    const ceilingVersion = cursor?.ceilingVersion ?? wallets[0].ceiling_version;
    if (ceilingVersion > wallets[0].ceiling_version) {
      invalidRequest('The ledger cursor exceeds the wallet history', 'INVALID_CURSOR');
    }
    const rows = await em.execute<LedgerRow[]>(`
      select id, wallet_id, transaction_id, direction, amount_cents::text, currency,
        balance_before_cents::text, balance_after_cents::text, wallet_version, created_at
      from dwp.wallet_ledger_entries
      where wallet_id = ? and wallet_version > ? and wallet_version <= ?
      order by wallet_version asc limit ?`, [walletId, cursor?.lastVersion ?? 0, ceilingVersion, limit + 1]);
    const page = rows.slice(0, limit);
    const entries: LedgerEntryResponse[] = page.map(row => ({
      id: row.id,
      walletId: row.wallet_id,
      transactionId: row.transaction_id,
      direction: row.direction,
      money: serializedMoney(row.amount_cents, row.currency),
      balanceBefore: serializedMoney(row.balance_before_cents, row.currency),
      balanceAfter: serializedMoney(row.balance_after_cents, row.currency),
      walletVersion: row.wallet_version,
      createdAt: new Date(row.created_at).toISOString(),
    }));
    const last = page.at(-1);
    const nextCursor = rows.length > limit && last
      ? encodeCursor({ v: 1, walletId, lastVersion: last.wallet_version, ceilingVersion })
      : null;
    return { walletId, entries, nextCursor };
  }

  private transactionResponse(row: TransactionRow | undefined) {
    if (!row) {
      throw new HttpException({ code: 'TRANSACTION_NOT_FOUND', message: 'The transaction does not exist' }, 404);
    }
    return {
      ...row.result,
      transactionId: row.id,
      providerId: row.provider_id,
      externalTransactionId: row.external_transaction_id,
      walletId: row.wallet_id,
      playerId: row.player_id,
      roundId: row.round_id,
      gameId: row.game_id,
      kind: row.kind,
      money: serializedMoney(row.amount_cents, row.currency),
      status: row.status,
      ...(row.reference_external_transaction_id === null ? {} : {
        referenceExternalTransactionId: row.reference_external_transaction_id,
      }),
      ...(row.reference_transaction_id === null ? {} : { referenceTransactionId: row.reference_transaction_id }),
      ...(row.failure_code === null ? {} : { failureCode: row.failure_code }),
      createdAt: new Date(row.created_at).toISOString(),
      ...(row.processed_at === null ? {} : { processedAt: new Date(row.processed_at).toISOString() }),
    };
  }
}
