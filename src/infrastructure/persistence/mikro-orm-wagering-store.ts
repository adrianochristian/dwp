import { ForeignKeyConstraintViolationException, UniqueConstraintViolationException } from '@mikro-orm/core';
import type { EntityManager, MikroORM } from '@mikro-orm/postgresql';
import { ApplicationError } from '../../application/application-error.js';
import type {
  InboxIdentity, ReceivedInbox, ReconciliationSnapshot, RetrySchedule, StoredWager, SubmissionResult,
  TransactionView, WageringReadPort, WageringStoreOptions, WageringStorePort, WageringTransactionPort,
} from '../../application/ports/wagering-store.js';
import { InboxMessage } from '../../domain/inbox-message.js';
import type { IntegrationEvent } from '../../domain/integration-event.js';
import { WalletLedgerEntry, type LedgerDirection } from '../../domain/ledger-entry.js';
import { Money } from '../../domain/money.js';
import { WagerTransaction, type WagerTransactionKind, type WagerTransactionStatus } from '../../domain/wager-transaction.js';
import { Wallet } from '../../domain/wallet.js';
import { WalletRecord } from './records.js';

interface TransactionReadRow {
  id: string;
  provider_id: string;
  external_transaction_id: string;
  wallet_id: string;
  player_id: string;
  round_id: string;
  game_id: string;
  kind: WagerTransactionKind;
  amount_cents: string;
  currency: string;
  reference_external_transaction_id: string | null;
  reference_transaction_id: string | null;
  status: WagerTransactionStatus;
  failure_code: string | null;
  result: Readonly<Record<string, unknown>> | null;
  created_at: string | Date;
  processed_at: string | Date | null;
}

interface TransactionRow extends Omit<TransactionReadRow, 'result'> {
  idempotency_key: string;
  payload_hash: string;
  result: SubmissionResult | null;
  retry_count: number;
  next_attempt_at: string | Date;
}

interface InboxRow {
  consumer_name: string;
  message_id: string;
  payload_hash: string;
  received_at: string | Date;
  processed_at: string | Date | null;
  result: SubmissionResult | null;
}

interface LedgerRow {
  id: string;
  wallet_id: string;
  transaction_id: string;
  direction: LedgerDirection;
  amount_cents: string;
  currency: string;
  balance_before_cents: string;
  balance_after_cents: string;
  wallet_version: number;
  created_at: string | Date;
}

const transactionColumns = `id, provider_id, external_transaction_id, wallet_id, player_id,
  round_id, game_id, kind, amount_cents::text, currency, reference_external_transaction_id,
  reference_transaction_id, status, failure_code, result, created_at, processed_at`;

function walletFromRecord(record: WalletRecord): Wallet {
  return Wallet.rehydrate({
    id: record.id,
    playerId: record.playerId,
    currency: record.currency,
    balance: Money.fromMinorUnits(record.balanceCents, record.currency),
    version: record.version,
    createdAt: record.createdAt,
    updatedAt: record.updatedAt,
  });
}

function transactionFromRow(row: TransactionRow): WagerTransaction {
  return WagerTransaction.rehydrate({
    id: row.id, providerId: row.provider_id, externalTransactionId: row.external_transaction_id,
    idempotencyKey: row.idempotency_key, payloadHash: row.payload_hash,
    walletId: row.wallet_id, playerId: row.player_id, roundId: row.round_id, gameId: row.game_id,
    kind: row.kind, money: Money.fromMinorUnits(BigInt(row.amount_cents), row.currency),
    referenceExternalTransactionId: row.reference_external_transaction_id ?? undefined,
    referenceTransactionId: row.reference_transaction_id ?? undefined,
    status: row.status, failureCode: row.failure_code ?? undefined,
    createdAt: new Date(row.created_at), processedAt: row.processed_at ? new Date(row.processed_at) : undefined,
  });
}

function storedWager(row: TransactionRow | undefined): StoredWager | undefined {
  return row === undefined ? undefined : {
    transaction: transactionFromRow(row), result: row.result,
    retryCount: row.retry_count, nextAttemptAt: new Date(row.next_attempt_at),
  };
}

function transactionView(row: TransactionReadRow | undefined): TransactionView | undefined {
  return row === undefined ? undefined : {
    id: row.id, providerId: row.provider_id, externalTransactionId: row.external_transaction_id,
    walletId: row.wallet_id, playerId: row.player_id, roundId: row.round_id, gameId: row.game_id,
    kind: row.kind, money: Money.fromMinorUnits(BigInt(row.amount_cents), row.currency),
    status: row.status, result: row.result, createdAt: new Date(row.created_at),
    ...(row.reference_external_transaction_id === null ? {} : { referenceExternalTransactionId: row.reference_external_transaction_id }),
    ...(row.reference_transaction_id === null ? {} : { referenceTransactionId: row.reference_transaction_id }),
    ...(row.failure_code === null ? {} : { failureCode: row.failure_code }),
    ...(row.processed_at === null ? {} : { processedAt: new Date(row.processed_at) }),
  };
}

class MikroOrmWageringTransaction implements WageringTransactionPort {
  private readonly lockedWallets = new Map<string, WalletRecord>();

  constructor(private readonly em: EntityManager) {}

  async now(): Promise<Date> {
    const row = (await this.em.execute<{ now: string }[]>('select clock_timestamp() as now'))[0];
    if (!row) throw new ApplicationError('internal', 'INTERNAL_ERROR', 'The database clock must return a timestamp');
    return new Date(row.now);
  }

  async insertWallet(wallet: Wallet): Promise<void> {
    try {
      await this.em.execute(`insert into dwp.wallets
        (id, player_id, currency, balance_cents, version, created_at, updated_at)
        values (?, ?, ?, ?, ?, ?, ?)`, [
        wallet.id, wallet.playerId, wallet.currency, wallet.balance.cents.toString(),
        wallet.version, wallet.createdAt, wallet.updatedAt,
      ]);
    } catch (error) {
      if (error instanceof UniqueConstraintViolationException) {
        throw new ApplicationError('conflict', 'WALLET_ALREADY_EXISTS', 'A wallet already exists for this player and currency', error);
      }
      throw error;
    }
  }

  async receiveInbox(identity: InboxIdentity): Promise<ReceivedInbox> {
    const proposed = InboxMessage.receive(identity);
    await this.em.execute(`insert into dwp.inbox_messages (consumer_name, message_id, payload_hash)
      values (?, ?, ?) on conflict do nothing`, [proposed.consumerName, proposed.messageId, proposed.payloadHash]);
    const row = (await this.em.execute<InboxRow[]>(`select * from dwp.inbox_messages
      where consumer_name = ? and message_id = ? for update`, [identity.consumerName, identity.messageId]))[0];
    if (!row) throw new ApplicationError('internal', 'INTERNAL_ERROR', 'The inbox identity must exist after insert');
    return {
      message: InboxMessage.rehydrate({
        consumerName: row.consumer_name, messageId: row.message_id, payloadHash: row.payload_hash,
        receivedAt: new Date(row.received_at), processedAt: row.processed_at ? new Date(row.processed_at) : undefined,
      }),
      result: row.result,
    };
  }

  async acceptTransaction(transaction: WagerTransaction): Promise<StoredWager | undefined> {
    try {
      await this.em.execute(`insert into dwp.wager_transactions
        (id, provider_id, external_transaction_id, idempotency_key, payload_hash,
         wallet_id, player_id, round_id, game_id, kind, amount_cents, currency,
         reference_external_transaction_id, status)
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING') on conflict do nothing`, [
        transaction.id, transaction.providerId, transaction.externalTransactionId, transaction.idempotencyKey, transaction.payloadHash,
        transaction.walletId, transaction.playerId, transaction.roundId, transaction.gameId, transaction.kind,
        transaction.money.cents.toString(), transaction.money.currency, transaction.referenceExternalTransactionId ?? null,
      ]);
    } catch (error) {
      if (error instanceof ForeignKeyConstraintViolationException) {
        throw new ApplicationError('not_found', 'WALLET_NOT_FOUND', 'The wallet does not exist', error);
      }
      throw error;
    }
    return storedWager((await this.em.execute<TransactionRow[]>(
      'select * from dwp.wager_transactions where idempotency_key = ? for update', [transaction.idempotencyKey],
    ))[0]);
  }

  async lockTransaction(id: string): Promise<StoredWager | undefined> {
    return storedWager((await this.em.execute<TransactionRow[]>(
      'select * from dwp.wager_transactions where id = ? for update', [id],
    ))[0]);
  }

  async lockWallet(id: string): Promise<Wallet | undefined> {
    // NO KEY UPDATE preserves compatibility with concurrent FK KEY SHARE checks.
    await this.em.execute('select id from dwp.wallets where id = ? for no key update', [id]);
    const record = await this.em.findOne(WalletRecord, { id });
    if (!record) return undefined;
    this.lockedWallets.set(id, record);
    return walletFromRecord(record);
  }

  async findReference(providerId: string, externalTransactionId: string): Promise<WagerTransaction | undefined> {
    const row = (await this.em.execute<TransactionRow[]>(`select * from dwp.wager_transactions
      where provider_id = ? and external_transaction_id = ?`, [providerId, externalTransactionId]))[0];
    return row === undefined ? undefined : transactionFromRow(row);
  }

  async hasSuccessfulReversal(referenceId: string): Promise<boolean> {
    const existing = await this.em.execute<{ id: string }[]>(`select id from dwp.wager_transactions
      where reference_transaction_id = ? and kind in ('REFUND','ROLLBACK') and status = 'PROCESSED' limit 1`, [referenceId]);
    return existing.length > 0;
  }

  async saveWallet(wallet: Wallet): Promise<void> {
    const record = this.lockedWallets.get(wallet.id);
    if (!record) throw new ApplicationError('internal', 'INTERNAL_ERROR', 'A wallet must be locked before it is saved');
    record.balanceCents = wallet.balance.cents;
    record.version = wallet.version;
    record.updatedAt = wallet.updatedAt;
    await this.em.flush();
  }

  async saveTransaction(transaction: WagerTransaction, result: SubmissionResult, retry?: RetrySchedule): Promise<void> {
    if (retry) {
      await this.em.execute(`update dwp.wager_transactions set status = ?, failure_code = ?,
        result = ?::jsonb, processed_at = ?, reference_transaction_id = ?, retry_count = ?, next_attempt_at = ? where id = ?`, [
        transaction.status, transaction.failureCode ?? null, JSON.stringify(result), transaction.processedAt ?? null,
        transaction.referenceTransactionId ?? null, retry.count, retry.nextAttemptAt, transaction.id,
      ]);
    } else {
      await this.em.execute(`update dwp.wager_transactions set status = ?, failure_code = ?, result = ?::jsonb where id = ?`, [
        transaction.status, transaction.failureCode ?? null, JSON.stringify(result), transaction.id,
      ]);
    }
  }

  async insertOpening(transaction: WagerTransaction, result: SubmissionResult): Promise<void> {
    await this.em.execute(`insert into dwp.wager_transactions
      (id, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id, player_id,
       round_id, game_id, kind, amount_cents, currency, status, result, created_at, processed_at)
      values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?)`, [
      transaction.id, transaction.providerId, transaction.externalTransactionId, transaction.idempotencyKey,
      transaction.payloadHash, transaction.walletId, transaction.playerId, transaction.roundId, transaction.gameId,
      transaction.kind, transaction.money.cents.toString(), transaction.money.currency, transaction.status,
      JSON.stringify(result), transaction.createdAt, transaction.processedAt,
    ]);
  }

  async appendLedger(entry: WalletLedgerEntry): Promise<void> {
    await this.em.execute(`insert into dwp.wallet_ledger_entries
      (id, wallet_id, transaction_id, direction, amount_cents, currency,
       balance_before_cents, balance_after_cents, wallet_version, created_at)
      values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, [
      entry.id, entry.walletId, entry.transactionId, entry.direction, entry.money.cents.toString(), entry.money.currency,
      entry.balanceBefore.cents.toString(), entry.balanceAfter.cents.toString(), entry.walletVersion, entry.createdAt,
    ]);
  }

  async enqueue<T>(event: IntegrationEvent<T>): Promise<void> {
    await this.em.execute(`insert into dwp.outbox_messages
      (id, aggregate_id, event_type, payload, occurred_at) values (?, ?, ?, ?::jsonb, ?)`, [
      event.eventId, event.aggregateId, event.eventType, JSON.stringify(event.toJSON()), event.occurredAt,
    ]);
  }

  async completeInbox(message: InboxMessage, result: SubmissionResult): Promise<void> {
    await this.em.execute(`update dwp.inbox_messages set processed_at = ?, result = ?::jsonb
      where consumer_name = ? and message_id = ?`, [message.processedAt, JSON.stringify(result), message.consumerName, message.messageId]);
  }
}

export class MikroOrmWageringStore implements WageringStorePort, WageringReadPort {
  constructor(private readonly orm: MikroORM, private readonly options: WageringStoreOptions = {}) {}

  transaction<T>(work: (transaction: WageringTransactionPort) => Promise<T>): Promise<T> {
    return this.orm.em.fork().transactional(async em => {
      const result = await work(new MikroOrmWageringTransaction(em));
      if (result !== undefined) await this.options.beforeCommit?.();
      return result;
    });
  }

  async readWallet(id: string): Promise<Wallet | undefined> {
    const record = await this.orm.em.fork().findOne(WalletRecord, { id });
    return record === null ? undefined : walletFromRecord(record);
  }

  async findTransaction(id: string): Promise<TransactionView | undefined> {
    const rows = await this.orm.em.fork().execute<TransactionReadRow[]>(
      `select ${transactionColumns} from dwp.wager_transactions where id = ?`, [id],
    );
    return transactionView(rows[0]);
  }

  async findExternalTransaction(providerId: string, externalTransactionId: string): Promise<TransactionView | undefined> {
    const rows = await this.orm.em.fork().execute<TransactionReadRow[]>(
      `select ${transactionColumns} from dwp.wager_transactions
       where provider_id = ? and external_transaction_id = ?`, [providerId, externalTransactionId],
    );
    return transactionView(rows[0]);
  }

  async ledgerCeiling(walletId: string): Promise<number | undefined> {
    const wallets = await this.orm.em.fork().execute<{ id: string; ceiling_version: number }[]>(`
      select w.id, coalesce((select max(l.wallet_version)
        from dwp.wallet_ledger_entries l where l.wallet_id = w.id), 0)::integer as ceiling_version
      from dwp.wallets w where w.id = ?`, [walletId]);
    return wallets[0]?.ceiling_version;
  }

  async ledgerPage(walletId: string, afterVersion: number, ceilingVersion: number, limit: number): Promise<WalletLedgerEntry[]> {
    const rows = await this.orm.em.fork().execute<LedgerRow[]>(`
      select id, wallet_id, transaction_id, direction, amount_cents::text, currency,
        balance_before_cents::text, balance_after_cents::text, wallet_version, created_at
      from dwp.wallet_ledger_entries
      where wallet_id = ? and wallet_version > ? and wallet_version <= ?
      order by wallet_version asc limit ?`, [walletId, afterVersion, ceilingVersion, limit]);
    return rows.map(row => WalletLedgerEntry.rehydrate({
      id: row.id, walletId: row.wallet_id, transactionId: row.transaction_id, direction: row.direction,
      money: Money.fromMinorUnits(BigInt(row.amount_cents), row.currency),
      balanceBefore: Money.fromMinorUnits(BigInt(row.balance_before_cents), row.currency),
      balanceAfter: Money.fromMinorUnits(BigInt(row.balance_after_cents), row.currency),
      walletVersion: row.wallet_version, createdAt: new Date(row.created_at),
    }));
  }

  async reconciliationSnapshot(walletId: string): Promise<ReconciliationSnapshot | undefined> {
    // One PostgreSQL statement gives balance and ledger aggregation one snapshot.
    const rows = await this.orm.em.fork().execute<{
      currency: string; stored_balance_cents: string; calculated_balance_cents: string; checked_entries: number;
    }[]>(`
      select w.currency, w.balance_cents::text as stored_balance_cents,
        coalesce(sum(case when l.direction = 'CREDIT'
          then l.amount_cents::numeric else -l.amount_cents::numeric end), 0)::text as calculated_balance_cents,
        count(l.id)::integer as checked_entries
      from dwp.wallets w left join dwp.wallet_ledger_entries l on l.wallet_id = w.id
      where w.id = ? group by w.id`, [walletId]);
    const row = rows[0];
    return row === undefined ? undefined : {
      storedBalance: Money.fromMinorUnits(BigInt(row.stored_balance_cents), row.currency),
      calculatedBalance: Money.fromMinorUnits(BigInt(row.calculated_balance_cents), row.currency),
      checkedEntries: row.checked_entries,
    };
  }
}
