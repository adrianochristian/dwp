import { HttpException } from '@nestjs/common';
import { UniqueConstraintViolationException } from '@mikro-orm/core';
import type { EntityManager, MikroORM } from '@mikro-orm/postgresql';
import { createHash } from 'node:crypto';
import { WalletBalanceChanged, WagerTransactionProcessed, type IntegrationEvent } from '../domain/integration-event.js';
import { LedgerDirection, WalletLedgerEntry } from '../domain/ledger-entry.js';
import { Money } from '../domain/money.js';
import { WagerTransaction, WagerTransactionKind } from '../domain/wager-transaction.js';
import { Wallet } from '../domain/wallet.js';
import { WalletRecord } from '../infrastructure/persistence/records.js';
import { uuid, walletRequest } from './contracts.js';

export interface WageringServiceOptions {
  /** Runs inside the transaction, before commit; useful for fault injection. */
  beforeCommit?: () => Promise<void> | void;
}

export class WageringService {
  constructor(private readonly orm: MikroORM, private readonly options: WageringServiceOptions = {}) {}

  async createWallet(body: unknown) {
    const input = walletRequest(body);
    const wallet = Wallet.open({ id: crypto.randomUUID(), ...input });
    try {
      return await this.orm.em.fork().transactional(async em => {
        await em.execute(`insert into dwp.wallets
          (id, player_id, currency, balance_cents, version, created_at, updated_at)
          values (?, ?, ?, ?, ?, ?, ?)`, [
          wallet.id, wallet.playerId, wallet.currency, wallet.balance.cents.toString(),
          wallet.version, wallet.createdAt, wallet.updatedAt,
        ]);
        if (wallet.balance.isPositive()) await this.opening(em, wallet);
        await this.options.beforeCommit?.();
        return this.walletResponse(wallet);
      });
    } catch (error) {
      if (error instanceof UniqueConstraintViolationException) {
        throw new HttpException({ code: 'WALLET_ALREADY_EXISTS', message: 'A wallet already exists for this player and currency' }, 409);
      }
      throw error;
    }
  }

  async getWallet(id: string) {
    const record = await this.orm.em.fork().findOne(WalletRecord, { id: uuid(id, 'walletId') });
    if (!record) throw new HttpException({ code: 'WALLET_NOT_FOUND', message: 'The wallet does not exist' }, 404);
    return this.walletResponse(this.walletFromRecord(record));
  }

  private walletFromRecord(record: WalletRecord): Wallet {
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

  private walletResponse(wallet: Wallet) {
    return { id: wallet.id, playerId: wallet.playerId, balance: wallet.balance.toJSON(), version: wallet.version };
  }

  private async opening(em: EntityManager, wallet: Wallet): Promise<void> {
    const transaction = WagerTransaction.create({
      id: crypto.randomUUID(),
      providerId: '__internal__',
      externalTransactionId: wallet.id,
      idempotencyKey: `opening:${wallet.id}`,
      payloadHash: createHash('sha256').update(JSON.stringify(this.walletResponse(wallet))).digest('hex'),
      walletId: wallet.id,
      playerId: wallet.playerId,
      roundId: '__opening__',
      gameId: '__opening__',
      kind: WagerTransactionKind.Opening,
      money: wallet.balance,
      createdAt: wallet.createdAt,
    });
    transaction.markProcessed(undefined, wallet.createdAt);
    const result = { transactionId: transaction.id, status: transaction.status, balance: wallet.balance.toJSON(), idempotentReplay: false };
    await em.execute(`insert into dwp.wager_transactions
      (id, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id, player_id,
       round_id, game_id, kind, amount_cents, currency, status, result, created_at, processed_at)
      values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?)`, [
      transaction.id, transaction.providerId, transaction.externalTransactionId, transaction.idempotencyKey,
      transaction.payloadHash, wallet.id, wallet.playerId, transaction.roundId, transaction.gameId,
      transaction.kind, transaction.money.cents.toString(), wallet.currency, transaction.status,
      JSON.stringify(result), transaction.createdAt, transaction.processedAt,
    ]);
    const entry = WalletLedgerEntry.create({
      id: crypto.randomUUID(), walletId: wallet.id, transactionId: transaction.id,
      direction: LedgerDirection.Credit, money: wallet.balance,
      balanceBefore: Money.zero(wallet.currency), balanceAfter: wallet.balance,
      walletVersion: wallet.version, createdAt: wallet.createdAt,
    });
    await this.insertLedger(em, entry);
    const context = { correlationId: transaction.id, occurredAt: wallet.createdAt };
    await this.enqueue(em, WagerTransactionProcessed.from(transaction, context));
    await this.enqueue(em, WalletBalanceChanged.from(wallet, entry, context));
  }

  private async insertLedger(em: EntityManager, entry: WalletLedgerEntry): Promise<void> {
    await em.execute(`insert into dwp.wallet_ledger_entries
      (id, wallet_id, transaction_id, direction, amount_cents, currency,
       balance_before_cents, balance_after_cents, wallet_version, created_at)
      values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, [
      entry.id, entry.walletId, entry.transactionId, entry.direction, entry.money.cents.toString(), entry.money.currency,
      entry.balanceBefore.cents.toString(), entry.balanceAfter.cents.toString(), entry.walletVersion, entry.createdAt,
    ]);
  }

  private async enqueue<T>(em: EntityManager, event: IntegrationEvent<T>): Promise<void> {
    await em.execute(`insert into dwp.outbox_messages
      (id, aggregate_id, event_type, payload, occurred_at) values (?, ?, ?, ?::jsonb, ?)`, [
      event.eventId, event.aggregateId, event.eventType, JSON.stringify(event.toJSON()), event.occurredAt,
    ]);
  }
}
