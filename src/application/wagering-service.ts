import { createHash } from 'node:crypto';
import { ApplicationError } from './application-error.js';
import { WalletBalanceChanged, WagerTransactionFailed, WagerTransactionPendingReference, WagerTransactionProcessed, WagerTransactionRejected } from '../domain/integration-event.js';
import { DomainError } from '../domain/domain-error.js';
import type { InboxMessage } from '../domain/inbox-message.js';
import { LedgerDirection, WalletLedgerEntry } from '../domain/ledger-entry.js';
import { Money } from '../domain/money.js';
import { WagerTransaction, WagerTransactionKind, WagerTransactionStatus } from '../domain/wager-transaction.js';
import { Wallet, type WalletMovement } from '../domain/wallet.js';
import { canonicalHash, invalidRequest, text, uuid, wagerPayload, wagerRequest, walletRequest } from './contracts.js';
import type { InboxContext, InboxIdentity, ReceivedInbox, StoredWager, SubmissionResult, WageringStorePort, WageringTransactionPort } from './ports/wagering-store.js';

export type { InboxContext, SubmissionResult, WageringStoreOptions as WageringServiceOptions } from './ports/wagering-store.js';

export class WageringService {
  constructor(private readonly store: WageringStorePort) {}

  async createWallet(body: unknown) {
    const input = walletRequest(body);
    const wallet = Wallet.open({ id: crypto.randomUUID(), ...input });
    return this.store.transaction(async transaction => {
      await transaction.insertWallet(wallet);
      if (wallet.balance.isPositive()) await this.opening(transaction, wallet);
      return this.walletResponse(wallet);
    });
  }

  async getWallet(id: string) {
    const wallet = await this.store.readWallet(uuid(id, 'walletId'));
    if (!wallet) throw new ApplicationError('not_found', 'WALLET_NOT_FOUND', 'The wallet does not exist');
    return this.walletResponse(wallet);
  }

  async submit(body: unknown, idempotencyKey: unknown, inboxContext?: InboxContext): Promise<SubmissionResult> {
    const input = wagerRequest(body);
    const key = text(idempotencyKey, 'Idempotency-Key', 500);
    const payloadHash = canonicalHash(wagerPayload(input));
    const inboxIdentity = inboxContext === undefined ? undefined : {
      consumerName: text(inboxContext.consumerName, 'consumerName'),
      messageId: text(inboxContext.messageId, 'messageId'),
      payloadHash: canonicalHash({ idempotencyKey: key, ...wagerPayload(input) }),
    };
    const proposed = WagerTransaction.create({ ...input, id: crypto.randomUUID(), idempotencyKey: key, payloadHash });
    return this.store.transaction(async transaction => {
      const inbox = inboxIdentity === undefined ? undefined : await this.receiveInbox(transaction, inboxIdentity);
      if (inbox?.message.isProcessed()) {
        if (!inbox.result) throw new ApplicationError('internal', 'INTERNAL_ERROR', 'A processed inbox message must have a persisted response');
        return this.completeSubmission(transaction, inbox.message, { ...inbox.result, idempotentReplay: true });
      }
      const stored = await transaction.acceptTransaction(proposed);
      this.assertAcceptedIdentity(stored, payloadHash);
      return this.processTransaction(transaction, stored, inbox?.message);
    });
  }

  async reprocess(id: string): Promise<SubmissionResult | undefined> {
    const transactionId = uuid(id, 'transactionId');
    return this.store.transaction(async transaction => {
      const stored = await transaction.lockTransaction(transactionId);
      if (!stored || stored.transaction.status !== WagerTransactionStatus.PendingReference) return undefined;
      const now = await transaction.now();
      if (stored.nextAttemptAt.getTime() > now.getTime()) return undefined;
      return this.processTransaction(transaction, stored);
    });
  }

  private async processTransaction(store: WageringTransactionPort, stored: StoredWager, inbox?: InboxMessage): Promise<SubmissionResult> {
    const transaction = stored.transaction;
    const previousStatus = transaction.status;
    if (transaction.isTerminal()) {
      if (!stored.result) throw new ApplicationError('internal', 'INTERNAL_ERROR', 'A terminal transaction must have a persisted response');
      return this.completeSubmission(store, inbox, { ...stored.result, idempotentReplay: true });
    }

    const wallet = await store.lockWallet(transaction.walletId);
    if (!wallet) throw new ApplicationError('not_found', 'WALLET_NOT_FOUND', 'The wallet does not exist');
    const at = await store.now();
    let movement: WalletMovement | undefined;
    let reference: WagerTransaction | undefined;
    let referencePending = false;
    try {
      if (wallet.playerId !== transaction.playerId) throw new DomainError('WALLET_PLAYER_MISMATCH', 'The wallet belongs to a different player');
      if (wallet.currency !== transaction.money.currency) throw new DomainError('CURRENCY_MISMATCH', 'The operation currency must match the wallet currency');
      if (transaction.referenceExternalTransactionId !== undefined) {
        if (at.getTime() - transaction.createdAt.getTime() >= 24 * 60 * 60 * 1_000) {
          throw new DomainError('REFERENCE_EXPIRED', 'The reference did not resolve within twenty-four hours');
        }
        reference = await store.findReference(transaction.providerId, transaction.referenceExternalTransactionId);
        if (reference) {
          try {
            transaction.validateReference(reference);
          } catch (error) {
            if (!(error instanceof DomainError) || error.code !== 'REFERENCE_PENDING') throw error;
            referencePending = true;
          }
        } else {
          referencePending = true;
        }
      }
      if (referencePending) {
        transaction.markPendingReference();
      } else if (!transaction.affectsBalance()) {
        transaction.markProcessed(reference?.id, at);
      } else {
        if (transaction.requiresReference() && reference) {
          const existing = await store.hasSuccessfulReversal(reference.id);
          if (existing) throw new DomainError('REVERSAL_ALREADY_PROCESSED', 'The reference already has a processed reversal');
        }
        movement = transaction.ledgerDirectionFor(reference) === LedgerDirection.Debit
          ? wallet.debit(transaction.money, at)
          : wallet.credit(transaction.money, at);
      }
    } catch (error) {
      if (!(error instanceof DomainError) || ![
        'WALLET_PLAYER_MISMATCH', 'CURRENCY_MISMATCH', 'INSUFFICIENT_FUNDS', 'MONEY_OVERFLOW', 'WALLET_VERSION_OVERFLOW',
        'REFERENCE_MISMATCH', 'REFERENCE_REJECTED', 'REFERENCE_FAILED', 'INVALID_REFERENCE_KIND',
        'REFERENCE_AMOUNT_MISMATCH', 'REVERSAL_ALREADY_PROCESSED', 'REFERENCE_EXPIRED',
      ].includes(error.code)) throw error;
      const failureCode = transaction.kind === WagerTransactionKind.Rollback && error.code === 'INSUFFICIENT_FUNDS'
        ? 'REVERSAL_INSUFFICIENT_FUNDS'
        : error.code;
      transaction.reject(failureCode);
    }

    const context = { correlationId: transaction.id, occurredAt: at };
    if (movement) {
      transaction.markProcessed(reference?.id, at);
      await store.saveWallet(wallet);
      const entry = WalletLedgerEntry.create({
        id: crypto.randomUUID(), walletId: wallet.id, transactionId: transaction.id,
        direction: transaction.ledgerDirectionFor(reference), money: transaction.money,
        ...movement, createdAt: at,
      });
      await store.appendLedger(entry);
      await store.enqueue(WalletBalanceChanged.from(wallet, entry, context));
      await store.enqueue(WagerTransactionProcessed.from(transaction, context));
    } else if (referencePending) {
      if (previousStatus !== WagerTransactionStatus.PendingReference) {
        await store.enqueue(WagerTransactionPendingReference.from(transaction, context));
      }
    } else if (transaction.status === WagerTransactionStatus.Processed) {
      await store.enqueue(WagerTransactionProcessed.from(transaction, context));
    } else {
      await store.enqueue(WagerTransactionRejected.from(transaction, context));
    }
    const result: SubmissionResult = {
      transactionId: transaction.id, status: transaction.status,
      balance: wallet.balance.toJSON(), idempotentReplay: false,
      ...(transaction.failureCode === undefined ? {} : { failureCode: transaction.failureCode }),
    };
    const retries = stored.retryCount + (referencePending ? 1 : 0);
    const nextAttempt = new Date(at.getTime() + Math.min(60, 2 ** Math.min(stored.retryCount, 6)) * 1_000);
    await store.saveTransaction(transaction, result, { count: retries, nextAttemptAt: nextAttempt });
    return this.completeSubmission(store, inbox, result);
  }

  async failSubmission(body: unknown, idempotencyKey: unknown, failureCode: string, inboxContext?: InboxContext): Promise<SubmissionResult> {
    const input = wagerRequest(body);
    const key = text(idempotencyKey, 'Idempotency-Key', 500);
    const code = text(failureCode, 'failureCode', 100);
    if (!/^[A-Z][A-Z0-9_]{0,99}$/.test(code)) invalidRequest('failureCode must be an uppercase stable identifier');
    const payloadHash = canonicalHash(wagerPayload(input));
    const proposed = WagerTransaction.create({ ...input, id: crypto.randomUUID(), idempotencyKey: key, payloadHash });
    const inboxIdentity = inboxContext === undefined ? undefined : {
      consumerName: text(inboxContext.consumerName, 'consumerName'),
      messageId: text(inboxContext.messageId, 'messageId'),
      payloadHash: canonicalHash({ idempotencyKey: key, ...wagerPayload(input) }),
    };
    return this.store.transaction(async store => {
      const inbox = inboxIdentity === undefined ? undefined : await this.receiveInbox(store, inboxIdentity);
      if (inbox?.message.isProcessed()) {
        if (!inbox.result) throw new ApplicationError('internal', 'INTERNAL_ERROR', 'A processed inbox message must have a persisted response');
        return this.completeSubmission(store, inbox.message, { ...inbox.result, idempotentReplay: true });
      }
      const stored = await store.acceptTransaction(proposed);
      this.assertAcceptedIdentity(stored, payloadHash);
      const transaction = stored.transaction;
      if (transaction.isTerminal()) {
        if (!stored.result) throw new ApplicationError('internal', 'INTERNAL_ERROR', 'A terminal transaction must have a persisted response');
        return this.completeSubmission(store, inbox?.message, { ...stored.result, idempotentReplay: true });
      }
      const wallet = await store.lockWallet(transaction.walletId);
      if (!wallet) throw new ApplicationError('not_found', 'WALLET_NOT_FOUND', 'The wallet does not exist');
      transaction.fail(code);
      const result: SubmissionResult = {
        transactionId: transaction.id, status: transaction.status,
        balance: wallet.balance.toJSON(), failureCode: code, idempotentReplay: false,
      };
      await store.saveTransaction(transaction, result);
      await store.enqueue(WagerTransactionFailed.from(transaction, { correlationId: transaction.id }));
      return this.completeSubmission(store, inbox?.message, result);
    });
  }

  private assertAcceptedIdentity(stored: StoredWager | undefined, payloadHash: string): asserts stored is StoredWager {
    if (!stored) throw new ApplicationError('conflict', 'EXTERNAL_IDENTITY_CONFLICT', 'This provider transaction already uses another idempotency key');
    if (!stored.transaction.matchesPayload(payloadHash)) {
      throw new ApplicationError('conflict', 'IDEMPOTENCY_CONFLICT', 'This idempotency key was used for a different business payload');
    }
  }

  private async receiveInbox(store: WageringTransactionPort, identity: InboxIdentity): Promise<ReceivedInbox> {
    const inbox = await store.receiveInbox(identity);
    if (inbox.message.payloadHash !== identity.payloadHash) {
      throw new ApplicationError('conflict', 'INBOX_MESSAGE_CONFLICT', 'This message identity was used for a different payload or idempotency key');
    }
    return inbox;
  }

  private async completeSubmission(store: WageringTransactionPort, inbox: InboxMessage | undefined, result: SubmissionResult): Promise<SubmissionResult> {
    if (inbox && !inbox.isProcessed()) {
      inbox.markProcessed(new Date());
      await store.completeInbox(inbox, result);
    }
    return result;
  }

  private walletResponse(wallet: Wallet) {
    return { id: wallet.id, playerId: wallet.playerId, balance: wallet.balance.toJSON(), version: wallet.version };
  }

  private async opening(store: WageringTransactionPort, wallet: Wallet): Promise<void> {
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
    await store.insertOpening(transaction, result);
    const entry = WalletLedgerEntry.create({
      id: crypto.randomUUID(), walletId: wallet.id, transactionId: transaction.id,
      direction: LedgerDirection.Credit, money: wallet.balance,
      balanceBefore: Money.zero(wallet.currency), balanceAfter: wallet.balance,
      walletVersion: wallet.version, createdAt: wallet.createdAt,
    });
    await store.appendLedger(entry);
    const context = { correlationId: transaction.id, occurredAt: wallet.createdAt };
    await store.enqueue(WagerTransactionProcessed.from(transaction, context));
    await store.enqueue(WalletBalanceChanged.from(wallet, entry, context));
  }

}

export type WageringUseCases = Pick<WageringService, 'createWallet' | 'getWallet' | 'submit' | 'reprocess' | 'failSubmission'>;
