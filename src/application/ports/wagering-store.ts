import type { InboxMessage } from '../../domain/inbox-message.js';
import type { IntegrationEvent } from '../../domain/integration-event.js';
import type { WalletLedgerEntry } from '../../domain/ledger-entry.js';
import type { Money, MoneyProps } from '../../domain/money.js';
import type { WagerTransaction, WagerTransactionKind, WagerTransactionStatus } from '../../domain/wager-transaction.js';
import type { Wallet } from '../../domain/wallet.js';

export interface SubmissionResult {
  transactionId: string;
  status: `${WagerTransactionStatus}`;
  balance: MoneyProps;
  idempotentReplay: boolean;
  failureCode?: string;
}

export interface InboxContext {
  consumerName: string;
  messageId: string;
}

export interface InboxIdentity extends InboxContext {
  payloadHash: string;
}

export interface ReceivedInbox {
  message: InboxMessage;
  result: SubmissionResult | null;
}

export interface StoredWager {
  transaction: WagerTransaction;
  result: SubmissionResult | null;
  retryCount: number;
  nextAttemptAt: Date;
}

export interface RetrySchedule {
  count: number;
  nextAttemptAt: Date;
}

export interface WageringStoreOptions {
  beforeCommit?: () => Promise<void> | void;
}

export interface WageringTransactionPort {
  now(): Promise<Date>;
  insertWallet(wallet: Wallet): Promise<void>;
  receiveInbox(identity: InboxIdentity): Promise<ReceivedInbox>;
  acceptTransaction(transaction: WagerTransaction): Promise<StoredWager | undefined>;
  lockTransaction(id: string): Promise<StoredWager | undefined>;
  lockWallet(id: string): Promise<Wallet | undefined>;
  findReference(providerId: string, externalTransactionId: string): Promise<WagerTransaction | undefined>;
  hasSuccessfulReversal(referenceId: string): Promise<boolean>;
  saveWallet(wallet: Wallet): Promise<void>;
  saveTransaction(transaction: WagerTransaction, result: SubmissionResult, retry?: RetrySchedule): Promise<void>;
  insertOpening(transaction: WagerTransaction, result: SubmissionResult): Promise<void>;
  appendLedger(entry: WalletLedgerEntry): Promise<void>;
  enqueue<T>(event: IntegrationEvent<T>): Promise<void>;
  completeInbox(message: InboxMessage, result: SubmissionResult): Promise<void>;
}

export interface WageringStorePort {
  transaction<T>(work: (transaction: WageringTransactionPort) => Promise<T>): Promise<T>;
  readWallet(id: string): Promise<Wallet | undefined>;
}

export interface TransactionView {
  id: string;
  providerId: string;
  externalTransactionId: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: `${WagerTransactionKind}`;
  money: Money;
  referenceExternalTransactionId?: string;
  referenceTransactionId?: string;
  status: `${WagerTransactionStatus}`;
  failureCode?: string;
  result: Readonly<Record<string, unknown>> | null;
  createdAt: Date;
  processedAt?: Date;
}

export interface ReconciliationSnapshot {
  storedBalance: Money;
  calculatedBalance: Money;
  checkedEntries: number;
}

export interface WageringReadPort {
  findTransaction(id: string): Promise<TransactionView | undefined>;
  findExternalTransaction(providerId: string, externalTransactionId: string): Promise<TransactionView | undefined>;
  ledgerCeiling(walletId: string): Promise<number | undefined>;
  ledgerPage(walletId: string, afterVersion: number, ceilingVersion: number, limit: number): Promise<WalletLedgerEntry[]>;
  reconciliationSnapshot(walletId: string): Promise<ReconciliationSnapshot | undefined>;
}
