import { createHash } from 'node:crypto';

// This model imports no production code. It operates on integer ledger deltas
// and opaque business identities rather than the application's aggregates.
export type OracleKind = 'OPENING' | 'BET' | 'WIN' | 'LOSS' | 'REFUND' | 'ROLLBACK';
export type OracleStatus = 'PENDING' | 'PENDING_REFERENCE' | 'PROCESSED' | 'REJECTED' | 'FAILED';
export interface OracleMoney { amount: string; currency: string }
export interface OracleBody {
  providerId: string;
  externalTransactionId: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: OracleKind;
  money: OracleMoney;
  referenceExternalTransactionId?: string;
}
export interface OracleInboxIdentity { consumerName: string; messageId: string }
export interface OracleRequest {
  body: OracleBody;
  key: string;
  inbox?: OracleInboxIdentity;
  failureCode?: string;
}
export interface OracleResult {
  transactionKey: string;
  status: OracleStatus;
  balance: OracleMoney;
  idempotentReplay: boolean;
  failureCode?: string;
}
export interface OracleWallet { id: string; playerId: string; currency: string; cents: bigint; version: number }
export interface OracleTransaction {
  key: string;
  body: OracleBody;
  cents: bigint;
  fingerprint: string;
  status: OracleStatus;
  referenceKey?: string;
  failureCode?: string;
  result?: OracleResult;
}
export interface OracleLedger {
  walletId: string;
  transactionKey: string;
  direction: 'DEBIT' | 'CREDIT';
  cents: bigint;
  before: bigint;
  after: bigint;
  version: number;
}
export interface OracleEvent {
  eventType: string;
  transactionKey: string;
  walletId: string;
  data: Record<string, unknown>;
}
export interface OracleInbox {
  identity: OracleInboxIdentity;
  fingerprint: string;
  request: OracleRequest;
  result: OracleResult;
}
export interface OracleState {
  wallets: Map<string, OracleWallet>;
  transactions: Map<string, OracleTransaction>;
  externals: Map<string, string>;
  ledger: OracleLedger[];
  events: OracleEvent[];
  inbox: Map<string, OracleInbox>;
}
export type OracleOutcome = { type: 'result'; result: OracleResult } | { type: 'conflict'; code: string };

const financialMaximum = 9223372036854775807n;
const versionMaximum = 2147483647;
const terminal = (status: OracleStatus) => ['PROCESSED', 'REJECTED', 'FAILED'].includes(status);
const externalIdentity = (provider: string, external: string) => JSON.stringify([provider, external]);
const inboxIdentity = (identity: OracleInboxIdentity) => JSON.stringify([identity.consumerName, identity.messageId]);

export function decimal(cents: bigint): string {
  const magnitude = cents < 0n ? -cents : cents;
  return `${cents < 0n ? '-' : ''}${magnitude / 100n}.${(magnitude % 100n).toString().padStart(2, '0')}`;
}

function cents(amount: string): bigint {
  if (!/^\d+\.\d{2}$/.test(amount)) throw new Error(`The model received an invalid amount: ${amount}`);
  const [whole, fractional] = amount.split('.');
  return BigInt(whole!) * 100n + BigInt(fractional!);
}

function businessFingerprint(body: OracleBody): string {
  return JSON.stringify([
    body.providerId, body.externalTransactionId, body.playerId, body.walletId, body.roundId,
    body.gameId, body.kind, cents(body.money.amount).toString(), body.money.currency,
    body.referenceExternalTransactionId ?? null,
  ]);
}

/** Known business keys written in canonical order, independently of the application serializer. */
function canonicalPayload(body: OracleBody, idempotencyKey?: string): Record<string, unknown> {
  return {
    externalTransactionId: body.externalTransactionId,
    gameId: body.gameId,
    ...(idempotencyKey === undefined ? {} : { idempotencyKey }),
    kind: body.kind,
    money: { amount: decimal(cents(body.money.amount)), currency: body.money.currency },
    playerId: body.playerId,
    providerId: body.providerId,
    ...(body.referenceExternalTransactionId === undefined ? {} : { referenceExternalTransactionId: body.referenceExternalTransactionId }),
    roundId: body.roundId,
    walletId: body.walletId,
  };
}

export function expectedBusinessHash(body: OracleBody): string {
  return createHash('sha256').update(JSON.stringify(canonicalPayload(body))).digest('hex');
}

export function expectedInboxHash(body: OracleBody, key: string): string {
  return createHash('sha256').update(JSON.stringify(canonicalPayload(body, key))).digest('hex');
}

class Conflict extends Error {
  constructor(readonly code: string) { super(code); }
}

export class WageringOracle {
  private state: OracleState = { wallets: new Map(), transactions: new Map(), externals: new Map(), ledger: [], events: [], inbox: new Map() };

  constructor(wallets: Array<{ id: string; playerId: string; cents: bigint }>) {
    for (const input of wallets) {
      const wallet: OracleWallet = { ...input, currency: 'BRL', version: 1 };
      this.state.wallets.set(wallet.id, wallet);
      if (wallet.cents === 0n) continue;
      const body: OracleBody = {
        providerId: '__internal__', externalTransactionId: wallet.id, playerId: wallet.playerId,
        walletId: wallet.id, roundId: '__opening__', gameId: '__opening__', kind: 'OPENING',
        money: { amount: decimal(wallet.cents), currency: 'BRL' },
      };
      const key = `opening:${wallet.id}`;
      const transaction: OracleTransaction = { key, body, cents: wallet.cents, fingerprint: businessFingerprint(body), status: 'PROCESSED' };
      transaction.result = this.result(transaction, wallet, false);
      this.state.transactions.set(key, transaction);
      this.state.externals.set(externalIdentity(body.providerId, body.externalTransactionId), key);
      const entry: OracleLedger = { walletId: wallet.id, transactionKey: key, direction: 'CREDIT', cents: wallet.cents, before: 0n, after: wallet.cents, version: 1 };
      this.state.ledger.push(entry);
      this.transactionEvent(this.state, transaction, 'WagerTransactionProcessed');
      this.balanceEvent(this.state, entry);
    }
  }

  snapshot(): OracleState { return structuredClone(this.state); }

  submit(request: OracleRequest): OracleOutcome {
    const draft = structuredClone(this.state);
    try {
      const result = this.execute(draft, structuredClone(request));
      this.state = draft;
      return { type: 'result', result: structuredClone(result) };
    } catch (error) {
      if (error instanceof Conflict) return { type: 'conflict', code: error.code };
      throw error;
    }
  }

  private execute(state: OracleState, request: OracleRequest): OracleResult {
    const fingerprint = businessFingerprint(request.body);
    const messageKey = request.inbox ? inboxIdentity(request.inbox) : undefined;
    const messageFingerprint = JSON.stringify([request.key, fingerprint]);
    const previousMessage = messageKey === undefined ? undefined : state.inbox.get(messageKey);
    if (previousMessage) {
      if (previousMessage.fingerprint !== messageFingerprint) throw new Conflict('INBOX_MESSAGE_CONFLICT');
      return { ...previousMessage.result, idempotentReplay: true };
    }

    let transaction = state.transactions.get(request.key);
    if (transaction && transaction.fingerprint !== fingerprint) throw new Conflict('IDEMPOTENCY_CONFLICT');
    if (!transaction) {
      const identity = externalIdentity(request.body.providerId, request.body.externalTransactionId);
      if (state.externals.has(identity)) throw new Conflict('EXTERNAL_IDENTITY_CONFLICT');
      transaction = { key: request.key, body: request.body, cents: cents(request.body.money.amount), fingerprint, status: 'PENDING' };
      state.transactions.set(request.key, transaction);
      state.externals.set(identity, request.key);
    }
    const wallet = state.wallets.get(transaction.body.walletId);
    if (!wallet) throw new Error('All generated requests must address one of the three fixture wallets');
    let result: OracleResult;
    if (terminal(transaction.status)) {
      if (!transaction.result) throw new Error('Terminal model transactions require an original result');
      result = { ...transaction.result, idempotentReplay: true };
    } else {
      this.settle(state, transaction, wallet, request.failureCode);
      result = this.result(transaction, wallet, false);
      transaction.result = structuredClone(result);
    }
    if (messageKey !== undefined && request.inbox) {
      state.inbox.set(messageKey, { identity: request.inbox, fingerprint: messageFingerprint, request, result: structuredClone(result) });
    }
    return result;
  }

  private settle(state: OracleState, transaction: OracleTransaction, wallet: OracleWallet, permanentFailure?: string): void {
    if (permanentFailure) {
      transaction.status = 'FAILED';
      transaction.failureCode = permanentFailure;
      this.transactionEvent(state, transaction, 'WagerTransactionFailed');
      return;
    }
    const body = transaction.body;
    let rejection: string | undefined;
    let pending = false;
    let reference: OracleTransaction | undefined;
    if (body.playerId !== wallet.playerId) rejection = 'WALLET_PLAYER_MISMATCH';
    else if (body.money.currency !== wallet.currency) rejection = 'CURRENCY_MISMATCH';
    else if (body.referenceExternalTransactionId !== undefined) {
      const referenceKey = state.externals.get(externalIdentity(body.providerId, body.referenceExternalTransactionId));
      reference = referenceKey ? state.transactions.get(referenceKey) : undefined;
      if (!reference) pending = true;
      else if (reference.body.walletId !== body.walletId || reference.body.playerId !== body.playerId
        || reference.body.money.currency !== body.money.currency || reference.body.roundId !== body.roundId) rejection = 'REFERENCE_MISMATCH';
      else if (reference.status === 'REJECTED') rejection = 'REFERENCE_REJECTED';
      else if (reference.status === 'FAILED') rejection = 'REFERENCE_FAILED';
      else if (reference.status !== 'PROCESSED') pending = true;
      else {
        const eligible = body.kind === 'ROLLBACK' ? ['BET', 'WIN', 'REFUND'] : ['BET'];
        if (!eligible.includes(reference.body.kind)) rejection = 'INVALID_REFERENCE_KIND';
        else if (['REFUND', 'ROLLBACK'].includes(body.kind) && transaction.cents !== reference.cents) rejection = 'REFERENCE_AMOUNT_MISMATCH';
      }
    }
    if (pending) {
      const firstAcceptance = transaction.status !== 'PENDING_REFERENCE';
      transaction.status = 'PENDING_REFERENCE';
      if (firstAcceptance) this.transactionEvent(state, transaction, 'WagerTransactionPendingReference');
      return;
    }
    if (!rejection && reference && ['REFUND', 'ROLLBACK'].includes(body.kind)) {
      // The agreed financial contract consumes the reference across reversal
      // kinds. Undoing a REFUND must target the REFUND, not spend its BET again.
      const alreadyReversed = [...state.transactions.values()].some(other => other.status === 'PROCESSED'
        && ['REFUND', 'ROLLBACK'].includes(other.body.kind) && other.referenceKey === reference!.key);
      if (alreadyReversed) rejection = 'REVERSAL_ALREADY_PROCESSED';
    }
    const debit = body.kind === 'BET' || (body.kind === 'ROLLBACK' && reference?.body.kind !== 'BET');
    const delta = body.kind === 'LOSS' ? 0n : debit ? -transaction.cents : transaction.cents;
    const next = wallet.cents + delta;
    if (!rejection && delta !== 0n) {
      if (wallet.version >= versionMaximum) rejection = 'WALLET_VERSION_OVERFLOW';
      else if (next < 0n) rejection = body.kind === 'ROLLBACK' ? 'REVERSAL_INSUFFICIENT_FUNDS' : 'INSUFFICIENT_FUNDS';
      else if (next > financialMaximum) rejection = 'MONEY_OVERFLOW';
    }
    if (rejection) {
      transaction.status = 'REJECTED';
      transaction.failureCode = rejection;
      this.transactionEvent(state, transaction, 'WagerTransactionRejected');
      return;
    }
    transaction.status = 'PROCESSED';
    if (reference) transaction.referenceKey = reference.key;
    if (delta !== 0n) {
      const entry: OracleLedger = {
        walletId: wallet.id, transactionKey: transaction.key, direction: delta < 0n ? 'DEBIT' : 'CREDIT',
        cents: transaction.cents, before: wallet.cents, after: next, version: wallet.version + 1,
      };
      wallet.cents = next;
      wallet.version += 1;
      state.ledger.push(entry);
      this.balanceEvent(state, entry);
    }
    this.transactionEvent(state, transaction, 'WagerTransactionProcessed');
  }

  private result(transaction: OracleTransaction, wallet: OracleWallet, replay: boolean): OracleResult {
    return {
      transactionKey: transaction.key, status: transaction.status,
      balance: { amount: decimal(wallet.cents), currency: wallet.currency }, idempotentReplay: replay,
      ...(transaction.failureCode ? { failureCode: transaction.failureCode } : {}),
    };
  }

  private transactionEvent(state: OracleState, transaction: OracleTransaction, eventType: string): void {
    state.events.push({ eventType, transactionKey: transaction.key, walletId: transaction.body.walletId, data: {
      walletId: transaction.body.walletId, providerId: transaction.body.providerId,
      externalTransactionId: transaction.body.externalTransactionId, kind: transaction.body.kind,
      status: transaction.status, money: { amount: decimal(transaction.cents), currency: transaction.body.money.currency },
      ...(transaction.failureCode ? { failureCode: transaction.failureCode } : {}),
      ...(transaction.body.referenceExternalTransactionId === undefined ? {} : { referenceExternalTransactionId: transaction.body.referenceExternalTransactionId }),
    } });
  }

  private balanceEvent(state: OracleState, entry: OracleLedger): void {
    state.events.push({ eventType: 'WalletBalanceChanged', transactionKey: entry.transactionKey, walletId: entry.walletId, data: {
      walletId: entry.walletId, direction: entry.direction, money: { amount: decimal(entry.cents), currency: 'BRL' },
      balanceBefore: { amount: decimal(entry.before), currency: 'BRL' },
      balanceAfter: { amount: decimal(entry.after), currency: 'BRL' }, walletVersion: entry.version,
    } });
  }
}
