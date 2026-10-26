import { DomainError } from './domain-error.js';
import { LedgerDirection } from './ledger-entry.js';
import type { Money } from './money.js';

export enum WagerTransactionKind {
  Opening = 'OPENING',
  Bet = 'BET',
  Win = 'WIN',
  Loss = 'LOSS',
  Refund = 'REFUND',
  Rollback = 'ROLLBACK',
}

export enum WagerTransactionStatus {
  Pending = 'PENDING',
  PendingReference = 'PENDING_REFERENCE',
  Processed = 'PROCESSED',
  Rejected = 'REJECTED',
  Failed = 'FAILED',
}

export interface CreateWagerTransactionProps {
  id: string;
  providerId: string;
  externalTransactionId: string;
  idempotencyKey: string;
  payloadHash: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: Money;
  referenceExternalTransactionId?: string | undefined;
  createdAt?: Date;
}

export interface WagerTransactionState extends Omit<CreateWagerTransactionProps, 'createdAt'> {
  createdAt: Date;
  status: WagerTransactionStatus;
  referenceTransactionId?: string | undefined;
  failureCode?: string | undefined;
  processedAt?: Date | undefined;
}

export class WagerTransaction {
  private _status: WagerTransactionStatus;
  private _referenceTransactionId: string | undefined;
  private _failureCode: string | undefined;
  private _processedAt: number | undefined;
  private readonly _createdAt: number;

  private constructor(
    public readonly id: string,
    public readonly providerId: string,
    public readonly externalTransactionId: string,
    public readonly idempotencyKey: string,
    public readonly payloadHash: string,
    public readonly walletId: string,
    public readonly playerId: string,
    public readonly roundId: string,
    public readonly gameId: string,
    public readonly kind: WagerTransactionKind,
    public readonly money: Money,
    public readonly referenceExternalTransactionId: string | undefined,
    state: Pick<WagerTransactionState, 'createdAt' | 'status' | 'referenceTransactionId' | 'failureCode' | 'processedAt'>,
  ) {
    this._status = state.status;
    this._referenceTransactionId = state.referenceTransactionId;
    this._failureCode = state.failureCode;
    this._createdAt = state.createdAt.getTime();
    this._processedAt = state.processedAt?.getTime();
  }

  static create(props: CreateWagerTransactionProps): WagerTransaction {
    if (!Object.values(WagerTransactionKind).includes(props.kind)) {
      throw new DomainError('INVALID_TRANSACTION_KIND', 'The transaction kind is invalid');
    }
    if (props.money.isNegative() || (props.kind !== WagerTransactionKind.Loss && !props.money.isPositive())) {
      throw new DomainError('INVALID_TRANSACTION_AMOUNT', 'Only LOSS may have a zero amount; negative amounts are forbidden');
    }
    if ((props.kind === WagerTransactionKind.Refund || props.kind === WagerTransactionKind.Rollback)
      && !props.referenceExternalTransactionId) {
      throw new DomainError('REFERENCE_REQUIRED', 'This transaction kind requires a reference');
    }
    return this.rehydrate({ ...props, createdAt: props.createdAt ?? new Date(), status: WagerTransactionStatus.Pending });
  }

  static rehydrate(state: WagerTransactionState): WagerTransaction {
    return new WagerTransaction(
      state.id, state.providerId, state.externalTransactionId, state.idempotencyKey, state.payloadHash,
      state.walletId, state.playerId, state.roundId, state.gameId, state.kind, state.money,
      state.referenceExternalTransactionId, state,
    );
  }

  get status(): WagerTransactionStatus { return this._status; }
  get referenceTransactionId(): string | undefined { return this._referenceTransactionId; }
  get failureCode(): string | undefined { return this._failureCode; }
  get createdAt(): Date { return new Date(this._createdAt); }
  get processedAt(): Date | undefined {
    return this._processedAt === undefined ? undefined : new Date(this._processedAt);
  }

  markProcessed(referenceTransactionId: string | undefined, at: Date): void {
    this.assertMutable();
    if (this.requiresReference() && !referenceTransactionId) {
      throw new DomainError('REFERENCE_REQUIRED', 'A processed reversal requires a resolved reference');
    }
    this._status = WagerTransactionStatus.Processed;
    this._referenceTransactionId = referenceTransactionId;
    this._processedAt = at.getTime();
  }

  markPendingReference(): void {
    this.assertMutable();
    if (!this.referenceExternalTransactionId) {
      throw new DomainError('REFERENCE_REQUIRED', 'An unresolved reference must be identified');
    }
    this._status = WagerTransactionStatus.PendingReference;
  }

  reject(code: string): void {
    this.assertMutable();
    this._status = WagerTransactionStatus.Rejected;
    this._failureCode = code;
  }

  fail(code: string): void {
    this.assertMutable();
    this._status = WagerTransactionStatus.Failed;
    this._failureCode = code;
  }

  isTerminal(): boolean {
    return this._status === WagerTransactionStatus.Processed
      || this._status === WagerTransactionStatus.Rejected
      || this._status === WagerTransactionStatus.Failed;
  }

  affectsBalance(): boolean { return this.kind !== WagerTransactionKind.Loss; }

  requiresReference(): boolean {
    return this.kind === WagerTransactionKind.Refund || this.kind === WagerTransactionKind.Rollback;
  }

  matchesPayload(hash: string): boolean { return this.payloadHash === hash; }

  validateReference(reference: WagerTransaction): void {
    if (reference.providerId !== this.providerId || reference.playerId !== this.playerId
      || reference.walletId !== this.walletId || reference.money.currency !== this.money.currency
      || reference.roundId !== this.roundId
      || reference.externalTransactionId !== this.referenceExternalTransactionId) {
      throw new DomainError('REFERENCE_MISMATCH', 'The reference must identify the same provider, player, wallet, currency and round');
    }
    if (reference.status === WagerTransactionStatus.Rejected) {
      throw new DomainError('REFERENCE_REJECTED', 'The referenced transaction was rejected');
    }
    if (reference.status === WagerTransactionStatus.Failed) {
      throw new DomainError('REFERENCE_FAILED', 'The referenced transaction failed permanently');
    }
    if (reference.status !== WagerTransactionStatus.Processed) {
      throw new DomainError('REFERENCE_PENDING', 'The referenced transaction has not been processed');
    }
    const allowedKinds = this.kind === WagerTransactionKind.Rollback
      ? [WagerTransactionKind.Bet, WagerTransactionKind.Win, WagerTransactionKind.Refund]
      : [WagerTransactionKind.Bet];
    if (!allowedKinds.includes(reference.kind)) {
      throw new DomainError('INVALID_REFERENCE_KIND', 'The referenced kind cannot be used for this operation');
    }
    if (this.requiresReference() && !this.money.equals(reference.money)) {
      throw new DomainError('REFERENCE_AMOUNT_MISMATCH', 'A reversal must use the full reference amount');
    }
  }

  ledgerDirectionFor(reference?: WagerTransaction): LedgerDirection {
    switch (this.kind) {
      case WagerTransactionKind.Bet: return LedgerDirection.Debit;
      case WagerTransactionKind.Opening:
      case WagerTransactionKind.Win:
      case WagerTransactionKind.Refund: return LedgerDirection.Credit;
      case WagerTransactionKind.Loss:
        throw new DomainError('NO_LEDGER_EFFECT', 'LOSS does not produce a ledger entry');
      case WagerTransactionKind.Rollback:
        if (!reference || ![
          WagerTransactionKind.Bet, WagerTransactionKind.Win, WagerTransactionKind.Refund,
        ].includes(reference.kind)) {
          throw new DomainError('INVALID_REFERENCE_KIND', 'ROLLBACK can only reverse BET, WIN or REFUND');
        }
        return reference.kind === WagerTransactionKind.Bet ? LedgerDirection.Credit : LedgerDirection.Debit;
    }
  }

  private assertMutable(): void {
    if (this.isTerminal()) {
      throw new DomainError('INVALID_TRANSACTION_STATE', 'A terminal transaction cannot change state');
    }
  }
}
