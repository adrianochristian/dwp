import { DomainError } from './domain-error.js';

export interface InboxMessageState {
  messageId: string;
  consumerName: string;
  payloadHash: string;
  receivedAt: Date;
  processedAt?: Date | undefined;
}

export interface ReceiveInboxProps {
  messageId: string;
  consumerName: string;
  payloadHash: string;
  receivedAt?: Date;
}

export class InboxMessage {
  private readonly state: Omit<InboxMessageState, 'receivedAt' | 'processedAt'>;
  private readonly receivedTimestamp: number;
  private processedTimestamp: number | undefined;

  private constructor(state: InboxMessageState) {
    this.state = Object.freeze({
      messageId: state.messageId,
      consumerName: state.consumerName,
      payloadHash: state.payloadHash,
    });
    this.receivedTimestamp = state.receivedAt.getTime();
    this.processedTimestamp = state.processedAt?.getTime();
  }

  static receive(props: ReceiveInboxProps): InboxMessage {
    if (!props.messageId || !props.consumerName || !props.payloadHash) {
      throw new DomainError('INVALID_INBOX_MESSAGE', 'Inbox identity and payload hash are required');
    }
    const receivedAt = props.receivedAt ?? new Date();
    if (!Number.isFinite(receivedAt.getTime())) {
      throw new DomainError('INVALID_MESSAGE_DATE', 'Received date must be valid');
    }
    return new InboxMessage({ ...props, receivedAt });
  }

  static rehydrate(state: InboxMessageState): InboxMessage {
    return new InboxMessage(state);
  }

  get messageId(): string { return this.state.messageId; }
  get consumerName(): string { return this.state.consumerName; }
  get payloadHash(): string { return this.state.payloadHash; }
  get receivedAt(): Date { return new Date(this.receivedTimestamp); }
  get processedAt(): Date | undefined {
    return this.processedTimestamp === undefined ? undefined : new Date(this.processedTimestamp);
  }

  isProcessed(): boolean { return this.processedTimestamp !== undefined; }

  markProcessed(at: Date): void {
    if (this.isProcessed()) {
      throw new DomainError('INVALID_INBOX_STATE', 'Inbox message has already been processed');
    }
    if (!Number.isFinite(at.getTime())) {
      throw new DomainError('INVALID_MESSAGE_DATE', 'Processed date must be valid');
    }
    this.processedTimestamp = at.getTime();
  }
}
