import type { MikroORM } from '@mikro-orm/postgresql';
import type { ShutdownOptions } from '../infrastructure/messaging/sqs.js';
import { normalizeShutdownOptions, waitForSettlement } from '../infrastructure/messaging/shutdown.js';

export interface ReferenceHandler {
  reprocess(id: string): Promise<unknown>;
}

export interface ReferenceWorkerOptions {
  batchSize?: number;
  onRetry?: (transactionId: string, error: unknown) => void;
  onResult?: (transactionId: string, result: unknown) => void;
}

export class ReferenceWorker {
  private readonly batchSize: number;
  private readonly active = new Set<Promise<number>>();
  private stopped = false;
  private stopTask: Promise<void> | undefined;

  constructor(
    private readonly orm: MikroORM,
    private readonly handler: ReferenceHandler,
    private readonly options: ReferenceWorkerOptions = {},
  ) {
    this.batchSize = options.batchSize ?? 20;
    if (!Number.isInteger(this.batchSize) || this.batchSize < 1 || this.batchSize > 100) {
      throw new Error('Reference batchSize must be an integer between 1 and 100');
    }
  }

  async runOnce(): Promise<number> {
    if (this.stopped) return 0;
    const batch = this.processBatch();
    this.active.add(batch);
    try {
      return await batch;
    } finally {
      this.active.delete(batch);
    }
  }

  quiesce(): void {
    this.stopped = true;
  }

  stop(input: number | ShutdownOptions = {}): Promise<void> {
    if (!this.stopTask) {
      const options = normalizeShutdownOptions(input);
      this.quiesce();
      this.stopTask = waitForSettlement(this.active, options.timeoutMs, options.signal).then(() => undefined);
    }
    return this.stopTask;
  }

  private async processBatch(): Promise<number> {
    // Selection holds no locks. Reprocessing checks the due time again under
    // the transaction row lock, before taking the wallet lock.
    const due = await this.orm.em.fork().execute<{ id: string }[]>(`
      select id from dwp.wager_transactions
      where status = 'PENDING_REFERENCE' and next_attempt_at <= clock_timestamp()
      order by next_attempt_at, id limit ?`, [this.batchSize]);
    if (this.stopped) return 0;
    const results = await Promise.allSettled(due.map(async ({ id }) => {
      try {
        const result = await this.handler.reprocess(id);
        if (result === undefined) return false;
        this.options.onResult?.(id, result);
        return true;
      } catch (error) {
        this.options.onRetry?.(id, error);
        return false;
      }
    }));
    const failure = results.find(result => result.status === 'rejected');
    if (failure?.status === 'rejected') throw failure.reason;
    return results.filter(result => result.status === 'fulfilled' && result.value).length;
  }
}
