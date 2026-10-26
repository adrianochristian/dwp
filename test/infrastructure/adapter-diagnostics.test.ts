import { afterAll, afterEach, beforeAll, describe, expect, test } from 'bun:test';
import { OutboxPublisher } from '../../dist/infrastructure/messaging/outbox-publisher.js';
import { ReferenceWorker } from '../../dist/application/reference-worker.js';
import { createWageringService } from '../../dist/infrastructure/persistence/wagering-services.js';
import { sqsDiagnosticCode, SqsCallTimeoutError } from '../../dist/infrastructure/messaging/sqs.js';
import { createCrashEnvironment, createFinancialEvent, financialState, receiveEvents, type CrashEnvironment } from '../fixtures/crash-support.js';

const integration = process.env.DWP_INTEGRATION === '1' ? describe : describe.skip;

integration('adapter diagnostics cannot change durable outcomes', () => {
  let environment: CrashEnvironment;
  const publishers: OutboxPublisher[] = [];
  const workers: ReferenceWorker[] = [];
  beforeAll(async () => { environment = await createCrashEnvironment('adapter_diag'); });
  afterEach(async () => {
    await Promise.all([...publishers.splice(0).map(value => value.stop()), ...workers.splice(0).map(value => value.stop())]);
    await environment.query('delete from dwp.outbox_messages');
    await environment.query("update dwp.wager_transactions set next_attempt_at = clock_timestamp() + interval '1 hour' where status = 'PENDING_REFERENCE'");
  });
  afterAll(async () => { await environment?.close(); });

  test('a throwing retry diagnostic preserves a fault-injected retry and its original event', async () => {
    const { eventId, walletId } = await createFinancialEvent(environment);
    const queueUrl = await environment.createQueue();
    let inject = true;
    let observed = 0;
    const publisher = new OutboxPublisher(environment.orm, environment.sqs, queueUrl, {
      beforeSend: () => { if (inject) throw new Error('injected before send'); },
      onRetry: () => { observed += 1; throw new Error('diagnostic sink unavailable'); },
    });
    publishers.push(publisher);
    expect(await publisher.runOnce()).toBe(0);
    expect(observed).toBe(1);
    expect((await environment.query<{ published_at: Date | null; attempts: number }>(
      'select published_at, attempts from dwp.outbox_messages where id = ?', [eventId],
    ))[0]).toEqual({ published_at: null, attempts: 1 });
    expect(await receiveEvents(environment.sqs, queueUrl)).toHaveLength(0);
    inject = false;
    await environment.query('update dwp.outbox_messages set next_attempt_at = clock_timestamp() where id = ?', [eventId]);
    expect(await publisher.runOnce()).toBe(1);
    expect((await receiveEvents(environment.sqs, queueUrl)).map(event => event.eventId)).toEqual([eventId]);
    expect(await financialState(environment, walletId)).toEqual({ balance_cents: '7500', calculated: '7500', debits: '1' });
  });

  test('a throwing lease diagnostic cannot overwrite a replacement owner or repeat a send', async () => {
    const { eventId } = await createFinancialEvent(environment);
    const queueUrl = await environment.createQueue();
    const owner = crypto.randomUUID();
    let observed = 0;
    const publisher = new OutboxPublisher(environment.orm, environment.sqs, queueUrl, {
      afterSend: async () => {
        await environment.query(`update dwp.outbox_messages set lease_token = ?,
          lease_until = clock_timestamp() + interval '30 seconds' where id = ?`, [owner, eventId]);
      },
      onLeaseLost: () => { observed += 1; throw new Error('diagnostic sink unavailable'); },
    });
    publishers.push(publisher);
    expect(await publisher.runOnce()).toBe(0);
    expect(observed).toBe(1);
    expect((await environment.query<{ lease_token: string; published_at: Date | null; attempts: number }>(
      'select lease_token, published_at, attempts from dwp.outbox_messages where id = ?', [eventId],
    ))[0]).toEqual({ lease_token: owner, published_at: null, attempts: 1 });
    expect((await receiveEvents(environment.sqs, queueUrl)).map(event => event.eventId)).toEqual([eventId]);
  });

  async function referenceFixture() {
    const service = createWageringService(environment.orm);
    const wallet = await service.createWallet({ playerId: crypto.randomUUID(), initialBalance: { amount: '100.00', currency: 'BRL' } });
    const bet = {
      providerId: 'diagnostic-provider', externalTransactionId: crypto.randomUUID(), playerId: wallet.playerId,
      walletId: wallet.id, roundId: 'diagnostic-round', gameId: 'diagnostic-game', kind: 'BET',
      money: { amount: '25.00', currency: 'BRL' },
    };
    const refund = { ...bet, kind: 'REFUND', externalTransactionId: crypto.randomUUID(), referenceExternalTransactionId: bet.externalTransactionId };
    const pending = await service.submit(refund, crypto.randomUUID());
    await environment.query('update dwp.wager_transactions set next_attempt_at = clock_timestamp() where id = ?', [pending.transactionId]);
    return { service, wallet, bet, pending };
  }

  test('a throwing result diagnostic cannot turn a committed reference into a retry', async () => {
    const { service, wallet, bet, pending } = await referenceFixture();
    await service.submit(bet, crypto.randomUUID());
    let retries = 0;
    const worker = new ReferenceWorker(environment.orm, service, {
      onResult: () => { throw new Error('diagnostic sink unavailable'); },
      onRetry: () => { retries += 1; throw new Error('diagnostic sink unavailable'); },
    });
    workers.push(worker);
    expect(await worker.runOnce()).toBe(1);
    expect(retries).toBe(0);
    expect((await environment.query<{ status: string }>('select status from dwp.wager_transactions where id = ?', [pending.transactionId]))[0]?.status).toBe('PROCESSED');
    expect(await financialState(environment, wallet.id)).toEqual({ balance_cents: '10000', calculated: '10000', debits: '1' });
  });

  test('a throwing retry diagnostic leaves a real precommit failure pending without money effects', async () => {
    const { wallet, pending } = await referenceFixture();
    const failing = createWageringService(environment.orm, { beforeCommit: () => { throw new Error('injected before commit'); } });
    const worker = new ReferenceWorker(environment.orm, failing, {
      onRetry: () => { throw new Error('diagnostic sink unavailable'); },
    });
    workers.push(worker);
    expect(await worker.runOnce()).toBe(0);
    expect((await environment.query<{ status: string }>('select status from dwp.wager_transactions where id = ?', [pending.transactionId]))[0]?.status).toBe('PENDING_REFERENCE');
    expect(await financialState(environment, wallet.id)).toEqual({ balance_cents: '10000', calculated: '10000', debits: '0' });
  });

  test('classifies timeout and cancellation using stable codes without diagnostic text', () => {
    expect(sqsDiagnosticCode(new SqsCallTimeoutError(100))).toBe('SQS_CALL_TIMEOUT');
    expect(sqsDiagnosticCode(Object.assign(new Error('opaque transport diagnostic'), { name: 'TimeoutError', code: 'ETIMEDOUT' })))
      .toBe('SQS_CALL_TIMEOUT');
    expect(sqsDiagnosticCode(new DOMException('opaque cancellation reason', 'AbortError'))).toBe('OPERATION_CANCELLED');
    expect(sqsDiagnosticCode(new Error('opaque transport diagnostic'), 'SQS_PUBLISH_FAILED')).toBe('SQS_PUBLISH_FAILED');
  });
});
