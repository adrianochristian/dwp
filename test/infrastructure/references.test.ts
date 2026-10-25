import { afterAll, afterEach, beforeAll, describe, expect, test } from 'bun:test';
import type { MikroORM } from '@mikro-orm/postgresql';
import { ReferenceWorker } from '../../dist/application/reference-worker.js';
import { WageringService } from '../../dist/application/wagering-service.js';
import { WageringQueries } from '../../dist/application/wagering-queries.js';
import { createDatabase } from '../../dist/infrastructure/persistence/database.js';

const integration = process.env.DWP_INTEGRATION === '1' ? describe : describe.skip;

integration('durable out-of-order reference processing', () => {
  const databaseName = `dwp_references_${crypto.randomUUID().replaceAll('-', '')}`;
  let admin: MikroORM;
  let orm: MikroORM;
  let service: WageringService;
  let queries: WageringQueries;

  beforeAll(async () => {
    admin = await createDatabase();
    await admin.em.getConnection().execute(`create database ${databaseName}`);
    const url = new URL(process.env.DATABASE_URL ?? 'postgresql://dwp:dwp@127.0.0.1:5432/dwp');
    url.pathname = `/${databaseName}`;
    orm = await createDatabase(url.toString());
    await orm.migrator.up();
    service = new WageringService(orm);
    queries = new WageringQueries(orm);
  });

  afterEach(async () => {
    if (orm) await orm.em.fork().execute(`update dwp.wager_transactions
      set next_attempt_at = clock_timestamp() + interval '1 hour' where status = 'PENDING_REFERENCE'`);
  });

  afterAll(async () => {
    await orm?.close(true);
    if (admin) {
      await admin.em.getConnection().execute(`drop database if exists ${databaseName} with (force)`);
      await admin.close(true);
    }
  });

  const query = <T>(sql: string, values: unknown[] = []) => orm.em.fork().execute<T[]>(sql, values);

  async function fixture(initial = '100.00') {
    const wallet = await service.createWallet({
      playerId: crypto.randomUUID(), initialBalance: { amount: initial, currency: 'BRL' },
    });
    const command = (kind: string, amount: string, referenceExternalTransactionId?: string) => ({
      providerId: 'reference-provider', externalTransactionId: crypto.randomUUID(),
      walletId: wallet.id, playerId: wallet.playerId, roundId: 'round', gameId: 'game', kind,
      money: { amount, currency: 'BRL' },
      ...(referenceExternalTransactionId === undefined ? {} : { referenceExternalTransactionId }),
    });
    return { wallet, command };
  }

  async function makeDue(transactionId: string): Promise<void> {
    await query(`update dwp.wager_transactions set next_attempt_at = clock_timestamp() - interval '1 second'
      where id = ? and status = 'PENDING_REFERENCE'`, [transactionId]);
  }

  async function events(transactionId: string, eventType: string): Promise<number> {
    return (await query<{ count: number }>(`select count(*)::integer as count
      from dwp.outbox_messages where aggregate_id = ? and event_type = ?`, [transactionId, eventType]))[0]!.count;
  }

  async function assertConsistent(walletId: string): Promise<void> {
    const result = await queries.reconcile(walletId);
    expect(result.consistent).toBe(true);
    expect(BigInt(result.storedBalance.amount.replace('.', '')) >= 0n).toBe(true);
    expect(result.storedBalance).toEqual(result.calculatedBalance);
  }

  test('processes REFUND and ROLLBACK delivered before their BET through a new worker instance', async () => {
    for (const kind of ['REFUND', 'ROLLBACK']) {
      const { wallet, command } = await fixture();
      const bet = command('BET', '25.00');
      const reversal = command(kind, '25.00', bet.externalTransactionId);
      const key = crypto.randomUUID();
      const pending = await service.submit(reversal, key);
      expect(pending.status).toBe('PENDING_REFERENCE');
      expect(await events(pending.transactionId, 'WagerTransactionPendingReference')).toBe(1);
      await query("update dwp.wager_transactions set next_attempt_at = clock_timestamp() + interval '1 hour' where id = ?", [pending.transactionId]);
      const oldWorker = new ReferenceWorker(orm, service);
      expect(await oldWorker.runOnce()).toBe(0);
      await oldWorker.stop();
      await service.submit(bet, crypto.randomUUID());
      await makeDue(pending.transactionId);
      const restarted = new ReferenceWorker(orm, new WageringService(orm));
      expect(await restarted.runOnce()).toBe(1);
      await restarted.stop();
      expect(await queries.getTransaction(pending.transactionId)).toMatchObject({
        transactionId: pending.transactionId, status: 'PROCESSED', balance: wallet.balance,
      });
      expect(await events(pending.transactionId, 'WagerTransactionProcessed')).toBe(1);
      expect(await query('select id from dwp.wallet_ledger_entries where transaction_id = ?', [pending.transactionId])).toHaveLength(1);
      expect(await service.submit(reversal, key)).toMatchObject({
        transactionId: pending.transactionId, status: 'PROCESSED', balance: wallet.balance, idempotentReplay: true,
      });
      await assertConsistent(wallet.id);
    }
  });

  test('persists exponential backoff from one second to sixty without repeating the pending event', async () => {
    const { wallet, command } = await fixture();
    const beforeFirst = Date.now();
    const pending = await service.submit(command('REFUND', '25.00', crypto.randomUUID()), crypto.randomUUID());
    const afterFirst = Date.now();
    const worker = new ReferenceWorker(orm, service);
    const delays = [1, 2, 4, 8, 16, 32, 60, 60];
    for (const [index, delay] of delays.entries()) {
      let before = beforeFirst;
      let after = afterFirst;
      if (index > 0) {
        await makeDue(pending.transactionId);
        before = Date.now();
        expect(await worker.runOnce()).toBe(1);
        after = Date.now();
      }
      const [row] = await query<{ retry_count: number; next_attempt_at: string; status: string }>(`
        select retry_count, next_attempt_at, status from dwp.wager_transactions where id = ?`, [pending.transactionId]);
      expect(row!.status).toBe('PENDING_REFERENCE');
      expect(row!.retry_count).toBe(index + 1);
      const nextAttempt = new Date(row!.next_attempt_at).getTime();
      expect(nextAttempt).toBeGreaterThanOrEqual(before + delay * 1000 - 100);
      expect(nextAttempt).toBeLessThanOrEqual(after + delay * 1000 + 100);
      await query("update dwp.wager_transactions set next_attempt_at = clock_timestamp() + interval '1 hour' where id = ?", [pending.transactionId]);
      expect(await worker.runOnce()).toBe(0);
    }
    expect(await events(pending.transactionId, 'WagerTransactionPendingReference')).toBe(1);
    expect(await query('select id from dwp.wallet_ledger_entries where transaction_id = ?', [pending.transactionId])).toHaveLength(0);
    await worker.stop();
    await assertConsistent(wallet.id);
  });

  test('expires references after twenty-four hours from persisted creation despite HTTP retries', async () => {
    const { wallet, command } = await fixture();
    const body = command('ROLLBACK', '25.00', crypto.randomUUID());
    const key = crypto.randomUUID();
    const pending = await service.submit(body, key);
    await query(`update dwp.wager_transactions set created_at = clock_timestamp() - interval '23 hours',
      next_attempt_at = clock_timestamp() - interval '1 second' where id = ?`, [pending.transactionId]);
    const createdAt = (await query<{ created_at: string }>('select created_at from dwp.wager_transactions where id = ?', [pending.transactionId]))[0]!.created_at;
    expect((await service.submit(body, key)).status).toBe('PENDING_REFERENCE');
    expect((await query<{ created_at: string }>('select created_at from dwp.wager_transactions where id = ?', [pending.transactionId]))[0]!.created_at).toBe(createdAt);
    await query(`update dwp.wager_transactions set created_at = clock_timestamp() - interval '24 hours 1 second',
      next_attempt_at = clock_timestamp() - interval '1 second' where id = ?`, [pending.transactionId]);
    const worker = new ReferenceWorker(orm, service);
    expect(await worker.runOnce()).toBe(1);
    await worker.stop();
    const terminal = await queries.getTransaction(pending.transactionId);
    expect(terminal).toMatchObject({
      status: 'REJECTED', failureCode: 'REFERENCE_EXPIRED', balance: wallet.balance,
    });
    expect(await events(pending.transactionId, 'WagerTransactionRejected')).toBe(1);
    expect(await events(pending.transactionId, 'WagerTransactionPendingReference')).toBe(1);
    expect(await query('select id from dwp.wallet_ledger_entries where transaction_id = ?', [pending.transactionId])).toHaveLength(0);
    expect(await service.submit(body, key)).toMatchObject({ status: 'REJECTED', failureCode: 'REFERENCE_EXPIRED', idempotentReplay: true });
    await assertConsistent(wallet.id);
  });

  test('serializes a scheduled retry and HTTP retry without duplicating the reversal', async () => {
    const { wallet, command } = await fixture();
    const bet = command('BET', '25.00');
    const body = command('REFUND', '25.00', bet.externalTransactionId);
    const key = crypto.randomUUID();
    const pending = await service.submit(body, key);
    await service.submit(bet, crypto.randomUUID());
    await makeDue(pending.transactionId);
    const worker = new ReferenceWorker(orm, service);
    const [, httpRetry] = await Promise.all([worker.runOnce(), service.submit(body, key)]);
    expect(httpRetry.status).toBe('PROCESSED');
    expect((await queries.getTransaction(pending.transactionId)).status).toBe('PROCESSED');
    expect(await query('select id from dwp.wallet_ledger_entries where transaction_id = ?', [pending.transactionId])).toHaveLength(1);
    expect(await events(pending.transactionId, 'WagerTransactionProcessed')).toBe(1);
    expect(await events(pending.transactionId, 'WagerTransactionPendingReference')).toBe(1);
    await worker.stop();
    await assertConsistent(wallet.id);
  });

  test('rechecks the persisted due time when two workers select the same pending reference', async () => {
    const { wallet, command } = await fixture();
    const pending = await service.submit(command('REFUND', '25.00', crypto.randomUUID()), crypto.randomUUID());
    await makeDue(pending.transactionId);
    let entered = 0;
    let release!: () => void;
    const bothSelected = new Promise<void>(resolve => { release = resolve; });
    const handler = {
      async reprocess(id: string) {
        entered += 1;
        if (entered === 2) release();
        await bothSelected;
        return service.reprocess(id);
      },
    };
    const first = new ReferenceWorker(orm, handler);
    const second = new ReferenceWorker(orm, handler);
    const results = await Promise.all([first.runOnce(), second.runOnce()]);
    expect(entered).toBe(2);
    expect(results.reduce((sum, count) => sum + count, 0)).toBe(1);
    const [row] = await query<{ retry_count: number }>('select retry_count from dwp.wager_transactions where id = ?', [pending.transactionId]);
    expect(row!.retry_count).toBe(2);
    expect(await events(pending.transactionId, 'WagerTransactionPendingReference')).toBe(1);
    await Promise.all([first.stop(), second.stop()]);
    await assertConsistent(wallet.id);
  }, 10_000);

  test('rejects a dependent operation when its reference was rejected or failed permanently', async () => {
    for (const referenceStatus of ['REJECTED', 'FAILED']) {
      const { wallet, command } = await fixture();
      const bet = command('BET', referenceStatus === 'REJECTED' ? '200.00' : '25.00');
      const dependent = command('REFUND', bet.money.amount, bet.externalTransactionId);
      const pending = await service.submit(dependent, crypto.randomUUID());
      if (referenceStatus === 'REJECTED') {
        expect((await service.submit(bet, crypto.randomUUID())).status).toBe('REJECTED');
      } else {
        expect((await service.failSubmission(bet, crypto.randomUUID(), 'PERMANENT_INFRASTRUCTURE_FAILURE', {
          consumerName: 'reference-test', messageId: crypto.randomUUID(),
        })).status).toBe('FAILED');
      }
      await makeDue(pending.transactionId);
      const worker = new ReferenceWorker(orm, service);
      expect(await worker.runOnce()).toBe(1);
      await worker.stop();
      expect(await queries.getTransaction(pending.transactionId)).toMatchObject({
        status: 'REJECTED', failureCode: referenceStatus === 'REJECTED' ? 'REFERENCE_REJECTED' : 'REFERENCE_FAILED',
      });
      expect(await events(pending.transactionId, 'WagerTransactionRejected')).toBe(1);
      expect(await query('select id from dwp.wallet_ledger_entries where transaction_id = ?', [pending.transactionId])).toHaveLength(0);
      await assertConsistent(wallet.id);
    }
  });
});
