import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import type { MikroORM } from '@mikro-orm/postgresql';
import { WageringService } from '../../dist/application/wagering-service.js';
import { createDatabase } from '../../dist/infrastructure/persistence/database.js';

const integration = process.env.DWP_INTEGRATION === '1' ? describe : describe.skip;

integration('permanent infrastructure failures remain auditably terminal', () => {
  const databaseName = `dwp_failed_${crypto.randomUUID().replaceAll('-', '')}`;
  let admin: MikroORM;
  let orm: MikroORM;
  let service: WageringService;

  beforeAll(async () => {
    admin = await createDatabase();
    await admin.em.getConnection().execute(`create database ${databaseName}`);
    const url = new URL(process.env.DATABASE_URL ?? 'postgresql://dwp:dwp@127.0.0.1:5432/dwp');
    url.pathname = `/${databaseName}`;
    orm = await createDatabase(url.toString());
    await orm.migrator.up();
    service = new WageringService(orm);
  });

  afterAll(async () => {
    await orm?.close(true);
    if (admin) {
      await admin.em.getConnection().execute(`drop database if exists ${databaseName} with (force)`);
      await admin.close(true);
    }
  });

  const query = <T>(sql: string, values: unknown[] = []) => orm.em.getConnection().execute<T[]>(sql, values);

  async function fixture() {
    const playerId = crypto.randomUUID();
    const wallet = await service.createWallet({ playerId, initialBalance: { amount: '100.00', currency: 'BRL' } });
    const body = {
      providerId: 'failed-provider', externalTransactionId: crypto.randomUUID(), playerId, walletId: wallet.id,
      roundId: 'failed-round', gameId: 'failed-game', kind: 'BET', money: { amount: '10.00', currency: 'BRL' },
    };
    const context = { consumerName: 'failed-integration', messageId: crypto.randomUUID() };
    return { wallet, body, context, key: `failed:${body.externalTransactionId}` };
  }

  async function assertConsistent(walletId: string): Promise<void> {
    const rows = await query<{ balance_cents: string; calculated: string }>(`select w.balance_cents::text,
      coalesce(sum(case when l.direction = 'CREDIT' then l.amount_cents::numeric else -l.amount_cents::numeric end), 0)::text as calculated
      from dwp.wallets w left join dwp.wallet_ledger_entries l on l.wallet_id = w.id
      where w.id = ? group by w.id`, [walletId]);
    expect(rows[0]!.balance_cents).toBe(rows[0]!.calculated);
    expect(BigInt(rows[0]!.balance_cents) >= 0n).toBe(true);
  }

  test('records FAILED, its snapshot, inbox and a concrete failure event without financial effects', async () => {
    const { wallet, body, context, key } = await fixture();
    const result = await service.failSubmission(body, key, 'RETRY_EXHAUSTED', context);
    expect(result).toMatchObject({ status: 'FAILED', failureCode: 'RETRY_EXHAUSTED', balance: wallet.balance, idempotentReplay: false });
    expect(await service.getWallet(wallet.id)).toEqual(wallet);
    expect(await query('select id from dwp.wallet_ledger_entries where transaction_id = ?', [result.transactionId])).toHaveLength(0);
    const events = await query<{ event_type: string }>("select event_type from dwp.outbox_messages where payload->'data'->>'transactionId' = ?", [result.transactionId]);
    expect(events).toEqual([{ event_type: 'WagerTransactionFailed' }]);
    expect(await query('select message_id from dwp.inbox_messages where consumer_name = ? and message_id = ? and processed_at is not null', [context.consumerName, context.messageId])).toHaveLength(1);
    expect(await service.submit(body, key, context)).toEqual({ ...result, idempotentReplay: true });
    expect(await service.failSubmission(body, key, 'OTHER_FAILURE', context)).toEqual({ ...result, idempotentReplay: true });
    expect(await service.submit(body, key)).toEqual({ ...result, idempotentReplay: true });
    await assertConsistent(wallet.id);
  });

  test('preserves an already processed transaction when acknowledgement infrastructure fails later', async () => {
    const { wallet, body, context, key } = await fixture();
    const original = await service.submit(body, key);
    expect(await service.failSubmission(body, key, 'ACKNOWLEDGEMENT_FAILURE', context)).toEqual({ ...original, idempotentReplay: true });
    expect((await service.getWallet(wallet.id)).balance.amount).toBe('90.00');
    expect(await query("select id from dwp.outbox_messages where event_type = 'WagerTransactionFailed' and payload->'data'->>'transactionId' = ?", [original.transactionId])).toHaveLength(0);
    expect(await query('select id from dwp.wallet_ledger_entries where transaction_id = ?', [original.transactionId])).toHaveLength(1);
    expect(await query('select message_id from dwp.inbox_messages where consumer_name = ? and message_id = ? and processed_at is not null', [context.consumerName, context.messageId])).toHaveLength(1);
    await assertConsistent(wallet.id);
  });

  test('fails a pending reference without applying its now-available financial effect', async () => {
    const { wallet, body, context, key } = await fixture();
    const bet = { ...body, externalTransactionId: crypto.randomUUID() };
    const refund = { ...body, kind: 'REFUND', referenceExternalTransactionId: bet.externalTransactionId };
    const pending = await service.submit(refund, key);
    expect(pending.status).toBe('PENDING_REFERENCE');
    await service.submit(bet, crypto.randomUUID());
    const beforeFailure = await service.getWallet(wallet.id);
    const failed = await service.failSubmission(refund, key, 'PERMANENT_DEPENDENCY_FAILURE', context);
    expect(failed).toMatchObject({ transactionId: pending.transactionId, status: 'FAILED', failureCode: 'PERMANENT_DEPENDENCY_FAILURE', balance: beforeFailure.balance });
    expect(await service.getWallet(wallet.id)).toEqual(beforeFailure);
    expect(await query('select id from dwp.wallet_ledger_entries where transaction_id = ?', [pending.transactionId])).toHaveLength(0);
    await assertConsistent(wallet.id);
  });

  test('rolls back FAILED and inbox auditing atomically when the precommit hook fails', async () => {
    const { wallet, body, context, key } = await fixture();
    const failing = new WageringService(orm, { beforeCommit: () => { throw new Error('injected-failure-audit-rollback'); } });
    await expect(failing.failSubmission(body, key, 'RETRY_EXHAUSTED', context)).rejects.toThrow('injected-failure-audit-rollback');
    expect(await query('select id from dwp.wager_transactions where idempotency_key = ?', [key])).toHaveLength(0);
    expect(await query('select message_id from dwp.inbox_messages where consumer_name = ? and message_id = ?', [context.consumerName, context.messageId])).toHaveLength(0);
    expect(await service.getWallet(wallet.id)).toEqual(wallet);
    expect(await query("select id from dwp.outbox_messages where payload->'data'->>'externalTransactionId' = ?", [body.externalTransactionId])).toHaveLength(0);
    await assertConsistent(wallet.id);
  });

  test('does not invent a FAILED transaction for invalid contracts or invalid failure identifiers', async () => {
    const { wallet, body, context, key } = await fixture();
    await expect(service.failSubmission({ ...body, kind: 'OPENING' }, key, 'RETRY_EXHAUSTED', context)).rejects.toMatchObject({ status: 400 });
    await expect(service.failSubmission(body, key, 'invalid code', context)).rejects.toMatchObject({ status: 400 });
    await expect(service.failSubmission({ ...body, money: { amount: 10, currency: 'BRL' } }, key, 'RETRY_EXHAUSTED', context)).rejects.toMatchObject({ status: 400 });
    expect(await query('select id from dwp.wager_transactions where idempotency_key = ?', [key])).toHaveLength(0);
    expect(await query('select message_id from dwp.inbox_messages where consumer_name = ? and message_id = ?', [context.consumerName, context.messageId])).toHaveLength(0);
    await assertConsistent(wallet.id);
  });
});
