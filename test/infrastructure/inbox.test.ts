import { createWageringService } from '../../dist/infrastructure/persistence/wagering-services.js';
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import type { MikroORM } from '@mikro-orm/postgresql';
import type { WageringService } from '../../dist/application/wagering-service.js';
import { createDatabase } from '../../dist/infrastructure/persistence/database.js';

const integration = process.env.DWP_INTEGRATION === '1' ? describe : describe.skip;

integration('inbox participates in the financial SQL transaction', () => {
  const databaseName = `dwp_inbox_${crypto.randomUUID().replaceAll('-', '')}`;
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
    service = createWageringService(orm);
  });

  afterAll(async () => {
    await orm?.close(true);
    if (admin) {
      await admin.em.getConnection().execute(`drop database if exists ${databaseName} with (force)`);
      await admin.close(true);
    }
  });

  const query = <T>(sql: string, values: unknown[] = []) => orm.em.getConnection().execute<T[]>(sql, values);

  async function fixture(amount = '10.00') {
    const playerId = crypto.randomUUID();
    const wallet = await service.createWallet({ playerId, initialBalance: { amount: '100.00', currency: 'BRL' } });
    const body = {
      providerId: 'inbox-provider', externalTransactionId: crypto.randomUUID(), playerId, walletId: wallet.id,
      roundId: 'inbox-round', gameId: 'inbox-game', kind: 'BET', money: { amount, currency: 'BRL' },
    };
    const context = { consumerName: 'inbox-integration', messageId: crypto.randomUUID() };
    return { wallet, body, context, key: `inbox:${body.externalTransactionId}` };
  }

  async function assertConsistent(walletId: string): Promise<void> {
    const rows = await query<{ balance_cents: string; calculated: string }>(`select w.balance_cents::text,
      coalesce(sum(case when l.direction = 'CREDIT' then l.amount_cents::numeric else -l.amount_cents::numeric end), 0)::text as calculated
      from dwp.wallets w left join dwp.wallet_ledger_entries l on l.wallet_id = w.id
      where w.id = ? group by w.id`, [walletId]);
    expect(rows[0]!.balance_cents).toBe(rows[0]!.calculated);
    expect(BigInt(rows[0]!.balance_cents) >= 0n).toBe(true);
  }

  test('deduplicates concurrent logical message identities and rejects a divergent payload or key', async () => {
    const { wallet, body, context, key } = await fixture();
    const results = await Promise.all(Array.from({ length: 10 }, () => service.submit(body, key, context)));
    expect(new Set(results.map(result => result.transactionId)).size).toBe(1);
    expect(results.filter(result => !result.idempotentReplay)).toHaveLength(1);
    const inbox = await query<{ result: unknown }>('select result from dwp.inbox_messages where consumer_name = ? and message_id = ? and processed_at is not null', [context.consumerName, context.messageId]);
    expect(inbox).toHaveLength(1);
    await expect(service.submit({ ...body, money: { amount: '11.00', currency: 'BRL' } }, key, context)).rejects.toMatchObject({ kind: 'conflict', code: 'INBOX_MESSAGE_CONFLICT' });
    await expect(service.submit(body, `${key}:other`, context)).rejects.toMatchObject({ kind: 'conflict', code: 'INBOX_MESSAGE_CONFLICT' });
    expect((await service.getWallet(wallet.id)).balance.amount).toBe('90.00');
    expect(await query('select id from dwp.wallet_ledger_entries where transaction_id = ?', [results[0]!.transactionId])).toHaveLength(1);
    await assertConsistent(wallet.id);
  });

  test('rolls back inbox, wager, debit and outbox together when failure occurs before commit', async () => {
    const { wallet, body, context, key } = await fixture();
    const failing = createWageringService(orm, { beforeCommit: () => { throw new Error('injected-inbox-rollback'); } });
    await expect(failing.submit(body, key, context)).rejects.toThrow('injected-inbox-rollback');
    expect(await query('select message_id from dwp.inbox_messages where consumer_name = ? and message_id = ?', [context.consumerName, context.messageId])).toHaveLength(0);
    expect(await query('select id from dwp.wager_transactions where idempotency_key = ?', [key])).toHaveLength(0);
    expect(await service.getWallet(wallet.id)).toEqual(wallet);
    expect(await query("select id from dwp.outbox_messages where payload->'data'->>'externalTransactionId' = ?", [body.externalTransactionId])).toHaveLength(0);
    expect((await service.submit(body, key, context)).status).toBe('PROCESSED');
    await assertConsistent(wallet.id);
  });

  test('records financial replays in the inbox atomically without repeating financial effects', async () => {
    const { wallet, body, context, key } = await fixture();
    const original = await service.submit(body, key);
    const failing = createWageringService(orm, { beforeCommit: () => { throw new Error('injected-replay-rollback'); } });
    await expect(failing.submit(body, key, context)).rejects.toThrow('injected-replay-rollback');
    expect(await query('select message_id from dwp.inbox_messages where consumer_name = ? and message_id = ?', [context.consumerName, context.messageId])).toHaveLength(0);
    expect(await service.submit(body, key, context)).toEqual({ ...original, idempotentReplay: true });
    expect(await query('select message_id from dwp.inbox_messages where consumer_name = ? and message_id = ? and processed_at is not null', [context.consumerName, context.messageId])).toHaveLength(1);
    expect(await query('select id from dwp.wallet_ledger_entries where transaction_id = ?', [original.transactionId])).toHaveLength(1);
    await assertConsistent(wallet.id);
  });

  test('commits terminal business rejections and accepted pending references with a processed inbox', async () => {
    const rejected = await fixture('101.00');
    const rejection = await service.submit(rejected.body, rejected.key, rejected.context);
    expect(rejection).toMatchObject({ status: 'REJECTED', failureCode: 'INSUFFICIENT_FUNDS' });
    expect(await service.submit(rejected.body, rejected.key, rejected.context)).toEqual({ ...rejection, idempotentReplay: true });
    expect(await query('select message_id from dwp.inbox_messages where consumer_name = ? and message_id = ? and processed_at is not null', [rejected.context.consumerName, rejected.context.messageId])).toHaveLength(1);
    await assertConsistent(rejected.wallet.id);

    const pending = await fixture();
    const bet = { ...pending.body, externalTransactionId: crypto.randomUUID() };
    const input = { ...pending.body, kind: 'REFUND', referenceExternalTransactionId: bet.externalTransactionId };
    const acceptance = await service.submit(input, pending.key, pending.context);
    expect(acceptance.status).toBe('PENDING_REFERENCE');
    expect(await query('select message_id from dwp.inbox_messages where consumer_name = ? and message_id = ? and processed_at is not null', [pending.context.consumerName, pending.context.messageId])).toHaveLength(1);
    await service.submit(bet, crypto.randomUUID());
    expect(await service.submit(input, pending.key, pending.context)).toEqual({ ...acceptance, idempotentReplay: true });
    const completed = await service.submit(input, pending.key);
    expect(completed).toMatchObject({ transactionId: acceptance.transactionId, status: 'PROCESSED', balance: pending.wallet.balance });
    await assertConsistent(pending.wallet.id);
  });
});
