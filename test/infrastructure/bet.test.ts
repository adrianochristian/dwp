import { createWageringService } from '../../dist/infrastructure/persistence/wagering-services.js';
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import type { MikroORM } from '@mikro-orm/postgresql';
import type { WageringService } from '../../dist/application/wagering-service.js';
import { canonicalHash } from '../../dist/application/contracts.js';
import { createDatabase } from '../../dist/infrastructure/persistence/database.js';

const integration = process.env.DWP_INTEGRATION === '1' ? describe : describe.skip;

integration('persistent BET processing', () => {
  const databaseName = `dwp_bet_${crypto.randomUUID().replaceAll('-', '')}`;
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

  async function fixture(amount = '100.00') {
    const playerId = crypto.randomUUID();
    const wallet = await service.createWallet({ playerId, initialBalance: { amount, currency: 'BRL' } });
    const body = {
      providerId: 'provider-a', externalTransactionId: crypto.randomUUID(), playerId, walletId: wallet.id,
      roundId: 'round-1', gameId: 'game-1', kind: 'BET', money: { amount: '25.00', currency: 'BRL' },
    };
    return { wallet, body, key: `provider-a:${body.externalTransactionId}` };
  }

  async function assertConsistent(walletId: string): Promise<void> {
    const rows = await query<{ balance_cents: string; calculated: string }>(`select w.balance_cents::text,
      coalesce(sum(case when l.direction = 'CREDIT' then l.amount_cents::numeric else -l.amount_cents::numeric end), 0)::text as calculated
      from dwp.wallets w left join dwp.wallet_ledger_entries l on l.wallet_id = w.id
      where w.id = ? group by w.id`, [walletId]);
    expect(rows[0]!.balance_cents).toBe(rows[0]!.calculated);
    expect(BigInt(rows[0]!.balance_cents) >= 0n).toBe(true);
  }

  test('atomically debits, appends one ledger entry and enqueues the two events', async () => {
    const { wallet, body, key } = await fixture();
    const result = await service.submit(body, key);
    expect(result).toMatchObject({ status: 'PROCESSED', balance: { amount: '75.00', currency: 'BRL' }, idempotentReplay: false });
    expect((await service.getWallet(wallet.id)).version).toBe(2);
    const rows = await query<{ direction: string; amount_cents: string }>(
      'select direction, amount_cents::text from dwp.wallet_ledger_entries where transaction_id = ?', [result.transactionId],
    );
    expect(rows).toEqual([{ direction: 'DEBIT', amount_cents: '2500' }]);
    const events = await query<{ event_type: string }>("select event_type from dwp.outbox_messages where payload->'data'->>'transactionId' = ? order by event_type", [result.transactionId]);
    expect(events.map(event => event.event_type)).toEqual(['WagerTransactionProcessed', 'WalletBalanceChanged']);
    await assertConsistent(wallet.id);
  });

  test('replays the original persisted balance after later transactions and a new service instance', async () => {
    const { wallet, body, key } = await fixture();
    const original = await service.submit(body, key);
    await service.submit({ ...body, externalTransactionId: crypto.randomUUID(), money: { amount: '10.00', currency: 'BRL' } }, crypto.randomUUID());
    expect((await service.getWallet(wallet.id)).balance.amount).toBe('65.00');
    const restarted = createWageringService(orm);
    expect(await restarted.submit(body, key)).toEqual({ ...original, idempotentReplay: true });
    expect(await query("select id from dwp.wallet_ledger_entries where wallet_id = ? and direction = 'DEBIT'", [wallet.id])).toHaveLength(2);
    await assertConsistent(wallet.id);
  });

  test('rejects divergent payload under one key and external identity under another key', async () => {
    const { wallet, body, key } = await fixture();
    await service.submit(body, key);
    await expect(service.submit({ ...body, money: { amount: '26.00', currency: 'BRL' } }, key)).rejects.toMatchObject({ kind: 'conflict', code: 'IDEMPOTENCY_CONFLICT' });
    await expect(service.submit(body, `${key}:other`)).rejects.toMatchObject({ kind: 'conflict', code: 'EXTERNAL_IDENTITY_CONFLICT' });
    expect((await service.getWallet(wallet.id)).balance.amount).toBe('75.00');
    expect(await query('select id from dwp.wager_transactions where wallet_id = ? and kind = ?', [wallet.id, 'BET'])).toHaveLength(1);
    await assertConsistent(wallet.id);
  });

  test('hashes sorted business fields while excluding transport and the body idempotency key', async () => {
    const { wallet, body, key } = await fixture();
    expect(canonicalHash({ b: { z: 1, a: 2 }, a: [3, 4] })).toBe(canonicalHash({ a: [3, 4], b: { a: 2, z: 1 } }));
    const original = await service.submit({ ...body, idempotencyKey: 'ignored', transport: 'first' }, key);
    const reordered = { money: { currency: 'BRL', amount: '25.00' }, kind: body.kind, gameId: body.gameId,
      roundId: body.roundId, walletId: body.walletId, playerId: body.playerId,
      externalTransactionId: body.externalTransactionId, providerId: body.providerId, idempotencyKey: 'also-ignored', transport: 'second' };
    expect(await service.submit(reordered, key)).toEqual({ ...original, idempotentReplay: true });
    await assertConsistent(wallet.id);
  });

  test('persists terminal insufficient-funds and identity rejections without ledger or version changes', async () => {
    for (const [changes, failureCode] of [
      [{ money: { amount: '101.00', currency: 'BRL' } }, 'INSUFFICIENT_FUNDS'],
      [{ money: { amount: '1.00', currency: 'USD' } }, 'CURRENCY_MISMATCH'],
      [{ playerId: 'another-player' }, 'WALLET_PLAYER_MISMATCH'],
    ] as const) {
      const { wallet, body, key } = await fixture();
      const result = await service.submit({ ...body, ...changes }, key);
      expect(result).toMatchObject({ status: 'REJECTED', failureCode, balance: wallet.balance, idempotentReplay: false });
      expect(await service.submit({ ...body, ...changes }, key)).toEqual({ ...result, idempotentReplay: true });
      expect((await service.getWallet(wallet.id)).version).toBe(1);
      expect(await query('select id from dwp.wallet_ledger_entries where transaction_id = ?', [result.transactionId])).toHaveLength(0);
      expect(await query("select id from dwp.outbox_messages where payload->'data'->>'transactionId' = ? and event_type = 'WagerTransactionRejected'", [result.transactionId])).toHaveLength(1);
      await assertConsistent(wallet.id);
    }
  });

  test('rolls back every BET effect before commit and permits a clean retry', async () => {
    const { wallet, body, key } = await fixture();
    const failing = createWageringService(orm, { beforeCommit: () => { throw new Error('injected-BET-before-commit'); } });
    await expect(failing.submit(body, key)).rejects.toThrow('injected-BET-before-commit');
    expect((await service.getWallet(wallet.id)).balance.amount).toBe('100.00');
    expect(await query('select id from dwp.wager_transactions where idempotency_key = ?', [key])).toHaveLength(0);
    expect(await query("select id from dwp.outbox_messages where payload->'data'->>'externalTransactionId' = ?", [body.externalTransactionId])).toHaveLength(0);
    expect(await query("select id from dwp.wallet_ledger_entries where wallet_id = ? and direction = 'DEBIT'", [wallet.id])).toHaveLength(0);
    expect((await service.submit(body, key)).status).toBe('PROCESSED');
    await assertConsistent(wallet.id);
  });

  test('uses the required header, validates kind and amount, and distinguishes an unknown wallet', async () => {
    const { body } = await fixture();
    await expect(service.submit(body, undefined)).rejects.toMatchObject({ kind: 'validation' });
    await expect(service.submit({ ...body, kind: 'OPENING' }, 'invalid-opening')).rejects.toMatchObject({ kind: 'validation' });
    await expect(service.submit({ ...body, money: { amount: '0.00', currency: 'BRL' } }, 'invalid-zero')).rejects.toMatchObject({ kind: 'validation' });
    await expect(service.submit({ ...body, walletId: crypto.randomUUID() }, 'missing-wallet')).rejects.toMatchObject({ kind: 'not_found' });
  });
});
