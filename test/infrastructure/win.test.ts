import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import type { MikroORM } from '@mikro-orm/postgresql';
import type { INestApplication } from '@nestjs/common';
import { WageringService } from '../../dist/application/wagering-service.js';
import { createApplication } from '../../dist/application.js';
import { createDatabase } from '../../dist/infrastructure/persistence/database.js';
import { canonicalHash, wagerPayload, wagerRequest } from '../../dist/application/contracts.js';

const integration = process.env.DWP_INTEGRATION === '1' ? describe : describe.skip;

integration('WIN credits and optional BET references', () => {
  const databaseName = `dwp_win_${crypto.randomUUID().replaceAll('-', '')}`;
  let admin: MikroORM;
  let orm: MikroORM;
  let service: WageringService;
  let app: INestApplication;
  let address: string;

  beforeAll(async () => {
    admin = await createDatabase();
    await admin.em.getConnection().execute(`create database ${databaseName}`);
    const url = new URL(process.env.DATABASE_URL ?? 'postgresql://dwp:dwp@127.0.0.1:5432/dwp');
    url.pathname = `/${databaseName}`;
    orm = await createDatabase(url.toString());
    await orm.migrator.up();
    service = new WageringService(orm);
    app = await createApplication(orm);
    await app.listen(0, '127.0.0.1');
    address = await app.getUrl();
  });

  afterAll(async () => {
    await app?.close();
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
    const win = {
      providerId: 'provider-a', externalTransactionId: crypto.randomUUID(), playerId, walletId: wallet.id,
      roundId: 'round-1', gameId: 'game-1', kind: 'WIN', money: { amount: '50.00', currency: 'BRL' },
    };
    return { wallet, win, key: `provider-a:${win.externalTransactionId}` };
  }

  async function assertConsistent(walletId: string): Promise<void> {
    const rows = await query<{ balance_cents: string; calculated: string }>(`select w.balance_cents::text,
      coalesce(sum(case when l.direction = 'CREDIT' then l.amount_cents::numeric else -l.amount_cents::numeric end), 0)::text as calculated
      from dwp.wallets w left join dwp.wallet_ledger_entries l on l.wallet_id = w.id
      where w.id = ? group by w.id`, [walletId]);
    expect(rows[0]!.balance_cents).toBe(rows[0]!.calculated);
    expect(BigInt(rows[0]!.balance_cents) >= 0n).toBe(true);
  }

  test('credits a WIN without a reference and keeps the original replay response', async () => {
    const { wallet, win, key } = await fixture('0.00');
    const original = await service.submit(win, key);
    expect(original).toMatchObject({ status: 'PROCESSED', balance: { amount: '50.00', currency: 'BRL' } });
    expect((await service.getWallet(wallet.id)).version).toBe(2);
    await service.submit({ ...win, externalTransactionId: crypto.randomUUID(), money: { amount: '5.00', currency: 'BRL' } }, crypto.randomUUID());
    expect(await service.submit(win, key)).toEqual({ ...original, idempotentReplay: true });
    expect(await query('select id from dwp.wallet_ledger_entries where transaction_id = ?', [original.transactionId])).toHaveLength(1);
    await assertConsistent(wallet.id);
  });

  test('validates an optional BET reference while allowing a different WIN amount', async () => {
    const { wallet, win, key } = await fixture();
    const betExternal = crypto.randomUUID();
    const bet = await service.submit({ ...win, kind: 'BET', externalTransactionId: betExternal, money: { amount: '25.00', currency: 'BRL' } }, crypto.randomUUID());
    const result = await service.submit({ ...win, referenceExternalTransactionId: betExternal }, key);
    expect(result.balance.amount).toBe('125.00');
    expect(result.status).toBe('PROCESSED');
    expect((await query<{ reference_transaction_id: string }>('select reference_transaction_id from dwp.wager_transactions where id = ?', [result.transactionId]))[0]!.reference_transaction_id).toBe(bet.transactionId);
    await assertConsistent(wallet.id);
  });

  test('persists an absent reference once, then processes the same identity after the BET arrives', async () => {
    const { wallet, win, key } = await fixture();
    const reference = crypto.randomUUID();
    const input = { ...win, referenceExternalTransactionId: reference };
    const pending = await service.submit(input, key);
    expect(pending.status).toBe('PENDING_REFERENCE');
    const created = (await query<{ created_at: string }>('select created_at from dwp.wager_transactions where id = ?', [pending.transactionId]))[0]!.created_at;
    expect((await service.submit(input, key)).status).toBe('PENDING_REFERENCE');
    expect(await query("select id from dwp.outbox_messages where event_type = 'WagerTransactionPendingReference' and payload->'data'->>'transactionId' = ?", [pending.transactionId])).toHaveLength(1);
    expect(await query('select id from dwp.wallet_ledger_entries where transaction_id = ?', [pending.transactionId])).toHaveLength(0);
    expect((await service.getWallet(wallet.id)).version).toBe(1);
    await service.submit({ ...win, kind: 'BET', externalTransactionId: reference, money: { amount: '25.00', currency: 'BRL' } }, crypto.randomUUID());
    const completed = await service.submit(input, key);
    expect(completed).toMatchObject({ transactionId: pending.transactionId, status: 'PROCESSED', balance: { amount: '125.00', currency: 'BRL' } });
    expect((await query<{ created_at: string }>('select created_at from dwp.wager_transactions where id = ?', [pending.transactionId]))[0]!.created_at).toBe(created);
    expect(await service.submit(input, key)).toEqual({ ...completed, idempotentReplay: true });
    await assertConsistent(wallet.id);
  });

  test('distinguishes rejected references, invalid reference kinds and cross-round references', async () => {
    const { wallet, win } = await fixture();
    const rejectedExternal = crypto.randomUUID();
    await service.submit({ ...win, kind: 'BET', externalTransactionId: rejectedExternal, money: { amount: '101.00', currency: 'BRL' } }, crypto.randomUUID());
    const priorWin = await service.submit(win, crypto.randomUUID());
    expect(priorWin.status).toBe('PROCESSED');
    const betExternal = crypto.randomUUID();
    await service.submit({ ...win, kind: 'BET', externalTransactionId: betExternal, money: { amount: '1.00', currency: 'BRL' } }, crypto.randomUUID());
    for (const [reference, changes, code] of [
      [rejectedExternal, {}, 'REFERENCE_REJECTED'],
      [win.externalTransactionId, {}, 'INVALID_REFERENCE_KIND'],
      [betExternal, { roundId: 'other-round' }, 'REFERENCE_MISMATCH'],
    ] as const) {
      const result = await service.submit({ ...win, externalTransactionId: crypto.randomUUID(), referenceExternalTransactionId: reference, ...changes }, crypto.randomUUID());
      expect(result).toMatchObject({ status: 'REJECTED', failureCode: code });
      expect(await query('select id from dwp.wallet_ledger_entries where transaction_id = ?', [result.transactionId])).toHaveLength(0);
    }
    await assertConsistent(wallet.id);
  });

  test('waits for a persisted pending BET and distinguishes a permanently failed reference', async () => {
    const { wallet, win } = await fixture();
    const pendingExternal = crypto.randomUUID();
    const betBody = { ...win, kind: 'BET', externalTransactionId: pendingExternal, money: { amount: '10.00', currency: 'BRL' } };
    const betKey = crypto.randomUUID();
    const betId = crypto.randomUUID();
    await query(`insert into dwp.wager_transactions
      (id, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id, player_id,
       round_id, game_id, kind, amount_cents, currency, status)
      values (?, ?, ?, ?, ?, ?, ?, ?, ?, 'BET', '1000', 'BRL', 'PENDING')`, [
      betId, betBody.providerId, pendingExternal, betKey, canonicalHash(wagerPayload(wagerRequest(betBody))),
      wallet.id, betBody.playerId, betBody.roundId, betBody.gameId,
    ]);
    const input = { ...win, referenceExternalTransactionId: pendingExternal };
    const winKey = crypto.randomUUID();
    expect((await service.submit(input, winKey)).status).toBe('PENDING_REFERENCE');
    expect((await service.submit(betBody, betKey)).transactionId).toBe(betId);
    expect((await service.submit(input, winKey)).status).toBe('PROCESSED');

    const failedExternal = crypto.randomUUID();
    await query(`insert into dwp.wager_transactions
      (id, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id, player_id,
       round_id, game_id, kind, amount_cents, currency, status, failure_code, result)
      values (?, ?, ?, ?, ?, ?, ?, ?, ?, 'BET', '1000', 'BRL', 'FAILED', 'PERMANENT_INFRASTRUCTURE', '{}')`, [
      crypto.randomUUID(), win.providerId, failedExternal, crypto.randomUUID(), 'a'.repeat(64),
      wallet.id, win.playerId, win.roundId, win.gameId,
    ]);
    const failed = await service.submit({ ...win, externalTransactionId: crypto.randomUUID(), referenceExternalTransactionId: failedExternal }, crypto.randomUUID());
    expect(failed).toMatchObject({ status: 'REJECTED', failureCode: 'REFERENCE_FAILED' });
    await assertConsistent(wallet.id);
  });

  test('resolves references only inside the same provider and rejects cross-wallet references', async () => {
    const first = await fixture();
    const second = await fixture();
    const betExternal = crypto.randomUUID();
    await service.submit({ ...first.win, kind: 'BET', externalTransactionId: betExternal, money: { amount: '1.00', currency: 'BRL' } }, crypto.randomUUID());
    const mismatch = await service.submit({ ...second.win, referenceExternalTransactionId: betExternal }, second.key);
    expect(mismatch).toMatchObject({ status: 'REJECTED', failureCode: 'REFERENCE_MISMATCH' });
    const absent = await service.submit({ ...first.win, providerId: 'other-provider', referenceExternalTransactionId: betExternal }, first.key);
    expect(absent.status).toBe('PENDING_REFERENCE');
    await assertConsistent(first.wallet.id);
    await assertConsistent(second.wallet.id);
  });

  test('rejects a credit that would overflow BIGINT without changing the balance', async () => {
    const { wallet, win, key } = await fixture('92233720368547758.07');
    const result = await service.submit({ ...win, money: { amount: '0.01', currency: 'BRL' } }, key);
    expect(result).toMatchObject({ status: 'REJECTED', failureCode: 'MONEY_OVERFLOW', balance: wallet.balance });
    expect((await service.getWallet(wallet.id)).version).toBe(1);
    await assertConsistent(wallet.id);
  });

  test('distinguishes processed, pending, rejected, conflicting and invalid requests over HTTP', async () => {
    const { wallet, win, key } = await fixture();
    const post = (body: unknown, header = key) => fetch(`${address}/wagering/transactions`, {
      method: 'POST', headers: { 'content-type': 'application/json', 'Idempotency-Key': header }, body: JSON.stringify(body),
    });
    expect((await post(win)).status).toBe(200);
    expect((await post({ ...win, money: { amount: '51.00', currency: 'BRL' } })).status).toBe(409);
    const pending = { ...win, externalTransactionId: crypto.randomUUID(), referenceExternalTransactionId: crypto.randomUUID() };
    expect((await post(pending, crypto.randomUUID())).status).toBe(202);
    const rejected = { ...win, externalTransactionId: crypto.randomUUID(), playerId: 'wrong-player' };
    expect((await post(rejected, crypto.randomUUID())).status).toBe(422);
    expect((await post({ ...win, referenceExternalTransactionId: win.externalTransactionId }, crypto.randomUUID())).status).toBe(400);
    await assertConsistent(wallet.id);
  });
});
