import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import type { MikroORM } from '@mikro-orm/postgresql';
import { WageringService } from '../../dist/application/wagering-service.js';
import { createDatabase } from '../../dist/infrastructure/persistence/database.js';

const integration = process.env.DWP_INTEGRATION === '1' ? describe : describe.skip;

integration('ROLLBACK inverts a processed financial transaction', () => {
  const databaseName = `dwp_rollback_${crypto.randomUUID().replaceAll('-', '')}`;
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

  async function fixture(initial = '100.00') {
    const playerId = crypto.randomUUID();
    const wallet = await service.createWallet({ playerId, initialBalance: { amount: initial, currency: 'BRL' } });
    const base = {
      providerId: 'provider-a', playerId, walletId: wallet.id,
      roundId: 'round-1', gameId: 'game-1',
    };
    const command = (kind: string, amount: string, reference?: string) => ({
      ...base, kind, externalTransactionId: crypto.randomUUID(), money: { amount, currency: 'BRL' },
      ...(reference === undefined ? {} : { referenceExternalTransactionId: reference }),
    });
    return { wallet, command };
  }

  async function assertConsistent(walletId: string): Promise<void> {
    const rows = await query<{ balance_cents: string; calculated: string }>(`select w.balance_cents::text,
      coalesce(sum(case when l.direction = 'CREDIT' then l.amount_cents::numeric else -l.amount_cents::numeric end), 0)::text as calculated
      from dwp.wallets w left join dwp.wallet_ledger_entries l on l.wallet_id = w.id
      where w.id = ? group by w.id`, [walletId]);
    expect(rows[0]!.balance_cents).toBe(rows[0]!.calculated);
    expect(BigInt(rows[0]!.balance_cents) >= 0n).toBe(true);
  }

  async function ledgerDirection(transactionId: string): Promise<string | undefined> {
    return (await query<{ direction: string }>('select direction from dwp.wallet_ledger_entries where transaction_id = ?', [transactionId]))[0]?.direction;
  }

  async function effects(walletId: string) {
    const [row] = await query<{ ledger_entries: string; balance_events: string; all_events: string }>(`select
      (select count(*)::text from dwp.wallet_ledger_entries where wallet_id = ?) as ledger_entries,
      (select count(*)::text from dwp.outbox_messages where event_type = 'WalletBalanceChanged'
        and payload->'data'->>'walletId' = ?) as balance_events,
      (select count(*)::text from dwp.outbox_messages where payload->'data'->>'walletId' = ?) as all_events`,
    [walletId, walletId, walletId]);
    return row!;
  }

  test('credits a reversed BET and keeps the original replay balance after later movement', async () => {
    const { wallet, command } = await fixture();
    const bet = command('BET', '25.00');
    await service.submit(bet, crypto.randomUUID());
    const rollback = command('ROLLBACK', '25.00', bet.externalTransactionId);
    const key = crypto.randomUUID();
    const original = await service.submit(rollback, key);
    expect(original).toMatchObject({ status: 'PROCESSED', balance: wallet.balance });
    expect(await ledgerDirection(original.transactionId)).toBe('CREDIT');
    await service.submit(command('WIN', '10.00'), crypto.randomUUID());
    expect(await service.submit(rollback, key)).toEqual({ ...original, idempotentReplay: true });
    expect((await service.getWallet(wallet.id)).balance.amount).toBe('110.00');
    await assertConsistent(wallet.id);
  });

  test('debits a reversed WIN and a reversed REFUND', async () => {
    const first = await fixture('0.00');
    const win = first.command('WIN', '50.00');
    await service.submit(win, crypto.randomUUID());
    const winRollback = await service.submit(first.command('ROLLBACK', '50.00', win.externalTransactionId), crypto.randomUUID());
    expect(winRollback).toMatchObject({ status: 'PROCESSED', balance: { amount: '0.00', currency: 'BRL' } });
    expect(await ledgerDirection(winRollback.transactionId)).toBe('DEBIT');
    await assertConsistent(first.wallet.id);

    const second = await fixture();
    const bet = second.command('BET', '25.00');
    await service.submit(bet, crypto.randomUUID());
    const refund = second.command('REFUND', '25.00', bet.externalTransactionId);
    const refunded = await service.submit(refund, crypto.randomUUID());
    expect(refunded).toMatchObject({ status: 'PROCESSED', balance: { amount: '100.00', currency: 'BRL' } });
    expect((await service.getWallet(second.wallet.id)).version).toBe(3);
    const refundRollback = await service.submit(second.command('ROLLBACK', '25.00', refund.externalTransactionId), crypto.randomUUID());
    expect(refundRollback).toMatchObject({ status: 'PROCESSED', balance: { amount: '75.00', currency: 'BRL' } });
    expect(await ledgerDirection(refundRollback.transactionId)).toBe('DEBIT');
    expect((await service.getWallet(second.wallet.id)).version).toBe(4);
    await assertConsistent(second.wallet.id);
  });

  test('persists an insufficient reversal rejection with its distinct failure code and no effects', async () => {
    const { wallet, command } = await fixture('0.00');
    const win = command('WIN', '80.00');
    await service.submit(win, crypto.randomUUID());
    await service.submit(command('BET', '75.00'), crypto.randomUUID());
    const before = await service.getWallet(wallet.id);
    const rollback = command('ROLLBACK', '80.00', win.externalTransactionId);
    const key = crypto.randomUUID();
    const rejected = await service.submit(rollback, key);
    expect(rejected).toMatchObject({ status: 'REJECTED', failureCode: 'REVERSAL_INSUFFICIENT_FUNDS', balance: before.balance });
    expect(await service.getWallet(wallet.id)).toEqual(before);
    expect(await ledgerDirection(rejected.transactionId)).toBeUndefined();
    await service.submit(command('WIN', '100.00'), crypto.randomUUID());
    expect(await service.submit(rollback, key)).toEqual({ ...rejected, idempotentReplay: true });
    await assertConsistent(wallet.id);
  });

  test('serializes two rollbacks of the same WIN into one debit without negative balance', async () => {
    const { wallet, command } = await fixture('0.00');
    const win = command('WIN', '80.00');
    await service.submit(win, crypto.randomUUID());
    const first = command('ROLLBACK', '80.00', win.externalTransactionId);
    const second = command('ROLLBACK', '80.00', win.externalTransactionId);
    const keys = [crypto.randomUUID(), crypto.randomUUID()];
    const results = await Promise.all([service.submit(first, keys[0]), service.submit(second, keys[1])]);
    expect(results.map(result => result.status).sort()).toEqual(['PROCESSED', 'REJECTED']);
    expect(results.find(result => result.status === 'REJECTED')?.failureCode).toBe('REVERSAL_ALREADY_PROCESSED');
    expect((await service.getWallet(wallet.id)).balance.amount).toBe('0.00');
    expect(await service.submit(first, keys[0])).toEqual({ ...results[0]!, idempotentReplay: true });
    expect(await service.submit(second, keys[1])).toEqual({ ...results[1]!, idempotentReplay: true });
    expect(await query("select l.id from dwp.wallet_ledger_entries l join dwp.wager_transactions t on t.id = l.transaction_id where l.wallet_id = ? and t.kind = 'ROLLBACK'", [wallet.id])).toHaveLength(1);
    await assertConsistent(wallet.id);
  });

  test('rejects ROLLBACK after REFUND of the same BET and replays the original rejection without effects', async () => {
    const { wallet, command } = await fixture();
    const bet = command('BET', '25.00');
    await service.submit(bet, crypto.randomUUID());
    const refunded = await service.submit(command('REFUND', '25.00', bet.externalTransactionId), crypto.randomUUID());
    expect(refunded).toMatchObject({ status: 'PROCESSED', balance: wallet.balance });
    const before = await service.getWallet(wallet.id);
    const initialEffects = await effects(wallet.id);
    const rollback = command('ROLLBACK', '25.00', bet.externalTransactionId);
    const key = crypto.randomUUID();
    const rejected = await service.submit(rollback, key);
    expect(rejected).toMatchObject({
      status: 'REJECTED', failureCode: 'REVERSAL_ALREADY_PROCESSED',
      balance: { amount: '100.00', currency: 'BRL' }, idempotentReplay: false,
    });
    expect(await service.getWallet(wallet.id)).toEqual(before);
    expect(await ledgerDirection(rejected.transactionId)).toBeUndefined();
    const rejectedEffects = await effects(wallet.id);
    expect(rejectedEffects.ledger_entries).toBe(initialEffects.ledger_entries);
    expect(rejectedEffects.balance_events).toBe(initialEffects.balance_events);
    expect(await query<{ event_type: string }>(`select event_type from dwp.outbox_messages
      where payload->'data'->>'transactionId' = ?`, [rejected.transactionId]))
      .toEqual([{ event_type: 'WagerTransactionRejected' }]);

    await service.submit(command('BET', '10.00'), crypto.randomUUID());
    const beforeReplay = await effects(wallet.id);
    expect(await service.submit(rollback, key)).toEqual({ ...rejected, idempotentReplay: true });
    expect((await service.getWallet(wallet.id)).balance.amount).toBe('90.00');
    expect(await effects(wallet.id)).toEqual(beforeReplay);
    await assertConsistent(wallet.id);
  });

  test('serializes REFUND versus ROLLBACK of the same BET into one credit and one persistent rejection', async () => {
    const { wallet, command } = await fixture();
    const bet = command('BET', '25.00');
    const original = await service.submit(bet, crypto.randomUUID());
    const refund = command('REFUND', '25.00', bet.externalTransactionId);
    const rollback = command('ROLLBACK', '25.00', bet.externalTransactionId);
    const keys = [crypto.randomUUID(), crypto.randomUUID()] as const;
    const results = await Promise.all([
      service.submit(refund, keys[0]), service.submit(rollback, keys[1]),
    ]);
    expect(results.map(result => result.status).sort()).toEqual(['PROCESSED', 'REJECTED']);
    const rejected = results.find(result => result.status === 'REJECTED')!;
    expect(rejected).toMatchObject({ failureCode: 'REVERSAL_ALREADY_PROCESSED', balance: wallet.balance });
    expect((await service.getWallet(wallet.id))).toMatchObject({ balance: wallet.balance, version: 3 });
    expect(await query(`select id from dwp.wager_transactions where reference_transaction_id = ?
      and kind in ('REFUND','ROLLBACK') and status = 'PROCESSED'`, [original.transactionId])).toHaveLength(1);
    expect(await query(`select id from dwp.wallet_ledger_entries where transaction_id in (?, ?)`,
      results.map(result => result.transactionId))).toHaveLength(1);
    expect(await ledgerDirection(rejected.transactionId)).toBeUndefined();
    expect(await query<{ event_type: string }>(`select event_type from dwp.outbox_messages
      where payload->'data'->>'transactionId' = ?`, [rejected.transactionId]))
      .toEqual([{ event_type: 'WagerTransactionRejected' }]);
    expect(await effects(wallet.id)).toMatchObject({ ledger_entries: '3', balance_events: '3' });

    await service.submit(command('BET', '10.00'), crypto.randomUUID());
    const beforeReplay = await effects(wallet.id);
    expect(await service.submit(refund, keys[0])).toEqual({ ...results[0]!, idempotentReplay: true });
    expect(await service.submit(rollback, keys[1])).toEqual({ ...results[1]!, idempotentReplay: true });
    expect((await service.getWallet(wallet.id)).balance.amount).toBe('90.00');
    expect(await effects(wallet.id)).toEqual(beforeReplay);
    await assertConsistent(wallet.id);
  });

  test('WIN and LOSS references do not block the first reversal of their BET', async () => {
    const { wallet, command } = await fixture();
    const bet = command('BET', '25.00');
    await service.submit(bet, crypto.randomUUID());
    const win = await service.submit(command('WIN', '10.00', bet.externalTransactionId), crypto.randomUUID());
    const loss = await service.submit(command('LOSS', '0.00', bet.externalTransactionId), crypto.randomUUID());
    expect(win.status).toBe('PROCESSED');
    expect(loss.status).toBe('PROCESSED');
    const rollback = await service.submit(command('ROLLBACK', '25.00', bet.externalTransactionId), crypto.randomUUID());
    expect(rollback).toMatchObject({ status: 'PROCESSED', balance: { amount: '110.00', currency: 'BRL' } });
    expect(await ledgerDirection(rollback.transactionId)).toBe('CREDIT');
    await assertConsistent(wallet.id);
  });

  test('rolling back a REFUND does not make its original BET eligible for another reversal', async () => {
    const { wallet, command } = await fixture();
    const bet = command('BET', '25.00');
    const betResult = await service.submit(bet, crypto.randomUUID());
    const refund = command('REFUND', '25.00', bet.externalTransactionId);
    expect((await service.submit(refund, crypto.randomUUID())).balance.amount).toBe('100.00');
    const refundRollback = await service.submit(command('ROLLBACK', '25.00', refund.externalTransactionId), crypto.randomUUID());
    expect(refundRollback).toMatchObject({ status: 'PROCESSED', balance: { amount: '75.00', currency: 'BRL' } });
    const before = await service.getWallet(wallet.id);
    const initialEffects = await effects(wallet.id);
    for (const kind of ['REFUND', 'ROLLBACK']) {
      const body = command(kind, '25.00', bet.externalTransactionId);
      const key = crypto.randomUUID();
      const rejected = await service.submit(body, key);
      expect(rejected).toMatchObject({
        status: 'REJECTED', failureCode: 'REVERSAL_ALREADY_PROCESSED', balance: before.balance,
      });
      expect(await ledgerDirection(rejected.transactionId)).toBeUndefined();
      expect(await service.submit(body, key)).toEqual({ ...rejected, idempotentReplay: true });
    }
    expect(await service.getWallet(wallet.id)).toEqual(before);
    const after = await effects(wallet.id);
    expect(after.ledger_entries).toBe(initialEffects.ledger_entries);
    expect(after.balance_events).toBe(initialEffects.balance_events);
    expect(await query(`select id from dwp.wager_transactions where reference_transaction_id = ?
      and kind in ('REFUND','ROLLBACK') and status = 'PROCESSED'`, [betResult.transactionId])).toHaveLength(1);
    await assertConsistent(wallet.id);
  });

  test('waits for an out-of-order WIN reference and resumes with the same transaction id', async () => {
    const { wallet, command } = await fixture('0.00');
    const win = command('WIN', '50.00');
    const rollback = command('ROLLBACK', '50.00', win.externalTransactionId);
    const key = crypto.randomUUID();
    const pending = await service.submit(rollback, key);
    expect(pending.status).toBe('PENDING_REFERENCE');
    expect((await service.getWallet(wallet.id)).version).toBe(1);
    await service.submit(win, crypto.randomUUID());
    const processed = await service.submit(rollback, key);
    expect(processed).toMatchObject({ transactionId: pending.transactionId, status: 'PROCESSED', balance: wallet.balance });
    expect(await ledgerDirection(processed.transactionId)).toBe('DEBIT');
    await assertConsistent(wallet.id);
  });

  test('rejects partial amounts, mismatched references and unsupported reference kinds', async () => {
    const { wallet, command } = await fixture();
    const bet = command('BET', '25.00');
    await service.submit(bet, crypto.randomUUID());
    const loss = command('LOSS', '0.00');
    await service.submit(loss, crypto.randomUUID());
    for (const [body, code] of [
      [command('ROLLBACK', '24.00', bet.externalTransactionId), 'REFERENCE_AMOUNT_MISMATCH'],
      [{ ...command('ROLLBACK', '25.00', bet.externalTransactionId), roundId: 'other-round' }, 'REFERENCE_MISMATCH'],
      [command('ROLLBACK', '1.00', loss.externalTransactionId), 'INVALID_REFERENCE_KIND'],
    ] as const) {
      const rejected = await service.submit(body, crypto.randomUUID());
      expect(rejected).toMatchObject({ status: 'REJECTED', failureCode: code });
      expect(await ledgerDirection(rejected.transactionId)).toBeUndefined();
    }
    const { referenceExternalTransactionId: _reference, ...missing } = command('ROLLBACK', '1.00', bet.externalTransactionId);
    await expect(service.submit(missing, crypto.randomUUID())).rejects.toMatchObject({ status: 400 });
    await assertConsistent(wallet.id);
  });
});
