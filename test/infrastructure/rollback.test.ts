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
    await service.submit(refund, crypto.randomUUID());
    const refundRollback = await service.submit(second.command('ROLLBACK', '25.00', refund.externalTransactionId), crypto.randomUUID());
    expect(refundRollback).toMatchObject({ status: 'PROCESSED', balance: { amount: '75.00', currency: 'BRL' } });
    expect(await ledgerDirection(refundRollback.transactionId)).toBe('DEBIT');
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

  test('allows one REFUND and one ROLLBACK to reverse the same BET by their distinct kinds', async () => {
    const { wallet, command } = await fixture();
    const bet = command('BET', '25.00');
    await service.submit(bet, crypto.randomUUID());
    const results = await Promise.all([
      service.submit(command('REFUND', '25.00', bet.externalTransactionId), crypto.randomUUID()),
      service.submit(command('ROLLBACK', '25.00', bet.externalTransactionId), crypto.randomUUID()),
    ]);
    expect(results.every(result => result.status === 'PROCESSED')).toBe(true);
    expect((await service.getWallet(wallet.id)).balance.amount).toBe('125.00');
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
