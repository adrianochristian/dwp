import { createWageringService } from '../../dist/infrastructure/persistence/wagering-services.js';
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import type { MikroORM } from '@mikro-orm/postgresql';
import type { INestApplication } from '@nestjs/common';
import type { WageringService } from '../../dist/application/wagering-service.js';
import { createApplication } from '../../dist/application.js';
import { createDatabase } from '../../dist/infrastructure/persistence/database.js';

const integration = process.env.DWP_INTEGRATION === '1' ? describe : describe.skip;

integration('LOSS records a result without moving balance', () => {
  const databaseName = `dwp_loss_${crypto.randomUUID().replaceAll('-', '')}`;
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
    service = createWageringService(orm);
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

  async function fixture() {
    const playerId = crypto.randomUUID();
    const wallet = await service.createWallet({ playerId, initialBalance: { amount: '100.00', currency: 'BRL' } });
    const loss = {
      providerId: 'provider-a', externalTransactionId: crypto.randomUUID(), playerId, walletId: wallet.id,
      roundId: 'round-1', gameId: 'game-1', kind: 'LOSS', money: { amount: '0.00', currency: 'BRL' },
    };
    return { wallet, loss, key: `provider-a:${loss.externalTransactionId}` };
  }

  async function assertConsistent(walletId: string): Promise<void> {
    const rows = await query<{ balance_cents: string; calculated: string }>(`select w.balance_cents::text,
      coalesce(sum(case when l.direction = 'CREDIT' then l.amount_cents::numeric else -l.amount_cents::numeric end), 0)::text as calculated
      from dwp.wallets w left join dwp.wallet_ledger_entries l on l.wallet_id = w.id
      where w.id = ? group by w.id`, [walletId]);
    expect(rows[0]!.balance_cents).toBe(rows[0]!.calculated);
    expect(BigInt(rows[0]!.balance_cents) >= 0n).toBe(true);
  }

  test('accepts zero and positive amounts without ledger, balance, version or balance event changes', async () => {
    for (const amount of ['0.00', '25.00']) {
      const { wallet, loss, key } = await fixture();
      const result = await service.submit({ ...loss, money: { amount, currency: 'BRL' } }, key);
      expect(result).toMatchObject({ status: 'PROCESSED', balance: wallet.balance, idempotentReplay: false });
      expect(await service.getWallet(wallet.id)).toEqual(wallet);
      expect(await query('select id from dwp.wallet_ledger_entries where transaction_id = ?', [result.transactionId])).toHaveLength(0);
      const events = await query<{ event_type: string }>("select event_type from dwp.outbox_messages where payload->'data'->>'transactionId' = ?", [result.transactionId]);
      expect(events).toEqual([{ event_type: 'WagerTransactionProcessed' }]);
      await assertConsistent(wallet.id);
    }
  });

  test('replays the stored LOSS balance after subsequent debits', async () => {
    const { wallet, loss, key } = await fixture();
    const original = await service.submit(loss, key);
    await service.submit({ ...loss, kind: 'BET', externalTransactionId: crypto.randomUUID(), money: { amount: '10.00', currency: 'BRL' } }, crypto.randomUUID());
    expect((await service.getWallet(wallet.id)).balance.amount).toBe('90.00');
    expect(await service.submit(loss, key)).toEqual({ ...original, idempotentReplay: true });
    expect(await query("select id from dwp.outbox_messages where payload->'data'->>'transactionId' = ?", [original.transactionId])).toHaveLength(1);
    await assertConsistent(wallet.id);
  });

  test('allows WIN and LOSS to reference the same BET independently', async () => {
    const { wallet, loss, key } = await fixture();
    const reference = crypto.randomUUID();
    await service.submit({ ...loss, kind: 'BET', externalTransactionId: reference, money: { amount: '10.00', currency: 'BRL' } }, crypto.randomUUID());
    const win = await service.submit({ ...loss, kind: 'WIN', externalTransactionId: crypto.randomUUID(), referenceExternalTransactionId: reference, money: { amount: '20.00', currency: 'BRL' } }, crypto.randomUUID());
    const beforeLoss = await service.getWallet(wallet.id);
    const result = await service.submit({ ...loss, referenceExternalTransactionId: reference }, key);
    expect(win.status).toBe('PROCESSED');
    expect(result.status).toBe('PROCESSED');
    expect(result.balance.amount).toBe('110.00');
    expect(await service.getWallet(wallet.id)).toEqual(beforeLoss);
    expect(await query('select id from dwp.wallet_ledger_entries where transaction_id = ?', [result.transactionId])).toHaveLength(0);
    await assertConsistent(wallet.id);
  });

  test('waits for an absent BET reference, then records LOSS without an additional balance version', async () => {
    const { wallet, loss, key } = await fixture();
    const reference = crypto.randomUUID();
    const input = { ...loss, referenceExternalTransactionId: reference };
    const pending = await service.submit(input, key);
    expect(pending.status).toBe('PENDING_REFERENCE');
    await service.submit({ ...loss, kind: 'BET', externalTransactionId: reference, money: { amount: '10.00', currency: 'BRL' } }, crypto.randomUUID());
    const beforeLoss = await service.getWallet(wallet.id);
    const completed = await service.submit(input, key);
    expect(completed).toMatchObject({ transactionId: pending.transactionId, status: 'PROCESSED', balance: beforeLoss.balance });
    expect(await service.getWallet(wallet.id)).toEqual(beforeLoss);
    expect(await query('select id from dwp.wallet_ledger_entries where transaction_id = ?', [pending.transactionId])).toHaveLength(0);
    await assertConsistent(wallet.id);
  });

  test('persists an invalid reference rejection and rejects negative input at the contract boundary', async () => {
    const { wallet, loss, key } = await fixture();
    const reference = crypto.randomUUID();
    await service.submit({ ...loss, kind: 'BET', externalTransactionId: reference, money: { amount: '10.00', currency: 'BRL' } }, crypto.randomUUID());
    const rejected = await service.submit({ ...loss, referenceExternalTransactionId: reference, roundId: 'other-round' }, key);
    expect(rejected).toMatchObject({ status: 'REJECTED', failureCode: 'REFERENCE_MISMATCH' });
    expect(await query('select id from dwp.wallet_ledger_entries where transaction_id = ?', [rejected.transactionId])).toHaveLength(0);
    await expect(service.submit({ ...loss, money: { amount: '-0.01', currency: 'BRL' } }, crypto.randomUUID())).rejects.toMatchObject({ kind: 'validation' });
    await assertConsistent(wallet.id);
  });

  test('returns a processed zero-amount LOSS over HTTP', async () => {
    const { wallet, loss, key } = await fixture();
    const response = await fetch(`${address}/wagering/transactions`, {
      method: 'POST', headers: { 'content-type': 'application/json', 'Idempotency-Key': key }, body: JSON.stringify(loss),
    });
    expect(response.status).toBe(200);
    expect(await response.json()).toMatchObject({ status: 'PROCESSED', balance: wallet.balance });
    expect(await service.getWallet(wallet.id)).toEqual(wallet);
    await assertConsistent(wallet.id);
  });
});
