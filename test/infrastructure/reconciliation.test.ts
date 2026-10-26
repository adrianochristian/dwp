import { createWageringQueries, createWageringService } from '../../dist/infrastructure/persistence/wagering-services.js';
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import type { MikroORM } from '@mikro-orm/postgresql';
import type { WageringService } from '../../dist/application/wagering-service.js';
import type { WageringQueries } from '../../dist/application/wagering-queries.js';
import { Money } from '../../dist/domain/money.js';
import { createDatabase } from '../../dist/infrastructure/persistence/database.js';

const integration = process.env.DWP_INTEGRATION === '1' ? describe : describe.skip;

integration('consistent financial reconciliation', () => {
  const databaseName = `dwp_reconcile_${crypto.randomUUID().replaceAll('-', '')}`;
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
    service = createWageringService(orm);
    queries = createWageringQueries(orm);
  });

  afterAll(async () => {
    await orm?.close(true);
    if (admin) {
      await admin.em.getConnection().execute(`drop database if exists ${databaseName} with (force)`);
      await admin.close(true);
    }
  });

  test('reconstructs zero and amounts beyond floating-point integer precision exactly', async () => {
    for (const amount of ['0.00', '900719925474099.31']) {
      const wallet = await service.createWallet({
        playerId: crypto.randomUUID(), initialBalance: { amount, currency: 'BRL' },
      });
      expect(await queries.reconcile(wallet.id)).toEqual({
        walletId: wallet.id,
        storedBalance: { amount, currency: 'BRL' },
        calculatedBalance: { amount, currency: 'BRL' },
        difference: { amount: '0.00', currency: 'BRL' },
        consistent: true,
        checkedEntries: amount === '0.00' ? 0 : 1,
      });
    }
  });

  test('uses one consistent snapshot while one hundred concurrent bets commit', async () => {
    const wallet = await service.createWallet({
      playerId: crypto.randomUUID(), initialBalance: { amount: '1000.00', currency: 'BRL' },
    });
    let completed = 0;
    const writers = Promise.all(Array.from({ length: 10 }, async () => {
      for (let index = 0; index < 10; index += 1) {
        const externalTransactionId = crypto.randomUUID();
        const result = await service.submit({
          providerId: 'reconciliation-provider', externalTransactionId,
          walletId: wallet.id, playerId: wallet.playerId, roundId: 'round', gameId: 'game',
          kind: 'BET', money: { amount: '1.00', currency: 'BRL' },
        }, `reconciliation:${externalTransactionId}`);
        expect(result.status).toBe('PROCESSED');
        completed += 1;
      }
    }));
    const snapshots: { checkedEntries: number; completedAtRead: number }[] = [];
    try {
      for (let index = 0; index < 10; index += 1) {
        const snapshot = await queries.reconcile(wallet.id);
        snapshots.push({ checkedEntries: snapshot.checkedEntries, completedAtRead: completed });
        expect(snapshot.consistent).toBe(true);
        expect(snapshot.difference).toEqual({ amount: '0.00', currency: 'BRL' });
        expect(snapshot.storedBalance).toEqual(snapshot.calculatedBalance);
        const expected = Money.fromMinorUnits(100_000n - BigInt(snapshot.checkedEntries - 1) * 100n, 'BRL');
        expect(snapshot.storedBalance).toEqual(expected.toJSON());
      }
    } finally {
      await writers;
    }
    expect(snapshots.some(snapshot => snapshot.checkedEntries > 1 && snapshot.checkedEntries < 101)).toBe(true);
    expect(snapshots.some(snapshot => snapshot.completedAtRead < 100)).toBe(true);
    expect(await queries.reconcile(wallet.id)).toEqual({
      walletId: wallet.id,
      storedBalance: { amount: '900.00', currency: 'BRL' },
      calculatedBalance: { amount: '900.00', currency: 'BRL' },
      difference: { amount: '0.00', currency: 'BRL' },
      consistent: true,
      checkedEntries: 101,
    });
  }, 20_000);

  test('reports signed discrepancies without changing balances or ledger entries', async () => {
    const wallet = await service.createWallet({
      playerId: crypto.randomUUID(), initialBalance: { amount: '10.00', currency: 'BRL' },
    });
    // Administrative fault injection is confined to this disposable database.
    // SET LOCAL restores normal trigger execution at the end of each transaction.
    const corruptMaterializedBalance = (cents: string) => orm.em.fork().transactional(async em => {
      await em.execute('set local session_replication_role = replica');
      await em.execute('update dwp.wallets set balance_cents = ? where id = ?', [cents, wallet.id]);
    });
    try {
      for (const [cents, amount, difference] of [['1001', '10.01', '0.01'], ['999', '9.99', '-0.01']] as const) {
        await corruptMaterializedBalance(cents);
        expect(await queries.reconcile(wallet.id)).toEqual({
          walletId: wallet.id,
          storedBalance: { amount, currency: 'BRL' },
          calculatedBalance: { amount: '10.00', currency: 'BRL' },
          difference: { amount: difference, currency: 'BRL' },
          consistent: false,
          checkedEntries: 1,
        });
        expect((await service.getWallet(wallet.id)).balance.amount).toBe(amount);
        const entries = await queries.ledger(wallet.id);
        expect(entries.entries).toHaveLength(1);
        expect(entries.entries[0]?.balanceAfter.amount).toBe('10.00');
        await expect(orm.em.fork().execute('update dwp.wallets set balance_cents = 1 where id = ?', [wallet.id])).rejects.toThrow();
      }
    } finally {
      await corruptMaterializedBalance('1000');
    }
    expect((await queries.reconcile(wallet.id)).consistent).toBe(true);
  });

  test('distinguishes malformed wallet identities from absent wallets', async () => {
    await expect(queries.reconcile('invalid')).rejects.toMatchObject({ kind: 'validation' });
    await expect(queries.reconcile(crypto.randomUUID())).rejects.toMatchObject({ kind: 'not_found' });
  });
});
