import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { MikroORM } from '@mikro-orm/postgresql';
import { Migrator } from '@mikro-orm/migrations';
import { WageringService } from '../../dist/application/wagering-service.js';
import { createDatabase } from '../../dist/infrastructure/persistence/database.js';
import { Migration20261025000100 } from '../../dist/infrastructure/persistence/migrations/Migration20261025000100.js';
import { Migration20261025000200 } from '../../dist/infrastructure/persistence/migrations/WageringSchema.js';
import { Migration20261025000300 } from '../../dist/infrastructure/persistence/migrations/Migration20261025000300.js';
import { WalletRecordSchema } from '../../dist/infrastructure/persistence/records.js';
import { createApplication } from '../../dist/application.js';
import type { INestApplication } from '@nestjs/common';

const integration = process.env.DWP_INTEGRATION === '1' ? describe : describe.skip;

integration('wallet application transactions', () => {
  const databaseName = `dwp_wallet_${crypto.randomUUID().replaceAll('-', '')}`;
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
    orm = await MikroORM.init({
      clientUrl: url.toString(), entities: [WalletRecordSchema], extensions: [Migrator],
      migrations: {
        migrationsList: [Migration20261025000100, Migration20261025000200, Migration20261025000300],
        snapshot: false, snapshotOnMigrate: false,
      },
    });
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

  async function assertConsistent(walletId: string): Promise<void> {
    const rows = await query<{ balance_cents: string; calculated: string }>(`select w.balance_cents::text,
      coalesce(sum(case when l.direction = 'CREDIT' then l.amount_cents::numeric else -l.amount_cents::numeric end), 0)::text as calculated
      from dwp.wallets w left join dwp.wallet_ledger_entries l on l.wallet_id = w.id
      where w.id = ? group by w.id`, [walletId]);
    expect(rows).toHaveLength(1);
    expect(rows[0]!.balance_cents).toBe(rows[0]!.calculated);
    expect(BigInt(rows[0]!.balance_cents) >= 0n).toBe(true);
  }

  test('opens and queries an exact balance with one OPENING, ledger and two outbox events', async () => {
    const wallet = await service.createWallet({ playerId: crypto.randomUUID(), initialBalance: { amount: '900719925474099.31', currency: 'BRL' } });
    expect(wallet.version).toBe(1);
    expect(wallet.balance).toEqual({ amount: '900719925474099.31', currency: 'BRL' });
    expect(await service.getWallet(wallet.id)).toEqual(wallet);
    const transactions = await query<{ kind: string; status: string }>('select kind, status from dwp.wager_transactions where wallet_id = ?', [wallet.id]);
    expect(transactions).toEqual([{ kind: 'OPENING', status: 'PROCESSED' }]);
    const entries = await query<{ direction: string; wallet_version: number }>('select direction, wallet_version from dwp.wallet_ledger_entries where wallet_id = ?', [wallet.id]);
    expect(entries).toEqual([{ direction: 'CREDIT', wallet_version: 1 }]);
    const events = await query<{ event_type: string }>("select event_type from dwp.outbox_messages where payload->'data'->>'walletId' = ? order by event_type", [wallet.id]);
    expect(events.map(event => event.event_type)).toEqual(['WagerTransactionProcessed', 'WalletBalanceChanged']);
    await assertConsistent(wallet.id);
  });

  test('opens at zero without financial transaction, ledger or balance event', async () => {
    const wallet = await service.createWallet({ playerId: crypto.randomUUID(), initialBalance: { amount: '0.00', currency: 'BRL' } });
    expect(wallet.balance.amount).toBe('0.00');
    expect(wallet.version).toBe(1);
    expect(await query('select id from dwp.wager_transactions where wallet_id = ?', [wallet.id])).toHaveLength(0);
    expect(await query('select id from dwp.wallet_ledger_entries where wallet_id = ?', [wallet.id])).toHaveLength(0);
    await assertConsistent(wallet.id);
  });

  test('rejects a duplicate player/currency identity without adding effects', async () => {
    const body = { playerId: crypto.randomUUID(), initialBalance: { amount: '100.00', currency: 'BRL' } };
    const wallet = await service.createWallet(body);
    await expect(service.createWallet(body)).rejects.toMatchObject({ status: 409 });
    expect(await query('select id from dwp.wallets where player_id = ?', [body.playerId])).toHaveLength(1);
    expect(await query('select id from dwp.wallet_ledger_entries where wallet_id = ?', [wallet.id])).toHaveLength(1);
    await assertConsistent(wallet.id);
  });

  test('rolls back opening, balance, ledger and events if the process fails before commit', async () => {
    const playerId = crypto.randomUUID();
    const failing = new WageringService(orm, { beforeCommit: () => { throw new Error('injected-before-commit'); } });
    const initialCounts = (await query<{ count: string }>('select count(*)::text from dwp.outbox_messages'))[0]!.count;
    await expect(failing.createWallet({ playerId, initialBalance: { amount: '100.00', currency: 'BRL' } })).rejects.toThrow('injected-before-commit');
    expect(await query('select id from dwp.wallets where player_id = ?', [playerId])).toHaveLength(0);
    expect(await query('select id from dwp.wager_transactions where player_id = ?', [playerId])).toHaveLength(0);
    expect((await query<{ count: string }>('select count(*)::text from dwp.outbox_messages'))[0]!.count).toBe(initialCounts);
  });

  test('distinguishes invalid contracts, unsupported currency and unknown wallet', async () => {
    await expect(service.createWallet({ playerId: '', initialBalance: { amount: '100.00', currency: 'BRL' } })).rejects.toMatchObject({ status: 400 });
    await expect(service.createWallet({ playerId: 'player', initialBalance: { amount: '-0.01', currency: 'BRL' } })).rejects.toMatchObject({ status: 400 });
    await expect(service.createWallet({ playerId: 'player', initialBalance: { amount: '1.00', currency: 'USD' } })).rejects.toMatchObject({ status: 400 });
    await expect(service.getWallet('bad-uuid')).rejects.toMatchObject({ status: 400 });
    await expect(service.getWallet(crypto.randomUUID())).rejects.toMatchObject({ status: 404 });
  });

  test('exposes wallet creation, retrieval and stable contract errors over HTTP', async () => {
    const body = { playerId: crypto.randomUUID(), initialBalance: { amount: '100.00', currency: 'BRL' } };
    const post = (payload: unknown) => fetch(`${address}/wallets`, {
      method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(payload),
    });
    const response = await post(body);
    expect(response.status).toBe(201);
    const wallet = await response.json() as { id: string; balance: { amount: string } };
    expect(wallet.balance.amount).toBe('100.00');
    const retrieved = await fetch(`${address}/wallets/${wallet.id}`);
    expect(retrieved.status).toBe(200);
    expect(await retrieved.json()).toEqual(wallet);
    expect((await post(body)).status).toBe(409);
    const invalid = await post({ ...body, initialBalance: { amount: 100, currency: 'BRL' } });
    expect(invalid.status).toBe(400);
    expect(await invalid.json()).toHaveProperty('code');
    await assertConsistent(wallet.id);
  });
});
