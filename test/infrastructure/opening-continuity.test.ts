import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import type { EntityManager, MikroORM } from '@mikro-orm/postgresql';
import { createDatabase } from '../../dist/infrastructure/persistence/database.js';

const integration = process.env.DWP_INTEGRATION === '1' ? describe : describe.skip;

integration('opening ledger successor continuity', () => {
  const databaseName = `dwp_opening_${crypto.randomUUID().replaceAll('-', '')}`;
  let admin: MikroORM;
  let orm: MikroORM;

  beforeAll(async () => {
    admin = await createDatabase();
    await admin.em.getConnection().execute(`create database ${databaseName}`);
    const url = new URL(process.env.DATABASE_URL ?? 'postgresql://dwp:dwp@127.0.0.1:5432/dwp');
    url.pathname = `/${databaseName}`;
    orm = await createDatabase(url.toString());
    await orm.migrator.up();
  });

  afterAll(async () => {
    await orm?.close(true);
    if (admin) {
      await admin.em.getConnection().execute(`drop database if exists ${databaseName} with (force)`);
      await admin.close(true);
    }
  });

  const run = <T>(work: (em: EntityManager) => Promise<T>) => orm.em.fork().transactional(work);

  async function financialTransaction(em: EntityManager, walletId: string, playerId: string, kind: 'OPENING' | 'WIN' | 'BET', amountCents: string): Promise<string> {
    const id = crypto.randomUUID();
    await em.execute(`insert into dwp.wager_transactions
      (id, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id, player_id,
       round_id, game_id, kind, amount_cents, currency, status, result, processed_at)
      values (?, 'provider', ?, ?, ?, ?, ?, 'round', 'game', ?, ?, 'BRL', 'PROCESSED', '{}'::jsonb, now())`,
    [id, id, id, 'a'.repeat(64), walletId, playerId, kind, amountCents]);
    return id;
  }

  async function ledger(em: EntityManager, walletId: string, transactionId: string, direction: 'CREDIT' | 'DEBIT', amountCents: string, beforeCents: string, afterCents: string, version: number): Promise<void> {
    await em.execute(`insert into dwp.wallet_ledger_entries
      (id, wallet_id, transaction_id, direction, amount_cents, currency, balance_before_cents, balance_after_cents, wallet_version)
      values (?, ?, ?, ?, ?, 'BRL', ?, ?, ?)`,
    [crypto.randomUUID(), walletId, transactionId, direction, amountCents, beforeCents, afterCents, version]);
  }

  async function balance(walletId: string) {
    const [row] = await orm.em.fork().execute<{
      balance_cents: string; calculated_cents: string; entries: number; version: number;
    }[]>(`select w.balance_cents::text, w.version, count(l.id)::int as entries,
      coalesce(sum(case when l.direction = 'CREDIT' then l.amount_cents::numeric else -l.amount_cents::numeric end), 0)::text as calculated_cents
      from dwp.wallets w left join dwp.wallet_ledger_entries l on l.wallet_id = w.id
      where w.id = ? group by w.id`, [walletId]);
    expect(row).toBeDefined();
    expect(row!.balance_cents).toBe(row!.calculated_cents);
    expect(BigInt(row!.balance_cents) >= 0n).toBe(true);
    return row!;
  }

  test('rejects an opening inserted after a zero wallet has already received a version-2 credit', async () => {
    const walletId = crypto.randomUUID();
    const playerId = crypto.randomUUID();
    await run(async em => {
      await em.execute("insert into dwp.wallets (id, player_id, currency, balance_cents, version) values (?, ?, 'BRL', 0, 1)", [walletId, playerId]);
    });
    await run(async em => {
      const win = await financialTransaction(em, walletId, playerId, 'WIN', '1000');
      await em.execute('update dwp.wallets set balance_cents = 1000, version = 2 where id = ?', [walletId]);
      await ledger(em, walletId, win, 'CREDIT', '1000', '0', '1000', 2);
    });
    expect(await balance(walletId)).toEqual({ balance_cents: '1000', calculated_cents: '1000', entries: 1, version: 2 });

    await expect(run(async em => {
      const opening = await financialTransaction(em, walletId, playerId, 'OPENING', '500');
      await ledger(em, walletId, opening, 'CREDIT', '500', '0', '500', 1);
    })).rejects.toMatchObject({ code: '23514' });
    expect(await balance(walletId)).toEqual({ balance_cents: '1000', calculated_cents: '1000', entries: 1, version: 2 });
    expect(await orm.em.fork().execute<{ id: string }[]>(
      "select id from dwp.wager_transactions where wallet_id = ? and kind = 'OPENING'", [walletId],
    )).toHaveLength(0);
  });

  test('allows an opening and its matching successor in the same SQL transaction', async () => {
    const walletId = crypto.randomUUID();
    const playerId = crypto.randomUUID();
    await run(async em => {
      await em.execute("insert into dwp.wallets (id, player_id, currency, balance_cents, version) values (?, ?, 'BRL', 500, 1)", [walletId, playerId]);
      const opening = await financialTransaction(em, walletId, playerId, 'OPENING', '500');
      await ledger(em, walletId, opening, 'CREDIT', '500', '0', '500', 1);
      const bet = await financialTransaction(em, walletId, playerId, 'BET', '200');
      await em.execute('update dwp.wallets set balance_cents = 300, version = 2 where id = ?', [walletId]);
      await ledger(em, walletId, bet, 'DEBIT', '200', '500', '300', 2);
    });
    expect(await balance(walletId)).toEqual({ balance_cents: '300', calculated_cents: '300', entries: 2, version: 2 });
  });

  test('rejects ledger TRUNCATE and reverses and reapplies the new guards without losing financial rows', async () => {
    const walletId = crypto.randomUUID();
    const playerId = crypto.randomUUID();
    await run(async em => {
      await em.execute("insert into dwp.wallets (id, player_id, currency, balance_cents, version) values (?, ?, 'BRL', 100, 1)", [walletId, playerId]);
      const opening = await financialTransaction(em, walletId, playerId, 'OPENING', '100');
      await ledger(em, walletId, opening, 'CREDIT', '100', '0', '100', 1);
    });
    const expectedBalance = { balance_cents: '100', calculated_cents: '100', entries: 1, version: 1 };
    const present = async () => {
      const [row] = await orm.em.fork().execute<{ trigger_exists: boolean; truncate_guard_exists: boolean; function_exists: boolean; wallets_exist: boolean }[]>(`
        select exists(select 1 from pg_trigger where tgrelid = 'dwp.wallet_ledger_entries'::regclass
          and tgname = 'opening_successor_consistency') as trigger_exists,
          exists(select 1 from pg_trigger where tgrelid = 'dwp.wallet_ledger_entries'::regclass
            and tgname = 'ledger_no_truncate') as truncate_guard_exists,
          to_regprocedure('dwp.validate_opening_successor()') is not null as function_exists,
          to_regclass('dwp.wallets') is not null as wallets_exist`);
      return row!;
    };
    expect(await present()).toEqual({ trigger_exists: true, truncate_guard_exists: true, function_exists: true, wallets_exist: true });
    await orm.migrator.down();
    expect(await present()).toEqual({ trigger_exists: false, truncate_guard_exists: false, function_exists: false, wallets_exist: true });
    expect(await balance(walletId)).toEqual(expectedBalance);
    await orm.migrator.up();
    expect(await present()).toEqual({ trigger_exists: true, truncate_guard_exists: true, function_exists: true, wallets_exist: true });
    await expect(run(async em => { await em.execute('truncate table dwp.wallet_ledger_entries'); })).rejects.toMatchObject({ code: '23514' });
    expect(await balance(walletId)).toEqual(expectedBalance);
  });
});
