import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { LockMode } from '@mikro-orm/core';
import { Migrator } from '@mikro-orm/migrations';
import { MikroORM, type EntityManager } from '@mikro-orm/postgresql';
import { createDatabase } from '../../dist/infrastructure/persistence/database.js';
import { Migration20261025000100 } from '../../dist/infrastructure/persistence/migrations/Migration20261025000100.js';
import { Migration20261025000200 } from '../../dist/infrastructure/persistence/migrations/WageringSchema.js';
import { Migration20261025000300 } from '../../dist/infrastructure/persistence/migrations/Migration20261025000300.js';
import { WalletRecord, WalletRecordSchema } from '../../dist/infrastructure/persistence/records.js';

const integration = process.env.DWP_INTEGRATION === '1' ? describe : describe.skip;

integration('database financial constraints', () => {
  const databaseName = `dwp_schema_${crypto.randomUUID().replaceAll('-', '')}`;
  let admin: MikroORM;
  let orm: MikroORM;

  beforeAll(async () => {
    admin = await createDatabase();
    await admin.em.getConnection().execute(`create database ${databaseName}`);
    const url = new URL(process.env.DATABASE_URL ?? 'postgresql://dwp:dwp@127.0.0.1:5432/dwp');
    url.pathname = `/${databaseName}`;
    orm = await MikroORM.init({
      clientUrl: url.toString(),
      entities: [WalletRecordSchema],
      extensions: [Migrator],
      migrations: {
        migrationsList: [Migration20261025000100, Migration20261025000200, Migration20261025000300],
        snapshot: false,
        snapshotOnMigrate: false,
      },
    });
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

  async function zeroWallet(em: EntityManager, id = crypto.randomUUID(), player = crypto.randomUUID()): Promise<string> {
    await em.execute('insert into dwp.wallets (id, player_id, currency, balance_cents, version) values (?, ?, ?, ?, 1)', [id, player, 'BRL', '0']);
    return id;
  }

  async function transaction(em: EntityManager, walletId: string, kind: string, amount: string, external = crypto.randomUUID(), reference?: { id: string; external: string }): Promise<string> {
    const id = crypto.randomUUID();
    const wallet = (await em.execute<{ player_id: string }[]>('select player_id from dwp.wallets where id = ?', [walletId]))[0]!;
    await em.execute(`insert into dwp.wager_transactions
      (id, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id, player_id,
       round_id, game_id, kind, amount_cents, currency, status, result, processed_at,
       reference_transaction_id, reference_external_transaction_id)
      values (?, 'provider', ?, ?, ?, ?, ?, 'round', 'game', ?, ?, 'BRL', 'PROCESSED', '{}', now(), ?, ?)`,
    [id, external, external, 'a'.repeat(64), walletId, wallet.player_id, kind, amount, reference?.id ?? null, reference?.external ?? null]);
    return id;
  }

  async function entry(em: EntityManager, walletId: string, tx: string, amount: string, before: string, after: string, version: number, direction = 'CREDIT'): Promise<string> {
    const id = crypto.randomUUID();
    await em.execute(`insert into dwp.wallet_ledger_entries
      (id, wallet_id, transaction_id, direction, amount_cents, currency, balance_before_cents, balance_after_cents, wallet_version)
      values (?, ?, ?, ?, ?, 'BRL', ?, ?, ?)`, [id, walletId, tx, direction, amount, before, after, version]);
    return id;
  }

  test('enforces wallet identity uniqueness, nonnegative balance and exact BIGINT mapping', async () => {
    const player = crypto.randomUUID();
    await run(em => zeroWallet(em, crypto.randomUUID(), player));
    await expect(run(em => zeroWallet(em, crypto.randomUUID(), player))).rejects.toThrow();
    await expect(run(async em => {
      await em.execute('insert into dwp.wallets (id, player_id, currency, balance_cents, version) values (?, ?, ?, ?, 1)', [crypto.randomUUID(), crypto.randomUUID(), 'BRL', '-1']);
    })).rejects.toThrow();

    const id = crypto.randomUUID();
    await run(async em => {
      await em.execute('insert into dwp.wallets (id, player_id, currency, balance_cents, version) values (?, ?, ?, ?, 1)', [id, crypto.randomUUID(), 'BRL', '9007199254740993']);
      const opening = await transaction(em, id, 'OPENING', '9007199254740993');
      await entry(em, id, opening, '9007199254740993', '0', '9007199254740993', 1);
    });
    await run(async em => {
      const record = await em.findOneOrFail(WalletRecord, { id }, { lockMode: LockMode.PESSIMISTIC_WRITE });
      expect(record.balanceCents).toBe(9007199254740993n);
    });
  });

  test('rejects balance writes without ledger and financial transactions without entries', async () => {
    const id = await run(em => zeroWallet(em));
    await expect(run(async em => {
      await em.execute('update dwp.wallets set balance_cents = 1, version = 2 where id = ?', [id]);
    })).rejects.toThrow('wallet');
    await expect(run(async em => { await transaction(em, id, 'WIN', '1'); })).rejects.toThrow('ledger');
    expect((await orm.em.getConnection().execute<{ balance_cents: string }[]>('select balance_cents from dwp.wallets where id = ?', [id]))[0]?.balance_cents).toBe('0');
  });

  test('enforces arithmetic, version continuity, transaction identity and immutable ledger', async () => {
    const id = await run(em => zeroWallet(em));
    let ledgerId = '';
    let credit = '';
    await run(async em => {
      credit = await transaction(em, id, 'WIN', '10');
      await em.execute('update dwp.wallets set balance_cents = 10, version = 2 where id = ?', [id]);
      ledgerId = await entry(em, id, credit, '10', '0', '10', 2);
    });
    await expect(run(async em => { await entry(em, id, credit, '10', '0', '10', 2); })).rejects.toThrow();
    await expect(run(async em => { await em.execute('update dwp.wallet_ledger_entries set amount_cents = 11 where id = ?', [ledgerId]); })).rejects.toThrow('immutable');
    await expect(run(async em => { await em.execute('delete from dwp.wallet_ledger_entries where id = ?', [ledgerId]); })).rejects.toThrow('immutable');
    await expect(run(async em => {
      const tx = await transaction(em, id, 'WIN', '10');
      await entry(em, id, tx, '10', '10', '30', 3);
    })).rejects.toThrow();
    await expect(run(async em => {
      const tx = await transaction(em, id, 'WIN', '10');
      await em.execute('update dwp.wallets set balance_cents = 20, version = 3 where id = ?', [id]);
      await entry(em, id, tx, '10', '10', '20', 4);
    })).rejects.toThrow();
  });

  test('enforces unique idempotency, provider identity, inbox identity and no LOSS ledger', async () => {
    const id = await run(em => zeroWallet(em));
    const external = crypto.randomUUID();
    const loss = await run(em => transaction(em, id, 'LOSS', '0', external));
    await expect(run(async em => { await transaction(em, id, 'LOSS', '0', external); })).rejects.toThrow();
    await expect(run(async em => {
      await em.execute(`insert into dwp.wager_transactions
        (id, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id, player_id,
         round_id, game_id, kind, amount_cents, currency, status, result, processed_at)
        select ?, provider_id, ?, idempotency_key, payload_hash, wallet_id, player_id,
          round_id, game_id, kind, amount_cents, currency, status, result, processed_at
        from dwp.wager_transactions where id = ?`, [crypto.randomUUID(), crypto.randomUUID(), loss]);
    })).rejects.toThrow();
    await expect(run(async em => {
      await em.execute(`insert into dwp.wager_transactions
        (id, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id, player_id,
         round_id, game_id, kind, amount_cents, currency, status, result, processed_at)
        select ?, provider_id, external_transaction_id, ?, payload_hash, wallet_id, player_id,
          round_id, game_id, kind, amount_cents, currency, status, result, processed_at
        from dwp.wager_transactions where id = ?`, [crypto.randomUUID(), crypto.randomUUID(), loss]);
    })).rejects.toThrow();
    await expect(run(async em => { await entry(em, id, loss, '1', '0', '1', 2); })).rejects.toThrow();
    await expect(run(async em => {
      await em.execute("update dwp.wager_transactions set status = 'PENDING', result = null, processed_at = null where id = ?", [loss]);
    })).rejects.toThrow('terminal');
    await run(async em => {
      await em.execute('insert into dwp.inbox_messages (consumer_name, message_id, payload_hash) values (?, ?, ?)', ['consumer', external, 'a'.repeat(64)]);
    });
    await expect(run(async em => {
      await em.execute('insert into dwp.inbox_messages (consumer_name, message_id, payload_hash) values (?, ?, ?)', ['consumer', external, 'a'.repeat(64)]);
    })).rejects.toThrow();
  });

  test('persists rejected identity mismatches but prevents processing them, including LOSS', async () => {
    const walletId = await run(em => zeroWallet(em));
    const rejected = crypto.randomUUID();
    await run(async em => {
      await em.execute(`insert into dwp.wager_transactions
        (id, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id, player_id,
         round_id, game_id, kind, amount_cents, currency, status, failure_code, result)
        values (?, 'provider', ?, ?, ?, ?, 'wrong-player', 'round', 'game', 'LOSS', 0, 'USD', 'REJECTED', 'CURRENCY_MISMATCH', '{}')`,
      [rejected, rejected, rejected, 'a'.repeat(64), walletId]);
    });
    expect((await orm.em.getConnection().execute<{ status: string }[]>('select status from dwp.wager_transactions where id = ?', [rejected]))[0]?.status).toBe('REJECTED');
    for (const mismatch of ['currency', 'player_id']) {
      await expect(run(async em => {
        const other = crypto.randomUUID();
        const player = mismatch === 'player_id' ? 'wrong-player' : (await em.execute<{ player_id: string }[]>('select player_id from dwp.wallets where id = ?', [walletId]))[0]!.player_id;
        await em.execute(`insert into dwp.wager_transactions
          (id, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id, player_id,
           round_id, game_id, kind, amount_cents, currency, status, result, processed_at)
          values (?, 'provider', ?, ?, ?, ?, ?, 'round', 'game', 'LOSS', 0, ?, 'PROCESSED', '{}', now())`,
        [other, other, other, 'a'.repeat(64), walletId, player, mismatch === 'currency' ? 'USD' : 'BRL']);
      })).rejects.toThrow('identity');
    }
  });

  test('allows one reversal per kind, rejects partial and mismatched references', async () => {
    const walletId = crypto.randomUUID();
    const external = crypto.randomUUID();
    let bet = '';
    await run(async em => {
      await em.execute("insert into dwp.wallets (id,player_id,currency,balance_cents,version) values (?,?,'BRL',100,1)", [walletId, crypto.randomUUID()]);
      const opening = await transaction(em, walletId, 'OPENING', '100');
      await entry(em, walletId, opening, '100', '0', '100', 1);
      bet = await transaction(em, walletId, 'BET', '25', external);
      await em.execute('update dwp.wallets set balance_cents = 75, version = 2 where id = ?', [walletId]);
      await entry(em, walletId, bet, '25', '100', '75', 2, 'DEBIT');
    });
    const reference = { id: bet, external };
    await expect(run(async em => {
      const refund = await transaction(em, walletId, 'REFUND', '24', crypto.randomUUID(), reference);
      await em.execute('update dwp.wallets set balance_cents = 99, version = 3 where id = ?', [walletId]);
      await entry(em, walletId, refund, '24', '75', '99', 3);
    })).rejects.toThrow('amount');
    const otherWallet = await run(em => zeroWallet(em));
    await expect(run(async em => {
      const refund = await transaction(em, otherWallet, 'REFUND', '25', crypto.randomUUID(), reference);
      await em.execute('update dwp.wallets set balance_cents = 25, version = 2 where id = ?', [otherWallet]);
      await entry(em, otherWallet, refund, '25', '0', '25', 2);
    })).rejects.toThrow('reference');
    await run(async em => {
      const refund = await transaction(em, walletId, 'REFUND', '25', crypto.randomUUID(), reference);
      await em.execute('update dwp.wallets set balance_cents = 100, version = 3 where id = ?', [walletId]);
      await entry(em, walletId, refund, '25', '75', '100', 3);
    });
    await expect(run(async em => { await transaction(em, walletId, 'REFUND', '25', crypto.randomUUID(), reference); })).rejects.toThrow();
    await run(async em => {
      const rollback = await transaction(em, walletId, 'ROLLBACK', '25', crypto.randomUUID(), reference);
      await em.execute('update dwp.wallets set balance_cents = 125, version = 4 where id = ?', [walletId]);
      await entry(em, walletId, rollback, '25', '100', '125', 4);
    });
    const balances = await orm.em.getConnection().execute<{ balance_cents: string; ledger_balance: string }[]>(`
      select w.balance_cents, (select sum(case when direction='CREDIT' then amount_cents else -amount_cents end)
      from dwp.wallet_ledger_entries where wallet_id = w.id)::text as ledger_balance from dwp.wallets w where id = ?`, [walletId]);
    expect(balances[0]).toEqual({ balance_cents: '125', ledger_balance: '125' });
  });

  test('reverses and reapplies all domain tables and constraints', async () => {
    await orm.migrator.down({ to: 'Migration20261025000100' });
    expect((await orm.em.getConnection().execute<{ exists: boolean }[]>("select to_regclass('dwp.wallets') is not null as exists"))[0]?.exists).toBe(false);
    await orm.migrator.up();
    expect((await orm.em.getConnection().execute<{ exists: boolean }[]>("select to_regclass('dwp.wallets') is not null as exists"))[0]?.exists).toBe(true);
  });
});
