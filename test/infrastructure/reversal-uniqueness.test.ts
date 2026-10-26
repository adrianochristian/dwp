import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import type { EntityManager, MikroORM } from '@mikro-orm/postgresql';
import { createDatabase } from '../../dist/infrastructure/persistence/database.js';

const integration = process.env.DWP_INTEGRATION === '1' ? describe : describe.skip;
type ReversalKind = 'REFUND' | 'ROLLBACK';
interface Reference { walletId: string; playerId: string; id: string; external: string }

integration('one successful reversal per referenced transaction', () => {
  const databaseName = `dwp_reversal_${crypto.randomUUID().replaceAll('-', '')}`;
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

  async function transaction(em: EntityManager, walletId: string, playerId: string, kind: 'OPENING' | 'BET' | ReversalKind, amountCents: string, reference?: Reference, status = 'PROCESSED') {
    const id = crypto.randomUUID();
    await em.execute(`insert into dwp.wager_transactions
      (id, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id, player_id,
       round_id, game_id, kind, amount_cents, currency, status, result, processed_at,
       reference_transaction_id, reference_external_transaction_id, failure_code)
      values (?, 'provider-sql', ?, ?, ?, ?, ?, 'round-sql', 'game-sql', ?, ?, 'BRL', ?, '{}'::jsonb,
        ?, ?, ?, ?)`,
    [id, id, `sql:${id}`, 'a'.repeat(64), walletId, playerId, kind, amountCents, status,
      status === 'PROCESSED' ? new Date() : null, reference?.id ?? null, reference?.external ?? null,
      status === 'REJECTED' ? 'REVERSAL_ALREADY_PROCESSED' : null]);
    return { id, external: id };
  }

  async function entry(em: EntityManager, walletId: string, transactionId: string, amountCents: string, beforeCents: string, afterCents: string, version: number, direction = 'CREDIT') {
    await em.execute(`insert into dwp.wallet_ledger_entries
      (id, wallet_id, transaction_id, direction, amount_cents, currency, balance_before_cents, balance_after_cents, wallet_version)
      values (?, ?, ?, ?, ?, 'BRL', ?, ?, ?)`,
    [crypto.randomUUID(), walletId, transactionId, direction, amountCents, beforeCents, afterCents, version]);
  }

  async function processedBet(): Promise<Reference> {
    const walletId = crypto.randomUUID();
    const playerId = crypto.randomUUID();
    return run(async em => {
      await em.execute("insert into dwp.wallets (id, player_id, currency, balance_cents, version) values (?, ?, 'BRL', 1000, 1)", [walletId, playerId]);
      const opening = await transaction(em, walletId, playerId, 'OPENING', '1000');
      await entry(em, walletId, opening.id, '1000', '0', '1000', 1);
      const bet = await transaction(em, walletId, playerId, 'BET', '100');
      await em.execute('update dwp.wallets set balance_cents = 900, version = 2 where id = ?', [walletId]);
      await entry(em, walletId, bet.id, '100', '1000', '900', 2, 'DEBIT');
      return { walletId, playerId, ...bet };
    });
  }

  async function reverse(reference: Reference, kind: ReversalKind): Promise<void> {
    await run(async em => {
      const [wallet] = await em.execute<{ balance_cents: string; version: number }[]>(
        'select balance_cents::text, version from dwp.wallets where id = ? for no key update', [reference.walletId],
      );
      const after = (BigInt(wallet!.balance_cents) + 100n).toString();
      const reversal = await transaction(em, reference.walletId, reference.playerId, kind, '100', reference);
      await em.execute('update dwp.wallets set balance_cents = ?, version = ? where id = ?', [after, wallet!.version + 1, reference.walletId]);
      await entry(em, reference.walletId, reversal.id, '100', wallet!.balance_cents, after, wallet!.version + 1);
    });
  }

  async function consistentBalance(walletId: string) {
    const [row] = await orm.em.fork().execute<{ balance_cents: string; calculated_cents: string; entries: number }[]>(`
      select w.balance_cents::text, count(l.id)::int as entries,
        coalesce(sum(case when l.direction = 'CREDIT' then l.amount_cents::numeric else -l.amount_cents::numeric end), 0)::text as calculated_cents
      from dwp.wallets w left join dwp.wallet_ledger_entries l on l.wallet_id = w.id
      where w.id = ? group by w.id`, [walletId]);
    expect(row).toBeDefined();
    expect(row!.balance_cents).toBe(row!.calculated_cents);
    expect(BigInt(row!.balance_cents) >= 0n).toBe(true);
    return row!;
  }

  async function indexDefinition(): Promise<string> {
    const [row] = await orm.em.fork().execute<{ definition: string }[]>(
      "select pg_get_indexdef('dwp.wager_reversal_once'::regclass) as definition",
    );
    return row!.definition;
  }

  async function financialSnapshot() {
    const snapshots: { table: string; count: string; checksum: string }[] = [];
    for (const table of ['wallets', 'wager_transactions', 'wallet_ledger_entries']) {
      const [row] = await orm.em.fork().execute<{ count: string; checksum: string }[]>(`
        select count(*)::text as count,
          md5(coalesce(string_agg(to_jsonb(t)::text, '' order by t.id), '')) as checksum
        from dwp.${table} t`);
      snapshots.push({ table, ...row! });
    }
    return snapshots;
  }

  test('rejects a second successful reversal through direct SQL in either cross-kind order while retaining rejected audit rows', async () => {
    for (const firstKind of ['REFUND', 'ROLLBACK'] as const) {
      const reference = await processedBet();
      await reverse(reference, firstKind);
      const secondKind = firstKind === 'REFUND' ? 'ROLLBACK' : 'REFUND';
      await expect(reverse(reference, secondKind)).rejects.toMatchObject({ code: '23505' });
      await run(async em => {
        await transaction(em, reference.walletId, reference.playerId, secondKind, '100', reference, 'REJECTED');
      });
      const reversals = await orm.em.fork().execute<{ kind: string; status: string }[]>(
        'select kind, status from dwp.wager_transactions where reference_transaction_id = ? order by status', [reference.id],
      );
      expect(reversals).toEqual([{ kind: firstKind, status: 'PROCESSED' }, { kind: secondKind, status: 'REJECTED' }]);
      expect(await consistentBalance(reference.walletId)).toEqual({ balance_cents: '1000', calculated_cents: '1000', entries: 3 });
    }
  });

  test('restores the previous index on down and reapplies the stricter index without changing financial data', async () => {
    const before = await financialSnapshot();
    expect(await indexDefinition()).toContain('(reference_transaction_id)');
    await orm.migrator.down({ to: 'Migration20261025000300' });
    expect(await indexDefinition()).toContain('(reference_transaction_id, kind)');
    expect(await financialSnapshot()).toEqual(before);
    await orm.migrator.up();
    expect(await indexDefinition()).toContain('(reference_transaction_id)');
    expect(await financialSnapshot()).toEqual(before);
    expect(await orm.migrator.getPending()).toHaveLength(0);
  });

  test('fails an upgrade atomically on legacy dual reversals and preserves the old index, migration history and financial rows', async () => {
    await orm.migrator.down({ to: 'Migration20261025000300' });
    const reference = await processedBet();
    await reverse(reference, 'REFUND');
    await reverse(reference, 'ROLLBACK');
    expect(await consistentBalance(reference.walletId)).toEqual({ balance_cents: '1100', calculated_cents: '1100', entries: 4 });
    const before = await financialSnapshot();
    const oldIndex = await indexDefinition();
    const migrationHistory = () => orm.em.fork().execute<{ name: string }[]>(
      'select name from public.mikro_orm_migrations order by id',
    );
    const historyBefore = await migrationHistory();
    await expect(orm.migrator.up()).rejects.toThrow();
    expect(await indexDefinition()).toBe(oldIndex);
    expect(await migrationHistory()).toEqual(historyBefore);
    expect(await financialSnapshot()).toEqual(before);
    expect((await orm.migrator.getPending()).map(migration => migration.name)).toContain('Migration20261026000100');
    expect(await consistentBalance(reference.walletId)).toEqual({ balance_cents: '1100', calculated_cents: '1100', entries: 4 });
  });
});
