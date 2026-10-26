import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import type { INestApplication } from '@nestjs/common';
import type { MikroORM } from '@mikro-orm/postgresql';
import { createApplication } from '../../dist/application.js';
import type { SubmissionResult } from '../../dist/application/wagering-service.js';
import { createDatabase } from '../../dist/infrastructure/persistence/database.js';

const integration = process.env.DWP_INTEGRATION === '1' ? describe : describe.skip;

integration('temporary PostgreSQL unavailability', () => {
  const databaseName = `dwp_pg_outage_${crypto.randomUUID().replaceAll('-', '')}`;
  let admin: MikroORM;
  let orm: MikroORM;
  let app: INestApplication;
  let address: string;
  let databaseCreated = false;

  beforeAll(async () => {
    admin = await createDatabase();
    await admin.em.getConnection().execute(`create database ${databaseName}`);
    databaseCreated = true;
    const url = new URL(process.env.DATABASE_URL ?? 'postgresql://dwp:dwp@127.0.0.1:5432/dwp');
    url.pathname = `/${databaseName}`;
    orm = await createDatabase(url.href);
    await orm.migrator.up();
    app = await createApplication(orm);
    await app.listen(0, '127.0.0.1');
    address = await app.getUrl();
  });

  async function restoreConnections(): Promise<void> {
    if (databaseCreated) await admin.em.getConnection().execute(`alter database ${databaseName} allow_connections true`);
  }

  afterAll(async () => {
    await restoreConnections();
    await app?.close();
    await orm?.close(true);
    if (admin) {
      if (databaseCreated) await admin.em.getConnection().execute(`drop database if exists ${databaseName} with (force)`);
      await admin.close(true);
    }
  });

  const query = <T>(sql: string, values: unknown[] = []) => orm.em.getConnection().execute<T[]>(sql, values);

  test('returns 503 during the outage and processes the same key once after the pool recovers', async () => {
    const opened = await fetch(`${address}/wallets`, {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ playerId: crypto.randomUUID(), initialBalance: { amount: '100.00', currency: 'BRL' } }),
    });
    expect(opened.status).toBe(201);
    const wallet = await opened.json() as { id: string; playerId: string };
    const externalTransactionId = crypto.randomUUID();
    const key = `pg-outage:${externalTransactionId}`;
    const payload = {
      providerId: 'pg-outage-provider', externalTransactionId, walletId: wallet.id, playerId: wallet.playerId,
      roundId: 'pg-outage-round', gameId: 'pg-outage-game', kind: 'BET', money: { amount: '25.00', currency: 'BRL' },
    };
    const submit = () => fetch(`${address}/wagering/transactions`, {
      method: 'POST', headers: { 'content-type': 'application/json', 'idempotency-key': key },
      body: JSON.stringify(payload), signal: AbortSignal.timeout(5_000),
    });
    const beforeEntries = await query<{ id: string }>('select id from dwp.wallet_ledger_entries order by id');
    const beforeEvents = await query<{ id: string }>('select id from dwp.outbox_messages order by id');
    expect(beforeEntries).toHaveLength(1);
    expect(beforeEvents).toHaveLength(2);

    try {
      await admin.em.getConnection().execute(`alter database ${databaseName} allow_connections false`);
      // Only this test database becomes unavailable; the administrative connection
      // and every other test database keep using the shared PostgreSQL server.
      await admin.em.getConnection().execute(`select pg_terminate_backend(pid) from pg_stat_activity
        where datname = ? and pid <> pg_backend_pid()`, [databaseName]);
      const unavailable = await submit();
      expect(unavailable.status).toBe(503);
      expect(await unavailable.json()).toMatchObject({ code: 'INFRASTRUCTURE_UNAVAILABLE' });
    } finally {
      await restoreConnections();
    }

    expect(await query('select id from dwp.wager_transactions where idempotency_key = ?', [key])).toHaveLength(0);
    expect(await query('select id from dwp.wallet_ledger_entries order by id')).toEqual(beforeEntries);
    expect(await query('select id from dwp.outbox_messages order by id')).toEqual(beforeEvents);
    expect(await query('select balance_cents::text, version from dwp.wallets where id = ?', [wallet.id]))
      .toEqual([{ balance_cents: '10000', version: 1 }]);

    // The application and its ORM pool remain alive throughout the outage.
    const processed = await submit();
    expect(processed.status).toBe(200);
    const result = await processed.json() as SubmissionResult;
    expect(result).toMatchObject({
      status: 'PROCESSED', balance: { amount: '75.00', currency: 'BRL' }, idempotentReplay: false,
    });
    const repeated = await submit();
    expect(repeated.status).toBe(200);
    expect(await repeated.json()).toEqual({ ...result, idempotentReplay: true });

    const [state] = await query<{ balance_cents: string; calculated: string; debits: string; entries: string }>(`
      select w.balance_cents::text,
        coalesce(sum(case when l.direction = 'CREDIT' then l.amount_cents::numeric else -l.amount_cents::numeric end), 0)::text as calculated,
        count(l.id) filter (where l.direction = 'DEBIT')::text as debits,
        count(l.id)::text as entries
      from dwp.wallets w left join dwp.wallet_ledger_entries l on l.wallet_id = w.id
      where w.id = ? group by w.id`, [wallet.id]);
    expect(state).toEqual({ balance_cents: '7500', calculated: '7500', debits: '1', entries: '2' });
    expect(BigInt(state!.balance_cents) >= 0n).toBe(true);
    expect(await query('select id from dwp.wager_transactions where idempotency_key = ?', [key])).toHaveLength(1);
    expect(await query('select id from dwp.outbox_messages')).toHaveLength(4);
    const reconciliation = await fetch(`${address}/wallets/${wallet.id}/reconciliation`, { method: 'POST' });
    expect(reconciliation.status).toBe(200);
    expect(await reconciliation.json()).toMatchObject({
      walletId: wallet.id, consistent: true, checkedEntries: 2,
      storedBalance: { amount: '75.00', currency: 'BRL' },
      calculatedBalance: { amount: '75.00', currency: 'BRL' },
      difference: { amount: '0.00', currency: 'BRL' },
    });
  }, 15_000);
});
