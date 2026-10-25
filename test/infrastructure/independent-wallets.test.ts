import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { LockMode } from '@mikro-orm/core';
import type { MikroORM } from '@mikro-orm/postgresql';
import { WageringService } from '../../dist/application/wagering-service.js';
import { createDatabase } from '../../dist/infrastructure/persistence/database.js';
import { WalletRecord } from '../../dist/infrastructure/persistence/records.js';

const integration = process.env.DWP_INTEGRATION === '1' ? describe : describe.skip;

integration('wallets progress independently under database contention', () => {
  const databaseName = `dwp_independent_${crypto.randomUUID().replaceAll('-', '')}`;
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

  async function bounded<T>(promise: Promise<T>, milliseconds: number): Promise<T> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      return await Promise.race([
        promise,
        new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new Error('An unrelated wallet was blocked')), milliseconds); }),
      ]);
    } finally { clearTimeout(timer); }
  }

  async function waitForDatabaseLockWaiter(): Promise<void> {
    const deadline = Date.now() + 5_000;
    while (Date.now() < deadline) {
      const rows = await orm.em.getConnection().execute<{ pid: number }[]>(
        "select pid from pg_stat_activity where datname = current_database() and wait_event_type = 'Lock'",
      );
      if (rows.length > 0) return;
      await Bun.sleep(10);
    }
    throw new Error('The first submission never reached the held PostgreSQL wallet lock');
  }

  test('a bet on wallet B commits while wallet A has a confirmed SQL lock waiter', async () => {
    const [walletA, walletB] = await Promise.all(Array.from({ length: 2 }, () => service.createWallet({
      playerId: crypto.randomUUID(), initialBalance: { amount: '100.00', currency: 'BRL' },
    })));
    const request = (wallet: NonNullable<typeof walletA>, amount: string) => ({
      providerId: 'provider-a', externalTransactionId: crypto.randomUUID(), playerId: wallet.playerId,
      walletId: wallet.id, roundId: 'round-a', gameId: 'game-a', kind: 'BET', money: { amount, currency: 'BRL' },
    });
    const bodyA = request(walletA!, '25.00');
    const bodyB = request(walletB!, '30.00');
    let releaseLock: () => void = () => undefined;
    const release = new Promise<void>(resolve => { releaseLock = resolve; });
    let signalAcquired: () => void = () => undefined;
    const acquired = new Promise<void>(resolve => { signalAcquired = resolve; });
    const blocker = orm.em.fork().transactional(async em => {
      await em.findOneOrFail(WalletRecord, { id: walletA!.id }, { lockMode: LockMode.PESSIMISTIC_WRITE });
      signalAcquired();
      await release;
    });
    let firstFinished = false;
    let pendingA: Promise<{ ok: true; value: Awaited<ReturnType<WageringService['submit']>> } | { ok: false; error: unknown }> | undefined;
    let pendingB: ReturnType<WageringService['submit']> | undefined;
    try {
      await Promise.race([acquired, blocker]);
      pendingA = service.submit(bodyA, `${bodyA.providerId}:${bodyA.externalTransactionId}`).then(
        value => { firstFinished = true; return { ok: true as const, value }; },
        error => { firstFinished = true; return { ok: false as const, error }; },
      );
      await waitForDatabaseLockWaiter();
      expect(firstFinished).toBe(false);
      pendingB = service.submit(bodyB, `${bodyB.providerId}:${bodyB.externalTransactionId}`);
      const resultB = await bounded(pendingB, 5_000);
      expect(resultB.status).toBe('PROCESSED');
      expect(resultB.balance.amount).toBe('70.00');
      expect(firstFinished).toBe(false);
      expect((await service.getWallet(walletB!.id)).balance.amount).toBe('70.00');
    } finally {
      releaseLock();
      await blocker;
      await Promise.allSettled([pendingA, pendingB].filter(promise => promise !== undefined));
    }
    const resultA = await pendingA!;
    if (!resultA.ok) throw resultA.error;
    expect(resultA.value.status).toBe('PROCESSED');
    expect(resultA.value.balance.amount).toBe('75.00');

    const rows = await orm.em.getConnection().execute<{ balance_cents: string; calculated: string; debits: number }[]>(`
      select w.balance_cents::text,
        coalesce(sum(case when l.direction = 'CREDIT' then l.amount_cents::numeric else -l.amount_cents::numeric end), 0)::text as calculated,
        count(*) filter (where l.direction = 'DEBIT')::int as debits
      from dwp.wallets w left join dwp.wallet_ledger_entries l on l.wallet_id = w.id
      where w.id in (?, ?) group by w.id`, [walletA!.id, walletB!.id]);
    expect(rows).toHaveLength(2);
    for (const row of rows) {
      expect(row.balance_cents).toBe(row.calculated);
      expect(BigInt(row.balance_cents) >= 0n).toBe(true);
      expect(row.debits).toBe(1);
    }
  }, 20_000);
});
