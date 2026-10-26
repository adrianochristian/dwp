import { createWageringService } from '../../dist/infrastructure/persistence/wagering-services.js';
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import type { MikroORM } from '@mikro-orm/postgresql';
import type { WageringService } from '../../dist/application/wagering-service.js';
import { createDatabase } from '../../dist/infrastructure/persistence/database.js';

const integration = process.env.DWP_INTEGRATION === '1' ? describe : describe.skip;

integration('persistent idempotency under concurrent submissions', () => {
  const databaseName = `dwp_dedup_${crypto.randomUUID().replaceAll('-', '')}`;
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
    service = createWageringService(orm);
  });

  afterAll(async () => {
    await orm?.close(true);
    if (admin) {
      await admin.em.getConnection().execute(`drop database if exists ${databaseName} with (force)`);
      await admin.close(true);
    }
  });

  async function assertConsistent(walletId: string): Promise<void> {
    const [row] = await orm.em.getConnection().execute<{ balance_cents: string; calculated: string }[]>(`
      select w.balance_cents::text,
        coalesce(sum(case when l.direction = 'CREDIT' then l.amount_cents::numeric else -l.amount_cents::numeric end), 0)::text as calculated
      from dwp.wallets w left join dwp.wallet_ledger_entries l on l.wallet_id = w.id
      where w.id = ? group by w.id`, [walletId]);
    expect(row).toBeDefined();
    expect(row!.balance_cents).toBe(row!.calculated);
    expect(BigInt(row!.balance_cents) >= 0n).toBe(true);
  }

  test('fifty simultaneous copies create one debit and retain the original response snapshot', async () => {
    const wallet = await service.createWallet({ playerId: crypto.randomUUID(), initialBalance: { amount: '100.00', currency: 'BRL' } });
    const request = {
      providerId: 'provider-a', externalTransactionId: crypto.randomUUID(), playerId: wallet.playerId,
      walletId: wallet.id, roundId: 'round-a', gameId: 'game-a', kind: 'BET',
      money: { amount: '25.00', currency: 'BRL' },
    };
    const key = `${request.providerId}:${request.externalTransactionId}`;
    const responses = await Promise.all(Array.from({ length: 50 }, () => service.submit(request, key)));
    expect(responses.filter(response => !response.idempotentReplay)).toHaveLength(1);
    expect(responses.filter(response => response.idempotentReplay)).toHaveLength(49);
    expect(new Set(responses.map(response => response.transactionId)).size).toBe(1);
    for (const response of responses) {
      expect(response.status).toBe('PROCESSED');
      expect(response.balance).toEqual({ amount: '75.00', currency: 'BRL' });
    }
    const debits = await orm.em.getConnection().execute<{ id: string }[]>(
      "select id from dwp.wallet_ledger_entries where wallet_id = ? and direction = 'DEBIT'", [wallet.id],
    );
    expect(debits).toHaveLength(1);
    expect((await service.getWallet(wallet.id)).balance.amount).toBe('75.00');
    await assertConsistent(wallet.id);

    const next = { ...request, externalTransactionId: crypto.randomUUID(), money: { amount: '5.00', currency: 'BRL' } };
    await service.submit(next, `${next.providerId}:${next.externalTransactionId}`);
    const replay = await service.submit(request, key);
    expect(replay).toEqual({ ...responses[0]!, idempotentReplay: true });
    expect(replay.balance.amount).toBe('75.00');
    expect((await service.getWallet(wallet.id)).balance.amount).toBe('70.00');
    expect(await orm.em.getConnection().execute<{ id: string }[]>(
      "select id from dwp.wallet_ledger_entries where wallet_id = ? and direction = 'DEBIT'", [wallet.id],
    )).toHaveLength(2);
    await assertConsistent(wallet.id);
  }, 20_000);
});
