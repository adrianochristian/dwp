import { createWageringService } from '../../dist/infrastructure/persistence/wagering-services.js';
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import type { MikroORM } from '@mikro-orm/postgresql';
import type { WageringService } from '../../dist/application/wagering-service.js';
import { createDatabase } from '../../dist/infrastructure/persistence/database.js';

const integration = process.env.DWP_INTEGRATION === '1' ? describe : describe.skip;

integration('competing bets against the same balance', () => {
  const databaseName = `dwp_funds_${crypto.randomUUID().replaceAll('-', '')}`;
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

  test('only one of two 80.00 bets spends a 100.00 balance and both retries keep their results', async () => {
    const wallet = await service.createWallet({ playerId: crypto.randomUUID(), initialBalance: { amount: '100.00', currency: 'BRL' } });
    const requests = Array.from({ length: 2 }, () => ({
      providerId: 'provider-a', externalTransactionId: crypto.randomUUID(), playerId: wallet.playerId,
      walletId: wallet.id, roundId: 'round-a', gameId: 'game-a', kind: 'BET',
      money: { amount: '80.00', currency: 'BRL' },
    }));
    const responses = await Promise.all(requests.map(body => service.submit(body, `${body.providerId}:${body.externalTransactionId}`)));
    const processed = responses.filter(response => response.status === 'PROCESSED');
    const rejected = responses.filter(response => response.status === 'REJECTED');
    expect(processed).toHaveLength(1);
    expect(rejected).toHaveLength(1);
    expect(rejected[0]!.failureCode).toBe('INSUFFICIENT_FUNDS');
    expect(processed[0]!.balance.amount).toBe('20.00');
    expect(rejected[0]!.balance.amount).toBe('20.00');
    expect((await service.getWallet(wallet.id)).balance.amount).toBe('20.00');

    const repeats = await Promise.all(requests.flatMap((body, index) => Array.from({ length: 10 }, async () => {
      const replay = await service.submit(body, `${body.providerId}:${body.externalTransactionId}`);
      expect(replay).toEqual({ ...responses[index]!, idempotentReplay: true });
      return replay;
    })));
    expect(repeats.every(response => response.idempotentReplay)).toBe(true);
    const debitRows = await orm.em.getConnection().execute<{ transaction_id: string }[]>(
      "select transaction_id from dwp.wallet_ledger_entries where wallet_id = ? and direction = 'DEBIT'", [wallet.id],
    );
    expect(debitRows).toEqual([{ transaction_id: processed[0]!.transactionId }]);
    expect(await orm.em.getConnection().execute<{ id: string }[]>(
      'select id from dwp.wallet_ledger_entries where transaction_id = ?', [rejected[0]!.transactionId],
    )).toHaveLength(0);
    const [consistency] = await orm.em.getConnection().execute<{ balance_cents: string; calculated: string }[]>(`
      select w.balance_cents::text,
        coalesce(sum(case when l.direction = 'CREDIT' then l.amount_cents::numeric else -l.amount_cents::numeric end), 0)::text as calculated
      from dwp.wallets w left join dwp.wallet_ledger_entries l on l.wallet_id = w.id
      where w.id = ? group by w.id`, [wallet.id]);
    expect(consistency).toEqual({ balance_cents: '2000', calculated: '2000' });
    expect(BigInt(consistency!.balance_cents) >= 0n).toBe(true);
  }, 20_000);
});
