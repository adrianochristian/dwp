import { afterAll, beforeAll, describe, expect, spyOn, test } from 'bun:test';
import type { MikroORM } from '@mikro-orm/postgresql';
import { createDatabase } from '../../dist/infrastructure/persistence/database.js';
import { createWageringService } from '../../dist/infrastructure/persistence/wagering-services.js';
import { ObservedWageringService } from '../../dist/observability/observed-wagering.js';
import { metrics } from '../../dist/observability/metrics.js';

const integration = process.env.DWP_INTEGRATION === '1' ? describe : describe.skip;

integration('diagnostic failures preserve the financial outcome', () => {
  const databaseName = `dwp_diagnostics_${crypto.randomUUID().replaceAll('-', '')}`;
  let admin: MikroORM;
  let orm: MikroORM;

  beforeAll(async () => {
    admin = await createDatabase();
    await admin.em.fork().execute(`create database ${databaseName}`);
    const url = new URL(process.env.DATABASE_URL ?? 'postgresql://dwp:dwp@127.0.0.1:5432/dwp');
    url.pathname = `/${databaseName}`;
    orm = await createDatabase(url.toString());
    await orm.migrator.up();
  });

  afterAll(async () => {
    await orm?.close(true);
    if (admin) {
      await admin.em.fork().execute(`drop database if exists ${databaseName} with (force)`);
      await admin.close(true);
    }
  });

  async function fixture() {
    const inner = createWageringService(orm);
    const wallet = await inner.createWallet({ playerId: crypto.randomUUID(), initialBalance: { amount: '100.00', currency: 'BRL' } });
    const body = { providerId: 'diagnostic-provider', externalTransactionId: crypto.randomUUID(), playerId: wallet.playerId,
      walletId: wallet.id, roundId: 'round', gameId: 'game', kind: 'BET', money: { amount: '25.00', currency: 'BRL' } };
    return { inner, wallet, body, key: crypto.randomUUID() };
  }

  test('a failed metrics or log sink does not replace a committed response or create extra effects on replay', async () => {
    for (const failingMetric of ['increment', 'observeDuration'] as const) {
      const { inner, wallet, body, key } = await fixture();
      const service = new ObservedWageringService(inner, 'http');
      const metric = spyOn(metrics, failingMetric).mockImplementation(() => { throw new Error('metrics unavailable'); });
      const output = spyOn(console, 'log').mockImplementation(() => { throw new Error('log unavailable'); });
      try {
        const result = await service.submit(body, key, { consumerName: 'diagnostic-test', messageId: crypto.randomUUID() });
        expect(result).toMatchObject({ status: 'PROCESSED', balance: { amount: '75.00', currency: 'BRL' }, idempotentReplay: false });
        expect(await service.submit(body, key)).toEqual({ ...result, idempotentReplay: true });
        const [state] = await orm.em.fork().execute<{ balance: string; reconstructed: string; debits: number; events: number }[]>(`
          select w.balance_cents::text as balance,
            (select sum(case when direction='CREDIT' then amount_cents else -amount_cents end)::text
              from dwp.wallet_ledger_entries where wallet_id=w.id) as reconstructed,
            (select count(*)::int from dwp.wallet_ledger_entries where wallet_id=w.id and direction='DEBIT') as debits,
            (select count(*)::int from dwp.outbox_messages where payload->'data'->>'transactionId'=?) as events
          from dwp.wallets w where w.id=?`, [result.transactionId, wallet.id]);
        expect(state).toEqual({ balance: '7500', reconstructed: '7500', debits: 1, events: 2 });
      } finally { metric.mockRestore(); output.mockRestore(); }
    }
  });

  test('a diagnostic failure cannot mask the original rollback error', async () => {
    const { wallet, body, key } = await fixture();
    const failure = new Error('original infrastructure failure');
    const service = new ObservedWageringService(createWageringService(orm, { beforeCommit: () => { throw failure; } }), 'http');
    const metric = spyOn(metrics, 'observeDuration').mockImplementation(() => { throw new Error('secondary diagnostics failure'); });
    const output = spyOn(console, 'log').mockImplementation(() => { throw new Error('log unavailable'); });
    try {
      await expect(service.submit(body, key)).rejects.toBe(failure);
      expect(await orm.em.fork().execute('select id from dwp.wager_transactions where idempotency_key=?', [key])).toHaveLength(0);
      const [state] = await orm.em.fork().execute<{ balance: string; entries: number }[]>(`
        select w.balance_cents::text as balance, count(l.id)::int as entries
        from dwp.wallets w left join dwp.wallet_ledger_entries l on l.wallet_id=w.id where w.id=? group by w.id`, [wallet.id]);
      expect(state).toEqual({ balance: '10000', entries: 1 });
    } finally { metric.mockRestore(); output.mockRestore(); }
  });
});
