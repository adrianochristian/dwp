import { afterAll, beforeAll, describe, expect, spyOn, test } from 'bun:test';
import type { MikroORM } from '@mikro-orm/postgresql';
import { createDatabase } from '../../dist/infrastructure/persistence/database.js';
import { metrics } from '../../dist/observability/metrics.js';
import { ObservedWageringQueries, ObservedWageringService } from '../../dist/observability/observed-wagering.js';

const integration = process.env.DWP_INTEGRATION === '1' ? describe : describe.skip;

function count(name: string, labels: string): number {
  const prefix = `dwp_${name}_total{${labels}} `;
  const line = metrics.renderPrometheus().split('\n').find(value => value.startsWith(prefix));
  return line === undefined ? 0 : Number(line.slice(prefix.length));
}

integration('committed financial observability without sensitive payloads', () => {
  const databaseName = `dwp_observed_${crypto.randomUUID().replaceAll('-', '')}`;
  let admin: MikroORM;
  let orm: MikroORM;
  let service: ObservedWageringService;
  let queries: ObservedWageringQueries;

  beforeAll(async () => {
    admin = await createDatabase();
    await admin.em.getConnection().execute(`create database ${databaseName}`);
    const url = new URL(process.env.DATABASE_URL ?? 'postgresql://dwp:dwp@127.0.0.1:5432/dwp');
    url.pathname = `/${databaseName}`;
    orm = await createDatabase(url.toString());
    await orm.migrator.up();
    service = new ObservedWageringService(orm, 'http');
    queries = new ObservedWageringQueries(orm);
  });

  afterAll(async () => {
    await orm?.close(true);
    if (admin) {
      await admin.em.getConnection().execute(`drop database if exists ${databaseName} with (force)`);
      await admin.close(true);
    }
  });

  async function fixture() {
    const playerId = crypto.randomUUID();
    const wallet = await service.createWallet({ playerId, initialBalance: { amount: '100.00', currency: 'BRL' } });
    const body = { providerId: 'observed-provider', externalTransactionId: crypto.randomUUID(), playerId, walletId: wallet.id,
      roundId: 'observed-round', gameId: 'observed-game', kind: 'BET', money: { amount: '25.00', currency: 'BRL' } };
    return { wallet, body, key: `private-idempotency-${crypto.randomUUID()}` };
  }

  test('counts a committed result once and a replay separately, with correlated logs containing identifiers only', async () => {
    const { wallet, body, key } = await fixture();
    const before = count('transactions', 'status="PROCESSED",role="http"');
    const duplicateBefore = count('duplicates', 'status="PROCESSED",role="http"');
    const logs = spyOn(console, 'log').mockImplementation(() => {});
    try {
      const context = { consumerName: 'observed-test', messageId: crypto.randomUUID() };
      const result = await service.submit(body, key, context);
      expect(await service.submit(body, key, context)).toEqual({ ...result, idempotentReplay: true });
      expect(count('transactions', 'status="PROCESSED",role="http"')).toBe(before + 1);
      expect(count('duplicates', 'status="PROCESSED",role="http"')).toBe(duplicateBefore + 1);
      const records = logs.mock.calls.map(call => JSON.parse(call[0] as string) as Record<string, unknown>);
      expect(records).toHaveLength(2);
      expect(records[0]).toMatchObject({
        correlationId: result.transactionId, transactionId: result.transactionId, walletId: wallet.id,
        providerId: body.providerId, messageId: context.messageId, status: 'PROCESSED',
      });
      const serialized = JSON.stringify(records);
      expect(serialized).not.toContain('25.00');
      expect(serialized).not.toContain('75.00');
      expect(serialized).not.toContain(key);
      expect(serialized).not.toContain(body.externalTransactionId);
      expect((await queries.reconcile(wallet.id)).consistent).toBe(true);
      expect((await service.getWallet(wallet.id)).balance.amount).toBe('75.00');
      const entries = await queries.ledger(wallet.id);
      expect(entries.entries.filter(entry => entry.direction === 'DEBIT')).toHaveLength(1);
    } finally {
      logs.mockRestore();
    }
  });

  test('records validation and nested driver lock failures without raw input, SQL or partial financial effects', async () => {
    const { wallet, body, key } = await fixture();
    const lockBefore = count('lock_conflicts', 'role="http"');
    const logs = spyOn(console, 'log').mockImplementation(() => {});
    try {
      await expect(service.submit({ providerId: 'unvalidated-private-value', payload: 'private-raw-payload' }, key))
        .rejects.toMatchObject({ status: 400 });
      await orm.em.fork().execute(`create function dwp.observed_lock_fault() returns trigger language plpgsql as $$
        begin raise exception 'private SQL detail that must not be logged' using errcode = '40P01'; end $$;
        create trigger observed_lock_fault before insert on dwp.wager_transactions
        for each row execute function dwp.observed_lock_fault()`);
      try {
        await expect(service.submit(body, key)).rejects.toThrow();
      } finally {
        await orm.em.fork().execute(`drop trigger observed_lock_fault on dwp.wager_transactions;
          drop function dwp.observed_lock_fault()`);
      }
      expect(count('lock_conflicts', 'role="http"')).toBe(lockBefore + 1);
      const records = logs.mock.calls.map(call => JSON.parse(call[0] as string) as Record<string, unknown>);
      expect(records).toHaveLength(2);
      expect(records[0]).toMatchObject({ event: 'wager_transaction_error', code: 'INVALID_REQUEST' });
      expect(records[1]).toMatchObject({ event: 'wager_transaction_error', code: 'LOCK_CONFLICT' });
      for (const record of records) {
        expect(record).not.toHaveProperty('walletId');
        expect(record).not.toHaveProperty('providerId');
        expect(record).not.toHaveProperty('transactionId');
      }
      const serialized = JSON.stringify(records);
      expect(serialized).not.toContain('private');
      expect(serialized).not.toContain(key);
      expect((await service.getWallet(wallet.id)).balance.amount).toBe('100.00');
      expect((await queries.reconcile(wallet.id)).consistent).toBe(true);
    } finally {
      logs.mockRestore();
    }
  });

  test('counts a detected reconciliation discrepancy without silently correcting it', async () => {
    const { wallet } = await fixture();
    const before = count('reconciliation_inconsistencies', 'role="http"');
    const setBalance = (cents: string) => orm.em.fork().transactional(async em => {
      await em.execute('set local session_replication_role = replica');
      await em.execute('update dwp.wallets set balance_cents = ? where id = ?', [cents, wallet.id]);
    });
    const output = spyOn(console, 'log').mockImplementation(() => {});
    const errors = spyOn(console, 'error').mockImplementation(() => {});
    try {
      await setBalance('9999');
      expect((await queries.reconcile(wallet.id)).consistent).toBe(false);
      expect(count('reconciliation_inconsistencies', 'role="http"')).toBe(before + 1);
      expect((await service.getWallet(wallet.id)).balance.amount).toBe('99.99');
      expect(JSON.stringify(output.mock.calls)).not.toContain('99.99');
      expect(JSON.stringify(errors.mock.calls)).not.toContain('99.99');
    } finally {
      await setBalance('10000');
      output.mockRestore();
      errors.mockRestore();
    }
    expect((await queries.reconcile(wallet.id)).consistent).toBe(true);
  });
});
