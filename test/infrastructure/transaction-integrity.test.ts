import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import type { MikroORM } from '@mikro-orm/postgresql';
import { canonicalHash, wagerPayload, wagerRequest } from '../../dist/application/contracts.js';
import { WageringQueries } from '../../dist/application/wagering-queries.js';
import { WageringService } from '../../dist/application/wagering-service.js';
import { createDatabase } from '../../dist/infrastructure/persistence/database.js';

const integration = process.env.DWP_INTEGRATION === '1' ? describe : describe.skip;

integration('immutable wagering identity and terminal audit retention', () => {
  const databaseName = `dwp_integrity_${crypto.randomUUID().replaceAll('-', '')}`;
  let admin: MikroORM;
  let orm: MikroORM;
  let service: WageringService;
  let queries: WageringQueries;

  beforeAll(async () => {
    admin = await createDatabase();
    await admin.em.getConnection().execute(`create database ${databaseName}`);
    const url = new URL(process.env.DATABASE_URL ?? 'postgresql://dwp:dwp@127.0.0.1:5432/dwp');
    url.pathname = `/${databaseName}`;
    orm = await createDatabase(url.toString());
    await orm.migrator.up();
    service = new WageringService(orm);
    queries = new WageringQueries(orm);
  });

  afterAll(async () => {
    await orm?.close(true);
    if (admin) {
      await admin.em.getConnection().execute(`drop database if exists ${databaseName} with (force)`);
      await admin.close(true);
    }
  });

  const query = <T>(sql: string, values: unknown[] = []) => orm.em.fork().execute<T[]>(sql, values);

  async function fixture() {
    const wallet = await service.createWallet({ playerId: crypto.randomUUID(), initialBalance: { amount: '100.00', currency: 'BRL' } });
    const command = (kind: string, amount: string, referenceExternalTransactionId?: string) => ({
      providerId: 'integrity-provider', externalTransactionId: crypto.randomUUID(),
      walletId: wallet.id, playerId: wallet.playerId, roundId: 'integrity-round', gameId: 'integrity-game', kind,
      money: { amount, currency: 'BRL' },
      ...(referenceExternalTransactionId === undefined ? {} : { referenceExternalTransactionId }),
    });
    return { wallet, command };
  }

  async function assertConsistent(walletId: string): Promise<void> {
    const reconciliation = await queries.reconcile(walletId);
    expect(reconciliation.consistent).toBe(true);
    expect(reconciliation.storedBalance).toEqual(reconciliation.calculatedBalance);
    expect(BigInt(reconciliation.storedBalance.amount.replace('.', '')) >= 0n).toBe(true);
  }

  async function identityChecksum(transactionId: string): Promise<string> {
    const [row] = await query<{ checksum: string }>(`select md5((to_jsonb(t) - array[
      'status','failure_code','result','retry_count','next_attempt_at','processed_at','reference_transaction_id'
      ]::text[])::text) as checksum from dwp.wager_transactions t where id = ?`, [transactionId]);
    return row!.checksum;
  }

  async function seedPending(body: unknown, key: string): Promise<string> {
    const input = wagerRequest(body);
    const id = crypto.randomUUID();
    await query(`insert into dwp.wager_transactions
      (id, provider_id, external_transaction_id, idempotency_key, payload_hash,
       wallet_id, player_id, round_id, game_id, kind, amount_cents, currency,
       reference_external_transaction_id, status)
      values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING')`, [
      id, input.providerId, input.externalTransactionId, key, canonicalHash(wagerPayload(input)),
      input.walletId, input.playerId, input.roundId, input.gameId, input.kind,
      input.money.cents.toString(), input.money.currency, input.referenceExternalTransactionId ?? null,
    ]);
    return id;
  }

  test('rejects deletion of REJECTED, FAILED and processed LOSS and retains their original replay after funds change', async () => {
    const { wallet, command } = await fixture();
    const cases = [
      { body: command('BET', '200.00'), key: crypto.randomUUID(), failed: false },
      { body: command('BET', '5.00'), key: crypto.randomUUID(), failed: true },
      { body: command('LOSS', '0.00'), key: crypto.randomUUID(), failed: false },
    ];
    const originals = await Promise.all(cases.map(async item => ({
      ...item,
      result: item.failed
        ? await service.failSubmission(item.body, item.key, 'TEST_FAILURE')
        : await service.submit(item.body, item.key),
    })));
    expect(originals.map(item => item.result.status)).toEqual(['REJECTED', 'FAILED', 'PROCESSED']);
    for (const item of originals) {
      expect(item.result.balance.amount).toBe('100.00');
      await expect(query('delete from dwp.wager_transactions where id = ?', [item.result.transactionId]))
        .rejects.toMatchObject({ code: '23514' });
    }
    await service.submit(command('WIN', '200.00'), crypto.randomUUID());
    expect((await service.getWallet(wallet.id)).balance.amount).toBe('300.00');
    for (const item of originals) {
      expect(await service.submit(item.body, item.key)).toEqual({ ...item.result, idempotentReplay: true });
      expect(await query('select id from dwp.wallet_ledger_entries where transaction_id = ?', [item.result.transactionId])).toHaveLength(0);
    }
    await assertConsistent(wallet.id);
  });

  test('protects every pending business field and hash, including null references, and applies the original WIN amount', async () => {
    const { wallet, command } = await fixture();
    const otherWallet = await service.createWallet({ playerId: crypto.randomUUID(), initialBalance: { amount: '0.00', currency: 'BRL' } });
    const bet = command('BET', '5.00');
    const win = command('WIN', '1.00', bet.externalTransactionId);
    const key = crypto.randomUUID();
    const pending = await service.submit(win, key);
    expect(pending.status).toBe('PENDING_REFERENCE');
    const before = await identityChecksum(pending.transactionId);
    const mutations: [string, unknown][] = [
      ['id', crypto.randomUUID()], ['provider_id', 'other-provider'],
      ['external_transaction_id', crypto.randomUUID()], ['idempotency_key', crypto.randomUUID()],
      ['payload_hash', 'b'.repeat(64)], ['wallet_id', otherWallet.id], ['player_id', 'other-player'],
      ['round_id', 'other-round'], ['game_id', 'other-game'], ['kind', 'BET'],
      ['amount_cents', '200'], ['currency', 'USD'], ['reference_external_transaction_id', null],
      ['created_at', new Date(Date.now() + 86_400_000)],
    ];
    for (const [column, value] of mutations) {
      await expect(query(`update dwp.wager_transactions set ${column} = ? where id = ?`, [value, pending.transactionId]))
        .rejects.toMatchObject({ code: '23514' });
    }
    expect(await identityChecksum(pending.transactionId)).toBe(before);
    const nullReferenceId = await seedPending(command('WIN', '1.00'), crypto.randomUUID());
    await expect(query('update dwp.wager_transactions set reference_external_transaction_id = ? where id = ?', [crypto.randomUUID(), nullReferenceId]))
      .rejects.toMatchObject({ code: '23514' });

    await service.submit(bet, crypto.randomUUID());
    const processed = await service.submit(win, key);
    expect(processed.status).toBe('PROCESSED');
    expect(processed.balance.amount).toBe('96.00');
    expect(await identityChecksum(pending.transactionId)).toBe(before);
    expect(await query<{ amount_cents: string; balance_after_cents: string }>(`
      select amount_cents::text, balance_after_cents::text from dwp.wallet_ledger_entries where transaction_id = ?`, [pending.transactionId]))
      .toEqual([{ amount_cents: '100', balance_after_cents: '9600' }]);
    expect(await service.submit(win, key)).toEqual({ ...processed, idempotentReplay: true });
    await assertConsistent(wallet.id);
    await assertConsistent(otherWallet.id);
  });

  test('allows pending response, retry and reference-resolution metadata to change until processing or rejection becomes terminal', async () => {
    for (const referenceAmount of ['25.00', '200.00']) {
      const { wallet, command } = await fixture();
      const bet = command('BET', referenceAmount);
      const refund = command('REFUND', referenceAmount, bet.externalTransactionId);
      const key = crypto.randomUUID();
      const pending = await service.submit(refund, key);
      const before = await identityChecksum(pending.transactionId);
      await service.submit(command('WIN', '10.00'), crypto.randomUUID());
      const retry = await service.submit(refund, key);
      expect(retry.status).toBe('PENDING_REFERENCE');
      expect(retry.balance.amount).toBe('110.00');
      const [retried] = await query<{ retry_count: number; result: { balance: { amount: string } }; next_attempt_at: string }>(
        'select retry_count, result, next_attempt_at from dwp.wager_transactions where id = ?', [pending.transactionId],
      );
      expect(retried!.retry_count).toBe(2);
      expect(retried!.result.balance.amount).toBe('110.00');
      expect(new Date(retried!.next_attempt_at).getTime()).toBeGreaterThan(Date.now());
      await service.submit(bet, crypto.randomUUID());
      const terminal = await service.submit(refund, key);
      if (referenceAmount === '25.00') {
        expect(terminal.status).toBe('PROCESSED');
        expect(await queries.getTransaction(terminal.transactionId)).toMatchObject({
          status: 'PROCESSED', referenceExternalTransactionId: bet.externalTransactionId,
        });
        const [resolved] = await query<{ reference_transaction_id: string | null; processed_at: string | null }>(
          'select reference_transaction_id, processed_at from dwp.wager_transactions where id = ?', [terminal.transactionId],
        );
        expect(resolved!.reference_transaction_id).not.toBeNull();
        expect(resolved!.processed_at).not.toBeNull();
      } else {
        expect(terminal.status).toBe('REJECTED');
        expect(terminal.failureCode).toBe('REFERENCE_REJECTED');
      }
      expect(terminal.balance.amount).toBe('110.00');
      expect(await identityChecksum(pending.transactionId)).toBe(before);
      expect(await service.submit(refund, key)).toEqual({ ...terminal, idempotentReplay: true });
      await assertConsistent(wallet.id);
    }
  });

  test('reverses and reapplies only the new guards without rewriting financial history', async () => {
    const { wallet, command } = await fixture();
    const loss = await service.submit(command('LOSS', '0.00'), crypto.randomUUID());
    const pendingId = await seedPending(command('WIN', '1.00'), crypto.randomUUID());
    const financialSnapshot = async () => {
      const snapshots: { table: string; count: string; checksum: string }[] = [];
      for (const table of ['wallets', 'wager_transactions', 'wallet_ledger_entries', 'inbox_messages', 'outbox_messages']) {
        const [snapshot] = await query<{ count: string; checksum: string }>(`
          select count(*)::text as count,
            md5(coalesce(string_agg(to_jsonb(t)::text, '' order by to_jsonb(t)::text), '')) as checksum
          from dwp.${table} t`);
        snapshots.push({ table, ...snapshot! });
      }
      return snapshots;
    };
    const guards = async () => {
      const [row] = await query<{ new_guards: number; previous_terminal_guard: boolean; new_function: boolean }>(`
        select (select count(*)::int from pg_trigger where tgrelid = 'dwp.wager_transactions'::regclass
          and tgname in ('transaction_identity_immutable','transaction_terminal_delete')) as new_guards,
          exists(select 1 from pg_trigger where tgrelid = 'dwp.wager_transactions'::regclass
            and tgname = 'transaction_terminal_guard') as previous_terminal_guard,
          to_regprocedure('dwp.guard_transaction_identity_and_delete()') is not null as new_function`);
      return row!;
    };
    const before = await financialSnapshot();
    expect(await guards()).toEqual({ new_guards: 2, previous_terminal_guard: true, new_function: true });
    await orm.migrator.down({ to: 'Migration20261026000100' });
    expect(await guards()).toEqual({ new_guards: 0, previous_terminal_guard: true, new_function: false });
    expect(await financialSnapshot()).toEqual(before);
    await expect(query("update dwp.wager_transactions set status = 'PENDING' where id = ?", [loss.transactionId]))
      .rejects.toMatchObject({ code: '23514' });
    await orm.migrator.up();
    expect(await guards()).toEqual({ new_guards: 2, previous_terminal_guard: true, new_function: true });
    expect(await financialSnapshot()).toEqual(before);
    await expect(query('delete from dwp.wager_transactions where id = ?', [loss.transactionId]))
      .rejects.toMatchObject({ code: '23514' });
    await expect(query('update dwp.wager_transactions set amount_cents = 200 where id = ?', [pendingId]))
      .rejects.toMatchObject({ code: '23514' });
    await assertConsistent(wallet.id);
  });
});
