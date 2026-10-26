import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import type { MikroORM } from '@mikro-orm/postgresql';
import { WageringService } from '../../dist/application/wagering-service.js';
import { createDatabase } from '../../dist/infrastructure/persistence/database.js';

const integration = process.env.DWP_INTEGRATION === '1' ? describe : describe.skip;

integration('full BET refunds', () => {
  const databaseName = `dwp_refund_${crypto.randomUUID().replaceAll('-', '')}`;
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

  const query = <T>(sql: string, values: unknown[] = []) => orm.em.getConnection().execute<T[]>(sql, values);

  async function fixture(initial = '100.00', amount = '25.00', processBet = true) {
    const playerId = crypto.randomUUID();
    const wallet = await service.createWallet({ playerId, initialBalance: { amount: initial, currency: 'BRL' } });
    const bet = {
      providerId: 'provider-a', externalTransactionId: crypto.randomUUID(), playerId, walletId: wallet.id,
      roundId: 'round-1', gameId: 'game-1', kind: 'BET', money: { amount, currency: 'BRL' },
    };
    const betResult = processBet ? await service.submit(bet, crypto.randomUUID()) : undefined;
    const refund = { ...bet, kind: 'REFUND', externalTransactionId: crypto.randomUUID(), referenceExternalTransactionId: bet.externalTransactionId };
    return { wallet, bet, betResult, refund, key: `provider-a:${refund.externalTransactionId}` };
  }

  async function assertConsistent(walletId: string): Promise<void> {
    const rows = await query<{ balance_cents: string; calculated: string }>(`select w.balance_cents::text,
      coalesce(sum(case when l.direction = 'CREDIT' then l.amount_cents::numeric else -l.amount_cents::numeric end), 0)::text as calculated
      from dwp.wallets w left join dwp.wallet_ledger_entries l on l.wallet_id = w.id
      where w.id = ? group by w.id`, [walletId]);
    expect(rows[0]!.balance_cents).toBe(rows[0]!.calculated);
    expect(BigInt(rows[0]!.balance_cents) >= 0n).toBe(true);
  }

  test('credits the complete processed BET once and replays the original result', async () => {
    const { wallet, bet, betResult, refund, key } = await fixture();
    const original = await service.submit(refund, key);
    expect(original).toMatchObject({ status: 'PROCESSED', balance: { amount: '100.00', currency: 'BRL' }, idempotentReplay: false });
    const entries = await query<{ direction: string; amount_cents: string }>('select direction, amount_cents::text from dwp.wallet_ledger_entries where transaction_id = ?', [original.transactionId]);
    expect(entries).toEqual([{ direction: 'CREDIT', amount_cents: '2500' }]);
    expect((await query<{ reference_transaction_id: string }>('select reference_transaction_id from dwp.wager_transactions where id = ?', [original.transactionId]))[0]!.reference_transaction_id).toBe(betResult!.transactionId);
    await service.submit({ ...bet, externalTransactionId: crypto.randomUUID(), money: { amount: '10.00', currency: 'BRL' } }, crypto.randomUUID());
    expect(await service.submit(refund, key)).toEqual({ ...original, idempotentReplay: true });
    expect((await service.getWallet(wallet.id)).balance.amount).toBe('90.00');
    await assertConsistent(wallet.id);
  });

  test('serializes two independent refunds into one credit and one persistent rejection', async () => {
    const { wallet, refund, key } = await fixture();
    const second = { ...refund, externalTransactionId: crypto.randomUUID() };
    const secondKey = crypto.randomUUID();
    const results = await Promise.all([service.submit(refund, key), service.submit(second, secondKey)]);
    expect(results.map(result => result.status).sort()).toEqual(['PROCESSED', 'REJECTED']);
    expect(results.find(result => result.status === 'REJECTED')?.failureCode).toBe('REVERSAL_ALREADY_PROCESSED');
    expect(await service.submit(refund, key)).toEqual({ ...results[0]!, idempotentReplay: true });
    expect(await service.submit(second, secondKey)).toEqual({ ...results[1]!, idempotentReplay: true });
    expect((await service.getWallet(wallet.id)).balance.amount).toBe('100.00');
    expect(await query("select l.id from dwp.wallet_ledger_entries l join dwp.wager_transactions t on t.id = l.transaction_id where l.wallet_id = ? and t.kind = 'REFUND'", [wallet.id])).toHaveLength(1);
    await assertConsistent(wallet.id);
  });

  test('rejects REFUND after ROLLBACK already restored the BET and replays the rejection snapshot', async () => {
    const { wallet, bet, betResult, refund, key } = await fixture();
    expect(betResult).toMatchObject({ status: 'PROCESSED', balance: { amount: '75.00', currency: 'BRL' } });
    const rollback = { ...refund, kind: 'ROLLBACK', externalTransactionId: crypto.randomUUID() };
    const reversed = await service.submit(rollback, crypto.randomUUID());
    expect(reversed).toMatchObject({ status: 'PROCESSED', balance: { amount: '100.00', currency: 'BRL' } });
    const beforeRefund = await service.getWallet(wallet.id);
    expect(beforeRefund.version).toBe(3);
    const balanceEventsBefore = await query<{ id: string }>(
      "select id from dwp.outbox_messages where event_type = 'WalletBalanceChanged' and aggregate_id = ? order by id", [wallet.id],
    );

    const rejected = await service.submit(refund, key);
    expect(rejected).toMatchObject({
      status: 'REJECTED', failureCode: 'REVERSAL_ALREADY_PROCESSED',
      balance: { amount: '100.00', currency: 'BRL' }, idempotentReplay: false,
    });
    expect(await service.getWallet(wallet.id)).toEqual(beforeRefund);
    expect(await query('select id from dwp.wallet_ledger_entries where transaction_id = ?', [rejected.transactionId])).toHaveLength(0);
    expect(await query("select event_type from dwp.outbox_messages where payload->'data'->>'transactionId' = ?", [rejected.transactionId]))
      .toEqual([{ event_type: 'WagerTransactionRejected' }]);
    expect(await query("select id from dwp.outbox_messages where event_type = 'WalletBalanceChanged' and payload->'data'->>'transactionId' = ?",
      [rejected.transactionId])).toHaveLength(0);
    expect(await query<{ id: string }>(
      "select id from dwp.outbox_messages where event_type = 'WalletBalanceChanged' and aggregate_id = ? order by id", [wallet.id],
    )).toEqual(balanceEventsBefore);
    await assertConsistent(wallet.id);

    await service.submit({ ...bet, externalTransactionId: crypto.randomUUID(), money: { amount: '10.00', currency: 'BRL' } }, crypto.randomUUID());
    const beforeReplay = await service.getWallet(wallet.id);
    expect(beforeReplay).toMatchObject({ balance: { amount: '90.00', currency: 'BRL' }, version: 4 });
    const eventsBeforeReplay = await query<{ id: string }>(
      "select id from dwp.outbox_messages where payload->'data'->>'walletId' = ? order by id", [wallet.id],
    );
    const ledgerBeforeReplay = await query<{ id: string }>(
      'select id from dwp.wallet_ledger_entries where wallet_id = ? order by id', [wallet.id],
    );
    expect(await service.submit(refund, key)).toEqual({ ...rejected, idempotentReplay: true });
    expect(await service.getWallet(wallet.id)).toEqual(beforeReplay);
    expect(await query<{ id: string }>(
      "select id from dwp.outbox_messages where payload->'data'->>'walletId' = ? order by id", [wallet.id],
    )).toEqual(eventsBeforeReplay);
    expect(await query<{ id: string }>(
      'select id from dwp.wallet_ledger_entries where wallet_id = ? order by id', [wallet.id],
    )).toEqual(ledgerBeforeReplay);
    await assertConsistent(wallet.id);
  });

  test('a rejected partial ROLLBACK does not consume the first full REFUND of the BET', async () => {
    const { wallet, betResult, refund, key } = await fixture();
    const partialRollback = {
      ...refund, kind: 'ROLLBACK', externalTransactionId: crypto.randomUUID(), money: { amount: '24.00', currency: 'BRL' },
    };
    const partialKey = crypto.randomUUID();
    const beforeRejection = await service.getWallet(wallet.id);
    expect(beforeRejection).toMatchObject({ balance: { amount: '75.00', currency: 'BRL' }, version: 2 });
    const rejected = await service.submit(partialRollback, partialKey);
    expect(rejected).toMatchObject({
      status: 'REJECTED', failureCode: 'REFERENCE_AMOUNT_MISMATCH',
      balance: { amount: '75.00', currency: 'BRL' }, idempotentReplay: false,
    });
    expect(await service.getWallet(wallet.id)).toEqual(beforeRejection);
    expect(await query('select id from dwp.wallet_ledger_entries where transaction_id = ?', [rejected.transactionId])).toHaveLength(0);
    expect(await query("select event_type from dwp.outbox_messages where payload->'data'->>'transactionId' = ?", [rejected.transactionId]))
      .toEqual([{ event_type: 'WagerTransactionRejected' }]);
    expect(await query("select id from dwp.outbox_messages where event_type = 'WalletBalanceChanged' and payload->'data'->>'transactionId' = ?",
      [rejected.transactionId])).toHaveLength(0);
    await assertConsistent(wallet.id);

    const processed = await service.submit(refund, key);
    expect(processed).toMatchObject({ status: 'PROCESSED', balance: { amount: '100.00', currency: 'BRL' }, idempotentReplay: false });
    const afterRefund = await service.getWallet(wallet.id);
    expect(afterRefund).toMatchObject({ balance: { amount: '100.00', currency: 'BRL' }, version: 3 });
    expect(await query("select kind from dwp.wager_transactions where reference_transaction_id = ? and status = 'PROCESSED' and kind in ('REFUND', 'ROLLBACK')",
      [betResult!.transactionId])).toEqual([{ kind: 'REFUND' }]);
    expect(await query('select direction, amount_cents::text from dwp.wallet_ledger_entries where transaction_id = ?', [processed.transactionId]))
      .toEqual([{ direction: 'CREDIT', amount_cents: '2500' }]);
    const eventsBeforeReplay = await query<{ id: string }>(
      "select id from dwp.outbox_messages where payload->'data'->>'walletId' = ? order by id", [wallet.id],
    );
    const ledgerBeforeReplay = await query<{ id: string }>(
      'select id from dwp.wallet_ledger_entries where wallet_id = ? order by id', [wallet.id],
    );
    expect(await service.submit(partialRollback, partialKey)).toEqual({ ...rejected, idempotentReplay: true });
    expect(await service.getWallet(wallet.id)).toEqual(afterRefund);
    expect(await query<{ id: string }>(
      "select id from dwp.outbox_messages where payload->'data'->>'walletId' = ? order by id", [wallet.id],
    )).toEqual(eventsBeforeReplay);
    expect(await query<{ id: string }>(
      'select id from dwp.wallet_ledger_entries where wallet_id = ? order by id', [wallet.id],
    )).toEqual(ledgerBeforeReplay);
    await assertConsistent(wallet.id);
  });

  test('waits when delivered before BET and applies under the original transaction identity', async () => {
    const { wallet, bet, refund, key } = await fixture('100.00', '25.00', false);
    const pending = await service.submit(refund, key);
    expect(pending.status).toBe('PENDING_REFERENCE');
    expect((await service.getWallet(wallet.id)).version).toBe(1);
    await service.submit(bet, crypto.randomUUID());
    const processed = await service.submit(refund, key);
    expect(processed).toMatchObject({ transactionId: pending.transactionId, status: 'PROCESSED', balance: wallet.balance });
    expect(await query('select id from dwp.wallet_ledger_entries where transaction_id = ?', [pending.transactionId])).toHaveLength(1);
    await assertConsistent(wallet.id);
  });

  test('rejects partial reversals and mismatched round, wallet and currency without effects', async () => {
    const first = await fixture();
    const other = await fixture();
    const cases = [
      { body: { ...first.refund, money: { amount: '24.99', currency: 'BRL' } }, code: 'REFERENCE_AMOUNT_MISMATCH', walletId: first.wallet.id },
      { body: { ...first.refund, roundId: 'other-round' }, code: 'REFERENCE_MISMATCH', walletId: first.wallet.id },
      { body: { ...other.refund, referenceExternalTransactionId: first.bet.externalTransactionId }, code: 'REFERENCE_MISMATCH', walletId: other.wallet.id },
      { body: { ...first.refund, money: { amount: '25.00', currency: 'USD' } }, code: 'CURRENCY_MISMATCH', walletId: first.wallet.id },
    ];
    for (const item of cases) {
      const body = { ...item.body, externalTransactionId: crypto.randomUUID() };
      const key = crypto.randomUUID();
      const rejected = await service.submit(body, key);
      expect(rejected).toMatchObject({ status: 'REJECTED', failureCode: item.code });
      expect(await service.submit(body, key)).toEqual({ ...rejected, idempotentReplay: true });
      expect(await query('select id from dwp.wallet_ledger_entries where transaction_id = ?', [rejected.transactionId])).toHaveLength(0);
      await assertConsistent(item.walletId);
    }
  });

  test('requires a processed BET, distinguishes rejected and failed references, and scopes lookup by provider', async () => {
    const first = await fixture('100.00', '101.00');
    const rejected = await service.submit(first.refund, first.key);
    expect(rejected).toMatchObject({ status: 'REJECTED', failureCode: 'REFERENCE_REJECTED' });
    const winExternal = crypto.randomUUID();
    await service.submit({ ...first.bet, kind: 'WIN', externalTransactionId: winExternal, money: { amount: '10.00', currency: 'BRL' } }, crypto.randomUUID());
    const invalid = await service.submit({ ...first.refund, externalTransactionId: crypto.randomUUID(), referenceExternalTransactionId: winExternal, money: { amount: '10.00', currency: 'BRL' } }, crypto.randomUUID());
    expect(invalid).toMatchObject({ status: 'REJECTED', failureCode: 'INVALID_REFERENCE_KIND' });
    const providerScoped = await service.submit({ ...first.refund, providerId: 'other-provider', externalTransactionId: crypto.randomUUID() }, crypto.randomUUID());
    expect(providerScoped.status).toBe('PENDING_REFERENCE');
    const failedExternal = crypto.randomUUID();
    await query(`insert into dwp.wager_transactions
      (id, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id, player_id,
       round_id, game_id, kind, amount_cents, currency, status, failure_code, result)
      values (?, ?, ?, ?, ?, ?, ?, ?, ?, 'BET', '2500', 'BRL', 'FAILED', 'PERMANENT_INFRASTRUCTURE', '{}')`, [
      crypto.randomUUID(), first.bet.providerId, failedExternal, crypto.randomUUID(), 'a'.repeat(64),
      first.wallet.id, first.bet.playerId, first.bet.roundId, first.bet.gameId,
    ]);
    const failed = await service.submit({ ...first.refund, externalTransactionId: crypto.randomUUID(), referenceExternalTransactionId: failedExternal, money: { amount: '25.00', currency: 'BRL' } }, crypto.randomUUID());
    expect(failed).toMatchObject({ status: 'REJECTED', failureCode: 'REFERENCE_FAILED' });
    await assertConsistent(first.wallet.id);
  });

  test('rejects a refund whose credit would overflow without changing the wallet', async () => {
    const { wallet, bet, refund, key } = await fixture('92233720368547758.07', '0.01');
    await service.submit({ ...bet, kind: 'WIN', externalTransactionId: crypto.randomUUID() }, crypto.randomUUID());
    const result = await service.submit(refund, key);
    expect(result).toMatchObject({ status: 'REJECTED', failureCode: 'MONEY_OVERFLOW', balance: wallet.balance });
    expect(await query('select id from dwp.wallet_ledger_entries where transaction_id = ?', [result.transactionId])).toHaveLength(0);
    await assertConsistent(wallet.id);
  });

  test('requires a nonempty reference and positive amount at the contract boundary', async () => {
    const { refund } = await fixture();
    const { referenceExternalTransactionId: _reference, ...withoutReference } = refund;
    await expect(service.submit(withoutReference, crypto.randomUUID())).rejects.toMatchObject({ status: 400 });
    await expect(service.submit({ ...refund, referenceExternalTransactionId: '' }, crypto.randomUUID())).rejects.toMatchObject({ status: 400 });
    await expect(service.submit({ ...refund, money: { amount: '0.00', currency: 'BRL' } }, crypto.randomUUID())).rejects.toMatchObject({ status: 400 });
  });
});
