import { createWageringQueries, createWageringService } from '../../dist/infrastructure/persistence/wagering-services.js';
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import type { MikroORM } from '@mikro-orm/postgresql';
import type { WageringService } from '../../dist/application/wagering-service.js';
import type { WageringQueries } from '../../dist/application/wagering-queries.js';
import { createDatabase } from '../../dist/infrastructure/persistence/database.js';

const integration = process.env.DWP_INTEGRATION === '1' ? describe : describe.skip;

integration('transaction lookup and stable ledger pagination', () => {
  const databaseName = `dwp_queries_${crypto.randomUUID().replaceAll('-', '')}`;
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
    service = createWageringService(orm);
    queries = createWageringQueries(orm);
  });

  afterAll(async () => {
    await orm?.close(true);
    if (admin) {
      await admin.em.getConnection().execute(`drop database if exists ${databaseName} with (force)`);
      await admin.close(true);
    }
  });

  function bet(wallet: { id: string; playerId: string }, amount = '1.00') {
    return {
      providerId: 'query-provider', externalTransactionId: crypto.randomUUID(),
      walletId: wallet.id, playerId: wallet.playerId, roundId: 'round', gameId: 'game',
      kind: 'BET', money: { amount, currency: 'BRL' },
    };
  }

  async function placeBet(wallet: { id: string; playerId: string }, amount = '1.00') {
    const body = bet(wallet, amount);
    return { body, result: await service.submit(body, `${body.providerId}:${body.externalTransactionId}`) };
  }

  async function assertConsistent(walletId: string): Promise<void> {
    const [row] = await orm.em.fork().execute<{ stored: string; calculated: string }[]>(`
      select w.balance_cents::text as stored,
        coalesce(sum(case when l.direction = 'CREDIT' then l.amount_cents::numeric else -l.amount_cents::numeric end), 0)::text as calculated
      from dwp.wallets w left join dwp.wallet_ledger_entries l on l.wallet_id = w.id
      where w.id = ? group by w.id`, [walletId]);
    expect(row).toBeDefined();
    expect(row!.stored).toBe(row!.calculated);
    expect(BigInt(row!.stored) >= 0n).toBe(true);
  }

  test('looks up the same immutable financial snapshot by internal and provider identities', async () => {
    const wallet = await service.createWallet({ playerId: crypto.randomUUID(), initialBalance: { amount: '100.00', currency: 'BRL' } });
    const { body, result } = await placeBet(wallet, '25.00');
    await placeBet(wallet, '5.00');
    const internal = await queries.getTransaction(result.transactionId);
    const external = await queries.getExternalTransaction(body.providerId, body.externalTransactionId);
    expect(internal).toEqual(external);
    expect(internal).toMatchObject({
      ...result, providerId: body.providerId, externalTransactionId: body.externalTransactionId,
      walletId: wallet.id, playerId: wallet.playerId, roundId: body.roundId, gameId: body.gameId,
      kind: 'BET', money: { amount: '25.00', currency: 'BRL' },
      balance: { amount: '75.00', currency: 'BRL' },
    });
    expect(internal).not.toHaveProperty('idempotencyKey');
    expect(internal).not.toHaveProperty('payloadHash');
    expect((await service.getWallet(wallet.id)).balance.amount).toBe('70.00');
    await expect(queries.getExternalTransaction('another-provider', body.externalTransactionId)).rejects.toMatchObject({ kind: 'not_found' });
    await assertConsistent(wallet.id);
  });

  test('includes rejected outcomes and preserves their observed balance', async () => {
    const wallet = await service.createWallet({ playerId: crypto.randomUUID(), initialBalance: { amount: '10.00', currency: 'BRL' } });
    const { result } = await placeBet(wallet, '20.00');
    expect(result.status).toBe('REJECTED');
    await placeBet(wallet, '5.00');
    expect(await queries.getTransaction(result.transactionId)).toMatchObject({
      ...result, status: 'REJECTED', balance: { amount: '10.00', currency: 'BRL' },
    });
    await assertConsistent(wallet.id);
  });

  test('keeps a page sequence bounded to its original history while new entries arrive', async () => {
    const wallet = await service.createWallet({ playerId: crypto.randomUUID(), initialBalance: { amount: '100.00', currency: 'BRL' } });
    for (let index = 0; index < 5; index += 1) await placeBet(wallet);
    const first = await queries.ledger(wallet.id, undefined, '2');
    expect(first.entries.map(entry => entry.walletVersion)).toEqual([1, 2]);
    expect(first.nextCursor).toBeString();
    expect(first.entries[0]).toMatchObject({
      direction: 'CREDIT', money: { amount: '100.00', currency: 'BRL' },
      balanceBefore: { amount: '0.00', currency: 'BRL' },
      balanceAfter: { amount: '100.00', currency: 'BRL' },
    });
    const inserted = await placeBet(wallet);
    const second = await queries.ledger(wallet.id, first.nextCursor, '2');
    const third = await queries.ledger(wallet.id, second.nextCursor, '2');
    const pages = [...first.entries, ...second.entries, ...third.entries];
    expect(pages.map(entry => entry.walletVersion)).toEqual([1, 2, 3, 4, 5, 6]);
    expect(new Set(pages.map(entry => entry.id)).size).toBe(6);
    expect(pages.some(entry => entry.transactionId === inserted.result.transactionId)).toBe(false);
    expect(third.nextCursor).toBeNull();
    const fresh = await queries.ledger(wallet.id);
    expect(fresh.entries.map(entry => entry.walletVersion)).toEqual([1, 2, 3, 4, 5, 6, 7]);
    expect(fresh.entries.at(-1)?.transactionId).toBe(inserted.result.transactionId);
    await assertConsistent(wallet.id);
  });

  test('returns an empty history for a zero opening and validates cursor ownership and limits', async () => {
    const wallet = await service.createWallet({ playerId: crypto.randomUUID(), initialBalance: { amount: '10.00', currency: 'BRL' } });
    const empty = await service.createWallet({ playerId: crypto.randomUUID(), initialBalance: { amount: '0.00', currency: 'BRL' } });
    await placeBet(wallet);
    const first = await queries.ledger(wallet.id, undefined, 1);
    expect(await queries.ledger(empty.id)).toEqual({ walletId: empty.id, entries: [], nextCursor: null });
    await expect(queries.ledger(empty.id, first.nextCursor)).rejects.toMatchObject({ kind: 'validation' });
    for (const cursor of ['', 'not-json', 'a===', [], null, Buffer.from('{"v":2}').toString('base64url')]) {
      await expect(queries.ledger(wallet.id, cursor)).rejects.toMatchObject({ kind: 'validation' });
    }
    for (const limit of ['0', '101', '1e1', '-1', ' 2', ['2'], null, 1.5]) {
      await expect(queries.ledger(wallet.id, undefined, limit)).rejects.toMatchObject({ kind: 'validation' });
    }
    await assertConsistent(wallet.id);
    await assertConsistent(empty.id);
  });

  test('distinguishes malformed identities and absent resources', async () => {
    await expect(queries.getTransaction('invalid')).rejects.toMatchObject({ kind: 'validation' });
    await expect(queries.getTransaction(crypto.randomUUID())).rejects.toMatchObject({ kind: 'not_found' });
    await expect(queries.getExternalTransaction('', 'external')).rejects.toMatchObject({ kind: 'validation' });
    await expect(queries.getExternalTransaction('provider', '')).rejects.toMatchObject({ kind: 'validation' });
    await expect(queries.getExternalTransaction('provider', crypto.randomUUID())).rejects.toMatchObject({ kind: 'not_found' });
    await expect(queries.ledger('invalid')).rejects.toMatchObject({ kind: 'validation' });
    await expect(queries.ledger(crypto.randomUUID())).rejects.toMatchObject({ kind: 'not_found' });
  });
});
