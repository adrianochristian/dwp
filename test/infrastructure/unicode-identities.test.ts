import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import type { MikroORM } from '@mikro-orm/postgresql';
import { canonicalHash, wagerPayload, wagerRequest } from '../../src/application/contracts.js';
import { WageringService } from '../../src/application/wagering-service.js';
import { createDatabase } from '../../src/infrastructure/persistence/database.js';

const integration = process.env.DWP_INTEGRATION === '1' ? describe : describe.skip;

integration('Unicode identities survive PostgreSQL round trips', () => {
  const databaseName = `dwp_unicode_${crypto.randomUUID().replaceAll('-', '')}`;
  let admin: MikroORM;
  let orm: MikroORM;
  let service: WageringService;

  beforeAll(async () => {
    admin = await createDatabase();
    await admin.em.fork().execute(`create database ${databaseName}`);
    const url = new URL(process.env.DATABASE_URL ?? 'postgresql://dwp:dwp@127.0.0.1:5432/dwp');
    url.pathname = `/${databaseName}`;
    orm = await createDatabase(url.toString());
    await orm.migrator.up();
    service = new WageringService(orm);
  });

  afterAll(async () => {
    await orm?.close(true);
    if (admin) {
      await admin.em.fork().execute(`drop database if exists ${databaseName} with (force)`);
      await admin.close(true);
    }
  });

  const query = <T>(sql: string, values: unknown[] = []) => orm.em.fork().execute<T[]>(sql, values);

  test('rejects malformed identity strings with INVALID_REQUEST and leaves no financial effects', async () => {
    await expect(service.createWallet({ playerId: '\ud800', initialBalance: { amount: '100.00', currency: 'BRL' } }))
      .rejects.toMatchObject({ status: 400, response: { code: 'INVALID_REQUEST' } });
    expect(await query('select id from dwp.wallets')).toHaveLength(0);

    const wallet = await service.createWallet({ playerId: 'valid-player', initialBalance: { amount: '100.00', currency: 'BRL' } });
    const body = {
      providerId: 'unicode-provider', externalTransactionId: 'valid-bet', playerId: wallet.playerId,
      walletId: wallet.id, roundId: 'round', gameId: 'game', kind: 'BET', money: { amount: '1.00', currency: 'BRL' },
    };
    for (const field of ['providerId', 'externalTransactionId', 'playerId', 'walletId', 'roundId', 'gameId']) {
      await expect(service.submit({ ...body, [field]: 'bad\udfff' }, 'unicode-bad'))
        .rejects.toMatchObject({ status: 400, response: { code: 'INVALID_REQUEST' } });
    }
    await expect(service.submit(body, 'bad\ud800')).rejects.toMatchObject({ status: 400, response: { code: 'INVALID_REQUEST' } });
    await expect(service.submit(body, 'valid-key', { consumerName: 'consumer', messageId: 'bad\ud800' }))
      .rejects.toMatchObject({ status: 400, response: { code: 'INVALID_REQUEST' } });
    expect(await service.getWallet(wallet.id)).toEqual(wallet);
    expect(await query("select id from dwp.wager_transactions where kind <> 'OPENING'")).toHaveLength(0);
    expect(await query('select message_id from dwp.inbox_messages')).toHaveLength(0);
    expect(await query('select id from dwp.wallet_ledger_entries')).toHaveLength(1);
    expect(await query('select id from dwp.outbox_messages')).toHaveLength(2);
  });

  test('preserves emoji and combining characters through wallet reads, canonical hashes and inbox replay', async () => {
    const playerId = 'player-😀-e\u0301';
    const wallet = await service.createWallet({ playerId, initialBalance: { amount: '100.00', currency: 'BRL' } });
    expect(wallet.playerId).toBe(playerId);
    expect((await service.getWallet(wallet.id)).playerId).toBe(playerId);
    const body = {
      providerId: 'provider-😀', externalTransactionId: 'bet-😀-e\u0301', playerId,
      walletId: wallet.id, roundId: 'round-😀', gameId: 'game-𐐀', kind: 'BET', money: { amount: '1.00', currency: 'BRL' },
    };
    const key = 'key-😀-e\u0301';
    const inbox = { consumerName: 'consumer-😀', messageId: 'message-😀-e\u0301' };
    const original = await service.submit(body, key, inbox);
    const stored = (await query<{ external_transaction_id: string; player_id: string; payload_hash: string }>(
      'select external_transaction_id, player_id, payload_hash from dwp.wager_transactions where id = ?', [original.transactionId],
    ))[0]!;
    expect(stored.external_transaction_id).toBe(body.externalTransactionId);
    expect(stored.player_id).toBe(playerId);
    expect(stored.payload_hash).toBe(canonicalHash(wagerPayload(wagerRequest(body))));
    const reordered = Object.fromEntries(Object.entries({ ...body, transport: 'ignored' }).reverse());
    expect(await service.submit(reordered, key, inbox)).toEqual({ ...original, idempotentReplay: true });

    // The two normalization forms remain distinct opaque provider identities.
    const composed = { ...body, externalTransactionId: 'bet-😀-é' };
    expect((await service.submit(composed, 'key-😀-é')).status).toBe('PROCESSED');
    expect((await service.getWallet(wallet.id)).balance.amount).toBe('98.00');
    const [reconciliation] = await query<{ stored: string; calculated: string }>(`select w.balance_cents::text as stored,
      sum(case when l.direction = 'CREDIT' then l.amount_cents::numeric else -l.amount_cents::numeric end)::text as calculated
      from dwp.wallets w join dwp.wallet_ledger_entries l on l.wallet_id = w.id where w.id = ? group by w.id`, [wallet.id]);
    expect(reconciliation).toEqual({ stored: '9800', calculated: '9800' });
    expect(await query('select id from dwp.wallet_ledger_entries where wallet_id = ?', [wallet.id])).toHaveLength(3);
  });
});
