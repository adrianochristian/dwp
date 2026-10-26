import { createWageringService } from '../../dist/infrastructure/persistence/wagering-services.js';
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import type { MikroORM } from '@mikro-orm/postgresql';
import type { WageringService } from '../../dist/application/wagering-service.js';
import { createDatabase } from '../../dist/infrastructure/persistence/database.js';

const integration = process.env.DWP_INTEGRATION === '1' ? describe : describe.skip;

interface Submission {
  transactionId: string;
  status: string;
  balance: { amount: string; currency: string };
  idempotentReplay: boolean;
  failureCode?: string;
}

integration('financial correctness across independent application processes', () => {
  const databaseName = `dwp_process_${crypto.randomUUID().replaceAll('-', '')}`;
  const children = new Set<ReturnType<typeof Bun.spawn>>();
  let admin: MikroORM;
  let orm: MikroORM;
  let service: WageringService;
  let databaseUrl: string;

  beforeAll(async () => {
    admin = await createDatabase();
    await admin.em.getConnection().execute(`create database ${databaseName}`);
    const url = new URL(process.env.DATABASE_URL ?? 'postgresql://dwp:dwp@127.0.0.1:5432/dwp');
    url.pathname = `/${databaseName}`;
    databaseUrl = url.toString();
    orm = await createDatabase(databaseUrl);
    await orm.migrator.up();
    service = createWageringService(orm);
  });

  afterAll(async () => {
    await Promise.all([...children].map(stop));
    await orm?.close(true);
    if (admin) {
      await admin.em.getConnection().execute(`drop database if exists ${databaseName} with (force)`);
      await admin.close(true);
    }
  });

  async function start() {
    const child = Bun.spawn([process.execPath, 'dist/main.js'], {
      env: { ...process.env, DATABASE_URL: databaseUrl, PORT: '0' }, stdout: 'pipe', stderr: 'pipe',
    });
    children.add(child);
    let timeout: ReturnType<typeof setTimeout> | undefined;
    const startup = async (): Promise<string> => {
      const reader = child.stdout.getReader();
      const decoder = new TextDecoder();
      let buffer = '';
      try {
        while (true) {
          const read = await reader.read();
          if (read.done) throw new Error(`Application process ${child.pid} exited before listening`);
          buffer += decoder.decode(read.value, { stream: true });
          let newline: number;
          while ((newline = buffer.indexOf('\n')) !== -1) {
            const line = buffer.slice(0, newline);
            buffer = buffer.slice(newline + 1);
            const record = JSON.parse(line) as { event?: string; address?: string };
            if (record.event === 'server_started' && record.address) return record.address.replace('0.0.0.0', '127.0.0.1');
          }
        }
      } finally {
        reader.releaseLock();
      }
    };
    try {
      const address = await Promise.race([
        startup(),
        new Promise<never>((_, reject) => { timeout = setTimeout(() => reject(new Error('Application startup timed out')), 10_000); }),
      ]);
      return { child, address };
    } finally {
      clearTimeout(timeout);
    }
  }

  async function stop(child: ReturnType<typeof Bun.spawn>): Promise<void> {
    if (child.exitCode === null) child.kill('SIGTERM');
    const timeout = setTimeout(() => child.kill('SIGKILL'), 5_000);
    try { await child.exited; } finally { clearTimeout(timeout); children.delete(child); }
  }

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

  async function post(address: string, body: { providerId: string; externalTransactionId: string }): Promise<Submission> {
    const response = await fetch(`${address}/wagering/transactions`, {
      method: 'POST', headers: { 'content-type': 'application/json', 'Idempotency-Key': `${body.providerId}:${body.externalTransactionId}` },
      body: JSON.stringify(body), signal: AbortSignal.timeout(10_000),
    });
    expect([200, 422]).toContain(response.status);
    return await response.json() as Submission;
  }

  test('three processes deduplicate, compete for funds and preserve snapshots after every process restarts', async () => {
    const instances = await Promise.all(Array.from({ length: 3 }, () => start()));
    expect(new Set(instances.map(instance => instance.child.pid)).size).toBe(3);
    const wallet = await service.createWallet({ playerId: crypto.randomUUID(), initialBalance: { amount: '100.00', currency: 'BRL' } });
    const body = {
      providerId: 'provider-a', externalTransactionId: crypto.randomUUID(), playerId: wallet.playerId,
      walletId: wallet.id, roundId: 'round-a', gameId: 'game-a', kind: 'BET', money: { amount: '25.00', currency: 'BRL' },
    };
    const duplicateResults = await Promise.all(Array.from({ length: 50 }, (_, index) => post(instances[index % 3]!.address, body)));
    expect(duplicateResults.filter(result => !result.idempotentReplay)).toHaveLength(1);
    expect(new Set(duplicateResults.map(result => result.transactionId)).size).toBe(1);
    expect((await service.getWallet(wallet.id)).balance.amount).toBe('75.00');
    expect(await orm.em.getConnection().execute<{ id: string }[]>(
      "select id from dwp.wallet_ledger_entries where wallet_id = ? and direction = 'DEBIT'", [wallet.id],
    )).toHaveLength(1);
    await assertConsistent(wallet.id);

    const disputed = await service.createWallet({ playerId: crypto.randomUUID(), initialBalance: { amount: '100.00', currency: 'BRL' } });
    const wagers = Array.from({ length: 3 }, () => ({ ...body, walletId: disputed.id, playerId: disputed.playerId,
      externalTransactionId: crypto.randomUUID(), money: { amount: '80.00', currency: 'BRL' } }));
    const race = await Promise.all(wagers.map((wager, index) => post(instances[index]!.address, wager)));
    expect(race.filter(result => result.status === 'PROCESSED')).toHaveLength(1);
    expect(race.filter(result => result.status === 'REJECTED')).toHaveLength(2);
    expect(race.filter(result => result.failureCode === 'INSUFFICIENT_FUNDS')).toHaveLength(2);
    expect((await service.getWallet(disputed.id)).balance.amount).toBe('20.00');
    await assertConsistent(disputed.id);

    await Promise.all(instances.map(instance => stop(instance.child)));
    const restarted = await start();
    expect(instances.some(instance => instance.child.pid === restarted.child.pid)).toBe(false);
    expect(await post(restarted.address, body)).toEqual({ ...duplicateResults[0]!, idempotentReplay: true });
    for (const [index, wager] of wagers.entries()) {
      expect(await post(restarted.address, wager)).toEqual({ ...race[index]!, idempotentReplay: true });
    }
    const current = await fetch(`${restarted.address}/wallets/${disputed.id}`, { signal: AbortSignal.timeout(5_000) });
    expect(current.status).toBe(200);
    expect(await current.json()).toMatchObject({ balance: { amount: '20.00', currency: 'BRL' } });
    expect(await orm.em.getConnection().execute<{ id: string }[]>(
      "select id from dwp.wallet_ledger_entries where wallet_id = ? and direction = 'DEBIT'", [disputed.id],
    )).toHaveLength(1);
    await assertConsistent(wallet.id);
    await assertConsistent(disputed.id);
  }, 30_000);
});
