import {
  CreateQueueCommand,
  DeleteMessageBatchCommand,
  DeleteQueueCommand,
  ReceiveMessageCommand,
  type SQSClient,
} from '@aws-sdk/client-sqs';
import type { MikroORM } from '@mikro-orm/postgresql';
import { WageringService } from '../../dist/application/wagering-service.js';
import { createDatabase } from '../../dist/infrastructure/persistence/database.js';
import { createSqsClient } from '../../dist/infrastructure/messaging/sqs.js';

export type CrashChild = Bun.Subprocess<'ignore', 'pipe', 'pipe'>;

export interface CrashEnvironment {
  orm: MikroORM;
  sqs: SQSClient;
  databaseUrl: string;
  query<T>(sql: string, parameters?: unknown[]): Promise<T[]>;
  createQueue(): Promise<string>;
  close(): Promise<void>;
}

export async function createCrashEnvironment(label: string): Promise<CrashEnvironment> {
  const suffix = crypto.randomUUID().replaceAll('-', '');
  const databaseName = `dwp_crash_${label.slice(0, 16)}_${suffix}`;
  const admin = await createDatabase();
  await admin.em.getConnection().execute(`create database ${databaseName}`);
  const url = new URL(process.env.DATABASE_URL ?? 'postgresql://dwp:dwp@127.0.0.1:5432/dwp');
  url.pathname = `/${databaseName}`;
  const orm = await createDatabase(url.href);
  await orm.migrator.up();
  const sqs = createSqsClient();
  const queues: string[] = [];
  return {
    orm,
    sqs,
    databaseUrl: url.href,
    query: <T>(sql: string, parameters: unknown[] = []) => orm.em.getConnection().execute<T[]>(sql, parameters),
    async createQueue() {
      const created = await sqs.send(new CreateQueueCommand({
        QueueName: `dwp_${label}_${crypto.randomUUID().replaceAll('-', '')}`,
      }));
      if (!created.QueueUrl) throw new Error('Crash fixture queue requires a URL');
      queues.push(created.QueueUrl);
      return created.QueueUrl;
    },
    async close() {
      await Promise.allSettled(queues.map(QueueUrl => sqs.send(new DeleteQueueCommand({ QueueUrl }))));
      sqs.destroy();
      await orm.close(true);
      try {
        await admin.em.getConnection().execute(`drop database if exists ${databaseName} with (force)`);
      } finally {
        await admin.close(true);
      }
    },
  };
}

export function spawnCrashFixture(path: string, environment: Record<string, string>): CrashChild {
  return Bun.spawn([process.execPath, path], {
    stdin: 'ignore', stdout: 'pipe', stderr: 'pipe', env: { ...process.env, ...environment },
  });
}

export async function waitForCheckpoint(child: CrashChild, event: string): Promise<Record<string, unknown>> {
  const reader = child.stdout.getReader();
  let timer: ReturnType<typeof setTimeout> | undefined;
  const reading = (async () => {
    let pending = '';
    const decoder = new TextDecoder();
    for (;;) {
      const chunk = await reader.read();
      if (chunk.done) throw new Error(`Crash fixture exited before ${event}; exitCode=${child.exitCode}`);
      pending += decoder.decode(chunk.value, { stream: true });
      const lines = pending.split('\n');
      pending = lines.pop() ?? '';
      for (const line of lines) {
        let value: unknown;
        try { value = JSON.parse(line); } catch { continue; }
        if (value !== null && typeof value === 'object' && 'event' in value && value.event === event) {
          return value as Record<string, unknown>;
        }
      }
    }
  })();
  try {
    return await Promise.race([
      reading,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error(`Timed out waiting for ${event}`)), 10_000);
      }),
    ]);
  } finally {
    clearTimeout(timer);
    await reader.cancel().catch(() => undefined);
    reader.releaseLock();
  }
}

export async function killCrashFixture(child: CrashChild): Promise<number> {
  child.kill('SIGKILL');
  return child.exited;
}

export async function cleanupCrashFixture(child: CrashChild | undefined): Promise<void> {
  if (!child) return;
  if (child.exitCode === null) child.kill('SIGKILL');
  await child.exited;
}

export async function receiveEvents(sqs: SQSClient, queueUrl: string, acknowledge = true): Promise<Record<string, unknown>[]> {
  const events: Record<string, unknown>[] = [];
  for (;;) {
    const received = await sqs.send(new ReceiveMessageCommand({
      QueueUrl: queueUrl, MaxNumberOfMessages: 10, WaitTimeSeconds: 0,
      VisibilityTimeout: acknowledge ? 30 : 0,
    }));
    if (!received.Messages?.length) return events;
    for (const message of received.Messages) events.push(JSON.parse(message.Body!) as Record<string, unknown>);
    if (!acknowledge) return events;
    await sqs.send(new DeleteMessageBatchCommand({
      QueueUrl: queueUrl,
      Entries: received.Messages.map((message, index) => ({ Id: String(index), ReceiptHandle: message.ReceiptHandle! })),
    }));
  }
}

export async function createFinancialEvent(environment: CrashEnvironment): Promise<{ eventId: string; walletId: string }> {
  const service = new WageringService(environment.orm);
  const wallet = await service.createWallet({
    playerId: crypto.randomUUID(), initialBalance: { amount: '100.00', currency: 'BRL' },
  });
  const externalTransactionId = crypto.randomUUID();
  const result = await service.submit({
    walletId: wallet.id, playerId: wallet.playerId, providerId: 'crash-provider',
    externalTransactionId, roundId: 'crash-round', gameId: 'crash-game', kind: 'BET',
    money: { amount: '25.00', currency: 'BRL' },
  }, `crash:${externalTransactionId}`);
  const [event] = await environment.query<{ id: string }>(`select id from dwp.outbox_messages
    where event_type = 'WagerTransactionProcessed' and payload->'data'->>'transactionId' = ?`, [result.transactionId]);
  if (!event) throw new Error('A processed wager requires a persisted integration event');
  // The fixture starts after all other integration events have already been delivered.
  await environment.query('update dwp.outbox_messages set published_at = clock_timestamp() where id <> ?', [event.id]);
  return { eventId: event.id, walletId: wallet.id };
}

export async function financialState(environment: CrashEnvironment, walletId: string) {
  const [row] = await environment.query<{ balance_cents: string; calculated: string; debits: string }>(`
    select w.balance_cents::text,
      coalesce(sum(case when l.direction = 'CREDIT' then l.amount_cents::numeric else -l.amount_cents::numeric end), 0)::text as calculated,
      count(l.id) filter (where l.direction = 'DEBIT')::text as debits
    from dwp.wallets w left join dwp.wallet_ledger_entries l on l.wallet_id = w.id
    where w.id = ? group by w.id`, [walletId]);
  if (!row) throw new Error('Expected wallet state after recovery');
  return row;
}
