import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import {
  CreateQueueCommand,
  DeleteQueueCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
  type SQSClient,
} from '@aws-sdk/client-sqs';
import type { MikroORM } from '@mikro-orm/postgresql';
import { WageringService } from '../../dist/application/wagering-service.js';
import { SqsConsumer, type WagerCommandEnvelope, type WagerCommandResult } from '../../dist/infrastructure/messaging/sqs-consumer.js';
import { createSqsClient } from '../../dist/infrastructure/messaging/sqs.js';
import { createDatabase } from '../../dist/infrastructure/persistence/database.js';

const integration = process.env.DWP_INTEGRATION === '1' ? describe : describe.skip;

interface CommitCheckpoint {
  event: 'consumer_committed_before_ack';
  messageId: string;
  transactionId: string;
  status: string;
}

interface FinancialState {
  balance_cents: string;
  calculated: string;
  version: number;
  debits: string;
  entries: string;
  wagers: string;
  inbox: string;
  events: string;
}

integration('consumer process crash after commit and before ack', () => {
  const suffix = crypto.randomUUID().replaceAll('-', '');
  const databaseName = `dwp_consumer_crash_${suffix}`;
  const consumerName = `crash-consumer-${suffix}`;
  let admin: MikroORM;
  let orm: MikroORM;
  let databaseUrl: string;
  let sqs: SQSClient;
  let commandsUrl: string;
  let dlqUrl: string;
  let child: Bun.Subprocess<'ignore', 'pipe', 'pipe'> | undefined;

  beforeAll(async () => {
    admin = await createDatabase();
    await admin.em.getConnection().execute(`create database ${databaseName}`);
    const url = new URL(process.env.DATABASE_URL ?? 'postgresql://dwp:dwp@127.0.0.1:5432/dwp');
    url.pathname = `/${databaseName}`;
    databaseUrl = url.href;
    orm = await createDatabase(databaseUrl);
    await orm.migrator.up();
    sqs = createSqsClient();
    const commands = await sqs.send(new CreateQueueCommand({
      QueueName: `consumer_crash_${suffix}.fifo`,
      Attributes: { FifoQueue: 'true', ContentBasedDeduplication: 'false', VisibilityTimeout: '1' },
    }));
    if (!commands.QueueUrl) throw new Error('Missing crash-test command queue URL');
    commandsUrl = commands.QueueUrl;
    const dlq = await sqs.send(new CreateQueueCommand({
      QueueName: `consumer_crash_dlq_${suffix}.fifo`, Attributes: { FifoQueue: 'true' },
    }));
    if (!dlq.QueueUrl) throw new Error('Missing crash-test dead-letter queue URL');
    dlqUrl = dlq.QueueUrl;
  });

  afterAll(async () => {
    if (child && child.exitCode === null) { child.kill('SIGKILL'); await child.exited; }
    if (commandsUrl) await sqs.send(new DeleteQueueCommand({ QueueUrl: commandsUrl }));
    if (dlqUrl) await sqs.send(new DeleteQueueCommand({ QueueUrl: dlqUrl }));
    sqs?.destroy();
    await orm?.close(true);
    if (admin) {
      await admin.em.getConnection().execute(`drop database if exists ${databaseName} with (force)`);
      await admin.close(true);
    }
  });

  const query = <T>(sql: string, values: unknown[] = []) => orm.em.getConnection().execute<T[]>(sql, values);

  async function financialState(walletId: string, messageId: string): Promise<FinancialState> {
    const [state] = await query<FinancialState>(`
      select w.balance_cents::text, w.version,
        (select coalesce(sum(case when direction = 'CREDIT' then amount_cents::numeric else -amount_cents::numeric end), 0)::text
          from dwp.wallet_ledger_entries where wallet_id = w.id) as calculated,
        (select count(*)::text from dwp.wallet_ledger_entries where wallet_id = w.id and direction = 'DEBIT') as debits,
        (select count(*)::text from dwp.wallet_ledger_entries where wallet_id = w.id) as entries,
        (select count(*)::text from dwp.wager_transactions where wallet_id = w.id and kind = 'BET') as wagers,
        (select count(*)::text from dwp.inbox_messages where consumer_name = ? and message_id = ? and processed_at is not null) as inbox,
        (select count(*)::text from dwp.outbox_messages) as events
      from dwp.wallets w where w.id = ?`, [consumerName, messageId, walletId]);
    expect(state).toBeDefined();
    expect(state!.balance_cents).toBe(state!.calculated);
    expect(BigInt(state!.balance_cents) >= 0n).toBe(true);
    return state!;
  }

  async function awaitCheckpoint(process: Bun.Subprocess<'ignore', 'pipe', 'pipe'>): Promise<CommitCheckpoint> {
    const reader = process.stdout.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    let timer: ReturnType<typeof setTimeout> | undefined;
    const checkpoint = async (): Promise<CommitCheckpoint> => {
      for (;;) {
        const chunk = await reader.read();
        if (chunk.done) throw new Error('Consumer fixture exited before its commit checkpoint');
        buffer += decoder.decode(chunk.value, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop() ?? '';
        for (const line of lines) {
          try {
            const value: unknown = JSON.parse(line);
            if (value !== null && typeof value === 'object'
              && 'event' in value && value.event === 'consumer_committed_before_ack') {
              return value as CommitCheckpoint;
            }
          } catch { /* Ignore unrelated diagnostic lines while waiting for the marker. */ }
        }
      }
    };
    try {
      return await Promise.race([
        checkpoint(),
        new Promise<never>((_, reject) => {
          timer = setTimeout(() => reject(new Error('Timed out waiting for the consumer commit checkpoint')), 10_000);
        }),
      ]);
    } finally {
      clearTimeout(timer);
      await reader.cancel();
      reader.releaseLock();
    }
  }

  test('redelivers the committed message after SIGKILL without repeating any financial effect', async () => {
    const service = new WageringService(orm);
    const wallet = await service.createWallet({
      playerId: crypto.randomUUID(), initialBalance: { amount: '100.00', currency: 'BRL' },
    });
    const externalTransactionId = crypto.randomUUID();
    const envelope: WagerCommandEnvelope = {
      messageId: crypto.randomUUID(), type: 'WagerTransactionRequested', occurredAt: new Date().toISOString(),
      data: {
        providerId: 'crash-provider', externalTransactionId, idempotencyKey: `crash:${externalTransactionId}`,
        playerId: wallet.playerId, walletId: wallet.id, roundId: 'crash-round', gameId: 'crash-game',
        kind: 'BET', money: { amount: '25.00', currency: 'BRL' },
      },
    };
    await sqs.send(new SendMessageCommand({
      QueueUrl: commandsUrl, MessageBody: JSON.stringify(envelope),
      MessageGroupId: wallet.id, MessageDeduplicationId: crypto.randomUUID(),
    }));

    let restarted: MikroORM | undefined;
    let consumer: SqsConsumer | undefined;
    child = Bun.spawn([process.execPath, 'test/fixtures/consumer-crash.ts'], {
      stdin: 'ignore', stdout: 'pipe', stderr: 'pipe',
      env: { ...process.env, DATABASE_URL: databaseUrl, COMMANDS_QUEUE_URL: commandsUrl, DLQ_QUEUE_URL: dlqUrl, CONSUMER_NAME: consumerName },
    });
    try {
      const checkpoint = await awaitCheckpoint(child);
      expect(checkpoint.messageId).toBe(envelope.messageId);
      expect(checkpoint.status).toBe('PROCESSED');
      expect(child.exitCode).toBeNull();
      const committed = await financialState(wallet.id, envelope.messageId);
      expect(committed).toEqual({
        balance_cents: '7500', calculated: '7500', version: 2,
        debits: '1', entries: '2', wagers: '1', inbox: '1', events: '4',
      });
      const inbox = await query<{ message_id: string; result: WagerCommandResult }>(
        'select message_id, result from dwp.inbox_messages where consumer_name = ? and message_id = ?',
        [consumerName, envelope.messageId],
      );
      expect(inbox).toHaveLength(1);
      expect(inbox[0]!.result).toMatchObject({ transactionId: checkpoint.transactionId, status: 'PROCESSED' });
      const events = await query<{ id: string }>('select id from dwp.outbox_messages order by id');

      child.kill('SIGKILL');
      expect(await child.exited).toBe(137);
      await Bun.sleep(1_100);

      restarted = await createDatabase(databaseUrl);
      let replay: WagerCommandResult | undefined;
      consumer = new SqsConsumer(new WageringService(restarted), sqs, commandsUrl, dlqUrl, {
        consumerName, visibilitySeconds: 1,
        afterCommitBeforeAck: result => { replay = result; },
      });
      // No second send: this is broker redelivery of the message left unacked by
      // the killed process, so FIFO deduplication cannot hide duplicate effects.
      expect(await consumer.runOnce({ waitTimeSeconds: 2, maxMessages: 1 })).toBe(1);
      expect(replay).toMatchObject({ transactionId: checkpoint.transactionId, status: 'PROCESSED' });
      expect(await financialState(wallet.id, envelope.messageId)).toEqual(committed);
      expect(await query('select id from dwp.outbox_messages order by id')).toEqual(events);
      expect(await query('select message_id, result from dwp.inbox_messages where consumer_name = ? and message_id = ?',
        [consumerName, envelope.messageId])).toEqual(inbox);
      for (const QueueUrl of [commandsUrl, dlqUrl]) {
        const remaining = await sqs.send(new ReceiveMessageCommand({ QueueUrl, WaitTimeSeconds: 0 }));
        expect(remaining.Messages ?? []).toHaveLength(0);
      }
    } finally {
      if (child.exitCode === null) { child.kill('SIGKILL'); await child.exited; }
      await consumer?.stop(1_000);
      await restarted?.close(true);
    }
  }, 20_000);
});
