import { afterAll, afterEach, beforeAll, beforeEach, expect } from 'bun:test';
import {
  CreateQueueCommand,
  DeleteQueueCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
  type SQSClient,
} from '@aws-sdk/client-sqs';
import type { MikroORM } from '@mikro-orm/postgresql';
import { WageringService } from '../../dist/application/wagering-service.js';
import { createDatabase } from '../../dist/infrastructure/persistence/database.js';
import { createSqsClient } from '../../dist/infrastructure/messaging/sqs.js';
import {
  SqsConsumer,
  type SqsConsumerOptions,
  type WagerCommandEnvelope,
  type WagerCommandHandler,
} from '../../dist/infrastructure/messaging/sqs-consumer.js';

export function consumerTestContext() {
  const databaseName = `dwp_consumer_${crypto.randomUUID().replaceAll('-', '')}`;
  const consumerName = 'wager-transactions-integration';
  const consumers = new Set<SqsConsumer>();
  let admin: MikroORM;
  let orm: MikroORM;
  let service: WageringService;
  let sqs: SQSClient;
  let commandsUrl: string;
  let dlqUrl: string;

  beforeAll(async () => {
    admin = await createDatabase();
    await admin.em.getConnection().execute(`create database ${databaseName}`);
    const url = new URL(process.env.DATABASE_URL ?? 'postgresql://dwp:dwp@127.0.0.1:5432/dwp');
    url.pathname = `/${databaseName}`;
    orm = await createDatabase(url.href);
    await orm.migrator.up();
    service = new WageringService(orm);
    sqs = createSqsClient();
  });

  beforeEach(async () => {
    const suffix = crypto.randomUUID().replaceAll('-', '');
    const commands = await sqs.send(new CreateQueueCommand({
      QueueName: `consumer_commands_${suffix}.fifo`,
      Attributes: { FifoQueue: 'true', ContentBasedDeduplication: 'false', VisibilityTimeout: '1' },
    }));
    const deadLetters = await sqs.send(new CreateQueueCommand({
      QueueName: `consumer_dlq_${suffix}.fifo`,
      Attributes: { FifoQueue: 'true', ContentBasedDeduplication: 'false' },
    }));
    if (!commands.QueueUrl || !deadLetters.QueueUrl) throw new Error('Consumer test queues require URLs');
    commandsUrl = commands.QueueUrl;
    dlqUrl = deadLetters.QueueUrl;
  });

  afterEach(async () => {
    await Promise.all([...consumers].map(consumer => consumer.stop(1_000)));
    consumers.clear();
    if (commandsUrl) await sqs.send(new DeleteQueueCommand({ QueueUrl: commandsUrl }));
    if (dlqUrl) await sqs.send(new DeleteQueueCommand({ QueueUrl: dlqUrl }));
  });

  afterAll(async () => {
    sqs?.destroy();
    await orm?.close(true);
    if (admin) {
      await admin.em.getConnection().execute(`drop database if exists ${databaseName} with (force)`);
      await admin.close(true);
    }
  });

  const query = <T>(sql: string, values: unknown[] = []) => orm.em.getConnection().execute<T[]>(sql, values);

  function consumer(handler: WagerCommandHandler = service, options: SqsConsumerOptions = {}): SqsConsumer {
    const instance = new SqsConsumer(handler, sqs, commandsUrl, dlqUrl, {
      consumerName, visibilitySeconds: 1, maxAttempts: 5, ...options,
    });
    consumers.add(instance);
    return instance;
  }

  async function fixture(amount = '10.00') {
    const playerId = crypto.randomUUID();
    const wallet = await service.createWallet({ playerId, initialBalance: { amount: '100.00', currency: 'BRL' } });
    const externalTransactionId = crypto.randomUUID();
    const envelope: WagerCommandEnvelope = {
      messageId: crypto.randomUUID(), type: 'WagerTransactionRequested', occurredAt: new Date().toISOString(),
      data: {
        providerId: 'consumer-provider', externalTransactionId, idempotencyKey: `consumer:${externalTransactionId}`,
        walletId: wallet.id, playerId, roundId: 'consumer-round', gameId: 'consumer-game',
        kind: 'BET', money: { amount, currency: 'BRL' },
      },
    };
    return { wallet, envelope };
  }

  async function send(envelope: WagerCommandEnvelope): Promise<void> {
    await sqs.send(new SendMessageCommand({
      QueueUrl: commandsUrl, MessageBody: JSON.stringify(envelope),
      MessageGroupId: String(envelope.data.walletId), MessageDeduplicationId: crypto.randomUUID(),
    }));
  }

  async function receive(queueUrl = commandsUrl, visibilityTimeout = 1) {
    const response = await sqs.send(new ReceiveMessageCommand({
      QueueUrl: queueUrl, WaitTimeSeconds: 0, MaxNumberOfMessages: 10,
      VisibilityTimeout: visibilityTimeout, MessageAttributeNames: ['All'],
      MessageSystemAttributeNames: ['ApproximateReceiveCount'],
    }));
    return response.Messages ?? [];
  }

  async function assertConsistent(walletId: string, expectedBalance: string, expectedDebits: number): Promise<void> {
    const rows = await query<{ balance_cents: string; calculated: string; debits: string }>(`
      select w.balance_cents::text,
        coalesce(sum(case when l.direction = 'CREDIT' then l.amount_cents::numeric else -l.amount_cents::numeric end), 0)::text as calculated,
        count(l.id) filter (where l.direction = 'DEBIT')::text as debits
      from dwp.wallets w left join dwp.wallet_ledger_entries l on l.wallet_id = w.id
      where w.id = ? group by w.id`, [walletId]);
    expect(rows).toEqual([{ balance_cents: expectedBalance, calculated: expectedBalance, debits: String(expectedDebits) }]);
    expect(BigInt(rows[0]!.balance_cents) >= 0n).toBe(true);
  }

  return {
    consumerName, consumer, fixture, send, receive, query, assertConsistent,
    get service() { return service; },
    get orm() { return orm; },
    get sqs() { return sqs; },
    get commandsUrl() { return commandsUrl; },
    get dlqUrl() { return dlqUrl; },
  };
}
