import { createWageringService } from '../../dist/infrastructure/persistence/wagering-services.js';
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import {
  CreateQueueCommand,
  DeleteMessageCommand,
  DeleteQueueCommand,
  GetQueueAttributesCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
  type SQSClient,
} from '@aws-sdk/client-sqs';
import type { MikroORM } from '@mikro-orm/postgresql';
import { createDatabase } from '../../dist/infrastructure/persistence/database.js';
import { createSqsClient } from '../../dist/infrastructure/messaging/sqs.js';

const integration = process.env.DWP_INTEGRATION === '1' ? describe : describe.skip;

async function deadline<T>(operation: Promise<T>, milliseconds: number, message: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  return Promise.race([
    operation,
    new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new Error(message)), milliseconds); }),
  ]).finally(() => clearTimeout(timer));
}

async function eventually(predicate: () => Promise<boolean>, milliseconds = 10_000): Promise<void> {
  const end = Date.now() + milliseconds;
  while (!await predicate()) {
    if (Date.now() >= end) throw new Error('The worker did not reach its expected durable state');
    await Bun.sleep(50);
  }
}

integration('real worker entrypoint', () => {
  const databaseName = `dwp_worker_${crypto.randomUUID().replaceAll('-', '')}`;
  const queueUrls: string[] = [];
  let databaseUrl: string;
  let admin: MikroORM;
  let orm: MikroORM;
  let sqs: SQSClient;
  let commandsUrl: string;
  let dlqUrl: string;
  let eventsUrl: string;
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
    const suffix = crypto.randomUUID().replaceAll('-', '');
    const queues = await Promise.all([
      { name: `worker_commands_${suffix}.fifo`, fifo: true },
      { name: `worker_dlq_${suffix}.fifo`, fifo: true },
      { name: `worker_events_${suffix}`, fifo: false },
    ].map(async ({ name, fifo }) => {
      const result = await sqs.send(new CreateQueueCommand({
        QueueName: name,
        ...(fifo ? { Attributes: { FifoQueue: 'true' } } : {}),
      }));
      if (!result.QueueUrl) throw new Error('The worker test requires all queue URLs');
      queueUrls.push(result.QueueUrl);
      return result.QueueUrl;
    }));
    [commandsUrl, dlqUrl, eventsUrl] = queues as [string, string, string];
  });

  afterAll(async () => {
    if (child && child.exitCode === null) {
      child.kill('SIGKILL');
      await child.exited;
    }
    await Promise.allSettled(queueUrls.map(QueueUrl => sqs.send(new DeleteQueueCommand({ QueueUrl }))));
    sqs?.destroy();
    await orm?.close(true);
    if (admin) {
      await admin.em.getConnection().execute(`drop database if exists ${databaseName} with (force)`);
      await admin.close(true);
    }
  });

  test('consumes, commits and publishes durable events, then shuts down cleanly on SIGTERM', async () => {
    const service = createWageringService(orm);
    const wallet = await service.createWallet({
      playerId: crypto.randomUUID(), initialBalance: { amount: '100.00', currency: 'BRL' },
    });
    const externalTransactionId = crypto.randomUUID();
    const envelope = {
      messageId: crypto.randomUUID(), type: 'WagerTransactionRequested', occurredAt: new Date().toISOString(),
      data: {
        providerId: 'worker-provider', externalTransactionId, idempotencyKey: `worker:${externalTransactionId}`,
        walletId: wallet.id, playerId: wallet.playerId, roundId: 'worker-round', gameId: 'worker-game',
        kind: 'BET', money: { amount: '25.00', currency: 'BRL' },
      },
    };
    child = Bun.spawn([process.execPath, 'dist/worker.js'], {
      env: {
        ...process.env, DATABASE_URL: databaseUrl, WORKER_ROLE: 'all',
        COMMAND_QUEUE_URL: commandsUrl, DLQ_QUEUE_URL: dlqUrl, EVENT_QUEUE_URL: eventsUrl,
      },
      stdin: 'ignore', stdout: 'pipe', stderr: 'pipe',
    });
    const running = child;
    const stderr = new Response(running.stderr).text();
    let announce!: (event: { role: string; pid: number }) => void;
    let rejectStartup!: (error: Error) => void;
    let announced = false;
    const started = new Promise<{ role: string; pid: number }>((resolve, reject) => {
      announce = resolve;
      rejectStartup = reject;
    });
    const stdout = (async () => {
      const reader = running.stdout.getReader();
      const decoder = new TextDecoder();
      let partial = '';
      try {
        while (true) {
          const next = await reader.read();
          if (next.done) break;
          partial += decoder.decode(next.value, { stream: true });
          const lines = partial.split('\n');
          partial = lines.pop() ?? '';
          for (const line of lines) {
            let value: unknown;
            try { value = JSON.parse(line); } catch { continue; }
            if (value !== null && typeof value === 'object' && 'event' in value && value.event === 'worker_started'
              && 'role' in value && typeof value.role === 'string' && 'pid' in value && typeof value.pid === 'number') {
              announced = true;
              announce({ role: value.role, pid: value.pid });
            }
          }
        }
        if (!announced) rejectStartup(new Error('The worker exited before announcing startup'));
      } catch (error) {
        rejectStartup(error instanceof Error ? error : new Error('The worker stdout stream failed'));
      } finally {
        reader.releaseLock();
      }
    })();
    const startedEvent = await deadline(started, 10_000, 'The worker did not announce startup');
    expect(startedEvent).toMatchObject({ role: 'all', pid: running.pid });
    await sqs.send(new SendMessageCommand({
      QueueUrl: commandsUrl, MessageBody: JSON.stringify(envelope),
      MessageGroupId: wallet.id, MessageDeduplicationId: crypto.randomUUID(),
    }));

    await eventually(async () => {
      if (running.exitCode !== null) throw new Error('The worker exited while processing the command');
      const current = await service.getWallet(wallet.id);
      const [outbox] = await orm.em.getConnection().execute<{ total: string; published: string }[]>(`
        select count(*)::text as total, count(*) filter (where published_at is not null)::text as published
        from dwp.outbox_messages`);
      return current.balance.amount === '75.00' && outbox?.total === '4' && outbox.published === '4';
    });
    await eventually(async () => {
      const queue = await sqs.send(new GetQueueAttributesCommand({
        QueueUrl: commandsUrl,
        AttributeNames: ['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible'],
      }));
      return queue.Attributes?.ApproximateNumberOfMessages === '0'
        && queue.Attributes.ApproximateNumberOfMessagesNotVisible === '0';
    });

    const shutdownStarted = performance.now();
    running.kill('SIGTERM');
    expect(await deadline(running.exited, 15_000, 'The worker did not drain on SIGTERM')).toBe(0);
    expect(performance.now() - shutdownStarted).toBeLessThan(15_000);
    await stdout;
    expect(await stderr).not.toContain('Unhandled');

    const [state] = await orm.em.getConnection().execute<{
      balance_cents: string; calculated: string; debits: string;
    }[]>(`
      select w.balance_cents::text,
        sum(case when l.direction = 'CREDIT' then l.amount_cents::numeric else -l.amount_cents::numeric end)::text as calculated,
        count(*) filter (where l.direction = 'DEBIT')::text as debits
      from dwp.wallets w join dwp.wallet_ledger_entries l on l.wallet_id = w.id where w.id = ? group by w.id`, [wallet.id]);
    expect(state).toEqual({ balance_cents: '7500', calculated: '7500', debits: '1' });
    expect(BigInt(state!.balance_cents) >= 0n).toBe(true);
    const inbox = await orm.em.getConnection().execute<{ consumer_name: string }[]>(`
      select consumer_name from dwp.inbox_messages where message_id = ? and processed_at is not null`, [envelope.messageId]);
    expect(inbox).toEqual([{ consumer_name: 'wager-transactions' }]);

    const published = await orm.em.getConnection().execute<{ id: string; event_type: string }[]>(
      'select id, event_type from dwp.outbox_messages where published_at is not null',
    );
    expect(published).toHaveLength(4);
    const received = new Set<string>();
    const eventDeadline = Date.now() + 5_000;
    while (received.size < 4 && Date.now() < eventDeadline) {
      const result = await sqs.send(new ReceiveMessageCommand({
        QueueUrl: eventsUrl, WaitTimeSeconds: 1, MaxNumberOfMessages: 10,
      }));
      for (const message of result.Messages ?? []) {
        const event = JSON.parse(message.Body ?? '{}') as { eventId: string; eventType: string };
        expect(published).toContainEqual({ id: event.eventId, event_type: event.eventType });
        received.add(event.eventId);
        await sqs.send(new DeleteMessageCommand({ QueueUrl: eventsUrl, ReceiptHandle: message.ReceiptHandle! }));
      }
    }
    expect(received).toEqual(new Set(published.map(event => event.id)));
    const commands = await sqs.send(new ReceiveMessageCommand({ QueueUrl: commandsUrl, WaitTimeSeconds: 0 }));
    expect(commands.Messages ?? []).toHaveLength(0);
    const deadLetters = await sqs.send(new ReceiveMessageCommand({ QueueUrl: dlqUrl, WaitTimeSeconds: 0 }));
    expect(deadLetters.Messages ?? []).toHaveLength(0);
  }, 30_000);
});
