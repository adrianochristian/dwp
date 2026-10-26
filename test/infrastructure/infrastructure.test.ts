import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import {
  ChangeMessageVisibilityCommand,
  DeleteMessageCommand,
  GetQueueAttributesCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
  type SQSClient,
} from '@aws-sdk/client-sqs';
import type { MikroORM } from '@mikro-orm/postgresql';
import { createDatabase } from '../../dist/infrastructure/persistence/database.js';
import { bootstrapQueues, type QueueUrls } from '../../dist/infrastructure/messaging/bootstrap-queues.js';
import { createSqsClient } from '../../dist/infrastructure/messaging/sqs.js';

const integration = process.env.DWP_INTEGRATION === '1' ? describe : describe.skip;

integration('real PostgreSQL and SQS infrastructure', () => {
  let orm: MikroORM;
  let control: MikroORM;
  const databaseName = `dwp_infra_${crypto.randomUUID().replaceAll('-', '')}`;
  let sqs: SQSClient;
  let queues: QueueUrls;

  beforeAll(async () => {
    control = await createDatabase();
    await control.em.getConnection().execute(`create database "${databaseName}"`);
    const url = new URL(process.env.DATABASE_URL ?? 'postgresql://dwp:dwp@127.0.0.1:5432/dwp');
    url.pathname = `/${databaseName}`;
    orm = await createDatabase(url.href);
    sqs = createSqsClient();
    queues = await bootstrapQueues(sqs);
    await orm.migrator.up();
  });

  afterAll(async () => {
    sqs?.destroy();
    await orm?.close(true);
    if (control) {
      await control.em.getConnection().execute(`drop database if exists "${databaseName}"`);
      await control.close(true);
    }
  });

  test('applies, reverses and reapplies the schema migration', async () => {
    const connection = orm.em.getConnection();
    const schema = () => connection.execute<{ exists: boolean }[]>(
      "select exists(select 1 from pg_namespace where nspname = 'dwp') as exists",
    );
    expect((await schema())[0]?.exists).toBe(true);
    await orm.migrator.down({ to: 0 });
    expect((await schema())[0]?.exists).toBe(false);
    await orm.migrator.up();
    expect((await schema())[0]?.exists).toBe(true);
    expect(await orm.migrator.getPending()).toHaveLength(0);
  });

  test('configures FIFO commands with five attempts and a separate event queue', async () => {
    const attributes = await sqs.send(new GetQueueAttributesCommand({
      QueueUrl: queues.commands,
      AttributeNames: ['FifoQueue', 'RedrivePolicy'],
    }));
    expect(attributes.Attributes?.FifoQueue).toBe('true');
    expect(JSON.parse(attributes.Attributes?.RedrivePolicy ?? '{}').maxReceiveCount).toBe('5');
    expect(queues.events).not.toBe(queues.commands);
    expect(queues.events.endsWith('.fifo')).toBe(false);
  });

  test('sends, redelivers and acknowledges a real FIFO message', async () => {
    const token = crypto.randomUUID();
    const sent = await sqs.send(new SendMessageCommand({
      QueueUrl: queues.commands,
      MessageBody: JSON.stringify({ infrastructureProbe: token }),
      MessageGroupId: token,
      MessageDeduplicationId: token,
    }));
    const receive = () => sqs.send(new ReceiveMessageCommand({
      QueueUrl: queues.commands,
      WaitTimeSeconds: 1,
      MaxNumberOfMessages: 1,
      MessageSystemAttributeNames: ['ApproximateReceiveCount'],
    }));
    const first = (await receive()).Messages?.[0];
    expect(first?.MessageId).toBe(sent.MessageId);
    expect(first?.ReceiptHandle).toBeDefined();
    await sqs.send(new ChangeMessageVisibilityCommand({
      QueueUrl: queues.commands,
      ReceiptHandle: first!.ReceiptHandle!,
      VisibilityTimeout: 0,
    }));
    const second = (await receive()).Messages?.[0];
    expect(second?.MessageId).toBe(sent.MessageId);
    expect(second?.Attributes?.ApproximateReceiveCount).toBe('2');
    await sqs.send(new DeleteMessageCommand({
      QueueUrl: queues.commands,
      ReceiptHandle: second!.ReceiptHandle!,
    }));
  });

  test('serves liveness from three independent Bun processes', async () => {
    const processes = Array.from({ length: 3 }, () => Bun.spawn([process.execPath, 'dist/main.js'], {
      env: { ...process.env, PORT: '0' },
      stdout: 'pipe',
      stderr: 'pipe',
    }));
    try {
      const servers = await Promise.all(processes.map(async (child) => {
        const reader = child.stdout.getReader();
        let timer: ReturnType<typeof setTimeout> | undefined;
        const first = await Promise.race([
          reader.read(),
          new Promise<never>((_, reject) => {
            timer = setTimeout(() => reject(new Error('Bun process startup timed out')), 10_000);
          }),
        ]).finally(() => clearTimeout(timer));
        reader.releaseLock();
        const started: { address: string } = JSON.parse(new TextDecoder().decode(first.value).trim());
        const address = started.address.replace('0.0.0.0', '127.0.0.1');
        const response = await fetch(`${address}/health/live`, { signal: AbortSignal.timeout(5_000) });
        expect(response.status).toBe(200);
        expect(await response.json()).toEqual({ status: 'ok' });
        return child.pid;
      }));
      expect(new Set(servers).size).toBe(3);
    } finally {
      for (const child of processes) child.kill('SIGTERM');
      await Promise.all(processes.map((child) => child.exited));
    }
  }, 15_000);
});
