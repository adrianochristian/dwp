import { afterAll, beforeAll, beforeEach, describe, expect, test } from 'bun:test';
import {
  CreateQueueCommand,
  DeleteMessageBatchCommand,
  DeleteQueueCommand,
  ReceiveMessageCommand,
  type SQSClient,
} from '@aws-sdk/client-sqs';
import type { MikroORM } from '@mikro-orm/postgresql';
import { createDatabase } from '../../dist/infrastructure/persistence/database.js';
import { OutboxPublisher } from '../../dist/infrastructure/messaging/outbox-publisher.js';
import { createSqsClient } from '../../dist/infrastructure/messaging/sqs.js';

const integration = process.env.DWP_INTEGRATION === '1' ? describe : describe.skip;

integration('transactional outbox publishers', () => {
  const databaseName = `dwp_outbox_${crypto.randomUUID().replaceAll('-', '')}`;
  let admin: MikroORM;
  let orm: MikroORM;
  let sqs: SQSClient;
  let queueUrl: string;

  beforeAll(async () => {
    admin = await createDatabase();
    await admin.em.getConnection().execute(`create database ${databaseName}`);
    const url = new URL(process.env.DATABASE_URL ?? 'postgresql://dwp:dwp@127.0.0.1:5432/dwp');
    url.pathname = `/${databaseName}`;
    orm = await createDatabase(url.href);
    await orm.migrator.up();
    sqs = createSqsClient();
    const created = await sqs.send(new CreateQueueCommand({ QueueName: databaseName }));
    if (!created.QueueUrl) throw new Error('Outbox test queue has no URL');
    queueUrl = created.QueueUrl;
  });

  beforeEach(async () => {
    await orm.em.getConnection().execute('delete from dwp.outbox_messages');
    await collectEvents();
  });

  afterAll(async () => {
    if (queueUrl) await sqs.send(new DeleteQueueCommand({ QueueUrl: queueUrl }));
    sqs?.destroy();
    await orm?.close(true);
    if (admin) {
      await admin.em.getConnection().execute(`drop database if exists ${databaseName} with (force)`);
      await admin.close(true);
    }
  });

  async function enqueue(count = 1): Promise<string[]> {
    const ids = Array.from({ length: count }, () => crypto.randomUUID());
    await orm.em.fork().transactional(async (em) => {
      for (const id of ids) {
        const payload = {
          eventId: id,
          aggregateId: crypto.randomUUID(),
          eventType: 'WagerTransactionProcessed',
          correlationId: crypto.randomUUID(),
          occurredAt: new Date().toISOString(),
          version: 1,
          data: { transactionId: crypto.randomUUID() },
        };
        await em.execute(`insert into dwp.outbox_messages
          (id,aggregate_id,event_type,payload,occurred_at) values (?, ?, ?, ?::jsonb, ?)`,
        [id, payload.aggregateId, payload.eventType, JSON.stringify(payload), payload.occurredAt]);
      }
    });
    return ids;
  }

  async function collectEvents(): Promise<{ eventId: string }[]> {
    const events: { eventId: string }[] = [];
    for (;;) {
      const received = await sqs.send(new ReceiveMessageCommand({
        QueueUrl: queueUrl, MaxNumberOfMessages: 10, WaitTimeSeconds: 0,
      }));
      if (!received.Messages?.length) return events;
      for (const message of received.Messages) events.push(JSON.parse(message.Body!) as { eventId: string });
      await sqs.send(new DeleteMessageBatchCommand({
        QueueUrl: queueUrl,
        Entries: received.Messages.map((message, index) => ({ Id: String(index), ReceiptHandle: message.ReceiptHandle! })),
      }));
    }
  }

  test('two publishers claim concurrently and publish each due event once', async () => {
    const ids = await enqueue(30);
    const left = new OutboxPublisher(orm, sqs, queueUrl, { batchSize: 20 });
    const right = new OutboxPublisher(orm, sqs, queueUrl, { batchSize: 20 });
    const published = await Promise.all([left.runOnce(), right.runOnce()]);
    expect(published.reduce((sum, count) => sum + count, 0)).toBe(30);
    const received = await collectEvents();
    expect(received).toHaveLength(30);
    expect(received.map((event) => event.eventId).sort()).toEqual([...ids].sort());
    const state = await orm.em.getConnection().execute<{ published: string; leased: string; attempts: string }[]>(`
      select count(*) filter (where published_at is not null)::text as published,
        count(*) filter (where lease_token is not null)::text as leased,
        sum(attempts)::text as attempts from dwp.outbox_messages
    `);
    expect(state[0]).toEqual({ published: '30', leased: '0', attempts: '30' });
    expect(await left.runOnce()).toBe(0);
    await Promise.all([left.stop(), right.stop()]);
  });

  test('recovers an abandoned lease and preserves the persisted eventId', async () => {
    const [id] = await enqueue();
    await orm.em.getConnection().execute(`update dwp.outbox_messages
      set lease_token = ?, lease_until = clock_timestamp() - interval '1 second', attempts = 1
      where id = ?`, [crypto.randomUUID(), id]);
    const publisher = new OutboxPublisher(orm, sqs, queueUrl);
    expect(await publisher.runOnce()).toBe(1);
    expect((await collectEvents()).map((event) => event.eventId)).toEqual([id!]);
    const state = await orm.em.getConnection().execute<{ attempts: number }[]>('select attempts from dwp.outbox_messages where id = ?', [id]);
    expect(state[0]?.attempts).toBe(2);
    await publisher.stop();
  });

  test('retries a send-before-ack failure using the same eventId', async () => {
    const [id] = await enqueue();
    let fail = true;
    const publisher = new OutboxPublisher(orm, sqs, queueUrl, {
      afterSend: () => { if (fail) throw new Error('send completed before publisher stopped'); },
    });
    expect(await publisher.runOnce()).toBe(0);
    const retry = await orm.em.getConnection().execute<{ attempts: number; published_at: Date | null; due: boolean; lease_token: string | null }[]>(`
      select attempts, published_at, next_attempt_at > clock_timestamp() as due, lease_token
      from dwp.outbox_messages where id = ?
    `, [id]);
    expect(retry[0]).toEqual({ attempts: 1, published_at: null, due: true, lease_token: null });
    expect(await publisher.runOnce()).toBe(0);
    fail = false;
    await orm.em.getConnection().execute('update dwp.outbox_messages set next_attempt_at = clock_timestamp() where id = ?', [id]);
    expect(await publisher.runOnce()).toBe(1);
    expect((await collectEvents()).map((event) => event.eventId)).toEqual([id!, id!]);
    await publisher.stop();
  });

  test('renews ownership while a slow publication is in progress', async () => {
    await enqueue();
    let release!: () => void;
    let started!: () => void;
    const sending = new Promise<void>((resolve) => { started = resolve; });
    const blocked = new Promise<void>((resolve) => { release = resolve; });
    const slow = new OutboxPublisher(orm, sqs, queueUrl, {
      leaseMs: 120,
      beforeSend: async () => { started(); await blocked; },
    });
    const other = new OutboxPublisher(orm, sqs, queueUrl, { leaseMs: 120 });
    const first = slow.runOnce();
    await sending;
    try {
      await Bun.sleep(260);
      expect(await other.runOnce()).toBe(0);
    } finally {
      release();
    }
    expect(await first).toBe(1);
    expect(await collectEvents()).toHaveLength(1);
    await Promise.all([slow.stop(), other.stop()]);
  });

  test('a publisher that lost its token cannot mark another lease as published', async () => {
    const [id] = await enqueue();
    const replacementToken = crypto.randomUUID();
    let lost = 0;
    const stale = new OutboxPublisher(orm, sqs, queueUrl, {
      afterSend: async () => {
        await orm.em.getConnection().execute(`update dwp.outbox_messages
          set lease_token = ?, lease_until = clock_timestamp() + interval '30 seconds' where id = ?`, [replacementToken, id]);
      },
      onLeaseLost: () => { lost += 1; },
    });
    expect(await stale.runOnce()).toBe(0);
    const state = await orm.em.getConnection().execute<{ published_at: Date | null; lease_token: string }[]>(
      'select published_at, lease_token from dwp.outbox_messages where id = ?', [id],
    );
    expect(state[0]).toEqual({ published_at: null, lease_token: replacementToken });
    expect(lost).toBe(1);
    expect((await collectEvents()).map((event) => event.eventId)).toEqual([id!]);
    await stale.stop();
  });
});
