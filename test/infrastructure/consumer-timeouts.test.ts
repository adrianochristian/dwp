import { afterEach, describe, expect, test } from 'bun:test';
import { ChangeMessageVisibilityCommand, SQSClient } from '@aws-sdk/client-sqs';
import { SqsCallTimeoutError } from '../../dist/infrastructure/messaging/sqs.js';
import { SqsConsumer } from '../../dist/infrastructure/messaging/sqs-consumer.js';
import { consumerTestContext } from './consumer-support.js';

const integration = process.env.DWP_INTEGRATION === '1' ? describe : describe.skip;

interface RequestMetadata {
  action: string;
  queueUrl: string | undefined;
}

integration('consumer transport deadlines and cancellation', () => {
  const context = consumerTestContext();
  const clients = new Set<SQSClient>();
  const gates = new Set<() => Promise<void>>();
  const workers = new Set<SqsConsumer>();

  afterEach(async () => {
    await Promise.allSettled([...workers].map(worker => worker.stop({ timeoutMs: 100, cleanupMs: 100 })));
    workers.clear();
    for (const client of clients) client.destroy();
    clients.clear();
    await Promise.allSettled([...gates].map(close => close()));
    gates.clear();
  });

  function gate(silent: (request: RequestMetadata) => boolean): SQSClient {
    const upstream = new URL(process.env.SQS_ENDPOINT ?? 'http://127.0.0.1:4566');
    const server = Bun.serve({
      hostname: '127.0.0.1', port: 0,
      async fetch(request) {
        const bytes = await request.arrayBuffer();
        const body = JSON.parse(new TextDecoder().decode(bytes)) as { QueueUrl?: string };
        const metadata = {
          action: request.headers.get('x-amz-target')?.split('.').at(-1) ?? '',
          queueUrl: body.QueueUrl,
        };
        if (silent(metadata)) return new Promise<Response>(() => undefined);
        const target = new URL(request.url);
        target.protocol = upstream.protocol;
        target.host = upstream.host;
        const response = await fetch(target, {
          method: request.method, headers: request.headers, body: bytes, signal: AbortSignal.timeout(3_000),
        });
        return new Response(response.body, { status: response.status, headers: response.headers });
      },
    });
    gates.add(async () => { await server.stop(true); });
    const client = new SQSClient({
      region: process.env.AWS_REGION ?? 'us-east-1', endpoint: server.url.href, useQueueUrlAsEndpoint: false,
      credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? '000000000000',
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? 'local',
      },
      maxAttempts: 1,
      requestHandler: { connectionTimeout: 3_000, requestTimeout: 25_000 },
    });
    clients.add(client);
    return client;
  }

  function worker(client: SQSClient, options: ConstructorParameters<typeof SqsConsumer>[4] = {}): SqsConsumer {
    const instance = new SqsConsumer(context.service, client, context.commandsUrl, context.dlqUrl, {
      consumerName: context.consumerName, visibilitySeconds: 1, operationTimeoutMs: 1_500, ...options,
    });
    workers.add(instance);
    return instance;
  }

  test('bounds a receive against a broker that accepts the connection without replying', async () => {
    let entered = 0;
    const client = gate(({ action }) => { if (action === 'ReceiveMessage') { entered += 1; return true; } return false; });
    const consumer = worker(client, { operationTimeoutMs: 100 });
    const started = performance.now();
    await expect(consumer.runOnce()).rejects.toBeInstanceOf(SqsCallTimeoutError);
    expect(performance.now() - started).toBeLessThan(1_000);
    expect(entered).toBe(1);
    await consumer.stop({ timeoutMs: 50, cleanupMs: 100 });
    expect(await consumer.runOnce()).toBe(0);
  });

  test('keeps visibility during a silent ack, then retries without another financial effect', async () => {
    const { wallet, envelope } = await context.fixture();
    let blockAck = true;
    let entered!: () => void;
    const blocked = new Promise<void>(resolve => { entered = resolve; });
    const client = gate(({ action }) => {
      if (blockAck && action === 'DeleteMessage') { entered(); return true; }
      return false;
    });
    const retries: string[] = [];
    const consumer = worker(client, { onRetry: observation => { retries.push(observation.failureCode ?? ''); } });
    await context.send(envelope);
    const running = consumer.runOnce();
    await blocked;
    await Bun.sleep(1_100);
    expect(await context.receive()).toHaveLength(0);
    expect(await running).toBe(1);
    expect(retries).toEqual(['ACK_FAILED']);
    expect(await context.query('select status, failure_code from dwp.wager_transactions where idempotency_key = ?',
      [envelope.data.idempotencyKey])).toEqual([{ status: 'PROCESSED', failure_code: null }]);
    blockAck = false;
    expect(await consumer.runOnce({ waitTimeSeconds: 2 })).toBe(1);
    expect(await context.receive()).toHaveLength(0);
    expect(await context.receive(context.dlqUrl)).toHaveLength(0);
    await context.assertConsistent(wallet.id, '9000', 1);
    expect(await context.query('select message_id from dwp.inbox_messages where consumer_name = ? and message_id = ?',
      [context.consumerName, envelope.messageId])).toHaveLength(1);
    expect(await context.query("select id from dwp.outbox_messages where event_type = 'WagerTransactionFailed' and payload->'data'->>'externalTransactionId' = ?",
      [envelope.data.externalTransactionId]))
      .toHaveLength(0);
  }, 10_000);

  test('retains a FAILED source command while its DLQ endpoint is silent and recovers with its original code', async () => {
    const { wallet, envelope } = await context.fixture();
    await context.service.failSubmission(envelope.data, envelope.data.idempotencyKey, 'PROCESSING_FAILED', {
      consumerName: context.consumerName, messageId: envelope.messageId,
    });
    let blockDlq = true;
    let entered!: () => void;
    const blocked = new Promise<void>(resolve => { entered = resolve; });
    const client = gate(({ action, queueUrl }) => {
      if (blockDlq && action === 'SendMessage' && queueUrl === context.dlqUrl) { entered(); return true; }
      return false;
    });
    const consumer = worker(client);
    await context.send(envelope);
    const outcome = consumer.runOnce().then(value => ({ value, error: undefined as unknown }), error => ({ value: undefined, error }));
    await blocked;
    await Bun.sleep(1_100);
    expect(await context.receive()).toHaveLength(0);
    expect(await outcome).toEqual({ value: 1, error: undefined });
    expect(await context.query('select status, failure_code from dwp.wager_transactions where idempotency_key = ?',
      [envelope.data.idempotencyKey])).toEqual([{ status: 'FAILED', failure_code: 'PROCESSING_FAILED' }]);
    blockDlq = false;
    expect(await consumer.runOnce({ waitTimeSeconds: 2 })).toBe(1);
    const messages = await context.receive(context.dlqUrl);
    expect(messages).toHaveLength(1);
    expect(messages[0]?.Body).toBe(JSON.stringify(envelope));
    expect(messages[0]?.MessageAttributes?.failureCode?.StringValue).toBe('PROCESSING_FAILED');
    expect(await context.receive()).toHaveLength(0);
    await context.assertConsistent(wallet.id, '10000', 0);
  }, 10_000);

  test('cancels a silent ack at the shutdown deadline and returns visibility without FAILED auditing', async () => {
    const { wallet, envelope } = await context.fixture();
    let entered!: () => void;
    const blocked = new Promise<void>(resolve => { entered = resolve; });
    const client = gate(({ action }) => { if (action === 'DeleteMessage') { entered(); return true; } return false; });
    const consumer = worker(client, { operationTimeoutMs: 10_000, maxAttempts: 1 });
    await context.send(envelope);
    const running = consumer.runOnce();
    await blocked;
    const started = performance.now();
    const stopping = consumer.stop({ timeoutMs: 30, cleanupMs: 100 });
    expect(await consumer.runOnce()).toBe(0);
    await stopping;
    expect(performance.now() - started).toBeLessThan(500);
    expect(await running).toBe(1);
    expect(await context.query('select status, failure_code from dwp.wager_transactions where idempotency_key = ?',
      [envelope.data.idempotencyKey])).toEqual([{ status: 'PROCESSED', failure_code: null }]);
    const returned = await context.receive();
    expect(returned).toHaveLength(1);
    await context.sqs.send(new ChangeMessageVisibilityCommand({
      QueueUrl: context.commandsUrl, ReceiptHandle: returned[0]!.ReceiptHandle!, VisibilityTimeout: 0,
    }));
    expect(await context.consumer().runOnce()).toBe(1);
    expect(await context.receive(context.dlqUrl)).toHaveLength(0);
    await context.assertConsistent(wallet.id, '9000', 1);
    expect(await context.query("select id from dwp.outbox_messages where event_type = 'WagerTransactionFailed' and payload->'data'->>'externalTransactionId' = ?",
      [envelope.data.externalTransactionId]))
      .toHaveLength(0);
  });

  test('propagates caller cancellation and lets a replacement consume the released message', async () => {
    const { wallet, envelope } = await context.fixture();
    let entered!: () => void;
    const blocked = new Promise<void>(resolve => { entered = resolve; });
    const client = gate(({ action }) => { if (action === 'DeleteMessage') { entered(); return true; } return false; });
    const consumer = worker(client, { operationTimeoutMs: 10_000, maxAttempts: 1 });
    const cancellation = new AbortController();
    await context.send(envelope);
    const outcome = consumer.runOnce({ signal: cancellation.signal }).then(() => undefined, error => error as unknown);
    await blocked;
    const reason = new Error('caller cancelled command transport');
    cancellation.abort(reason);
    expect(await outcome).toBe(reason);
    await consumer.stop({ timeoutMs: 50, cleanupMs: 100 });
    expect(await context.consumer().runOnce()).toBe(1);
    expect(await context.receive()).toHaveLength(0);
    expect(await context.receive(context.dlqUrl)).toHaveLength(0);
    await context.assertConsistent(wallet.id, '9000', 1);
    expect(await context.query("select id from dwp.outbox_messages where event_type = 'WagerTransactionFailed' and payload->'data'->>'externalTransactionId' = ?",
      [envelope.data.externalTransactionId]))
      .toHaveLength(0);
  });

  test('quiesces an active silent receive without waiting for its operation deadline', async () => {
    let entered!: () => void;
    const blocked = new Promise<void>(resolve => { entered = resolve; });
    const client = gate(({ action }) => { if (action === 'ReceiveMessage') { entered(); return true; } return false; });
    const consumer = worker(client, { operationTimeoutMs: 10_000 });
    const running = consumer.runOnce({ waitTimeSeconds: 20 });
    await blocked;
    const started = performance.now();
    await consumer.stop({ timeoutMs: 50, cleanupMs: 100 });
    expect(await running).toBe(0);
    expect(performance.now() - started).toBeLessThan(300);
    expect(await consumer.runOnce()).toBe(0);
  });

  test('wakes an existing retry delay immediately when quiescing', async () => {
    let retried!: () => void;
    const delayed = new Promise<void>(resolve => { retried = resolve; });
    const client = gate(({ action }) => action === 'ReceiveMessage');
    const consumer = worker(client, { operationTimeoutMs: 100, onError: () => retried() });
    const running = consumer.run();
    await delayed;
    const started = performance.now();
    await consumer.stop({ timeoutMs: 50, cleanupMs: 100 });
    await running;
    expect(performance.now() - started).toBeLessThan(300);
    expect(await consumer.runOnce()).toBe(0);
  });
});
