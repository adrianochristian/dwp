import { afterEach, describe, expect, test } from 'bun:test';
import { CreateQueueCommand, DeleteQueueCommand, ReceiveMessageCommand, SendMessageCommand, type SQSClient } from '@aws-sdk/client-sqs';
import { createSqsClient, sqsCall, SqsCallTimeoutError } from '../../dist/infrastructure/messaging/sqs.js';

const integration = process.env.DWP_INTEGRATION === '1' ? describe : describe.skip;

integration('SQS SDK attempts and whole-call deadlines', () => {
  const clients: SQSClient[] = [];
  const servers: ReturnType<typeof Bun.serve>[] = [];

  afterEach(async () => {
    for (const client of clients.splice(0)) client.destroy();
    await Promise.all(servers.splice(0).map(server => server.stop(true)));
  });

  function client(endpoint?: string): SQSClient {
    const previous = process.env.SQS_ENDPOINT;
    try {
      if (endpoint) process.env.SQS_ENDPOINT = endpoint;
      const value = createSqsClient();
      clients.push(value);
      return value;
    } finally {
      if (previous === undefined) delete process.env.SQS_ENDPOINT;
      else process.env.SQS_ENDPOINT = previous;
    }
  }

  test('keeps three SDK attempts and makes the 25-second per-attempt timeout effective', async () => {
    let requests = 0;
    const server = Bun.serve({ hostname: '127.0.0.1', port: 0, fetch() {
      requests += 1;
      return Response.json({ __type: 'ServiceUnavailable', message: 'retryable probe' }, { status: 503 });
    } });
    servers.push(server);
    const sqs = client(server.url.href);
    await expect(sqsCall(signal => sqs.send(new SendMessageCommand({
      QueueUrl: server.url.href, MessageBody: 'deadline probe',
    }), { abortSignal: signal }), { timeoutMs: 5_000 })).rejects.toThrow();
    expect(await sqs.config.maxAttempts()).toBe(3);
    expect(requests).toBe(3);
    const handler = sqs.config.requestHandler as { httpHandlerConfigs(): Record<string, unknown> };
    expect(handler.httpHandlerConfigs()).toMatchObject({
      connectionTimeout: 3_000, requestTimeout: 25_000, throwOnRequestTimeout: true,
    });
  }, 8_000);

  test('bounds a broker connection that accepts a request and never sends a response', async () => {
    let requests = 0;
    const server = Bun.serve({ hostname: '127.0.0.1', port: 0, fetch: async () => {
      requests += 1;
      return new Promise<Response>(() => undefined);
    } });
    servers.push(server);
    const sqs = client(server.url.href);
    const started = performance.now();
    await expect(sqsCall(signal => sqs.send(new SendMessageCommand({
      QueueUrl: server.url.href, MessageBody: 'deadline probe',
    }), { abortSignal: signal }), { timeoutMs: 100 })).rejects.toBeInstanceOf(SqsCallTimeoutError);
    expect(performance.now() - started).toBeLessThan(1_000);
    expect(requests).toBe(1);
  });

  test('preserves a 20-second long-poll request and propagates explicit caller cancellation', async () => {
    let waitTime: number | undefined;
    let received!: () => void;
    const accepted = new Promise<void>(resolve => { received = resolve; });
    const server = Bun.serve({ hostname: '127.0.0.1', port: 0, fetch: async request => {
      const body = await request.json() as { WaitTimeSeconds?: number };
      waitTime = body.WaitTimeSeconds;
      received();
      return new Promise<Response>(() => undefined);
    } });
    servers.push(server);
    const sqs = client(server.url.href);
    const caller = new AbortController();
    const result = sqsCall(signal => sqs.send(new ReceiveMessageCommand({
      QueueUrl: server.url.href, WaitTimeSeconds: 20,
    }), { abortSignal: signal }), { signal: caller.signal }).catch(error => error as unknown);
    await accepted;
    expect(waitTime).toBe(20);
    const reason = new Error('caller stopped its poll');
    caller.abort(reason);
    expect(await result).toBe(reason);
  });

  test('completes a healthy empty 20-second long poll against real MiniStack', async () => {
    const sqs = client();
    const created = await sqsCall(signal => sqs.send(new CreateQueueCommand({
      QueueName: `deadline_poll_${crypto.randomUUID().replaceAll('-', '')}`,
    }), { abortSignal: signal }));
    if (!created.QueueUrl) throw new Error('Long-poll test queue requires a URL');
    try {
      const started = performance.now();
      const response = await sqsCall(signal => sqs.send(new ReceiveMessageCommand({
        QueueUrl: created.QueueUrl, WaitTimeSeconds: 20, MaxNumberOfMessages: 1,
      }), { abortSignal: signal }));
      expect(response.Messages ?? []).toHaveLength(0);
      expect(performance.now() - started).toBeGreaterThanOrEqual(19_000);
      expect(performance.now() - started).toBeLessThan(26_000);
    } finally {
      await sqsCall(signal => sqs.send(new DeleteQueueCommand({ QueueUrl: created.QueueUrl }), { abortSignal: signal }));
    }
  }, 28_000);
});
