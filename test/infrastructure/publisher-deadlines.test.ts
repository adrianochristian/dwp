import { afterAll, afterEach, beforeAll, describe, expect, test } from 'bun:test';
import { SQSClient } from '@aws-sdk/client-sqs';
import { OutboxPublisher } from '../../dist/infrastructure/messaging/outbox-publisher.js';
import { createCrashEnvironment, createFinancialEvent, financialState, receiveEvents, type CrashEnvironment } from '../fixtures/crash-support.js';

const integration = process.env.DWP_INTEGRATION === '1' ? describe : describe.skip;

integration('publisher transport and shutdown deadlines', () => {
  let environment: CrashEnvironment;
  const publishers: OutboxPublisher[] = [];
  const clients: SQSClient[] = [];
  const servers: ReturnType<typeof Bun.serve>[] = [];

  beforeAll(async () => { environment = await createCrashEnvironment('pub_deadline'); });
  afterEach(async () => {
    await Promise.all(publishers.splice(0).map(publisher => publisher.stop({ timeoutMs: 50, cleanupMs: 100 })));
    for (const client of clients.splice(0)) client.destroy();
    await Promise.all(servers.splice(0).map(server => server.stop(true)));
    await environment.query('delete from dwp.outbox_messages');
  });
  afterAll(async () => { await environment?.close(); });

  function silentBroker(): { sqs: SQSClient; entered: Promise<void> } {
    let entered!: () => void;
    const accepted = new Promise<void>(resolve => { entered = resolve; });
    const server = Bun.serve({ hostname: '127.0.0.1', port: 0, fetch: async () => {
      entered();
      return new Promise<Response>(() => undefined);
    } });
    servers.push(server);
    const sqs = new SQSClient({
      endpoint: server.url.href, useQueueUrlAsEndpoint: false, region: 'us-east-1',
      credentials: { accessKeyId: '000000000000', secretAccessKey: 'local' }, maxAttempts: 3,
      requestHandler: { connectionTimeout: 3_000, requestTimeout: 25_000, throwOnRequestTimeout: true },
    });
    clients.push(sqs);
    return { sqs, entered: accepted };
  }

  function publisher(sqs: SQSClient, queueUrl: string, options: ConstructorParameters<typeof OutboxPublisher>[3] = {}) {
    const instance = new OutboxPublisher(environment.orm, sqs, queueUrl, { batchSize: 1, leaseMs: 300, ...options });
    publishers.push(instance);
    return instance;
  }

  test('a silent broker produces a persisted retry and a replacement publishes the original event', async () => {
    const { eventId, walletId } = await createFinancialEvent(environment);
    const queueUrl = await environment.createQueue();
    const broker = silentBroker();
    const stalled = publisher(broker.sqs, queueUrl, { operationTimeoutMs: 100 });
    expect(await stalled.runOnce()).toBe(0);
    const [pending] = await environment.query<{ published_at: Date | null; lease_token: string | null; attempts: number; retry: boolean }>(`
      select published_at, lease_token, attempts, next_attempt_at > clock_timestamp() as retry
      from dwp.outbox_messages where id = ?`, [eventId]);
    expect(pending).toEqual({ published_at: null, lease_token: null, attempts: 1, retry: true });
    await Bun.sleep(1_100);
    expect(await publisher(environment.sqs, queueUrl).runOnce()).toBe(1);
    expect((await receiveEvents(environment.sqs, queueUrl)).map(event => event.eventId)).toEqual([eventId]);
    expect(await financialState(environment, walletId)).toEqual({ balance_cents: '7500', calculated: '7500', debits: '1' });
  });

  test('shutdown aborts a silent send and releases only its still-owned lease', async () => {
    const { eventId, walletId } = await createFinancialEvent(environment);
    const queueUrl = await environment.createQueue();
    const broker = silentBroker();
    const stalled = publisher(broker.sqs, queueUrl, { operationTimeoutMs: 10_000 });
    const running = stalled.runOnce();
    await broker.entered;
    const started = performance.now();
    await stalled.stop({ timeoutMs: 20, cleanupMs: 100 });
    expect(performance.now() - started).toBeLessThan(500);
    expect(await running).toBe(0);
    expect(await stalled.runOnce()).toBe(0);
    expect((await environment.query<{ published_at: Date | null; lease_token: string | null }>(
      'select published_at, lease_token from dwp.outbox_messages where id = ?', [eventId],
    ))[0]).toEqual({ published_at: null, lease_token: null });
    expect(await publisher(environment.sqs, queueUrl).runOnce()).toBe(1);
    expect((await receiveEvents(environment.sqs, queueUrl)).map(event => event.eventId)).toEqual([eventId]);
    expect(await financialState(environment, walletId)).toEqual({ balance_cents: '7500', calculated: '7500', debits: '1' });
  });

  test('shutdown never releases a lease that was taken over by another owner', async () => {
    const { eventId } = await createFinancialEvent(environment);
    const queueUrl = await environment.createQueue();
    const broker = silentBroker();
    const stalled = publisher(broker.sqs, queueUrl, { operationTimeoutMs: 10_000 });
    const running = stalled.runOnce();
    await broker.entered;
    const replacement = crypto.randomUUID();
    await environment.query(`update dwp.outbox_messages set lease_token = ?,
      lease_until = clock_timestamp() + interval '30 seconds' where id = ?`, [replacement, eventId]);
    await stalled.stop({ timeoutMs: 10, cleanupMs: 100 });
    expect(await running).toBe(0);
    expect((await environment.query<{ lease_token: string; published_at: Date | null }>(
      'select lease_token, published_at from dwp.outbox_messages where id = ?', [eventId],
    ))[0]).toEqual({ lease_token: replacement, published_at: null });
  });

  test('a blocked SQL heartbeat and lease cleanup cannot hold publisher.stop indefinitely', async () => {
    const { eventId } = await createFinancialEvent(environment);
    const queueUrl = await environment.createQueue();
    const broker = silentBroker();
    const stalled = publisher(broker.sqs, queueUrl, { leaseMs: 120, operationTimeoutMs: 10_000 });
    const running = stalled.runOnce();
    await broker.entered;
    let release!: () => void;
    let acquired!: () => void;
    const blocked = new Promise<void>(resolve => { release = resolve; });
    const locked = new Promise<void>(resolve => { acquired = resolve; });
    const holder = environment.orm.em.fork().transactional(async em => {
      await em.execute('select id from dwp.outbox_messages where id = ? for update', [eventId]);
      acquired();
      await blocked;
    });
    try {
      await locked;
      await Bun.sleep(70);
      const started = performance.now();
      await stalled.stop({ timeoutMs: 20, cleanupMs: 50 });
      expect(performance.now() - started).toBeLessThan(500);
      expect(await running).toBe(0);
    } finally {
      release();
      await holder;
    }
    const replacement = publisher(environment.sqs, queueUrl);
    let published = 0;
    const expires = performance.now() + 1_000;
    while (published === 0 && performance.now() < expires) {
      published = await replacement.runOnce();
      if (published === 0) await Bun.sleep(10);
    }
    expect(published).toBe(1);
    expect((await receiveEvents(environment.sqs, queueUrl)).map(event => event.eventId)).toEqual([eventId]);
  });
});
