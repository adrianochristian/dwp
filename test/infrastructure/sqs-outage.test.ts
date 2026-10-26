import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { SQSClient } from '@aws-sdk/client-sqs';
import { OutboxPublisher } from '../../dist/infrastructure/messaging/outbox-publisher.js';
import {
  createCrashEnvironment, createFinancialEvent, financialState, receiveEvents,
  type CrashEnvironment,
} from '../fixtures/crash-support.js';

const integration = process.env.DWP_INTEGRATION === '1' ? describe : describe.skip;

integration('SQS unavailability after a financial commit', () => {
  let environment: CrashEnvironment;
  let routedSqs: SQSClient;
  let publisher: OutboxPublisher | undefined;
  let closeGate: (() => Promise<void>) | undefined;
  let available = false;
  let refused = 0;
  let forwarded = 0;

  beforeAll(async () => {
    environment = await createCrashEnvironment('sqs_outage');
    const upstream = new URL(process.env.SQS_ENDPOINT ?? 'http://127.0.0.1:4566');
    const gate = Bun.serve({
      hostname: '127.0.0.1',
      port: 0,
      async fetch(request) {
        if (!available) {
          refused += 1;
          return new Response(JSON.stringify({ __type: 'ServiceUnavailable', message: 'SQS temporarily unavailable' }), {
            status: 503,
            headers: { 'content-type': 'application/x-amz-json-1.0' },
          });
        }
        forwarded += 1;
        const target = new URL(request.url);
        target.protocol = upstream.protocol;
        target.host = upstream.host;
        const response = await fetch(target, {
          method: request.method,
          headers: request.headers,
          body: await request.arrayBuffer(),
          signal: AbortSignal.timeout(3_000),
        });
        return new Response(response.body, { status: response.status, headers: response.headers });
      },
    });
    closeGate = async () => { await gate.stop(true); };
    routedSqs = new SQSClient({
      region: process.env.AWS_REGION ?? 'us-east-1',
      endpoint: gate.url.href,
      useQueueUrlAsEndpoint: false,
      credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? '000000000000',
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? 'local',
      },
      maxAttempts: 1,
      requestHandler: { connectionTimeout: 1_000, requestTimeout: 3_000 },
    });
  });

  afterAll(async () => {
    await publisher?.stop();
    routedSqs?.destroy();
    await closeGate?.();
    await environment?.close();
  });

  test('keeps the committed event durable and publishes it after a real broker connection recovers', async () => {
    const { eventId, walletId } = await createFinancialEvent(environment);
    const queueUrl = await environment.createQueue();
    const original = (await environment.query<{ payload: Record<string, unknown> }>(
      'select payload from dwp.outbox_messages where id = ?', [eventId],
    ))[0]!.payload;
    publisher = new OutboxPublisher(environment.orm, routedSqs, queueUrl, { batchSize: 1 });

    expect(await publisher.runOnce()).toBe(0);
    expect(refused).toBe(1);
    expect(forwarded).toBe(0);
    const pending = await environment.query<{
      published_at: Date | null; lease_token: string | null; attempts: number; retry_scheduled: boolean;
    }>(`select published_at, lease_token, attempts, next_attempt_at > clock_timestamp() as retry_scheduled
      from dwp.outbox_messages where id = ?`, [eventId]);
    expect(pending[0]).toEqual({ published_at: null, lease_token: null, attempts: 1, retry_scheduled: true });
    expect(await receiveEvents(environment.sqs, queueUrl)).toHaveLength(0);
    expect(await financialState(environment, walletId)).toEqual({ balance_cents: '7500', calculated: '7500', debits: '1' });

    available = true;
    // The retry uses its persisted deadline rather than retrying in a tight loop.
    expect(await publisher.runOnce()).toBe(0);
    expect(forwarded).toBe(0);
    await Bun.sleep(1_100);
    expect(await publisher.runOnce()).toBe(1);
    expect(refused).toBe(1);
    expect(forwarded).toBe(1);
    const recovered = await receiveEvents(environment.sqs, queueUrl);
    expect(recovered).toEqual([original]);
    expect(recovered[0]?.eventId).toBe(eventId);
    expect((await environment.query<{ published: boolean; lease_token: string | null; attempts: number }>(`
      select published_at is not null as published, lease_token, attempts
      from dwp.outbox_messages where id = ?`, [eventId],
    ))[0]).toEqual({ published: true, lease_token: null, attempts: 2 });
    const balances = await financialState(environment, walletId);
    expect(balances).toEqual({ balance_cents: '7500', calculated: '7500', debits: '1' });
    expect(BigInt(balances.balance_cents) >= 0n).toBe(true);
  }, 15_000);
});
