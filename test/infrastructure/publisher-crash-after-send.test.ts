import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { OutboxPublisher } from '../../dist/infrastructure/messaging/outbox-publisher.js';
import {
  cleanupCrashFixture, createCrashEnvironment, createFinancialEvent, financialState,
  killCrashFixture, receiveEvents, spawnCrashFixture, waitForCheckpoint,
  type CrashChild, type CrashEnvironment,
} from '../fixtures/crash-support.js';

const integration = process.env.DWP_INTEGRATION === '1' ? describe : describe.skip;

integration('publisher process killed after SQS send', () => {
  let environment: CrashEnvironment;
  let child: CrashChild | undefined;
  let publisher: OutboxPublisher | undefined;

  beforeAll(async () => { environment = await createCrashEnvironment('pub_after'); });
  afterAll(async () => {
    await cleanupCrashFixture(child);
    await publisher?.stop();
    await environment?.close();
  });

  test('republication retains the eventId after a crash before recording delivery', async () => {
    const { eventId, walletId } = await createFinancialEvent(environment);
    const queueUrl = await environment.createQueue();
    child = spawnCrashFixture('test/fixtures/publisher-crash.ts', {
      DATABASE_URL: environment.databaseUrl, EVENTS_QUEUE_URL: queueUrl, CRASH_PHASE: 'after-send',
    });
    const checkpoint = await waitForCheckpoint(child, 'publisher_checkpoint');
    expect(checkpoint).toMatchObject({ phase: 'after-send', eventId });
    const original = await receiveEvents(environment.sqs, queueUrl, false);
    expect(original.map(event => event.eventId)).toEqual([eventId]);
    expect((await environment.query<{ published_at: Date | null }>(
      'select published_at from dwp.outbox_messages where id = ?', [eventId],
    ))[0]?.published_at).toBeNull();
    expect(await killCrashFixture(child)).toBe(137);

    await Bun.sleep(450);
    publisher = new OutboxPublisher(environment.orm, environment.sqs, queueUrl, { leaseMs: 300, batchSize: 1 });
    expect(await publisher.runOnce()).toBe(1);
    const delivered = await receiveEvents(environment.sqs, queueUrl);
    expect(delivered.map(event => event.eventId)).toEqual([eventId, eventId]);
    expect(delivered[0]).toEqual(delivered[1]);
    expect(delivered[0]).toEqual(original[0]);
    expect((await environment.query<{ published: boolean; lease_token: string | null; attempts: number }>(`
      select published_at is not null as published, lease_token, attempts from dwp.outbox_messages where id = ?`, [eventId],
    ))[0]).toEqual({ published: true, lease_token: null, attempts: 2 });
    const balances = await financialState(environment, walletId);
    expect(balances).toEqual({ balance_cents: '7500', calculated: '7500', debits: '1' });
    expect(BigInt(balances.balance_cents) >= 0n).toBe(true);
  }, 15_000);
});
