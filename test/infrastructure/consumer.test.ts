import { describe, expect, test } from 'bun:test';
import { consumerTestContext } from './consumer-support.js';

const integration = process.env.DWP_INTEGRATION === '1' ? describe : describe.skip;

integration('SQS command consumer with persistent financial effects', () => {
  const context = consumerTestContext();
  const { consumerName, consumer, fixture, send, receive, query, assertConsistent } = context;

  test('deduplicates the envelope messageId after redelivery with a different FIFO deduplication id', async () => {
    const { wallet, envelope } = await fixture();
    const worker = consumer();
    await send(envelope);
    expect(await worker.runOnce()).toBe(1);
    // Both AWS sends use a fresh FIFO deduplication id; only the SQL inbox can deduplicate this identity.
    await send(envelope);
    expect(await worker.runOnce()).toBe(1);
    expect(await receive()).toHaveLength(0);
    expect(await query('select message_id from dwp.inbox_messages where consumer_name = ? and message_id = ? and processed_at is not null',
      [consumerName, envelope.messageId])).toHaveLength(1);
    expect(await query('select id from dwp.wager_transactions where idempotency_key = ?', [envelope.data.idempotencyKey])).toHaveLength(1);
    expect(await query("select id from dwp.outbox_messages where payload->'data'->>'externalTransactionId' = ?",
      [envelope.data.externalTransactionId])).toHaveLength(1);
    await assertConsistent(wallet.id, '9000', 1);
  });

  test('redelivers a committed command after failure before ack without duplicating inbox, debit or events', async () => {
    const { wallet, envelope } = await fixture();
    let first = true;
    const worker = consumer(context.service, { afterCommitBeforeAck: () => {
      if (first) { first = false; throw new Error('injected failure after commit before ack'); }
    } });
    await send(envelope);
    expect(await worker.runOnce()).toBe(1);
    await assertConsistent(wallet.id, '9000', 1);
    expect(await worker.runOnce({ waitTimeSeconds: 2 })).toBe(1);
    expect(await receive()).toHaveLength(0);
    expect(await receive(context.dlqUrl)).toHaveLength(0);
    expect(await query('select message_id from dwp.inbox_messages where consumer_name = ? and message_id = ?',
      [consumerName, envelope.messageId])).toHaveLength(1);
    expect(await query("select id from dwp.outbox_messages where payload->'data'->>'externalTransactionId' = ?",
      [envelope.data.externalTransactionId])).toHaveLength(1);
    await assertConsistent(wallet.id, '9000', 1);
  });

  test('acks a terminal insufficient-funds rejection without retrying or producing a ledger debit', async () => {
    const { wallet, envelope } = await fixture('101.00');
    const worker = consumer();
    await send(envelope);
    expect(await worker.runOnce()).toBe(1);
    expect(await worker.runOnce()).toBe(0);
    expect(await receive(context.dlqUrl)).toHaveLength(0);
    const transactions = await query<{ status: string; failure_code: string }>(
      'select status, failure_code from dwp.wager_transactions where idempotency_key = ?', [envelope.data.idempotencyKey],
    );
    expect(transactions).toEqual([{ status: 'REJECTED', failure_code: 'INSUFFICIENT_FUNDS' }]);
    expect(await query('select message_id from dwp.inbox_messages where consumer_name = ? and message_id = ? and processed_at is not null',
      [consumerName, envelope.messageId])).toHaveLength(1);
    await assertConsistent(wallet.id, '10000', 0);
  });


});
