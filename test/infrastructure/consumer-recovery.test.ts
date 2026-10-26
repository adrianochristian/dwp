import { createWageringService } from '../../dist/infrastructure/persistence/wagering-services.js';
import { describe, expect, test } from 'bun:test';
import { ChangeMessageVisibilityCommand, SendMessageCommand } from '@aws-sdk/client-sqs';
import type { WagerCommandHandler } from '../../dist/infrastructure/messaging/sqs-consumer.js';
import { consumerTestContext } from './consumer-support.js';

const integration = process.env.DWP_INTEGRATION === '1' ? describe : describe.skip;

integration('SQS command retry, DLQ and controlled shutdown', () => {
  const context = consumerTestContext();
  const { consumerName, consumer, fixture, send, receive, query, assertConsistent } = context;

  test('moves a malformed envelope to DLQ with the original body and a stable failureCode', async () => {
    const worker = consumer();
    const body = '{"messageId":"bad-envelope","type":"UnknownCommand"}';
    await context.sqs.send(new SendMessageCommand({
      QueueUrl: context.commandsUrl, MessageBody: body, MessageGroupId: 'invalid', MessageDeduplicationId: crypto.randomUUID(),
    }));
    expect(await worker.runOnce()).toBe(1);
    expect(await receive()).toHaveLength(0);
    const deadLetters = await receive(context.dlqUrl);
    expect(deadLetters).toHaveLength(1);
    expect(deadLetters[0]!.Body).toBe(body);
    expect(deadLetters[0]!.MessageAttributes?.failureCode?.StringValue).toBe('INVALID_COMMAND_ENVELOPE');
    expect(deadLetters[0]!.MessageAttributes?.sourceMessageId?.StringValue).toBeString();
  });

  test('dead-letters application validation errors immediately without retrying the financial command', async () => {
    const { wallet, envelope } = await fixture();
    envelope.data.kind = 'OPENING';
    const worker = consumer();
    await send(envelope);
    expect(await worker.runOnce()).toBe(1);
    expect(await receive()).toHaveLength(0);
    const deadLetters = await receive(context.dlqUrl);
    expect(deadLetters).toHaveLength(1);
    expect(deadLetters[0]!.MessageAttributes?.failureCode?.StringValue).toBe('INVALID_TRANSACTION_KIND');
    expect(await query('select id from dwp.wager_transactions where idempotency_key = ?', [envelope.data.idempotencyKey])).toHaveLength(0);
    await assertConsistent(wallet.id, '10000', 0);
  });

  test('dead-letters a conflicting application inbox identity immediately while preserving the original result', async () => {
    const { wallet, envelope } = await fixture();
    const worker = consumer();
    await send(envelope);
    expect(await worker.runOnce()).toBe(1);
    const changed = {
      ...envelope,
      data: { ...envelope.data, externalTransactionId: crypto.randomUUID(), idempotencyKey: crypto.randomUUID() },
    };
    await send(changed);
    expect(await worker.runOnce()).toBe(1);
    expect(await receive()).toHaveLength(0);
    const deadLetters = await receive(context.dlqUrl);
    expect(deadLetters).toHaveLength(1);
    expect(deadLetters[0]!.MessageAttributes?.failureCode?.StringValue).toBe('INBOX_MESSAGE_CONFLICT');
    expect(await query('select id from dwp.wager_transactions where idempotency_key = ?', [changed.data.idempotencyKey])).toHaveLength(0);
    expect(await query('select message_id from dwp.inbox_messages where consumer_name = ? and message_id = ?',
      [consumerName, envelope.messageId])).toHaveLength(1);
    await assertConsistent(wallet.id, '9000', 1);
  });

  test('rolls back a precommit failure on every attempt and dead-letters after the configured limit', async () => {
    const { wallet, envelope } = await fixture();
    let attempts = 0;
    const failingService = createWageringService(context.orm, { beforeCommit: () => {
      attempts += 1;
      throw new Error('injected consumer transaction rollback');
    } });
    const failingHandler: WagerCommandHandler = failingService;
    // The wrapper keeps the real use case and database; exhaustion auditing is a separate handler capability.
    const handler: WagerCommandHandler = {
      submit: (data, key, context) => failingHandler.submit(data, key, context),
    };
    const worker = consumer(handler, { maxAttempts: 2 });
    await send(envelope);
    expect(await worker.runOnce()).toBe(1);
    expect(attempts).toBe(1);
    expect(await query('select message_id from dwp.inbox_messages where consumer_name = ? and message_id = ?',
      [consumerName, envelope.messageId])).toHaveLength(0);
    await assertConsistent(wallet.id, '10000', 0);
    expect(await worker.runOnce({ waitTimeSeconds: 2 })).toBe(1);
    expect(attempts).toBe(2);
    expect(await receive()).toHaveLength(0);
    const deadLetters = await receive(context.dlqUrl);
    expect(deadLetters).toHaveLength(1);
    expect(deadLetters[0]!.Body).toBe(JSON.stringify(envelope));
    expect(deadLetters[0]!.MessageAttributes?.failureCode?.StringValue).toBe('RETRY_EXHAUSTED');
    expect(await query('select id from dwp.wager_transactions where idempotency_key = ?', [envelope.data.idempotencyKey])).toHaveLength(0);
    expect(await query('select message_id from dwp.inbox_messages where consumer_name = ? and message_id = ?',
      [consumerName, envelope.messageId])).toHaveLength(0);
    await assertConsistent(wallet.id, '10000', 0);
  });

  test('audits exhausted retries as FAILED with inbox and one failure event before sending the original command to DLQ', async () => {
    const { wallet, envelope } = await fixture();
    let attempts = 0;
    const failingService = createWageringService(context.orm, { beforeCommit: () => {
      attempts += 1;
      throw new Error('injected financial commit failure');
    } });
    const handler: WagerCommandHandler = {
      submit: (data, key, inbox) => failingService.submit(data, key, inbox),
      failSubmission: (data, key, code, inbox) => context.service.failSubmission(data, key, code, inbox),
    };
    const worker = consumer(handler, { maxAttempts: 2 });
    await send(envelope);
    expect(await worker.runOnce()).toBe(1);
    expect(attempts).toBe(1);
    expect(await query('select id from dwp.wager_transactions where idempotency_key = ?', [envelope.data.idempotencyKey])).toHaveLength(0);
    expect(await query('select message_id from dwp.inbox_messages where consumer_name = ? and message_id = ?',
      [consumerName, envelope.messageId])).toHaveLength(0);
    await assertConsistent(wallet.id, '10000', 0);

    expect(await worker.runOnce({ waitTimeSeconds: 2 })).toBe(1);
    expect(attempts).toBe(2);
    expect(await receive()).toHaveLength(0);
    const deadLetters = await receive(context.dlqUrl);
    expect(deadLetters).toHaveLength(1);
    expect(deadLetters[0]!.Body).toBe(JSON.stringify(envelope));
    expect(deadLetters[0]!.MessageAttributes?.failureCode?.StringValue).toBe('RETRY_EXHAUSTED');
    const transactions = await query<{ id: string; status: string; failure_code: string; result: unknown }>(
      'select id, status, failure_code, result from dwp.wager_transactions where idempotency_key = ?', [envelope.data.idempotencyKey],
    );
    expect(transactions).toHaveLength(1);
    const failed = transactions[0]!;
    expect(failed).toMatchObject({ status: 'FAILED', failure_code: 'RETRY_EXHAUSTED' });
    expect(failed.result).toMatchObject({
      transactionId: failed.id, status: 'FAILED', failureCode: 'RETRY_EXHAUSTED', balance: wallet.balance,
    });
    expect(await query('select message_id from dwp.inbox_messages where consumer_name = ? and message_id = ? and processed_at is not null',
      [consumerName, envelope.messageId])).toHaveLength(1);
    expect(await query("select event_type from dwp.outbox_messages where payload->'data'->>'transactionId' = ?",
      [failed.id])).toEqual([{ event_type: 'WagerTransactionFailed' }]);
    expect(await query('select id from dwp.wallet_ledger_entries where transaction_id = ?', [failed.id])).toHaveLength(0);
    expect(await context.service.getWallet(wallet.id)).toEqual(wallet);
    await assertConsistent(wallet.id, '10000', 0);
  });

  test('retains the source command when DLQ publication fails and dead-letters a persisted FAILED replay', async () => {
    const { wallet, envelope } = await fixture();
    let submitCalls = 0;
    let failedSend = false;
    const failingService = createWageringService(context.orm, { beforeCommit: () => {
      throw new Error('injected financial commit failure before failed audit');
    } });
    const handler: WagerCommandHandler = {
      submit: (data, key, inbox) => {
        submitCalls += 1;
        return (submitCalls === 1 ? failingService : context.service).submit(data, key, inbox);
      },
      failSubmission: (data, key, code, inbox) => context.service.failSubmission(data, key, code, inbox),
    };
    const middlewareName = `consumer-dlq-send-fault-${crypto.randomUUID()}`;
    // All messages still use real MiniStack. Only the first DLQ transport attempt is interrupted.
    context.sqs.middlewareStack.add((next, command) => async args => {
      const queueUrl = (args.input as { QueueUrl?: string }).QueueUrl;
      if (!failedSend && command.commandName === 'SendMessageCommand' && queueUrl === context.dlqUrl) {
        failedSend = true;
        throw new Error('injected DLQ transport failure');
      }
      return next(args);
    }, { step: 'initialize', name: middlewareName });
    try {
      const worker = consumer(handler, { maxAttempts: 1 });
      await send(envelope);
      await expect(worker.runOnce()).rejects.toThrow('injected DLQ transport failure');
      expect(failedSend).toBe(true);
      expect(await receive(context.dlqUrl)).toHaveLength(0);
      expect(await query('select status, failure_code from dwp.wager_transactions where idempotency_key = ?',
        [envelope.data.idempotencyKey])).toEqual([{ status: 'FAILED', failure_code: 'RETRY_EXHAUSTED' }]);
      expect(await query('select message_id from dwp.inbox_messages where consumer_name = ? and message_id = ? and processed_at is not null',
        [consumerName, envelope.messageId])).toHaveLength(1);
      await assertConsistent(wallet.id, '10000', 0);

      expect(await worker.runOnce({ waitTimeSeconds: 2 })).toBe(1);
      expect(submitCalls).toBe(2);
      expect(await receive()).toHaveLength(0);
      const deadLetters = await receive(context.dlqUrl);
      expect(deadLetters).toHaveLength(1);
      expect(deadLetters[0]!.Body).toBe(JSON.stringify(envelope));
      expect(deadLetters[0]!.MessageAttributes?.failureCode?.StringValue).toBe('RETRY_EXHAUSTED');
      expect(await query("select id from dwp.outbox_messages where event_type = 'WagerTransactionFailed' and payload->'data'->>'externalTransactionId' = ?",
        [envelope.data.externalTransactionId])).toHaveLength(1);
      expect(await context.service.getWallet(wallet.id)).toEqual(wallet);
      await assertConsistent(wallet.id, '10000', 0);
    } finally {
      context.sqs.middlewareStack.remove(middlewareName);
    }
  });

  test('renews message visibility while a slow handler is still processing', async () => {
    const { wallet, envelope } = await fixture();
    let started!: () => void;
    let release!: () => void;
    const entered = new Promise<void>(resolve => { started = resolve; });
    const blocked = new Promise<void>(resolve => { release = resolve; });
    const realHandler: WagerCommandHandler = context.service;
    const handler: WagerCommandHandler = { submit: async (data, key, context) => {
      started();
      await blocked;
      return realHandler.submit(data, key, context);
    } };
    const worker = consumer(handler);
    await send(envelope);
    const running = worker.runOnce();
    await entered;
    try {
      await Bun.sleep(1_600);
      expect(await receive()).toHaveLength(0);
    } finally {
      release();
    }
    expect(await running).toBe(1);
    expect(await receive()).toHaveLength(0);
    await assertConsistent(wallet.id, '9000', 1);
  });

  test('returns visibility on shutdown timeout and prevents a late handler from acknowledging the released message', async () => {
    const { wallet, envelope } = await fixture();
    let started!: () => void;
    let release!: () => void;
    const entered = new Promise<void>(resolve => { started = resolve; });
    const blocked = new Promise<void>(resolve => { release = resolve; });
    const realHandler: WagerCommandHandler = context.service;
    const handler: WagerCommandHandler = { submit: async (data, key, context) => {
      started();
      await blocked;
      return realHandler.submit(data, key, context);
    } };
    const worker = consumer(handler);
    await send(envelope);
    const running = worker.runOnce();
    await entered;
    try {
      await worker.stop(10);
      const returned = await receive();
      expect(returned).toHaveLength(1);
      await context.sqs.send(new ChangeMessageVisibilityCommand({
        QueueUrl: context.commandsUrl, ReceiptHandle: returned[0]!.ReceiptHandle!, VisibilityTimeout: 0,
      }));
    } finally {
      release();
    }
    expect(await running).toBe(1);
    const replacement = consumer();
    expect(await replacement.runOnce()).toBe(1);
    expect(await receive()).toHaveLength(0);
    expect(await query('select message_id from dwp.inbox_messages where consumer_name = ? and message_id = ?',
      [consumerName, envelope.messageId])).toHaveLength(1);
    await assertConsistent(wallet.id, '9000', 1);
  });
});
