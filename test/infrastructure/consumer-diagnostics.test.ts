import { describe, expect, test } from 'bun:test';
import { ApplicationError } from '../../dist/application/application-error.js';
import type { SqsConsumerObservation, WagerCommandHandler } from '../../dist/infrastructure/messaging/sqs-consumer.js';
import { consumerTestContext } from './consumer-support.js';

const integration = process.env.DWP_INTEGRATION === '1' ? describe : describe.skip;

integration('consumer diagnostics preserve financial outcomes and safe identifiers', () => {
  const context = consumerTestContext();

  function assertSafe(value: unknown, forbidden: readonly string[]): void {
    const serialized = JSON.stringify(value) ?? '';
    for (const item of forbidden) expect(serialized.includes(item)).toBe(false);
    expect(serialized.includes('idempotencyKey')).toBe(false);
    expect(serialized.includes('money')).toBe(false);
    expect(serialized.includes('amount')).toBe(false);
    expect(serialized.includes('Authorization')).toBe(false);
  }

  test('processed and error observers can throw without changing commit or ack', async () => {
    const { wallet, envelope } = await context.fixture();
    envelope.data.Authorization = 'Bearer diagnostic-secret';
    const processed: SqsConsumerObservation[] = [];
    const diagnostics: { code: unknown; observation: SqsConsumerObservation | undefined; message: unknown }[] = [];
    const consumer = context.consumer(context.service, {
      onProcessed: observation => {
        processed.push({ ...observation });
        throw new Error('diagnostic-secret 999999.99 SQL payload');
      },
      onError: (error, observation) => {
        diagnostics.push({ code: (error as { code?: unknown }).code, message: (error as Error).message, observation });
        throw new Error('the diagnostic sink is unavailable');
      },
    });
    await context.send(envelope);
    expect(await consumer.runOnce()).toBe(1);
    expect(await context.receive()).toHaveLength(0);
    expect(await context.receive(context.dlqUrl)).toHaveLength(0);
    const [transaction] = await context.query<{ id: string; status: string }>(
      'select id, status from dwp.wager_transactions where idempotency_key = ?', [envelope.data.idempotencyKey]);
    expect(processed).toHaveLength(1);
    expect(processed[0]).toMatchObject({
      messageId: envelope.messageId, walletId: wallet.id, providerId: envelope.data.providerId,
      transactionId: transaction!.id, correlationId: transaction!.id, status: 'PROCESSED',
    });
    expect(diagnostics).toHaveLength(1);
    expect(diagnostics[0]?.code).toBe('DIAGNOSTIC_CALLBACK_FAILED');
    expect(diagnostics[0]?.observation).toMatchObject(processed[0]!);
    assertSafe({ processed, diagnostics }, [String(envelope.data.idempotencyKey), 'diagnostic-secret', '999999.99', 'SQL payload']);
    await context.assertConsistent(wallet.id, '9000', 1);
  });

  test('retry diagnostics retain available IDs and cannot prevent recovery', async () => {
    const { wallet, envelope } = await context.fixture();
    let attempts = 0;
    const retries: SqsConsumerObservation[] = [];
    const diagnostics: SqsConsumerObservation[] = [];
    const handler: WagerCommandHandler = {
      submit: (data, key, inbox) => {
        if (attempts++ === 0) throw new ApplicationError('unavailable', 'DEPENDENCY_UNAVAILABLE', 'raw secret 999999.99');
        return context.service.submit(data, key, inbox);
      },
    };
    const consumer = context.consumer(handler, {
      onRetry: observation => { retries.push({ ...observation }); throw new Error('observer raw secret'); },
      onError: (_error, observation) => { if (observation) diagnostics.push({ ...observation }); throw new Error('sink failed'); },
    });
    await context.send(envelope);
    expect(await consumer.runOnce()).toBe(1);
    expect(retries).toHaveLength(1);
    expect(retries[0]).toMatchObject({
      messageId: envelope.messageId, correlationId: envelope.messageId,
      walletId: wallet.id, providerId: envelope.data.providerId, failureStage: 'processing', attempt: 1,
    });
    expect(retries[0]?.transactionId).toBeUndefined();
    expect(diagnostics[0]).toMatchObject({ messageId: envelope.messageId, walletId: wallet.id, failureCode: 'DIAGNOSTIC_CALLBACK_FAILED' });
    expect(await consumer.runOnce({ waitTimeSeconds: 2 })).toBe(1);
    expect(await context.receive()).toHaveLength(0);
    assertSafe({ retries, diagnostics }, [String(envelope.data.idempotencyKey), 'raw secret', '999999.99']);
    await context.assertConsistent(wallet.id, '9000', 1);
  });

  test('ack exhaustion is transport-only, preserves committed status and reports it in DLQ metadata', async () => {
    const { wallet, envelope } = await context.fixture();
    let audits = 0;
    let failAck = true;
    const deadLetters: SqsConsumerObservation[] = [];
    const diagnostics: SqsConsumerObservation[] = [];
    const handler: WagerCommandHandler = {
      submit: (data, key, inbox) => context.service.submit(data, key, inbox),
      failSubmission: (data, key, code, inbox) => { audits += 1; return context.service.failSubmission(data, key, code, inbox); },
    };
    const middleware = `diagnostic-ack-${crypto.randomUUID()}`;
    context.sqs.middlewareStack.add((next, command) => async args => {
      if (failAck && command.commandName === 'DeleteMessageCommand'
        && (args.input as { QueueUrl?: string }).QueueUrl === context.commandsUrl) {
        failAck = false;
        const error = new Error('raw transport secret');
        error.name = 'TimeoutError';
        Object.assign(error, { code: 'ETIMEDOUT' });
        throw error;
      }
      return next(args);
    }, { step: 'initialize', name: middleware });
    try {
      const consumer = context.consumer(handler, {
        maxAttempts: 1,
        onDeadLetter: observation => { deadLetters.push({ ...observation }); throw new Error('dead-letter metric failed'); },
        onError: (_error, observation) => { if (observation) diagnostics.push({ ...observation }); throw new Error('error sink failed'); },
      });
      await context.send(envelope);
      expect(await consumer.runOnce()).toBe(1);
      expect(audits).toBe(0);
      expect(await context.receive()).toHaveLength(0);
      const queued = await context.receive(context.dlqUrl);
      expect(queued).toHaveLength(1);
      expect(queued[0]?.Body).toBe(JSON.stringify(envelope));
      expect(queued[0]?.MessageAttributes?.failureCode?.StringValue).toBe('ACK_FAILED');
      expect(queued[0]?.MessageAttributes?.failureStage?.StringValue).toBe('ack');
      expect(queued[0]?.MessageAttributes?.transactionStatus?.StringValue).toBe('PROCESSED');
      expect(queued[0]?.MessageAttributes?.causeCode?.StringValue).toBe('SQS_CALL_TIMEOUT');
      const [transaction] = await context.query<{ id: string; status: string; failure_code: string | null }>(
        'select id, status, failure_code from dwp.wager_transactions where idempotency_key = ?', [envelope.data.idempotencyKey]);
      expect(transaction).toMatchObject({ status: 'PROCESSED', failure_code: null });
      expect(deadLetters).toHaveLength(1);
      expect(deadLetters[0]).toMatchObject({
        failureCode: 'ACK_FAILED', failureStage: 'ack', causeCode: 'SQS_CALL_TIMEOUT',
        messageId: envelope.messageId, walletId: wallet.id, providerId: envelope.data.providerId,
        transactionId: transaction!.id, correlationId: transaction!.id, status: 'PROCESSED',
      });
      expect(diagnostics[0]).toMatchObject({ messageId: envelope.messageId, transactionId: transaction!.id, failureCode: 'DIAGNOSTIC_CALLBACK_FAILED' });
      assertSafe({ deadLetters, diagnostics }, [String(envelope.data.idempotencyKey), 'raw transport secret']);
      expect(await context.query("select id from dwp.outbox_messages where event_type='WagerTransactionFailed' and payload->'data'->>'externalTransactionId' = ?",
        [envelope.data.externalTransactionId])).toHaveLength(0);
      await context.assertConsistent(wallet.id, '9000', 1);
    } finally {
      context.sqs.middlewareStack.remove(middleware);
    }
  });
});
