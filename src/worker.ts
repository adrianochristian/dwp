import { OutboxPublisher } from './infrastructure/messaging/outbox-publisher.js';
import { createSqsClient, getQueueUrl, queueNames } from './infrastructure/messaging/sqs.js';
import { createDatabase } from './infrastructure/persistence/database.js';
import { SqsConsumer } from './infrastructure/messaging/sqs-consumer.js';
import { ReferenceWorker } from './application/reference-worker.js';
import { ObservedWageringService } from './observability/observed-wagering.js';
import { createOperationalApplication } from './observability/operational-server.js';
import { metrics } from './observability/metrics.js';
import { log } from './observability/logger.js';
import type { INestApplication } from '@nestjs/common';

export async function runWorker(): Promise<void> {
  const role = process.env.WORKER_ROLE ?? 'all';
  if (!['publisher', 'consumer', 'reference', 'all'].includes(role)) throw new Error('Invalid WORKER_ROLE');
  const metricsPort = process.env.WORKER_METRICS_PORT ?? '0';
  if (!/^\d+$/.test(metricsPort) || BigInt(metricsPort) > 65535n) throw new Error('Invalid WORKER_METRICS_PORT');
  const database = await createDatabase();
  const sqs = createSqsClient();
  let publisher: OutboxPublisher | undefined;
  let consumer: SqsConsumer | undefined;
  let references: ReferenceWorker | undefined;
  let operational: INestApplication | undefined;
  let stopping = false;
  let resolveStop: () => void;
  const stopped = new Promise<void>(resolve => { resolveStop = resolve; });
  const requestStop = () => { stopping = true; resolveStop(); };
  process.on('SIGINT', requestStop);
  process.on('SIGTERM', requestStop);
  try {
    operational = await createOperationalApplication(database, sqs);
    await operational.listen(Number(metricsPort), '127.0.0.1');
    const loops: Promise<unknown>[] = [];
    if (role === 'publisher' || role === 'all') {
      publisher = new OutboxPublisher(database, sqs,
        process.env.EVENT_QUEUE_URL ?? await getQueueUrl(sqs, queueNames.events), {
          onRetry: message => {
            metrics.increment('retries', { role: 'publisher' });
            log('outbox_retry', { messageId: message.id, correlationId: message.id, attempt: message.attempts, code: 'SQS_PUBLISH_FAILED' });
          },
          onLeaseLost: message => log('outbox_lease_lost', { messageId: message.id, code: 'OUTBOX_LEASE_LOST' }),
        });
      const current = publisher;
      loops.push((async () => {
        while (!stopping) {
          try { await current.runOnce(); } catch {
            log('publisher_cycle_failed', { code: 'INFRASTRUCTURE_UNAVAILABLE' });
          }
          if (!stopping) await Bun.sleep(250);
        }
      })());
    }
    if (role === 'consumer' || role === 'all') {
      const [commands, deadLetters] = await Promise.all([
        process.env.COMMAND_QUEUE_URL ?? getQueueUrl(sqs, queueNames.commands),
        process.env.DLQ_QUEUE_URL ?? getQueueUrl(sqs, queueNames.deadLetters),
      ]);
      consumer = new SqsConsumer(new ObservedWageringService(database, 'sqs'), sqs, commands, deadLetters, {
        onError: () => log('consumer_cycle_failed', { code: 'INFRASTRUCTURE_UNAVAILABLE' }),
        onRetry: observation => {
          metrics.increment('retries', { role: 'consumer' });
          log('command_retry', { messageId: observation.messageId, transactionId: observation.transactionId,
            code: observation.failureCode, attempt: observation.attempt });
        },
        onDeadLetter: observation => {
          metrics.increment('dlq', { role: 'consumer' });
          log('command_dead_lettered', { messageId: observation.messageId, transactionId: observation.transactionId,
            code: observation.failureCode, status: observation.status, attempt: observation.attempt });
        },
      });
      loops.push(consumer.run());
    }
    if (role === 'reference' || role === 'all') {
      references = new ReferenceWorker(database, new ObservedWageringService(database, 'reference'), {
        onRetry: id => {
          metrics.increment('retries', { role: 'reference' });
          log('reference_retry_failed', { transactionId: id, correlationId: id, code: 'INFRASTRUCTURE_UNAVAILABLE' });
        },
        onResult: (id, result) => {
          if (result && typeof result === 'object' && 'status' in result && result.status === 'PENDING_REFERENCE') {
            metrics.increment('retries', { role: 'reference' });
            log('reference_pending', { transactionId: id, correlationId: id, status: 'PENDING_REFERENCE' });
          }
        },
      });
      const current = references;
      loops.push((async () => {
        while (!stopping) {
          try { await current.runOnce(); } catch {
            log('reference_cycle_failed', { code: 'INFRASTRUCTURE_UNAVAILABLE' });
          }
          if (!stopping) await Bun.sleep(500);
        }
      })());
    }
    console.log(JSON.stringify({ event: 'worker_started', role, pid: process.pid, metricsAddress: await operational.getUrl() }));
    await Promise.race([Promise.all(loops), stopped]);
  } finally {
    stopping = true;
    process.off('SIGINT', requestStop);
    process.off('SIGTERM', requestStop);
    await consumer?.stop(10_000);
    await references?.stop();
    await publisher?.stop();
    await operational?.close();
    sqs.destroy();
    await database.close(true);
  }
}

if (import.meta.main) await runWorker();
