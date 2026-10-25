import { OutboxPublisher } from './infrastructure/messaging/outbox-publisher.js';
import { createSqsClient, getQueueUrl, queueNames } from './infrastructure/messaging/sqs.js';
import { createDatabase } from './infrastructure/persistence/database.js';
import { SqsConsumer } from './infrastructure/messaging/sqs-consumer.js';
import { WageringService } from './application/wagering-service.js';
import { ReferenceWorker } from './application/reference-worker.js';

export async function runWorker(): Promise<void> {
  const role = process.env.WORKER_ROLE ?? 'all';
  if (!['publisher', 'consumer', 'reference', 'all'].includes(role)) throw new Error('Invalid WORKER_ROLE');
  const database = await createDatabase();
  const sqs = createSqsClient();
  let publisher: OutboxPublisher | undefined;
  let consumer: SqsConsumer | undefined;
  let references: ReferenceWorker | undefined;
  let stopping = false;
  let resolveStop: () => void;
  const stopped = new Promise<void>(resolve => { resolveStop = resolve; });
  const requestStop = () => { stopping = true; resolveStop(); };
  process.on('SIGINT', requestStop);
  process.on('SIGTERM', requestStop);
  try {
    const loops: Promise<unknown>[] = [];
    if (role === 'publisher' || role === 'all') {
      publisher = new OutboxPublisher(database, sqs,
        process.env.EVENT_QUEUE_URL ?? await getQueueUrl(sqs, queueNames.events));
      const current = publisher;
      loops.push((async () => {
        while (!stopping) {
          try { await current.runOnce(); } catch {
            console.error(JSON.stringify({ event: 'publisher_cycle_failed', code: 'INFRASTRUCTURE_UNAVAILABLE' }));
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
      consumer = new SqsConsumer(new WageringService(database), sqs, commands, deadLetters, {
        onError: () => console.error(JSON.stringify({ event: 'consumer_cycle_failed', code: 'INFRASTRUCTURE_UNAVAILABLE' })),
      });
      loops.push(consumer.run());
    }
    if (role === 'reference' || role === 'all') {
      references = new ReferenceWorker(database, new WageringService(database));
      const current = references;
      loops.push((async () => {
        while (!stopping) {
          try { await current.runOnce(); } catch {
            console.error(JSON.stringify({ event: 'reference_cycle_failed', code: 'INFRASTRUCTURE_UNAVAILABLE' }));
          }
          if (!stopping) await Bun.sleep(500);
        }
      })());
    }
    console.log(JSON.stringify({ event: 'worker_started', role, pid: process.pid }));
    await Promise.race([Promise.all(loops), stopped]);
  } finally {
    stopping = true;
    process.off('SIGINT', requestStop);
    process.off('SIGTERM', requestStop);
    await consumer?.stop(10_000);
    await references?.stop();
    await publisher?.stop();
    sqs.destroy();
    await database.close(true);
  }
}

if (import.meta.main) await runWorker();
