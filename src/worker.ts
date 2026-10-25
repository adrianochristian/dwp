import { OutboxPublisher } from './infrastructure/messaging/outbox-publisher.js';
import { createSqsClient, getQueueUrl, queueNames } from './infrastructure/messaging/sqs.js';
import { createDatabase } from './infrastructure/persistence/database.js';

export async function runWorker(): Promise<void> {
  const role = process.env.WORKER_ROLE ?? 'publisher';
  if (role !== 'publisher') throw new Error('WORKER_ROLE must be publisher');
  const database = await createDatabase();
  const sqs = createSqsClient();
  let publisher: OutboxPublisher | undefined;
  let stopping = false;
  const requestStop = () => { stopping = true; };
  process.on('SIGINT', requestStop);
  process.on('SIGTERM', requestStop);
  try {
    publisher = new OutboxPublisher(database, sqs, await getQueueUrl(sqs, queueNames.events));
    console.log(JSON.stringify({ event: 'worker_started', role, pid: process.pid }));
    while (!stopping) {
      try {
        await publisher.runOnce();
      } catch {
        console.error(JSON.stringify({ event: 'publisher_cycle_failed', code: 'INFRASTRUCTURE_UNAVAILABLE' }));
      }
      if (!stopping) await Bun.sleep(250);
    }
  } finally {
    process.off('SIGINT', requestStop);
    process.off('SIGTERM', requestStop);
    await publisher?.stop();
    sqs.destroy();
    await database.close(true);
  }
}

if (import.meta.main) await runWorker();
