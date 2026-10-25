import { createDatabase } from '../../dist/infrastructure/persistence/database.js';
import { OutboxPublisher, type OutboxDelivery } from '../../dist/infrastructure/messaging/outbox-publisher.js';
import { createSqsClient } from '../../dist/infrastructure/messaging/sqs.js';

const phase = process.env.CRASH_PHASE;
const queueUrl = process.env.EVENTS_QUEUE_URL;
if ((phase !== 'before-send' && phase !== 'after-send') || !queueUrl) {
  throw new Error('Publisher crash fixture requires CRASH_PHASE and EVENTS_QUEUE_URL');
}
const orm = await createDatabase();
const sqs = createSqsClient();
const checkpoint = async (message: OutboxDelivery): Promise<void> => {
  console.log(JSON.stringify({ event: 'publisher_checkpoint', phase, eventId: message.id }));
  await new Promise<never>(() => { setInterval(() => undefined, 1_000); });
};
const publisher = new OutboxPublisher(orm, sqs, queueUrl, {
  leaseMs: 300,
  batchSize: 1,
  ...(phase === 'before-send' ? { beforeSend: checkpoint } : { afterSend: checkpoint }),
});
try {
  const published = await publisher.runOnce();
  throw new Error(`Publisher fixture did not reach its checkpoint; published=${published}`);
} finally {
  await publisher.stop();
  sqs.destroy();
  await orm.close(true);
}
