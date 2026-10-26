import { createWageringService } from '../../dist/infrastructure/persistence/wagering-services.js';
import { SqsConsumer } from '../../dist/infrastructure/messaging/sqs-consumer.js';
import { createSqsClient } from '../../dist/infrastructure/messaging/sqs.js';
import { createDatabase } from '../../dist/infrastructure/persistence/database.js';

function required(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing fixture configuration: ${name}`);
  return value;
}

const orm = await createDatabase(required('DATABASE_URL'));
const sqs = createSqsClient();
const consumer = new SqsConsumer(
  createWageringService(orm), sqs, required('COMMANDS_QUEUE_URL'), required('DLQ_QUEUE_URL'), {
    consumerName: required('CONSUMER_NAME'),
    visibilitySeconds: 1,
    afterCommitBeforeAck: async (result, message) => {
      console.log(JSON.stringify({
        event: 'consumer_committed_before_ack',
        messageId: message.messageId,
        transactionId: result.transactionId,
        status: result.status,
      }));
      // The parent kills this process. A referenced timer keeps the fixture alive
      // while the consumer's own heartbeat continues extending visibility.
      await new Promise<never>(() => { setInterval(() => undefined, 1_000); });
    },
  },
);

try {
  await consumer.runOnce({ waitTimeSeconds: 2, maxMessages: 1 });
  throw new Error('Consumer returned without reaching its crash checkpoint');
} finally {
  await consumer.stop(0);
  sqs.destroy();
  await orm.close(true);
}
