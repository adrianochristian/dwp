import {
  CreateQueueCommand,
  GetQueueAttributesCommand,
  SetQueueAttributesCommand,
  type SQSClient,
} from '@aws-sdk/client-sqs';
import { createSqsClient, queueNames } from './sqs.js';

export interface QueueUrls {
  commands: string;
  deadLetters: string;
  events: string;
}

export async function bootstrapQueues(client: SQSClient): Promise<QueueUrls> {
  const deadLetters = await client.send(new CreateQueueCommand({
    QueueName: queueNames.deadLetters,
    Attributes: { FifoQueue: 'true', MessageRetentionPeriod: '1209600' },
  }));
  if (!deadLetters.QueueUrl) throw new Error('Missing dead-letter queue URL');
  const attributes = await client.send(new GetQueueAttributesCommand({
    QueueUrl: deadLetters.QueueUrl,
    AttributeNames: ['QueueArn'],
  }));
  const arn = attributes.Attributes?.QueueArn;
  if (!arn) throw new Error('Missing dead-letter queue ARN');

  const commands = await client.send(new CreateQueueCommand({
    QueueName: queueNames.commands,
    Attributes: { FifoQueue: 'true', VisibilityTimeout: '30', ReceiveMessageWaitTimeSeconds: '20' },
  }));
  if (!commands.QueueUrl) throw new Error('Missing command queue URL');
  await client.send(new SetQueueAttributesCommand({
    QueueUrl: commands.QueueUrl,
    Attributes: { RedrivePolicy: JSON.stringify({ deadLetterTargetArn: arn, maxReceiveCount: '5' }) },
  }));

  const events = await client.send(new CreateQueueCommand({ QueueName: queueNames.events }));
  if (!events.QueueUrl) throw new Error('Missing event queue URL');
  return { commands: commands.QueueUrl, deadLetters: deadLetters.QueueUrl, events: events.QueueUrl };
}

if (import.meta.main) {
  const client = createSqsClient();
  try {
    await bootstrapQueues(client);
    console.log(JSON.stringify({ event: 'queues_ready', queues: Object.values(queueNames) }));
  } finally {
    client.destroy();
  }
}
