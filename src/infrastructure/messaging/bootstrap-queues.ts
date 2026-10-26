import {
  CreateQueueCommand,
  GetQueueAttributesCommand,
  SetQueueAttributesCommand,
  type SQSClient,
} from '@aws-sdk/client-sqs';
import { createSqsClient, queueNames, sqsCall } from './sqs.js';

export interface QueueUrls {
  commands: string;
  deadLetters: string;
  events: string;
}

export async function bootstrapQueues(client: SQSClient, signal?: AbortSignal): Promise<QueueUrls> {
  const deadLetters = await sqsCall(abortSignal => client.send(new CreateQueueCommand({
    QueueName: queueNames.deadLetters,
    Attributes: { FifoQueue: 'true', MessageRetentionPeriod: '1209600' },
  }), { abortSignal }), { signal });
  if (!deadLetters.QueueUrl) throw new Error('Missing dead-letter queue URL');
  const attributes = await sqsCall(abortSignal => client.send(new GetQueueAttributesCommand({
    QueueUrl: deadLetters.QueueUrl,
    AttributeNames: ['QueueArn'],
  }), { abortSignal }), { signal });
  const arn = attributes.Attributes?.QueueArn;
  if (!arn) throw new Error('Missing dead-letter queue ARN');

  const commands = await sqsCall(abortSignal => client.send(new CreateQueueCommand({
    QueueName: queueNames.commands,
    Attributes: { FifoQueue: 'true', VisibilityTimeout: '30', ReceiveMessageWaitTimeSeconds: '20' },
  }), { abortSignal }), { signal });
  if (!commands.QueueUrl) throw new Error('Missing command queue URL');
  await sqsCall(abortSignal => client.send(new SetQueueAttributesCommand({
    QueueUrl: commands.QueueUrl,
    Attributes: { RedrivePolicy: JSON.stringify({ deadLetterTargetArn: arn, maxReceiveCount: '5' }) },
  }), { abortSignal }), { signal });

  const events = await sqsCall(abortSignal => client.send(new CreateQueueCommand({ QueueName: queueNames.events }), { abortSignal }), { signal });
  if (!events.QueueUrl) throw new Error('Missing event queue URL');
  return { commands: commands.QueueUrl, deadLetters: deadLetters.QueueUrl, events: events.QueueUrl };
}

if (import.meta.main) {
  const client = createSqsClient();
  try {
    await bootstrapQueues(client);
    try {
      console.log(JSON.stringify({ event: 'queues_ready', queues: Object.values(queueNames) }));
    } catch { /* Queue creation already succeeded; its diagnostic is best-effort. */ }
  } finally {
    client.destroy();
  }
}
