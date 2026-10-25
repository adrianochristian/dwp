import { GetQueueUrlCommand, SQSClient } from '@aws-sdk/client-sqs';

export const queueNames = Object.freeze({
  commands: 'wager-transactions.fifo',
  deadLetters: 'wager-transactions-dlq.fifo',
  events: 'wager-events',
});

export function createSqsClient(): SQSClient {
  return new SQSClient({
    region: process.env.AWS_REGION ?? 'us-east-1',
    endpoint: process.env.SQS_ENDPOINT ?? 'http://127.0.0.1:4566',
    credentials: {
      accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? '000000000000',
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? 'local',
    },
    maxAttempts: 3,
    requestHandler: { connectionTimeout: 3_000, requestTimeout: 25_000 },
  });
}

export async function getQueueUrl(client: SQSClient, name: string, abortSignal?: AbortSignal): Promise<string> {
  const result = await client.send(new GetQueueUrlCommand({ QueueName: name }), abortSignal ? { abortSignal } : {});
  if (!result.QueueUrl) throw new Error(`Missing URL for queue ${name}`);
  return result.QueueUrl;
}
