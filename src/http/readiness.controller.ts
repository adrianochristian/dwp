import { GetQueueAttributesCommand, type SQSClient } from '@aws-sdk/client-sqs';
import type { MikroORM } from '@mikro-orm/postgresql';
import { Controller, Get, Header, Inject, ServiceUnavailableException } from '@nestjs/common';
import { getQueueUrl, queueNames } from '../infrastructure/messaging/sqs.js';
import { metrics } from '../observability/metrics.js';

@Controller()
export class OperationalController {
  constructor(
    @Inject('Database') private readonly database: MikroORM,
    @Inject('SqsClient') private readonly sqsClient: SQSClient,
  ) {}

  @Get('health/ready')
  async ready(): Promise<{ status: 'ok' }> {
    const signal = AbortSignal.timeout(3_000);
    try {
      await Promise.all([
        this.database.em.fork().getConnection().execute('SELECT 1'),
        ...[queueNames.commands, queueNames.events].map(async name => {
          const url = await getQueueUrl(this.sqsClient, name, signal);
          await this.sqsClient.send(new GetQueueAttributesCommand({
            QueueUrl: url,
            AttributeNames: ['QueueArn'],
          }), { abortSignal: signal });
        }),
      ]);
      return { status: 'ok' };
    } catch {
      throw new ServiceUnavailableException({ code: 'DEPENDENCIES_UNAVAILABLE', status: 'unavailable' });
    }
  }

  @Get('metrics')
  @Header('Content-Type', 'text/plain; version=0.0.4; charset=utf-8')
  async metrics(): Promise<string> {
    const [row] = await this.database.em.fork().getConnection().execute<{ lag_seconds: string }[]>(`
      select coalesce(greatest(extract(epoch from (clock_timestamp() - min(occurred_at))), 0), 0)::text as lag_seconds
      from dwp.outbox_messages where published_at is null`);
    metrics.setOutboxLag(Number(row?.lag_seconds ?? 0));
    return metrics.renderPrometheus();
  }
}
