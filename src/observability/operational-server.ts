import 'reflect-metadata';
import type { SQSClient } from '@aws-sdk/client-sqs';
import type { MikroORM } from '@mikro-orm/postgresql';
import { Module, type INestApplication } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { HealthController } from '../health.controller.js';
import { ApiErrorFilter } from '../http/error.filter.js';
import { OperationalController } from '../http/readiness.controller.js';

@Module({ controllers: [HealthController, OperationalController] })
class OperationalModule {}

export async function createOperationalApplication(database: MikroORM, sqsClient: SQSClient): Promise<INestApplication> {
  const app = await NestFactory.create({
    module: OperationalModule,
    providers: [
      { provide: 'Database', useValue: database },
      { provide: 'SqsClient', useValue: sqsClient },
    ],
  }, { logger: false });
  app.useGlobalFilters(new ApiErrorFilter());
  return app;
}
