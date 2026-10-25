import 'reflect-metadata';
import type { INestApplication } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module.js';
import type { MikroORM } from '@mikro-orm/postgresql';
import { ApiErrorFilter } from './http/error.filter.js';

export async function createApplication(database?: MikroORM): Promise<INestApplication> {
  const app = await NestFactory.create(database ? AppModule.withDatabase(database) : AppModule, { logger: false });
  app.useGlobalFilters(new ApiErrorFilter());
  app.enableShutdownHooks(['SIGINT', 'SIGTERM']);
  return app;
}
