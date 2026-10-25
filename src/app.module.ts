import { Module, type DynamicModule } from '@nestjs/common';
import type { MikroORM } from '@mikro-orm/postgresql';
import { WageringService } from './application/wagering-service.js';
import { WageringQueries } from './application/wagering-queries.js';
import { WageringController } from './http/wagering.controller.js';
import { HealthController } from './health.controller.js';
import { OperationalController } from './http/readiness.controller.js';
import { createSqsClient } from './infrastructure/messaging/sqs.js';
import { ObservedWageringQueries, ObservedWageringService } from './observability/observed-wagering.js';
import { APP_GUARD } from '@nestjs/core';
import { AccessGuard } from './auth/access.guard.js';
import { createIdentityPort } from './auth/keycloak-identity.js';
import { PROVIDER_IDENTITY_PORT } from './auth/provider-identity.js';

@Module({ controllers: [HealthController] })
export class AppModule {
  static withDatabase(database: MikroORM): DynamicModule {
    const sqs = createSqsClient();
    return {
      module: AppModule,
      controllers: [WageringController, OperationalController],
      providers: [
        { provide: WageringService, useValue: new ObservedWageringService(database, 'http') },
        { provide: WageringQueries, useValue: new ObservedWageringQueries(database) },
        { provide: 'Database', useValue: database },
        { provide: 'SqsClient', useValue: sqs },
        { provide: PROVIDER_IDENTITY_PORT, useValue: createIdentityPort() },
        { provide: APP_GUARD, useClass: AccessGuard },
        { provide: 'DatabaseLifecycle', useValue: {
          onApplicationShutdown: async () => { sqs.destroy(); await database.close(true); },
        } },
      ],
    };
  }
}
