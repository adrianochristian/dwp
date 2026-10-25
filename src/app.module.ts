import { Module, type DynamicModule } from '@nestjs/common';
import type { MikroORM } from '@mikro-orm/postgresql';
import { WageringService } from './application/wagering-service.js';
import { WageringQueries } from './application/wagering-queries.js';
import { WageringController } from './http/wagering.controller.js';
import { HealthController } from './health.controller.js';

@Module({ controllers: [HealthController] })
export class AppModule {
  static withDatabase(database: MikroORM): DynamicModule {
    return {
      module: AppModule,
      controllers: [WageringController],
      providers: [
        { provide: WageringService, useValue: new WageringService(database) },
        { provide: WageringQueries, useValue: new WageringQueries(database) },
        { provide: 'DatabaseLifecycle', useValue: { onApplicationShutdown: () => database.close(true) } },
      ],
    };
  }
}
