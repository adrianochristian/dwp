import type { MikroORM } from '@mikro-orm/postgresql';
import { WageringService } from '../../application/wagering-service.js';
import { WageringQueries } from '../../application/wagering-queries.js';
import type { WageringStoreOptions } from '../../application/ports/wagering-store.js';
import { MikroOrmWageringStore } from './mikro-orm-wagering-store.js';

export function createWageringService(orm: MikroORM, options: WageringStoreOptions = {}): WageringService {
  return new WageringService(new MikroOrmWageringStore(orm, options));
}

export function createWageringQueries(orm: MikroORM): WageringQueries {
  return new WageringQueries(new MikroOrmWageringStore(orm));
}
