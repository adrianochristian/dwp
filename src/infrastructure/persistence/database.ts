import { Migrator } from '@mikro-orm/migrations';
import { MikroORM } from '@mikro-orm/postgresql';
import { Migration20261025000100 } from './migrations/Migration20261025000100.js';
import { Migration20261025000200 } from './migrations/WageringSchema.js';
import { Migration20261025000300 } from './migrations/Migration20261025000300.js';
import { WalletRecordSchema } from './records.js';

export async function createDatabase(databaseUrl?: string): Promise<MikroORM> {
  return MikroORM.init({
    clientUrl: databaseUrl ?? process.env.DATABASE_URL ?? 'postgresql://dwp:dwp@127.0.0.1:5432/dwp',
    entities: [WalletRecordSchema],
    discovery: { warnWhenNoEntities: false },
    extensions: [Migrator],
    migrations: {
      migrationsList: [Migration20261025000100, Migration20261025000200, Migration20261025000300],
      tableName: 'mikro_orm_migrations',
      snapshot: false,
      snapshotOnMigrate: false,
      transactional: true,
      allOrNothing: true,
    },
    pool: { min: 0, max: 10 },
    debug: false,
  });
}
