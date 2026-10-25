import { createDatabase } from './database.js';

export async function migrate(direction: 'up' | 'down'): Promise<void> {
  const orm = await createDatabase();
  try {
    await orm.migrator[direction]();
  } finally {
    await orm.close(true);
  }
}

if (import.meta.main) {
  const direction = process.argv[2];
  if (direction !== 'up' && direction !== 'down') {
    throw new Error('Usage: migrate.js <up|down>');
  }
  await migrate(direction);
}
