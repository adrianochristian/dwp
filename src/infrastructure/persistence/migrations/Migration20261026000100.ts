import { Migration } from '@mikro-orm/migrations';

export class Migration20261026000100 extends Migration {
  override up(): void {
    this.addSql(`
      lock table dwp.wager_transactions in access exclusive mode;
      drop index dwp.wager_reversal_once;
      create unique index wager_reversal_once on dwp.wager_transactions(reference_transaction_id)
        where status = 'PROCESSED' and kind in ('REFUND','ROLLBACK');
    `);
  }

  override down(): void {
    this.addSql(`
      lock table dwp.wager_transactions in access exclusive mode;
      drop index dwp.wager_reversal_once;
      create unique index wager_reversal_once on dwp.wager_transactions(reference_transaction_id, kind)
        where status = 'PROCESSED' and kind in ('REFUND','ROLLBACK');
    `);
  }
}
