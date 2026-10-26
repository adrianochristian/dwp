import { Migration } from '@mikro-orm/migrations';

export class Migration20261025000300 extends Migration {
  override up(): void {
    this.addSql(`
      create function dwp.validate_opening_successor() returns trigger language plpgsql as $$
      declare
        successor_before bigint;
      begin
        select balance_before_cents into successor_before from dwp.wallet_ledger_entries
          where wallet_id = NEW.wallet_id and wallet_version = 2;
        if found and successor_before <> NEW.balance_after_cents then
          raise exception 'opening breaks successor ledger balance continuity' using errcode = '23514';
        end if;
        return null;
      end $$;
      create constraint trigger opening_successor_consistency after insert on dwp.wallet_ledger_entries
        deferrable initially deferred for each row when (NEW.wallet_version = 1)
        execute function dwp.validate_opening_successor();
      create trigger ledger_no_truncate before truncate on dwp.wallet_ledger_entries
        for each statement execute function dwp.reject_ledger_mutation();
    `);
  }

  override down(): void {
    this.addSql(`
      drop trigger ledger_no_truncate on dwp.wallet_ledger_entries;
      drop trigger opening_successor_consistency on dwp.wallet_ledger_entries;
      drop function dwp.validate_opening_successor();
    `);
  }
}
