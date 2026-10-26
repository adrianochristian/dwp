import { Migration } from '@mikro-orm/migrations';

export class Migration20261026000200 extends Migration {
  override up(): void {
    this.addSql(`
      lock table dwp.wager_transactions in access exclusive mode;
      create function dwp.guard_transaction_identity_and_delete() returns trigger language plpgsql as $$
      begin
        if TG_OP = 'DELETE' then
          if OLD.status in ('PROCESSED','REJECTED','FAILED') then
            raise exception 'terminal wager transaction cannot be deleted' using errcode = '23514';
          end if;
          return OLD;
        end if;
        if row(NEW.id, NEW.provider_id, NEW.external_transaction_id, NEW.idempotency_key,
          NEW.payload_hash, NEW.wallet_id, NEW.player_id, NEW.round_id, NEW.game_id, NEW.kind,
          NEW.amount_cents, NEW.currency, NEW.reference_external_transaction_id, NEW.created_at)
          is distinct from
          row(OLD.id, OLD.provider_id, OLD.external_transaction_id, OLD.idempotency_key,
          OLD.payload_hash, OLD.wallet_id, OLD.player_id, OLD.round_id, OLD.game_id, OLD.kind,
          OLD.amount_cents, OLD.currency, OLD.reference_external_transaction_id, OLD.created_at) then
          raise exception 'wager transaction business identity is immutable' using errcode = '23514';
        end if;
        return NEW;
      end $$;
      create trigger transaction_identity_immutable before update on dwp.wager_transactions
        for each row execute function dwp.guard_transaction_identity_and_delete();
      create trigger transaction_terminal_delete before delete on dwp.wager_transactions
        for each row execute function dwp.guard_transaction_identity_and_delete();
    `);
  }

  override down(): void {
    this.addSql(`
      lock table dwp.wager_transactions in access exclusive mode;
      drop trigger transaction_terminal_delete on dwp.wager_transactions;
      drop trigger transaction_identity_immutable on dwp.wager_transactions;
      drop function dwp.guard_transaction_identity_and_delete();
    `);
  }
}
