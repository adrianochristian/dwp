import { Migration } from '@mikro-orm/migrations';

export class Migration20261025000200 extends Migration {
  override up(): void {
    this.addSql(`
      create table dwp.wallets (
        id uuid primary key,
        player_id text not null check (length(player_id) between 1 and 200),
        currency char(3) not null check (currency = 'BRL'),
        balance_cents bigint not null check (balance_cents >= 0),
        version integer not null check (version >= 1),
        created_at timestamptz not null default now(),
        updated_at timestamptz not null default now(),
        unique (player_id, currency),
        unique (id, player_id, currency),
        unique (id, currency)
      );
      create table dwp.wager_transactions (
        id uuid primary key,
        provider_id text not null check (length(provider_id) between 1 and 200),
        external_transaction_id text not null check (length(external_transaction_id) between 1 and 200),
        idempotency_key text not null unique check (length(idempotency_key) between 1 and 500),
        payload_hash char(64) not null check (payload_hash ~ '^[a-f0-9]{64}$'),
        wallet_id uuid not null,
        player_id text not null,
        round_id text not null,
        game_id text not null,
        kind text not null check (kind in ('OPENING','BET','WIN','LOSS','REFUND','ROLLBACK')),
        amount_cents bigint not null check (amount_cents >= 0),
        currency char(3) not null check (currency ~ '^[A-Z]{3}$'),
        reference_external_transaction_id text,
        reference_transaction_id uuid references dwp.wager_transactions(id),
        status text not null check (status in ('PENDING','PENDING_REFERENCE','PROCESSED','REJECTED','FAILED')),
        failure_code text,
        result jsonb,
        retry_count integer not null default 0 check (retry_count >= 0),
        next_attempt_at timestamptz not null default now(),
        created_at timestamptz not null default now(),
        processed_at timestamptz,
        unique (provider_id, external_transaction_id),
        unique (id, wallet_id, currency),
        foreign key (wallet_id) references dwp.wallets(id),
        check (kind = 'LOSS' or amount_cents > 0),
        check (kind not in ('REFUND','ROLLBACK') or reference_external_transaction_id is not null),
        check (reference_transaction_id is null or reference_transaction_id <> id),
        check ((status in ('REJECTED','FAILED')) = (failure_code is not null)),
        check (status not in ('PROCESSED','REJECTED','FAILED') or result is not null),
        check (status <> 'PROCESSED' or processed_at is not null)
      );
      create unique index wager_reversal_once on dwp.wager_transactions(reference_transaction_id, kind)
        where status = 'PROCESSED' and kind in ('REFUND','ROLLBACK');
      create index wager_pending_reference_due on dwp.wager_transactions(next_attempt_at, id)
        where status = 'PENDING_REFERENCE';
      create index wager_wallet_created on dwp.wager_transactions(wallet_id, created_at, id);
      create table dwp.wallet_ledger_entries (
        id uuid primary key,
        wallet_id uuid not null,
        transaction_id uuid not null,
        direction text not null check (direction in ('DEBIT','CREDIT')),
        amount_cents bigint not null check (amount_cents > 0),
        currency char(3) not null,
        balance_before_cents bigint not null check (balance_before_cents >= 0),
        balance_after_cents bigint not null check (balance_after_cents >= 0),
        wallet_version integer not null check (wallet_version >= 1),
        created_at timestamptz not null default now(),
        unique (transaction_id, wallet_id),
        unique (wallet_id, wallet_version),
        foreign key (wallet_id, currency) references dwp.wallets(id, currency),
        foreign key (transaction_id, wallet_id, currency) references dwp.wager_transactions(id, wallet_id, currency),
        check ((direction = 'CREDIT' and balance_before_cents::numeric + amount_cents::numeric = balance_after_cents::numeric)
          or (direction = 'DEBIT' and balance_before_cents::numeric - amount_cents::numeric = balance_after_cents::numeric))
      );
      create index ledger_wallet_cursor on dwp.wallet_ledger_entries(wallet_id, wallet_version, id);
      create table dwp.inbox_messages (
        consumer_name text not null,
        message_id text not null,
        payload_hash char(64) not null check (payload_hash ~ '^[a-f0-9]{64}$'),
        received_at timestamptz not null default now(),
        processed_at timestamptz,
        result jsonb,
        primary key (consumer_name, message_id)
      );
      create table dwp.outbox_messages (
        id uuid primary key,
        aggregate_id uuid not null,
        event_type text not null,
        payload jsonb not null,
        occurred_at timestamptz not null,
        published_at timestamptz,
        attempts integer not null default 0 check (attempts >= 0),
        next_attempt_at timestamptz not null default now(),
        lease_until timestamptz,
        lease_token uuid,
        check ((lease_until is null) = (lease_token is null)),
        check (payload->>'eventId' = id::text)
      );
      create index outbox_pending_due on dwp.outbox_messages(next_attempt_at, occurred_at, id) where published_at is null;

      create function dwp.guard_wallet_change() returns trigger language plpgsql as $$
      begin
        if TG_OP = 'INSERT' then
          if NEW.version <> 1 then raise exception 'wallet must start at version 1' using errcode = '23514'; end if;
        else
          if NEW.id <> OLD.id or NEW.player_id <> OLD.player_id or NEW.currency <> OLD.currency then
            raise exception 'wallet identity is immutable' using errcode = '23514';
          end if;
          if NEW.balance_cents = OLD.balance_cents and NEW.version <> OLD.version then
            raise exception 'unchanged balance must preserve wallet version' using errcode = '23514';
          elsif NEW.balance_cents <> OLD.balance_cents and NEW.version <> OLD.version + 1 then
            raise exception 'balance change must increment wallet version once' using errcode = '23514';
          end if;
        end if;
        return NEW;
      end $$;
      create trigger wallet_change_guard before insert or update on dwp.wallets
        for each row execute function dwp.guard_wallet_change();

      create function dwp.guard_transaction_terminal() returns trigger language plpgsql as $$
      begin
        if OLD.status in ('PROCESSED','REJECTED','FAILED') and NEW is distinct from OLD then
          raise exception 'terminal wager transaction is immutable' using errcode = '23514';
        end if;
        return NEW;
      end $$;
      create trigger transaction_terminal_guard before update on dwp.wager_transactions
        for each row execute function dwp.guard_transaction_terminal();

      create function dwp.reject_ledger_mutation() returns trigger language plpgsql as $$
      begin
        raise exception 'ledger entries are immutable' using errcode = '23514';
      end $$;
      create trigger ledger_immutable before update or delete on dwp.wallet_ledger_entries
        for each row execute function dwp.reject_ledger_mutation();

      create function dwp.validate_wallet_ledger() returns trigger language plpgsql as $$
      declare
        wallet dwp.wallets%rowtype;
        latest dwp.wallet_ledger_entries%rowtype;
        target uuid;
      begin
        if TG_TABLE_NAME = 'wallets' then target := NEW.id; else target := NEW.wallet_id; end if;
        select * into wallet from dwp.wallets where id = target;
        select * into latest from dwp.wallet_ledger_entries where wallet_id = target order by wallet_version desc limit 1;
        if not found then
          if wallet.balance_cents <> 0 or wallet.version <> 1 then
            raise exception 'wallet balance requires matching ledger' using errcode = '23514';
          end if;
        elsif latest.balance_after_cents <> wallet.balance_cents or latest.wallet_version <> wallet.version then
          raise exception 'wallet and ledger final state differ' using errcode = '23514';
        end if;
        return null;
      end $$;
      create constraint trigger wallet_ledger_consistency after insert or update on dwp.wallets
        deferrable initially deferred for each row execute function dwp.validate_wallet_ledger();
      create constraint trigger ledger_wallet_consistency after insert on dwp.wallet_ledger_entries
        deferrable initially deferred for each row execute function dwp.validate_wallet_ledger();

      create function dwp.validate_ledger_entry() returns trigger language plpgsql as $$
      declare
        prior dwp.wallet_ledger_entries%rowtype;
        wager dwp.wager_transactions%rowtype;
        reference dwp.wager_transactions%rowtype;
        expected_direction text;
      begin
        select * into wager from dwp.wager_transactions where id = NEW.transaction_id;
        if wager.status <> 'PROCESSED' or wager.kind = 'LOSS' or wager.amount_cents <> NEW.amount_cents then
          raise exception 'ledger requires matching processed financial transaction' using errcode = '23514';
        end if;
        expected_direction := case when wager.kind = 'BET' then 'DEBIT' else 'CREDIT' end;
        if wager.kind = 'ROLLBACK' then
          select * into reference from dwp.wager_transactions where id = wager.reference_transaction_id;
          expected_direction := case when reference.kind = 'BET' then 'CREDIT' else 'DEBIT' end;
        end if;
        if expected_direction <> NEW.direction then
          raise exception 'ledger direction differs from transaction' using errcode = '23514';
        end if;
        if NEW.wallet_version = 1 then
          if wager.kind <> 'OPENING' or NEW.balance_before_cents <> 0 then
            raise exception 'ledger version 1 is reserved for opening' using errcode = '23514';
          end if;
        else
          if wager.kind = 'OPENING' then raise exception 'opening must use version 1' using errcode = '23514'; end if;
          select * into prior from dwp.wallet_ledger_entries
            where wallet_id = NEW.wallet_id and wallet_version = NEW.wallet_version - 1;
          if not found then
            if NEW.wallet_version <> 2 or NEW.balance_before_cents <> 0 then
              raise exception 'ledger version continuity is broken' using errcode = '23514';
            end if;
          elsif prior.balance_after_cents <> NEW.balance_before_cents then
            raise exception 'ledger balance continuity is broken' using errcode = '23514';
          end if;
        end if;
        return null;
      end $$;
      create constraint trigger ledger_entry_consistency after insert on dwp.wallet_ledger_entries
        deferrable initially deferred for each row execute function dwp.validate_ledger_entry();

      create function dwp.validate_transaction_ledger() returns trigger language plpgsql as $$
      declare
        wager dwp.wager_transactions%rowtype;
        wallet dwp.wallets%rowtype;
        reference dwp.wager_transactions%rowtype;
        has_entry boolean;
      begin
        select * into wager from dwp.wager_transactions where id = NEW.id;
        if wager.status = 'PROCESSED' then
          select * into wallet from dwp.wallets where id = wager.wallet_id;
          if wallet.player_id <> wager.player_id or wallet.currency <> wager.currency then
            raise exception 'processed transaction wallet identity differs' using errcode = '23514';
          end if;
        end if;
        select exists(select 1 from dwp.wallet_ledger_entries where transaction_id = wager.id and wallet_id = wager.wallet_id) into has_entry;
        if (wager.status = 'PROCESSED' and wager.kind <> 'LOSS') <> has_entry then
          raise exception 'processed financial transaction and ledger must agree' using errcode = '23514';
        end if;
        if wager.status = 'PROCESSED' and wager.reference_external_transaction_id is not null then
          select * into reference from dwp.wager_transactions where id = wager.reference_transaction_id;
          if not found or reference.status <> 'PROCESSED' or reference.provider_id <> wager.provider_id
            or reference.external_transaction_id <> wager.reference_external_transaction_id
            or reference.wallet_id <> wager.wallet_id or reference.player_id <> wager.player_id
            or reference.currency <> wager.currency or reference.round_id <> wager.round_id then
            raise exception 'processed transaction reference is invalid' using errcode = '23514';
          end if;
          if (wager.kind in ('WIN','LOSS','REFUND') and reference.kind <> 'BET')
            or (wager.kind = 'ROLLBACK' and reference.kind not in ('BET','WIN','REFUND'))
            or (wager.kind in ('REFUND','ROLLBACK') and wager.amount_cents <> reference.amount_cents) then
            raise exception 'processed transaction reference kind or amount differs' using errcode = '23514';
          end if;
        end if;
        return null;
      end $$;
      create constraint trigger transaction_ledger_consistency after insert or update on dwp.wager_transactions
        deferrable initially deferred for each row execute function dwp.validate_transaction_ledger();
    `);
  }

  override down(): void {
    this.addSql(`
      drop table dwp.outbox_messages;
      drop table dwp.inbox_messages;
      drop table dwp.wallet_ledger_entries;
      drop table dwp.wager_transactions;
      drop table dwp.wallets;
      drop function dwp.validate_transaction_ledger();
      drop function dwp.validate_ledger_entry();
      drop function dwp.validate_wallet_ledger();
      drop function dwp.reject_ledger_mutation();
      drop function dwp.guard_transaction_terminal();
      drop function dwp.guard_wallet_change();
    `);
  }
}
