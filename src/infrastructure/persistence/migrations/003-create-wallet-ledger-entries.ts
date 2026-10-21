// ====================================================================================================
//                             MIGRATION 003: WALLET_LEDGER_ENTRIES
// ====================================================================================================
// O ledger só recebe INSERT. UPDATE, DELETE e TRUNCATE são barrados por trigger, para qualquer papel.
import { Migration } from "@mikro-orm/migrations";                                             // base das migrations

export class CreateWalletLedgerEntries extends Migration {
  override name = "003-create-wallet-ledger-entries";

  override up(): void {
    this.addSql(`
      create table wallet_ledger_entries (
        id             uuid          primary key,
        wallet_id      uuid          not null,
        transaction_id uuid          not null references wager_transactions (id),
        direction      varchar(6)    not null,
        amount         numeric(18,2) not null,
        currency       char(3)       not null,
        balance_before numeric(18,2) not null,
        balance_after  numeric(18,2) not null,
        wallet_version integer       not null,
        created_at     timestamptz   not null,

        constraint wallet_ledger_entries_direction_valid check (direction in ('DEBIT','CREDIT')),
        constraint wallet_ledger_entries_amount_positive check (amount > 0),
        constraint wallet_ledger_entries_balance_before_non_negative check (balance_before >= 0),
        constraint wallet_ledger_entries_balance_after_non_negative check (balance_after >= 0),
        constraint wallet_ledger_entries_wallet_version_positive check (wallet_version >= 1),
        constraint wallet_ledger_entries_arithmetic check (
          (direction = 'CREDIT' and balance_after = balance_before + amount)
          or (direction = 'DEBIT' and balance_after = balance_before - amount)
        ),
        constraint wallet_ledger_entries_one_per_transaction unique (wallet_id, transaction_id),
        constraint wallet_ledger_entries_wallet_version_unique unique (wallet_id, wallet_version),
        constraint wallet_ledger_entries_wallet_currency_fk foreign key (wallet_id, currency) references wallets (id, currency)
      )`);

    this.addSql(`
      create function wallet_ledger_entries_forbid_change() returns trigger language plpgsql as $$
      begin
        raise exception 'wallet_ledger_entries: % proibido, o ledger so recebe INSERT', tg_op using errcode = 'P0001';
      end
      $$`);
    this.addSql(`create trigger wallet_ledger_entries_no_update_delete before update or delete on wallet_ledger_entries for each row execute function wallet_ledger_entries_forbid_change()`);
    this.addSql(`create trigger wallet_ledger_entries_no_truncate before truncate on wallet_ledger_entries for each statement execute function wallet_ledger_entries_forbid_change()`);
  }

  override down(): void {
    this.addSql(`drop table wallet_ledger_entries`);
    this.addSql(`drop function wallet_ledger_entries_forbid_change()`);
  }
}
