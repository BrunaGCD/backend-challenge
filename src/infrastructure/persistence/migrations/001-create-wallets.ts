// ====================================================================================================
//                                   MIGRATION 001: WALLETS
// ====================================================================================================
import { Migration } from "@mikro-orm/migrations";                                             // base das migrations

export class CreateWallets extends Migration {
  override name = "001-create-wallets";

  override up(): void {
    this.addSql(`
      create table wallets (
        id         uuid          primary key,
        player_id  uuid          not null,
        currency   char(3)       not null,
        balance    numeric(18,2) not null,
        version    integer       not null,
        created_at timestamptz   not null,
        updated_at timestamptz   not null,
        constraint wallets_currency_format       check (currency ~ '^[A-Z]{3}$'),
        constraint wallets_balance_non_negative  check (balance >= 0),
        constraint wallets_version_positive      check (version >= 1),
        constraint wallets_player_currency_unique unique (player_id, currency),
        constraint wallets_id_currency_unique     unique (id, currency)
      )`);
  }

  override down(): void {
    this.addSql(`drop table wallets`);
  }
}
