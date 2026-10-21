// ====================================================================================================
//                       MIGRATION 005: PROTEÇÃO DA WALLET E DA LIGAÇÃO COM O LEDGER
// ====================================================================================================
// A version da wallet é o optimistic lock do MikroORM (version: true). O ORM não manda a coluna no
// INSERT e lê de volta o valor que o banco gravou, então o "nasce em 1" passa a ser default do banco.
import { Migration } from "@mikro-orm/migrations";                                             // base das migrations

export class ProtectWalletsAndLedger extends Migration {
  override name = "005-protect-wallets-and-ledger";

  override up(): void {
    this.addSql(`alter table wallets alter column version set default 1`);                    // toda wallet nasce na versão 1
  }

  override down(): void {
    this.addSql(`alter table wallets alter column version drop default`);
  }
}
