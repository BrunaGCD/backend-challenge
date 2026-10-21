// ====================================================================================================
//                       MIGRATION 005: PROTEÇÃO DA WALLET E DA LIGAÇÃO COM O LEDGER
// ====================================================================================================
// O que antes só o código garantia passa a valer no banco, para qualquer cliente:
//   1. a wallet não é apagada, dono, moeda, id e created_at não mudam, e a version só sobe de 1 em 1,
//      junto com o saldo (e nasce em 1: é o default que o optimistic lock do MikroORM espera);
//   2. o lançamento aponta para (transação, wallet), então não pode ser de uma transação de outra wallet;
//   3. só existe lançamento de transação PROCESSED, que não seja LOSS, com o mesmo valor e moeda;
//   4. uma transação OPENING por wallet;
//   5. no commit, o saldo e a version da wallet são o balance_after e o wallet_version do último
//      lançamento dela (ou saldo zero e versão 1, sem lançamento nenhum).
// O 3 e o 5 são constraint triggers adiados para o commit (DEFERRABLE INITIALLY DEFERRED): no meio da
// transação a wallet já mudou e o lançamento ainda não entrou (ou o contrário), e o Unit of Work faz os
// INSERTs antes dos UPDATEs, então o lançamento de uma pendente entra antes de ela virar PROCESSED.
// As mensagens não levam valores, porque podem acabar num log.
import { Migration } from "@mikro-orm/migrations";                                             // base das migrations

export class ProtectWalletsAndLedger extends Migration {
  override name = "005-protect-wallets-and-ledger";

  override up(): void {
    this.addSql(`alter table wallets alter column version set default 1`);                    // toda wallet nasce na versão 1

    // ----------------------------------------------------------------------------------------------------
    //                    1. WALLET: SEM DELETE, CAMPOS FIXOS, VERSION SÓ COM O SALDO
    // ----------------------------------------------------------------------------------------------------
    this.addSql(`
      create function wallets_guard() returns trigger language plpgsql as $$
      begin
        if tg_op = 'DELETE' then
          raise exception 'wallets: DELETE proibido (wallet %)', old.id using errcode = 'P0001';
        end if;
        if new.id is distinct from old.id
          or new.player_id is distinct from old.player_id
          or new.currency is distinct from old.currency
          or new.created_at is distinct from old.created_at then
          raise exception 'wallets: id, player_id, currency e created_at da wallet % nao mudam', old.id using errcode = 'P0001';
        end if;
        if new.balance = old.balance and new.version <> old.version then
          raise exception 'wallets: a version da wallet % so muda junto com o saldo', old.id using errcode = 'P0001';
        end if;
        if new.balance <> old.balance and new.version <> old.version + 1 then
          raise exception 'wallets: o saldo da wallet % mudou, entao a version precisa subir exatamente 1', old.id using errcode = 'P0001';
        end if;
        return new;
      end
      $$`);
    this.addSql(`create trigger wallets_guard_row before update or delete on wallets for each row execute function wallets_guard()`);

    this.addSql(`
      create function wallets_forbid_truncate() returns trigger language plpgsql as $$
      begin
        raise exception 'wallets: TRUNCATE proibido' using errcode = 'P0001';
      end
      $$`);
    this.addSql(`create trigger wallets_no_truncate before truncate on wallets for each statement execute function wallets_forbid_truncate()`);

    // ----------------------------------------------------------------------------------------------------
    //                       2. LANÇAMENTO LIGADO À TRANSAÇÃO E À WALLET DELA
    // ----------------------------------------------------------------------------------------------------
    this.addSql(`alter table wager_transactions add constraint wager_transactions_id_wallet_unique unique (id, wallet_id)`);   // alvo da FK composta
    this.addSql(`alter table wallet_ledger_entries add constraint wallet_ledger_entries_transaction_wallet_fk foreign key (transaction_id, wallet_id) references wager_transactions (id, wallet_id)`);

    // ----------------------------------------------------------------------------------------------------
    //                3. SÓ LANÇAMENTO DE TRANSAÇÃO PROCESSED, NÃO LOSS, COM O MESMO VALOR
    // ----------------------------------------------------------------------------------------------------
    this.addSql(`
      create function wallet_ledger_entries_check_transaction() returns trigger language plpgsql as $$
      declare
        entry_transaction record;
      begin
        select status, kind, amount, currency into entry_transaction from wager_transactions where id = new.transaction_id;
        if entry_transaction.status <> 'PROCESSED' or entry_transaction.kind = 'LOSS'
          or entry_transaction.amount <> new.amount or entry_transaction.currency <> new.currency then
          raise exception 'wallet_ledger_entries: o lancamento % precisa de uma transacao PROCESSED, que nao seja LOSS, com o mesmo valor e moeda', new.id using errcode = 'P0001';
        end if;
        return null;
      end
      $$`);
    this.addSql(`
      create constraint trigger wallet_ledger_entries_matches_transaction
        after insert on wallet_ledger_entries deferrable initially deferred
        for each row execute function wallet_ledger_entries_check_transaction()`);

    // ----------------------------------------------------------------------------------------------------
    //                                 4. UM OPENING POR WALLET
    // ----------------------------------------------------------------------------------------------------
    this.addSql(`create unique index wager_transactions_single_opening on wager_transactions (wallet_id) where kind = 'OPENING'`);

    // ----------------------------------------------------------------------------------------------------
    //           5. NO COMMIT: SALDO E VERSION DA WALLET == ÚLTIMO LANÇAMENTO (OU ZERO E 1)
    // ----------------------------------------------------------------------------------------------------
    this.addSql(`
      create function wallets_check_matches_last_entry() returns trigger language plpgsql as $$
      declare
        checked_wallet_id uuid;
        stored_balance    numeric(18,2);
        stored_version    integer;
        last_balance      numeric(18,2);
        last_version      integer;
      begin
        if tg_table_name = 'wallets' then
          checked_wallet_id := new.id;
        else
          checked_wallet_id := new.wallet_id;
        end if;
        select balance, version into stored_balance, stored_version from wallets where id = checked_wallet_id;
        select e.balance_after, e.wallet_version into last_balance, last_version
          from wallet_ledger_entries e
         where e.wallet_id = checked_wallet_id
         order by e.wallet_version desc
         limit 1;
        if not found then
          last_balance := 0;
          last_version := 1;
        end if;
        if stored_balance <> last_balance or stored_version <> last_version then
          raise exception 'wallets: saldo e version da wallet % nao batem com o ultimo lancamento do ledger', checked_wallet_id using errcode = 'P0001';
        end if;
        return null;
      end
      $$`);
    this.addSql(`
      create constraint trigger wallets_matches_last_entry
        after insert or update on wallets deferrable initially deferred
        for each row execute function wallets_check_matches_last_entry()`);
    this.addSql(`
      create constraint trigger wallet_ledger_entries_matches_wallet
        after insert on wallet_ledger_entries deferrable initially deferred
        for each row execute function wallets_check_matches_last_entry()`);
  }

  override down(): void {
    this.addSql(`drop trigger wallet_ledger_entries_matches_wallet on wallet_ledger_entries`);
    this.addSql(`drop trigger wallets_matches_last_entry on wallets`);
    this.addSql(`drop function wallets_check_matches_last_entry()`);
    this.addSql(`drop index wager_transactions_single_opening`);
    this.addSql(`drop trigger wallet_ledger_entries_matches_transaction on wallet_ledger_entries`);
    this.addSql(`drop function wallet_ledger_entries_check_transaction()`);
    this.addSql(`alter table wallet_ledger_entries drop constraint wallet_ledger_entries_transaction_wallet_fk`);
    this.addSql(`alter table wager_transactions drop constraint wager_transactions_id_wallet_unique`);
    this.addSql(`drop trigger wallets_no_truncate on wallets`);
    this.addSql(`drop function wallets_forbid_truncate()`);
    this.addSql(`drop trigger wallets_guard_row on wallets`);
    this.addSql(`drop function wallets_guard()`);
    this.addSql(`alter table wallets alter column version drop default`);
  }
}
