// ====================================================================================================
//                   MIGRATION 006: DIREÇÃO DO LANÇAMENTO, REFERÊNCIAS E OUTBOX NO BANCO
// ====================================================================================================
// Mais garantias que antes só o código dava passam a valer no banco, para qualquer cliente:
//   1. a direção do lançamento vem do tipo da transação: BET só DEBIT; WIN, REFUND e OPENING só CREDIT;
//      ROLLBACK o inverso da referência (de BET é CREDIT, de WIN ou REFUND é DEBIT);
//   2. a referência é da mesma wallet (chave estrangeira composta);
//   3. transação PROCESSED com referência: mesmo provedor, player, rodada e moeda, tipo permitido pela
//      regra 3 do desafio e, em REFUND e ROLLBACK, o mesmo valor (vale também para o LOSS, que não tem
//      lançamento, por isso é um trigger em wager_transactions e não no ledger);
//   4. a outbox não perde evento: sem DELETE nem TRUNCATE, published_at não volta a nulo, e o evento
//      gravado (id, aggregate_id, event_type, payload, occurred_at) não muda;
//   5. o saldo observado gravado na transação não é negativo.
// O 1 entra na função do trigger adiado da 005 (create or replace, porque a 005 já está aplicada nos
// bancos que existem), e o down devolve a versão da 005. O 3 também é adiado para o commit, como os da 005.
// As mensagens não levam valores, porque podem acabar num log.
import { Migration } from "@mikro-orm/migrations";                                             // base das migrations

export class ProtectReferencesAndOutbox extends Migration {
  override name = "006-protect-references-and-outbox";

  override up(): void {
    // ----------------------------------------------------------------------------------------------------
    //                 1. DIREÇÃO DO LANÇAMENTO PELO TIPO, NO TRIGGER ADIADO DA 005
    // ----------------------------------------------------------------------------------------------------
    this.addSql(`
      create or replace function wallet_ledger_entries_check_transaction() returns trigger language plpgsql as $$
      declare
        entry_transaction  record;
        reference_kind     varchar(16);
        expected_direction varchar(6);
      begin
        select status, kind, amount, currency, reference_transaction_id into entry_transaction from wager_transactions where id = new.transaction_id;
        if entry_transaction.status <> 'PROCESSED' or entry_transaction.kind = 'LOSS'
          or entry_transaction.amount <> new.amount or entry_transaction.currency <> new.currency then
          raise exception 'wallet_ledger_entries: o lancamento % precisa de uma transacao PROCESSED, que nao seja LOSS, com o mesmo valor e moeda', new.id using errcode = 'P0001';
        end if;
        if entry_transaction.kind = 'BET' then
          expected_direction := 'DEBIT';
        elsif entry_transaction.kind in ('WIN','REFUND','OPENING') then
          expected_direction := 'CREDIT';
        elsif entry_transaction.kind = 'ROLLBACK' then
          select kind into reference_kind from wager_transactions where id = entry_transaction.reference_transaction_id;
          if reference_kind = 'BET' then
            expected_direction := 'CREDIT';
          elsif reference_kind in ('WIN','REFUND') then
            expected_direction := 'DEBIT';
          end if;
        end if;
        if expected_direction is null or new.direction <> expected_direction then
          raise exception 'wallet_ledger_entries: a direcao do lancamento % nao bate com o tipo da transacao (ou da referencia, no ROLLBACK)', new.id using errcode = 'P0001';
        end if;
        return null;
      end
      $$`);

    // ----------------------------------------------------------------------------------------------------
    //                           2. A REFERÊNCIA É DA MESMA WALLET
    // ----------------------------------------------------------------------------------------------------
    this.addSql(`alter table wager_transactions add constraint wager_transactions_reference_same_wallet_fk foreign key (reference_transaction_id, wallet_id) references wager_transactions (id, wallet_id)`);   // alvo: o UNIQUE (id, wallet_id) da 005

    // ----------------------------------------------------------------------------------------------------
    //          3. TRANSAÇÃO PROCESSED COM REFERÊNCIA: MESMA IDENTIDADE, TIPO PERMITIDO, MESMO VALOR
    // ----------------------------------------------------------------------------------------------------
    this.addSql(`
      create function wager_transactions_check_reference() returns trigger language plpgsql as $$
      declare
        checked    record;
        referenced record;
      begin
        select kind, status, provider_id, player_id, round_id, currency, amount, reference_transaction_id into checked from wager_transactions where id = new.id;
        if checked.status <> 'PROCESSED' or checked.reference_transaction_id is null then
          return null;
        end if;
        select kind, provider_id, player_id, round_id, currency, amount into referenced from wager_transactions where id = checked.reference_transaction_id;
        if referenced.provider_id is distinct from checked.provider_id
          or referenced.player_id is distinct from checked.player_id
          or referenced.round_id is distinct from checked.round_id
          or referenced.currency is distinct from checked.currency then
          raise exception 'wager_transactions: a transacao % e a referencia dela precisam ter o mesmo provedor, player, rodada e moeda', new.id using errcode = 'P0001';
        end if;
        if not ((checked.kind = 'REFUND' and referenced.kind = 'BET')
          or (checked.kind = 'ROLLBACK' and referenced.kind in ('BET','WIN','REFUND'))
          or (checked.kind in ('WIN','LOSS') and referenced.kind = 'BET')) then
          raise exception 'wager_transactions: o tipo da referencia da transacao % nao e permitido para o tipo dela', new.id using errcode = 'P0001';
        end if;
        if checked.kind in ('REFUND','ROLLBACK') and referenced.amount <> checked.amount then
          raise exception 'wager_transactions: a reversao % precisa ter o mesmo valor da referencia', new.id using errcode = 'P0001';
        end if;
        return null;
      end
      $$`);
    this.addSql(`
      create constraint trigger wager_transactions_matches_reference
        after insert or update on wager_transactions deferrable initially deferred
        for each row execute function wager_transactions_check_reference()`);

    // ----------------------------------------------------------------------------------------------------
    //                    4. OUTBOX: NENHUM EVENTO SOME, NEM VOLTA A FICAR PENDENTE
    // ----------------------------------------------------------------------------------------------------
    this.addSql(`
      create function outbox_messages_guard() returns trigger language plpgsql as $$
      begin
        if tg_op = 'DELETE' then
          raise exception 'outbox_messages: DELETE proibido (evento %)', old.id using errcode = 'P0001';
        end if;
        if old.published_at is not null and new.published_at is null then
          raise exception 'outbox_messages: o evento % ja foi publicado e published_at nao volta a nulo', old.id using errcode = 'P0001';
        end if;
        if new.id is distinct from old.id
          or new.aggregate_id is distinct from old.aggregate_id
          or new.event_type is distinct from old.event_type
          or new.payload is distinct from old.payload
          or new.occurred_at is distinct from old.occurred_at then
          raise exception 'outbox_messages: id, aggregate_id, event_type, payload e occurred_at do evento % nao mudam', old.id using errcode = 'P0001';
        end if;
        return new;
      end
      $$`);
    this.addSql(`create trigger outbox_messages_guard_row before update or delete on outbox_messages for each row execute function outbox_messages_guard()`);

    this.addSql(`
      create function outbox_messages_forbid_truncate() returns trigger language plpgsql as $$
      begin
        raise exception 'outbox_messages: TRUNCATE proibido' using errcode = 'P0001';
      end
      $$`);
    this.addSql(`create trigger outbox_messages_no_truncate before truncate on outbox_messages for each statement execute function outbox_messages_forbid_truncate()`);

    // ----------------------------------------------------------------------------------------------------
    //                         5. SALDO OBSERVADO NÃO NEGATIVO NA TRANSAÇÃO
    // ----------------------------------------------------------------------------------------------------
    this.addSql(`alter table wager_transactions add constraint wager_transactions_balance_after_non_negative check (balance_after >= 0)`);
  }

  override down(): void {
    this.addSql(`alter table wager_transactions drop constraint wager_transactions_balance_after_non_negative`);
    this.addSql(`drop trigger outbox_messages_no_truncate on outbox_messages`);
    this.addSql(`drop function outbox_messages_forbid_truncate()`);
    this.addSql(`drop trigger outbox_messages_guard_row on outbox_messages`);
    this.addSql(`drop function outbox_messages_guard()`);
    this.addSql(`drop trigger wager_transactions_matches_reference on wager_transactions`);
    this.addSql(`drop function wager_transactions_check_reference()`);
    this.addSql(`alter table wager_transactions drop constraint wager_transactions_reference_same_wallet_fk`);

    // a função do trigger adiado volta a ser exatamente a da 005, sem a conferência da direção
    this.addSql(`
      create or replace function wallet_ledger_entries_check_transaction() returns trigger language plpgsql as $$
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
  }
}
