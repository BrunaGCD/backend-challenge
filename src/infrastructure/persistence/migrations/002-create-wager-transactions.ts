// ====================================================================================================
//                               MIGRATION 002: WAGER_TRANSACTIONS
// ====================================================================================================
// Toda garantia de unicidade e de imutabilidade da transação mora aqui, não só no código: com várias
// instâncias, o banco é o único ponto que todas compartilham.
import { Migration } from "@mikro-orm/migrations";                                             // base das migrations

export class CreateWagerTransactions extends Migration {
  override name = "002-create-wager-transactions";

  override up(): void {
    this.addSql(`
      create table wager_transactions (
        id                                uuid          primary key,
        provider_id                       varchar(128),
        external_transaction_id           varchar(128),
        idempotency_key                   varchar(255)  not null,
        payload_hash                      char(64)      not null,
        wallet_id                         uuid          not null references wallets (id),
        player_id                         uuid          not null,
        round_id                          varchar(128),
        game_id                           varchar(128),
        kind                              varchar(16)   not null,
        amount                            numeric(18,2) not null,
        currency                          char(3)       not null,
        reference_external_transaction_id varchar(128),
        reference_transaction_id          uuid          references wager_transactions (id),
        status                            varchar(24)   not null,
        failure_code                      varchar(64),
        balance_after                     numeric(18,2),
        balance_currency                  char(3),
        correlation_id                    varchar(128)  not null,
        attempts                          integer       not null default 0,
        next_attempt_at                   timestamptz,
        created_at                        timestamptz   not null,
        updated_at                        timestamptz   not null,
        processed_at                      timestamptz,

        constraint wager_transactions_idempotency_key_unique unique (idempotency_key),
        constraint wager_transactions_provider_external_unique unique (provider_id, external_transaction_id),
        constraint wager_transactions_kind_valid check (kind in ('OPENING','BET','WIN','LOSS','REFUND','ROLLBACK')),
        constraint wager_transactions_status_valid check (status in ('PENDING_REFERENCE','PROCESSED','REJECTED','FAILED')),
        constraint wager_transactions_amount_non_negative check (amount >= 0),
        constraint wager_transactions_amount_positive_unless_loss check (kind = 'LOSS' or amount > 0),
        constraint wager_transactions_currency_format check (currency ~ '^[A-Z]{3}$'),
        constraint wager_transactions_reference_by_kind check (
          (kind in ('REFUND','ROLLBACK') and reference_external_transaction_id is not null)
          or (kind in ('BET','OPENING') and reference_external_transaction_id is null)
          or kind in ('WIN','LOSS')
        ),
        constraint wager_transactions_failure_code_iff_failed check ((status in ('REJECTED','FAILED')) = (failure_code is not null)),
        constraint wager_transactions_processed_at_iff_processed check ((status = 'PROCESSED') = (processed_at is not null)),
        constraint wager_transactions_processed_reversal_has_reference check (not (kind in ('REFUND','ROLLBACK') and status = 'PROCESSED' and reference_transaction_id is null)),
        constraint wager_transactions_opening_has_no_provider_fields check (
          (kind = 'OPENING' and provider_id is null and external_transaction_id is null and round_id is null and game_id is null)
          or (kind <> 'OPENING' and provider_id is not null and external_transaction_id is not null and round_id is not null and game_id is not null)
        ),
        constraint wager_transactions_payload_hash_format check (payload_hash ~ '^[0-9a-f]{64}$'),
        constraint wager_transactions_balance_pair check ((balance_after is null) = (balance_currency is null)),
        constraint wager_transactions_attempts_non_negative check (attempts >= 0)
      )`);

    // cada transação é revertida no máximo uma vez, somando REFUND e ROLLBACK (interpretação I1)
    this.addSql(`
      create unique index wager_transactions_single_reversal
        on wager_transactions (reference_transaction_id)
        where kind in ('REFUND','ROLLBACK') and status = 'PROCESSED'`);

    // o que o worker de pendentes varre
    this.addSql(`
      create index wager_transactions_pending_due
        on wager_transactions (next_attempt_at)
        where status = 'PENDING_REFERENCE'`);

    this.addSql(`
      create function wager_transactions_guard() returns trigger language plpgsql as $$
      begin
        if tg_op = 'DELETE' then
          raise exception 'wager_transactions: DELETE proibido (transacao %)', old.id using errcode = 'P0001';
        end if;
        if old.status in ('PROCESSED','REJECTED','FAILED') then
          raise exception 'wager_transactions: transacao % ja esta em % (terminal) e nao pode mudar', old.id, old.status using errcode = 'P0001';
        end if;
        if new.id is distinct from old.id
          or new.provider_id is distinct from old.provider_id
          or new.external_transaction_id is distinct from old.external_transaction_id
          or new.idempotency_key is distinct from old.idempotency_key
          or new.payload_hash is distinct from old.payload_hash
          or new.wallet_id is distinct from old.wallet_id
          or new.player_id is distinct from old.player_id
          or new.round_id is distinct from old.round_id
          or new.game_id is distinct from old.game_id
          or new.kind is distinct from old.kind
          or new.amount is distinct from old.amount
          or new.currency is distinct from old.currency
          or new.reference_external_transaction_id is distinct from old.reference_external_transaction_id
          or new.correlation_id is distinct from old.correlation_id
          or new.created_at is distinct from old.created_at then
          raise exception 'wager_transactions: colunas de identidade e de negocio da transacao % nao mudam', old.id using errcode = 'P0001';
        end if;
        return new;
      end
      $$`);
    this.addSql(`create trigger wager_transactions_guard_row before update or delete on wager_transactions for each row execute function wager_transactions_guard()`);

    this.addSql(`
      create function wager_transactions_forbid_truncate() returns trigger language plpgsql as $$
      begin
        raise exception 'wager_transactions: TRUNCATE proibido' using errcode = 'P0001';
      end
      $$`);
    this.addSql(`create trigger wager_transactions_no_truncate before truncate on wager_transactions for each statement execute function wager_transactions_forbid_truncate()`);
  }

  override down(): void {
    this.addSql(`drop table wager_transactions`);                                               // leva junto índices e triggers
    this.addSql(`drop function wager_transactions_guard()`);
    this.addSql(`drop function wager_transactions_forbid_truncate()`);
  }
}
