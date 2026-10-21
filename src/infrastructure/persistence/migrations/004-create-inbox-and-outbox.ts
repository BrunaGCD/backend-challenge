// ====================================================================================================
//                          MIGRATION 004: INBOX_MESSAGES E OUTBOX_MESSAGES
// ====================================================================================================
import { Migration } from "@mikro-orm/migrations";                                             // base das migrations

export class CreateInboxAndOutbox extends Migration {
  override name = "004-create-inbox-and-outbox";

  override up(): void {
    this.addSql(`
      create table inbox_messages (
        consumer_name varchar(128) not null,
        message_id    varchar(128) not null,
        payload_hash  char(64)     not null,
        received_at   timestamptz  not null,
        processed_at  timestamptz,
        constraint inbox_messages_pkey primary key (consumer_name, message_id),
        constraint inbox_messages_payload_hash_format check (payload_hash ~ '^[0-9a-f]{64}$')
      )`);

    this.addSql(`
      create table outbox_messages (
        id              uuid        primary key,
        aggregate_id    uuid        not null,
        event_type      varchar(64) not null,
        payload         jsonb       not null,
        occurred_at     timestamptz not null,
        attempts        integer     not null default 0,
        next_attempt_at timestamptz not null,
        published_at    timestamptz,
        last_error      text,
        constraint outbox_messages_attempts_non_negative check (attempts >= 0)
      )`);

    // o que os publishers varrem
    this.addSql(`create index outbox_messages_due on outbox_messages (next_attempt_at) where published_at is null`);
  }

  override down(): void {
    this.addSql(`drop table outbox_messages`);
    this.addSql(`drop table inbox_messages`);
  }
}
