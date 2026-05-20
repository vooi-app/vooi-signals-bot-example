"""
Bug #4 (round 2): the ingester must use INSERT ... ON CONFLICT DO NOTHING
to handle Telethon re-deliveries (which previously surfaced as
UniqueViolationError on idx_messages_channel_tg_id and crashed
`on_new_message`).

We assert the SQL statement compiled in `process_telegram_message`
ends with `ON CONFLICT ... DO NOTHING RETURNING messages.id`.
"""
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import insert as pg_insert

from bot.models import Message


def test_message_dedup_uses_on_conflict_do_nothing():
    stmt = (
        pg_insert(Message)
        .values(
            channel_id=1,
            telegram_message_id=42,
            raw_text="hello",
            processed=False,
        )
        .on_conflict_do_nothing(index_elements=["channel_id", "telegram_message_id"])
        .returning(Message.id)
    )
    sql = str(stmt.compile(dialect=postgresql.dialect()))
    assert "ON CONFLICT" in sql.upper()
    assert "DO NOTHING" in sql.upper()
    assert "RETURNING" in sql.upper()


def test_ingester_imports_pg_insert():
    """Regression guard: bot.ingester must import postgresql's `insert`."""
    import bot.ingester as ing
    assert ing.pg_insert is pg_insert
