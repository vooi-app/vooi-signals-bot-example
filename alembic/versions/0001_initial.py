"""initial schema

Revision ID: 0001_initial
Revises:
Create Date: 2026-05-08 00:00:00.000000

Creates all tables from spec §4 v1.5:
- channels, messages, signals (with prompt_version)
- orders (with symbol, side)
- positions (with sl_strategy, sl_moved_to_be_at, status including open_pending_tp_sl)
- trades, events
- vooi_api_calls, vooi_errors
- runtime_state
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers
revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # -----------------------------------------------------------------
    # channels
    # -----------------------------------------------------------------
    op.create_table(
        "channels",
        sa.Column("id", sa.BigInteger(), nullable=False, autoincrement=True),
        sa.Column("telegram_id", sa.BigInteger(), nullable=False),
        sa.Column("username", sa.Text(), nullable=True),
        sa.Column("title", sa.Text(), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column(
            "added_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("telegram_id"),
    )

    # -----------------------------------------------------------------
    # messages
    # -----------------------------------------------------------------
    op.create_table(
        "messages",
        sa.Column("id", sa.BigInteger(), nullable=False, autoincrement=True),
        sa.Column("channel_id", sa.BigInteger(), nullable=False),
        sa.Column("telegram_message_id", sa.BigInteger(), nullable=False),
        sa.Column("raw_text", sa.Text(), nullable=True),
        sa.Column(
            "received_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.Column("processed", sa.Boolean(), nullable=False, server_default="false"),
        sa.ForeignKeyConstraint(["channel_id"], ["channels.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "idx_messages_channel_tg_id",
        "messages",
        ["channel_id", "telegram_message_id"],
        unique=True,
    )

    # -----------------------------------------------------------------
    # signals
    # -----------------------------------------------------------------
    op.create_table(
        "signals",
        sa.Column("id", sa.BigInteger(), nullable=False, autoincrement=True),
        sa.Column("message_id", sa.BigInteger(), nullable=False),
        sa.Column("channel_id", sa.BigInteger(), nullable=False),
        sa.Column("is_signal", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("symbol", sa.Text(), nullable=True),
        sa.Column("side", sa.Text(), nullable=True),
        sa.Column("entry_prices", sa.Text(), nullable=True),   # JSON array
        sa.Column("take_profits", sa.Text(), nullable=True),   # JSON array
        sa.Column("stop_loss", sa.Numeric(precision=28, scale=10), nullable=True),
        sa.Column("leverage", sa.Integer(), nullable=True),
        sa.Column("chain", sa.Text(), nullable=True),
        sa.Column("skip_reason", sa.Text(), nullable=True),
        # A12: prompt_version for regression tracking
        sa.Column("prompt_version", sa.Text(), nullable=True),
        sa.Column(
            "parsed_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.ForeignKeyConstraint(["message_id"], ["messages.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("idx_signals_symbol_side", "signals", ["symbol", "side"])
    op.create_index("idx_signals_parsed_at", "signals", ["parsed_at"])

    # -----------------------------------------------------------------
    # orders
    # NOTE: symbol TEXT NOT NULL and side TEXT NOT NULL required for conflict check §8.1
    # -----------------------------------------------------------------
    op.create_table(
        "orders",
        sa.Column("id", sa.BigInteger(), nullable=False, autoincrement=True),
        sa.Column("signal_id", sa.BigInteger(), nullable=True),
        sa.Column("client_order_id", sa.Text(), nullable=True),
        sa.Column("vooi_order_id", sa.Text(), nullable=True),
        sa.Column("exchange", sa.Text(), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=False),        # Required per spec §4
        sa.Column("side", sa.Text(), nullable=False),          # Required per spec §4
        sa.Column("order_type", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False, server_default="submitting"),
        sa.Column("price", sa.Numeric(precision=28, scale=10), nullable=True),
        sa.Column("trigger_price", sa.Numeric(precision=28, scale=10), nullable=True),
        sa.Column("size", sa.Numeric(precision=28, scale=10), nullable=True),
        sa.Column("filled_size", sa.Numeric(precision=28, scale=10), nullable=True),
        sa.Column("avg_fill_price", sa.Numeric(precision=28, scale=10), nullable=True),
        sa.Column("reduce_only", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("leverage", sa.Integer(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.Column("filled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("cancelled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("raw_response", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(["signal_id"], ["signals.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("client_order_id"),
        sa.UniqueConstraint("vooi_order_id"),
    )
    op.create_index("idx_orders_status", "orders", ["status"])
    op.create_index("idx_orders_exchange_symbol", "orders", ["exchange", "symbol"])
    op.create_index("idx_orders_signal_id", "orders", ["signal_id"])
    op.create_index("idx_orders_vooi_order_id", "orders", ["vooi_order_id"])

    # -----------------------------------------------------------------
    # positions — full v1.5 schema
    # -----------------------------------------------------------------
    op.create_table(
        "positions",
        sa.Column("id", sa.BigInteger(), nullable=False, autoincrement=True),
        sa.Column("signal_id", sa.BigInteger(), nullable=True),
        sa.Column("entry_order_id", sa.BigInteger(), nullable=False),
        sa.Column("sl_order_id", sa.BigInteger(), nullable=True),
        sa.Column("tp_order_id", sa.BigInteger(), nullable=True),
        sa.Column("exchange", sa.Text(), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("side", sa.Text(), nullable=False),
        sa.Column("entry_price", sa.Numeric(precision=28, scale=10), nullable=False),
        sa.Column("size", sa.Numeric(precision=28, scale=10), nullable=False),
        sa.Column("leverage", sa.Integer(), nullable=False),
        sa.Column("margin_mode", sa.Text(), nullable=True),
        sa.Column("liquidation_price", sa.Numeric(precision=28, scale=10), nullable=True),
        sa.Column("tp_price_initial", sa.Numeric(precision=28, scale=10), nullable=True),
        sa.Column("sl_price_initial", sa.Numeric(precision=28, scale=10), nullable=True),
        sa.Column("sl_price_current", sa.Numeric(precision=28, scale=10), nullable=True),
        sa.Column("sl_moved_to_be_at", sa.DateTime(timezone=True), nullable=True),
        # A13: sl_strategy for future trailing SL
        sa.Column("sl_strategy", sa.Text(), nullable=False, server_default="fixed"),
        # Status values per spec §4.5:
        # 'open_pending_tp_sl' | 'open' | 'closed_tp' | 'closed_sl' |
        # 'closed_breakeven' | 'closed_manual' | 'liquidated'
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column(
            "opened_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.Column("closed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("close_price", sa.Numeric(precision=28, scale=10), nullable=True),
        sa.Column("realized_pnl_usd", sa.Numeric(precision=28, scale=10), nullable=True),
        sa.Column("fees_paid_usd", sa.Numeric(precision=28, scale=10), nullable=True),
        sa.Column("funding_paid_usd", sa.Numeric(precision=28, scale=10), nullable=True),
        sa.Column("close_reason", sa.Text(), nullable=True),
        sa.Column(
            "last_synced_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.ForeignKeyConstraint(["entry_order_id"], ["orders.id"]),
        sa.ForeignKeyConstraint(["signal_id"], ["signals.id"]),
        sa.ForeignKeyConstraint(["sl_order_id"], ["orders.id"]),
        sa.ForeignKeyConstraint(["tp_order_id"], ["orders.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("idx_positions_status", "positions", ["status"])
    # Partial indexes per spec §4.5
    op.create_index(
        "idx_positions_pending_tp_sl",
        "positions",
        ["exchange", "symbol"],
        postgresql_where=sa.text("status = 'open_pending_tp_sl'"),
    )
    op.create_index(
        "idx_positions_be_pending",
        "positions",
        ["exchange", "symbol"],
        postgresql_where=sa.text("status = 'open' AND sl_moved_to_be_at IS NULL"),
    )

    # -----------------------------------------------------------------
    # trades
    # -----------------------------------------------------------------
    op.create_table(
        "trades",
        sa.Column("id", sa.BigInteger(), nullable=False, autoincrement=True),
        sa.Column("position_id", sa.BigInteger(), nullable=False),
        sa.Column("exchange", sa.Text(), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("side", sa.Text(), nullable=False),
        sa.Column("entry_price", sa.Numeric(precision=28, scale=10), nullable=False),
        sa.Column("exit_price", sa.Numeric(precision=28, scale=10), nullable=False),
        sa.Column("size", sa.Numeric(precision=28, scale=10), nullable=False),
        sa.Column("leverage", sa.Integer(), nullable=False),
        sa.Column("realized_pnl_usd", sa.Numeric(precision=28, scale=10), nullable=True),
        sa.Column("fees_paid_usd", sa.Numeric(precision=28, scale=10), nullable=True),
        sa.Column("funding_paid_usd", sa.Numeric(precision=28, scale=10), nullable=True),
        sa.Column("close_reason", sa.Text(), nullable=True),
        sa.Column("opened_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "closed_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.ForeignKeyConstraint(["position_id"], ["positions.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("idx_trades_closed_at", "trades", ["closed_at"])
    op.create_index("idx_trades_position_id", "trades", ["position_id"])

    # -----------------------------------------------------------------
    # events
    # -----------------------------------------------------------------
    op.create_table(
        "events",
        sa.Column("id", sa.BigInteger(), nullable=False, autoincrement=True),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("level", sa.Text(), nullable=False, server_default="INFO"),
        sa.Column("signal_id", sa.BigInteger(), nullable=True),
        sa.Column("order_id", sa.BigInteger(), nullable=True),
        sa.Column("position_id", sa.BigInteger(), nullable=True),
        sa.Column("exchange", sa.Text(), nullable=True),
        sa.Column("symbol", sa.Text(), nullable=True),
        sa.Column("message", sa.Text(), nullable=True),
        sa.Column("data_json", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("idx_events_type", "events", ["event_type"])
    op.create_index("idx_events_created_at", "events", ["created_at"])

    # -----------------------------------------------------------------
    # vooi_api_calls
    # -----------------------------------------------------------------
    op.create_table(
        "vooi_api_calls",
        sa.Column("id", sa.BigInteger(), nullable=False, autoincrement=True),
        sa.Column("correlation_id", sa.Text(), nullable=False),
        sa.Column("method", sa.Text(), nullable=False),
        sa.Column("path", sa.Text(), nullable=False),
        sa.Column("request_body", sa.Text(), nullable=True),   # Authorization redacted
        sa.Column("response_status", sa.Integer(), nullable=True),
        sa.Column("response_body", sa.Text(), nullable=True),
        sa.Column("duration_ms", sa.Integer(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("idx_vooi_api_calls_created_at", "vooi_api_calls", ["created_at"])
    op.create_index(
        "idx_vooi_api_calls_correlation_id", "vooi_api_calls", ["correlation_id"]
    )

    # -----------------------------------------------------------------
    # vooi_errors
    # -----------------------------------------------------------------
    op.create_table(
        "vooi_errors",
        sa.Column("id", sa.BigInteger(), nullable=False, autoincrement=True),
        sa.Column("correlation_id", sa.Text(), nullable=False),
        sa.Column("method", sa.Text(), nullable=False),
        sa.Column("path", sa.Text(), nullable=False),
        sa.Column("request_body", sa.Text(), nullable=True),
        sa.Column("error_kind", sa.Text(), nullable=False),
        # error_kind: 'http_4xx' | 'http_5xx' | 'timeout' | 'connect_failed' | 'ssl' | 'parse'
        sa.Column("error_detail", sa.Text(), nullable=True),
        sa.Column("response_status", sa.Integer(), nullable=True),
        sa.Column("response_body", sa.Text(), nullable=True),
        sa.Column("duration_ms", sa.Integer(), nullable=True),
        sa.Column("reported", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("idx_vooi_errors_created_at", "vooi_errors", ["created_at"])
    op.create_index("idx_vooi_errors_error_kind", "vooi_errors", ["error_kind"])
    op.create_index("idx_vooi_errors_reported", "vooi_errors", ["reported"])

    # -----------------------------------------------------------------
    # runtime_state  (key-value store)
    # -----------------------------------------------------------------
    op.create_table(
        "runtime_state",
        sa.Column("key", sa.Text(), nullable=False),
        sa.Column("value", sa.Text(), nullable=True),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.PrimaryKeyConstraint("key"),
    )


def downgrade() -> None:
    op.drop_table("runtime_state")
    op.drop_table("vooi_errors")
    op.drop_table("vooi_api_calls")
    op.drop_table("events")
    op.drop_table("trades")
    op.drop_index("idx_positions_be_pending", table_name="positions")
    op.drop_index("idx_positions_pending_tp_sl", table_name="positions")
    op.drop_index("idx_positions_status", table_name="positions")
    op.drop_table("positions")
    op.drop_index("idx_orders_vooi_order_id", table_name="orders")
    op.drop_index("idx_orders_signal_id", table_name="orders")
    op.drop_index("idx_orders_exchange_symbol", table_name="orders")
    op.drop_index("idx_orders_status", table_name="orders")
    op.drop_table("orders")
    op.drop_index("idx_signals_parsed_at", table_name="signals")
    op.drop_index("idx_signals_symbol_side", table_name="signals")
    op.drop_table("signals")
    op.drop_index("idx_messages_channel_tg_id", table_name="messages")
    op.drop_table("messages")
    op.drop_table("channels")
