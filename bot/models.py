"""
SQLAlchemy ORM models — matches DB schema from spec §4 v1.5
All financial values stored as NUMERIC (Python Decimal)
"""
from datetime import datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


# ---------------------------------------------------------------------------
# channels
# ---------------------------------------------------------------------------
class Channel(Base):
    __tablename__ = "channels"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    telegram_id: Mapped[int] = mapped_column(BigInteger, unique=True, nullable=False)
    username: Mapped[Optional[str]] = mapped_column(Text)
    title: Mapped[Optional[str]] = mapped_column(Text)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    added_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    messages: Mapped[list["Message"]] = relationship("Message", back_populates="channel")


# ---------------------------------------------------------------------------
# messages
# ---------------------------------------------------------------------------
class Message(Base):
    __tablename__ = "messages"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    channel_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("channels.id"), nullable=False
    )
    telegram_message_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    raw_text: Mapped[Optional[str]] = mapped_column(Text)
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    processed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    channel: Mapped["Channel"] = relationship("Channel", back_populates="messages")
    signals: Mapped[list["Signal"]] = relationship("Signal", back_populates="message")

    __table_args__ = (
        Index("idx_messages_channel_tg_id", "channel_id", "telegram_message_id", unique=True),
    )


# ---------------------------------------------------------------------------
# signals
# ---------------------------------------------------------------------------
class Signal(Base):
    __tablename__ = "signals"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    message_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("messages.id"), nullable=False
    )
    channel_id: Mapped[int] = mapped_column(BigInteger, nullable=False)

    is_signal: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    symbol: Mapped[Optional[str]] = mapped_column(Text)
    side: Mapped[Optional[str]] = mapped_column(Text)  # 'buy' | 'sell'

    # JSON arrays stored as text (serialized), parsed at read time
    entry_prices_json: Mapped[Optional[str]] = mapped_column("entry_prices", Text)
    take_profits_json: Mapped[Optional[str]] = mapped_column("take_profits", Text)

    stop_loss: Mapped[Optional[Decimal]] = mapped_column(Numeric(precision=28, scale=10))
    leverage: Mapped[Optional[int]] = mapped_column(Integer)
    chain: Mapped[Optional[str]] = mapped_column(Text)

    skip_reason: Mapped[Optional[str]] = mapped_column(Text)
    prompt_version: Mapped[Optional[str]] = mapped_column(Text)

    parsed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    message: Mapped["Message"] = relationship("Message", back_populates="signals")
    orders: Mapped[list["Order"]] = relationship("Order", back_populates="signal")
    positions: Mapped[list["Position"]] = relationship("Position", back_populates="signal")

    __table_args__ = (
        Index("idx_signals_symbol_side", "symbol", "side"),
        Index("idx_signals_parsed_at", "parsed_at"),
    )


# ---------------------------------------------------------------------------
# orders
# ---------------------------------------------------------------------------
class Order(Base):
    __tablename__ = "orders"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    signal_id: Mapped[Optional[int]] = mapped_column(
        BigInteger, ForeignKey("signals.id"), nullable=True
    )

    # Pre-inserted before request is sent (for idempotency / reconciler)
    client_order_id: Mapped[Optional[str]] = mapped_column(Text, unique=True)
    vooi_order_id: Mapped[Optional[str]] = mapped_column(Text, unique=True)

    exchange: Mapped[str] = mapped_column(Text, nullable=False)
    symbol: Mapped[str] = mapped_column(Text, nullable=False)
    side: Mapped[str] = mapped_column(Text, nullable=False)  # 'buy' | 'sell'

    # 'entry' | 'stopLoss' | 'takeProfit'
    order_type: Mapped[str] = mapped_column(Text, nullable=False)

    # 'submitting' | 'pending' | 'open' | 'filled' | 'cancelled' | 'rejected' | 'expired'
    status: Mapped[str] = mapped_column(Text, nullable=False, default="submitting")

    price: Mapped[Optional[Decimal]] = mapped_column(Numeric(precision=28, scale=10))
    trigger_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(precision=28, scale=10))
    size: Mapped[Optional[Decimal]] = mapped_column(Numeric(precision=28, scale=10))
    filled_size: Mapped[Optional[Decimal]] = mapped_column(Numeric(precision=28, scale=10))
    avg_fill_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(precision=28, scale=10))

    reduce_only: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    leverage: Mapped[Optional[int]] = mapped_column(Integer)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )
    filled_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    cancelled_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    # Raw VOOI API response
    raw_response: Mapped[Optional[str]] = mapped_column(Text)

    signal: Mapped[Optional["Signal"]] = relationship("Signal", back_populates="orders")

    __table_args__ = (
        Index("idx_orders_status", "status"),
        Index("idx_orders_exchange_symbol", "exchange", "symbol"),
        Index("idx_orders_signal_id", "signal_id"),
        Index("idx_orders_vooi_order_id", "vooi_order_id"),
    )


# ---------------------------------------------------------------------------
# positions
# ---------------------------------------------------------------------------
class Position(Base):
    __tablename__ = "positions"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    signal_id: Mapped[Optional[int]] = mapped_column(
        BigInteger, ForeignKey("signals.id"), nullable=True
    )
    entry_order_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("orders.id"), nullable=False
    )
    sl_order_id: Mapped[Optional[int]] = mapped_column(
        BigInteger, ForeignKey("orders.id"), nullable=True
    )
    tp_order_id: Mapped[Optional[int]] = mapped_column(
        BigInteger, ForeignKey("orders.id"), nullable=True
    )

    exchange: Mapped[str] = mapped_column(Text, nullable=False)
    symbol: Mapped[str] = mapped_column(Text, nullable=False)
    side: Mapped[str] = mapped_column(Text, nullable=False)  # 'buy' | 'sell'

    # Provisional on creation; updated to avgEntryPrice on SSE fill
    entry_price: Mapped[Decimal] = mapped_column(Numeric(precision=28, scale=10), nullable=False)
    size: Mapped[Decimal] = mapped_column(Numeric(precision=28, scale=10), nullable=False)
    leverage: Mapped[int] = mapped_column(Integer, nullable=False)
    margin_mode: Mapped[Optional[str]] = mapped_column(Text)
    liquidation_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(precision=28, scale=10))

    tp_price_initial: Mapped[Optional[Decimal]] = mapped_column(Numeric(precision=28, scale=10))
    sl_price_initial: Mapped[Optional[Decimal]] = mapped_column(Numeric(precision=28, scale=10))
    sl_price_current: Mapped[Optional[Decimal]] = mapped_column(Numeric(precision=28, scale=10))
    sl_moved_to_be_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    # 'fixed' | future: 'trailing'
    sl_strategy: Mapped[str] = mapped_column(Text, nullable=False, default="fixed")

    # Status values:
    # 'submitting'          entry order being submitted (transient)
    # 'open'                entry posted with atomic SL+TP; live
    # 'closed_tp'           TP hit
    # 'closed_sl'           SL hit (original SL)
    # 'closed_breakeven'    BE SL hit
    # 'closed_manual'       operator closed manually
    # 'liquidated'
    status: Mapped[str] = mapped_column(Text, nullable=False)

    opened_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    closed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    close_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(precision=28, scale=10))
    realized_pnl_usd: Mapped[Optional[Decimal]] = mapped_column(Numeric(precision=28, scale=10))
    fees_paid_usd: Mapped[Optional[Decimal]] = mapped_column(Numeric(precision=28, scale=10))
    funding_paid_usd: Mapped[Optional[Decimal]] = mapped_column(Numeric(precision=28, scale=10))
    close_reason: Mapped[Optional[str]] = mapped_column(Text)

    last_synced_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    # Tracks when status last changed.
    status_updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    signal: Mapped[Optional["Signal"]] = relationship("Signal", back_populates="positions")
    entry_order: Mapped["Order"] = relationship("Order", foreign_keys=[entry_order_id])
    sl_order: Mapped[Optional["Order"]] = relationship("Order", foreign_keys=[sl_order_id])
    tp_order: Mapped[Optional["Order"]] = relationship("Order", foreign_keys=[tp_order_id])

    __table_args__ = (
        Index("idx_positions_status", "status"),
        Index(
            "idx_positions_pending_tp_sl",
            "exchange",
            "symbol",
            postgresql_where="status = 'open_pending_tp_sl'",
        ),
        Index(
            "idx_positions_be_pending",
            "exchange",
            "symbol",
            postgresql_where="status = 'open' AND sl_moved_to_be_at IS NULL",
        ),
    )


# ---------------------------------------------------------------------------
# trades  (closed trade summaries)
# ---------------------------------------------------------------------------
class Trade(Base):
    __tablename__ = "trades"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    position_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("positions.id"), nullable=False
    )
    exchange: Mapped[str] = mapped_column(Text, nullable=False)
    symbol: Mapped[str] = mapped_column(Text, nullable=False)
    side: Mapped[str] = mapped_column(Text, nullable=False)
    entry_price: Mapped[Decimal] = mapped_column(Numeric(precision=28, scale=10), nullable=False)
    exit_price: Mapped[Decimal] = mapped_column(Numeric(precision=28, scale=10), nullable=False)
    size: Mapped[Decimal] = mapped_column(Numeric(precision=28, scale=10), nullable=False)
    leverage: Mapped[int] = mapped_column(Integer, nullable=False)
    realized_pnl_usd: Mapped[Optional[Decimal]] = mapped_column(Numeric(precision=28, scale=10))
    fees_paid_usd: Mapped[Optional[Decimal]] = mapped_column(Numeric(precision=28, scale=10))
    funding_paid_usd: Mapped[Optional[Decimal]] = mapped_column(Numeric(precision=28, scale=10))
    close_reason: Mapped[Optional[str]] = mapped_column(Text)
    opened_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    closed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        Index("idx_trades_closed_at", "closed_at"),
        Index("idx_trades_position_id", "position_id"),
    )


# ---------------------------------------------------------------------------
# events  (structured event log for console + reports)
# ---------------------------------------------------------------------------
class Event(Base):
    __tablename__ = "events"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    level: Mapped[str] = mapped_column(Text, nullable=False, default="INFO")
    signal_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    order_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    position_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    exchange: Mapped[Optional[str]] = mapped_column(Text)
    symbol: Mapped[Optional[str]] = mapped_column(Text)
    message: Mapped[Optional[str]] = mapped_column(Text)
    data_json: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        Index("idx_events_type", "event_type"),
        Index("idx_events_created_at", "created_at"),
    )


# ---------------------------------------------------------------------------
# vooi_api_calls  (full audit log of every VOOI API call)
# ---------------------------------------------------------------------------
class VooiApiCall(Base):
    __tablename__ = "vooi_api_calls"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    correlation_id: Mapped[str] = mapped_column(Text, nullable=False)
    method: Mapped[str] = mapped_column(Text, nullable=False)
    path: Mapped[str] = mapped_column(Text, nullable=False)
    request_body: Mapped[Optional[str]] = mapped_column(Text)  # Authorization redacted
    response_status: Mapped[Optional[int]] = mapped_column(Integer)
    response_body: Mapped[Optional[str]] = mapped_column(Text)
    duration_ms: Mapped[Optional[int]] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        Index("idx_vooi_api_calls_created_at", "created_at"),
        Index("idx_vooi_api_calls_correlation_id", "correlation_id"),
    )


# ---------------------------------------------------------------------------
# vooi_errors  (errors from VOOI API calls, separate table for easy querying)
# ---------------------------------------------------------------------------
class VooiError(Base):
    __tablename__ = "vooi_errors"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    correlation_id: Mapped[str] = mapped_column(Text, nullable=False)
    method: Mapped[str] = mapped_column(Text, nullable=False)
    path: Mapped[str] = mapped_column(Text, nullable=False)
    request_body: Mapped[Optional[str]] = mapped_column(Text)  # Authorization redacted
    error_kind: Mapped[str] = mapped_column(Text, nullable=False)
    # error_kind values: 'http_4xx' | 'http_5xx' | 'timeout' | 'connect_failed' | 'ssl' | 'parse'
    error_detail: Mapped[Optional[str]] = mapped_column(Text)
    response_status: Mapped[Optional[int]] = mapped_column(Integer)
    response_body: Mapped[Optional[str]] = mapped_column(Text)
    duration_ms: Mapped[Optional[int]] = mapped_column(Integer)
    reported: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        Index("idx_vooi_errors_created_at", "created_at"),
        Index("idx_vooi_errors_error_kind", "error_kind"),
        Index("idx_vooi_errors_reported", "reported"),
    )


# ---------------------------------------------------------------------------
# runtime_state  (key-value store for bot state across restarts)
# ---------------------------------------------------------------------------
class RuntimeState(Base):
    __tablename__ = "runtime_state"

    key: Mapped[str] = mapped_column(Text, primary_key=True)
    value: Mapped[Optional[str]] = mapped_column(Text)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )
