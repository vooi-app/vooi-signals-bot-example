"""
SSE (Server-Sent Events) listener for VOOI /exchange/updates.
Maintains in-memory price cache and dispatches fill/position/price events.
Per spec §2, §5 (sse_listener task).
"""
import asyncio
import json
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Optional

import httpx
import structlog
from httpx_sse import aconnect_sse

from bot.config import settings
from bot.db import session_scope
from bot.models import Order, Position
from bot.streamer import emit_event

log = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Global price cache — accessed by breakeven_watcher
# ---------------------------------------------------------------------------
# price_cache[(exchange, symbol)] = Decimal price
price_cache: dict[tuple[str, str], Decimal] = {}

# Timestamps for staleness checks
# price_cache_updated_at[(exchange, symbol)] = time.monotonic()
price_cache_updated_at: dict[tuple[str, str], float] = {}

# Last SSE event time — used by reconciler to detect SSE silence
sse_last_event_at: float = 0.0


async def sse_listener_task() -> None:
    """
    Main SSE listener loop.
    Connects to GET /exchange/updates and dispatches frames.
    Auto-reconnects with exponential backoff on failure.
    """
    backoff = settings.sse_reconnect_backoff_sec
    max_backoff = 60

    while True:
        try:
            log.info("sse_connecting")
            await _run_sse_connection()
            backoff = settings.sse_reconnect_backoff_sec  # reset on clean close
        except asyncio.CancelledError:
            log.info("sse_listener_cancelled")
            break
        except Exception as e:
            log.error("sse_connection_error", error=str(e), reconnect_in=backoff)
            await emit_event(
                "SSE_RECONNECT",
                level="WARN",
                message=f"SSE disconnected: {str(e)[:100]}. Reconnecting in {backoff}s",
            )
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)


async def _run_sse_connection() -> None:
    """Establish and process one SSE connection."""
    global sse_last_event_at

    async with httpx.AsyncClient(
        base_url=settings.vooi_api_base_url,
        headers={
            "Authorization": f"Bearer {settings.vooi_api_key}",
            "Accept": "text/event-stream",
        },
        timeout=httpx.Timeout(None, connect=10.0),
    ) as client:
        async with aconnect_sse(client, "GET", "/exchange/updates") as event_source:
            # Reset silence counter on successful connect — VOOI's stream is
            # idle most of the time (only emits on account events), so we
            # must not let the previous run's stale timestamp keep firing
            # `reconciler_sse_silent` warnings forever.
            sse_last_event_at = time.monotonic()
            log.info("sse_connected")
            async for sse in event_source.aiter_sse():
                sse_last_event_at = time.monotonic()
                if sse.data:
                    try:
                        payload = json.loads(sse.data)
                        frames = payload if isinstance(payload, list) else [payload]
                        for frame in frames:
                            if isinstance(frame, dict):
                                await dispatch_frame(frame)
                    except json.JSONDecodeError:
                        log.debug("sse_non_json_frame", data=sse.data[:100])
                    except Exception as e:
                        log.error("sse_frame_dispatch_error", error=str(e))


async def dispatch_frame(frame: dict[str, Any]) -> None:
    """Route SSE frame to appropriate handler by type."""
    frame_type = frame.get("type") or frame.get("event") or ""

    if frame_type in ("order", "ORDER"):
        await on_order_frame(frame)
    elif frame_type in ("position", "POSITION"):
        await on_position_frame(frame)
    elif frame_type in ("marketPrice", "MARKET_PRICE", "price", "PRICE"):
        await on_market_price_frame(frame)
    elif frame_type in ("account", "ACCOUNT"):
        await on_account_frame(frame)
    else:
        log.debug("sse_unknown_frame_type", frame_type=frame_type)


async def on_order_frame(frame: dict[str, Any]) -> None:
    """Handle order update frame — update orders table."""
    order_data = frame.get("data") or frame

    vooi_order_id = str(order_data.get("orderId") or order_data.get("id") or "")
    client_order_id = str(order_data.get("clientOrderId") or "")
    status = str(order_data.get("status") or "")
    avg_price = order_data.get("avgPrice") or order_data.get("avgFillPrice")
    filled_size = order_data.get("filledSize") or order_data.get("filledQty")
    exchange = str(order_data.get("exchange") or "")
    symbol = str(order_data.get("asset") or order_data.get("symbol") or "")

    if not vooi_order_id and not client_order_id:
        return

    try:
        async with session_scope() as session:
            from sqlalchemy import select

            # НОВЫЙ-06: prioritized lookup. The previous OR query could match two
            # different rows (one by vooi_order_id, one by client_order_id) and
            # blow up scalar_one_or_none() with MultipleResultsFound, killing
            # the SSE dispatch loop. Look up by vooi_order_id first (canonical),
            # fall back to client_order_id only if no row found.
            order = None
            if vooi_order_id:
                r = await session.execute(
                    select(Order).where(Order.vooi_order_id == vooi_order_id)
                )
                order = r.scalar_one_or_none()
            if order is None and client_order_id:
                r = await session.execute(
                    select(Order).where(Order.client_order_id == client_order_id)
                )
                order = r.scalar_one_or_none()

            entry_just_filled = False
            entry_order_id_local: Optional[int] = None
            avg_fill_for_post_fill: Optional[Decimal] = None

            if order:
                # Map VOOI status to internal status
                internal_status = _map_order_status(status)
                prev_status = order.status
                order.status = internal_status
                order.updated_at = datetime.now(timezone.utc)

                if avg_price:
                    order.avg_fill_price = Decimal(str(avg_price))
                if filled_size:
                    order.filled_size = Decimal(str(filled_size))

                if internal_status == "filled":
                    order.filled_at = datetime.now(timezone.utc)
                    # Trigger post-fill TP+SL placement once per entry order
                    # (spec §8.7). Guard on prev_status so reconnect replays
                    # don't re-fire — and the placer itself is idempotent on
                    # position.status == 'open_pending_tp_sl'.
                    if order.order_type == "entry" and prev_status != "filled":
                        entry_just_filled = True
                        entry_order_id_local = order.id
                        if order.avg_fill_price is not None:
                            avg_fill_for_post_fill = order.avg_fill_price

                elif internal_status in ("cancelled", "expired"):
                    order.cancelled_at = datetime.now(timezone.utc)

        # Fire post-fill placement OUTSIDE the session_scope so the entry
        # status change is committed before TP/SL inserts happen.
        if entry_just_filled and entry_order_id_local is not None:
            from bot.post_fill_placer import on_entry_filled
            await on_entry_filled(entry_order_id_local, avg_fill_for_post_fill)
    except Exception as e:
        log.error("sse_order_frame_error", error=str(e))


async def on_position_frame(frame: dict[str, Any]) -> None:
    """Handle position update frame — sync positions table."""
    pos_data = frame.get("data") or frame

    exchange = str(pos_data.get("exchange") or "")
    symbol = str(pos_data.get("asset") or pos_data.get("symbol") or "")
    size = pos_data.get("size") or pos_data.get("qty")
    entry_price = pos_data.get("entryPrice") or pos_data.get("avgEntryPrice")
    liquidation_price = pos_data.get("liquidationPrice")
    unrealized_pnl = pos_data.get("unrealizedPnl")
    realized_pnl = pos_data.get("realizedPnl")

    if not exchange or not symbol:
        return

    try:
        size_decimal = Decimal(str(size)) if size is not None else Decimal("0")
        close = abs(size_decimal) == 0

        async with session_scope() as session:
            from sqlalchemy import select

            from sqlalchemy import and_
            result = await session.execute(
                select(Position).where(
                    and_(
                        Position.exchange == exchange,
                        Position.symbol == symbol,
                        Position.status.in_(["open", "open_pending_tp_sl"]),
                    )
                ).limit(1)
            )
            position = result.scalar_one_or_none()

            if position:
                if entry_price:
                    position.entry_price = Decimal(str(entry_price))
                if liquidation_price:
                    position.liquidation_price = Decimal(str(liquidation_price))
                position.last_synced_at = datetime.now(timezone.utc)

                if close:
                    await _handle_position_close(session, position, pos_data)

    except Exception as e:
        log.error("sse_position_frame_error", error=str(e))


async def on_market_price_frame(frame: dict[str, Any]) -> None:
    """Handle market price frame — update price cache and fire BE evaluator."""
    price_data = frame.get("data") or frame

    exchange = str(price_data.get("exchange") or "")
    symbol = str(price_data.get("asset") or price_data.get("symbol") or "")
    price = price_data.get("price") or price_data.get("markPrice") or price_data.get("indexPrice")

    if not (exchange and symbol and price is not None):
        return

    key = (exchange.lower(), symbol.upper())
    price_decimal = Decimal(str(price))
    price_cache[key] = price_decimal
    price_cache_updated_at[key] = time.monotonic()
    log.debug("price_cache_updated", exchange=exchange, symbol=symbol, price=price)

    # Event-driven breakeven: feed the new price into the evaluator. Late
    # import avoids a circular dependency at module load time.
    try:
        from bot.breakeven_watcher import evaluate_breakeven_trigger
        await evaluate_breakeven_trigger(exchange, symbol, price_decimal)
    except Exception as e:
        log.error("breakeven_evaluate_failed", exchange=exchange, symbol=symbol, error=str(e))


async def on_account_frame(frame: dict[str, Any]) -> None:
    """Handle account update — persist equity for DD calculation."""
    account_data = frame.get("data") or frame

    equity = account_data.get("equity") or account_data.get("totalEquity")
    if equity is not None:
        try:
            async with session_scope() as session:
                from sqlalchemy import select
                result = await session.execute(
                    select(RuntimeState).where(RuntimeState.key == "account_equity_usd")
                )
                from bot.models import RuntimeState
                state = result.scalar_one_or_none()
                if state:
                    state.value = str(equity)
                else:
                    session.add(RuntimeState(key="account_equity_usd", value=str(equity)))
        except Exception as e:
            log.warning("account_equity_persist_failed", error=str(e))


async def _handle_position_close(
    session,
    position: Position,
    pos_data: dict[str, Any],
) -> None:
    """Classify and record position close."""
    close_price = pos_data.get("closePrice") or pos_data.get("exitPrice") or pos_data.get("markPrice")
    realized_pnl = pos_data.get("realizedPnl") or pos_data.get("pnl")
    fees = pos_data.get("fees") or pos_data.get("feesPaid")
    funding = pos_data.get("funding") or pos_data.get("fundingPaid")

    close_reason = await classify_close(position, pos_data)

    position.status = close_reason
    position.closed_at = datetime.now(timezone.utc)
    if close_price:
        position.close_price = Decimal(str(close_price))
    if realized_pnl:
        position.realized_pnl_usd = Decimal(str(realized_pnl))
    if fees:
        position.fees_paid_usd = Decimal(str(fees))
    if funding:
        position.funding_paid_usd = Decimal(str(funding))
    position.close_reason = close_reason

    log.info(
        "position_closed",
        position_id=position.id,
        symbol=position.symbol,
        close_reason=close_reason,
        close_price=close_price,
        realized_pnl=realized_pnl,
    )

    await emit_event(
        "POSITION_CLOSED",
        position_id=position.id,
        exchange=position.exchange,
        symbol=position.symbol,
        message=f"{position.side} {close_reason} price={close_price} pnl={realized_pnl}",
    )

    # Drop any pending breakeven trigger so the SSE evaluator doesn't try to
    # fire a BE move on a closed position. Late import avoids a cycle.
    try:
        from bot.breakeven_watcher import unregister_breakeven_trigger
        unregister_breakeven_trigger(position.id)
    except Exception as e:
        log.debug("breakeven_unregister_failed", position_id=position.id, error=str(e))

    # Update DD state after close
    from bot.router import update_dd_state
    await update_dd_state(session)


async def classify_close(position: Position, pos_data: dict[str, Any]) -> str:
    """
    Classify position close reason.
    Returns one of: closed_tp, closed_sl, closed_breakeven, closed_manual, liquidated
    """
    close_type = str(pos_data.get("closeType") or pos_data.get("reason") or "")

    # Check explicit close type from exchange
    close_type_lower = close_type.lower()

    # НОВЫЙ-10: precise matching. The previous `"sl" in close_type_lower` matched
    # words like "wholesale" / "false". Use a tokenized match against canonical
    # strings, with a substring fallback only for the descriptive forms.
    _TP_TOKENS = {"tp", "take_profit", "take-profit", "takeprofit"}
    _SL_TOKENS = {"sl", "stop_loss", "stop-loss", "stoploss"}

    if "liquidat" in close_type_lower:
        return "liquidated"
    if close_type_lower in _TP_TOKENS or "take_profit" in close_type_lower or "take-profit" in close_type_lower:
        return "closed_tp"
    if close_type_lower in _SL_TOKENS or "stop_loss" in close_type_lower or "stop-loss" in close_type_lower:
        # Check if SL was moved to breakeven
        if position.sl_moved_to_be_at is not None:
            return "closed_breakeven"
        return "closed_sl"

    # Infer from close price vs TP/SL prices
    close_price_raw = pos_data.get("closePrice") or pos_data.get("exitPrice")
    if close_price_raw and position.tp_price_initial and position.sl_price_initial:
        close_price = Decimal(str(close_price_raw))
        tp = position.tp_price_initial
        sl = position.sl_price_current or position.sl_price_initial

        # For long: TP is above entry, SL is below
        if position.side == "buy":
            if close_price >= tp * Decimal("0.999"):
                return "closed_tp"
            if close_price <= sl * Decimal("1.001"):
                if position.sl_moved_to_be_at is not None:
                    return "closed_breakeven"
                return "closed_sl"
        else:
            if close_price <= tp * Decimal("1.001"):
                return "closed_tp"
            if close_price >= sl * Decimal("0.999"):
                if position.sl_moved_to_be_at is not None:
                    return "closed_breakeven"
                return "closed_sl"

    return "closed_manual"


def _map_order_status(vooi_status: str) -> str:
    """Map VOOI API order status to internal status."""
    mapping = {
        "OPEN": "open",
        "PENDING": "pending",
        "FILLED": "filled",
        "EXECUTED": "filled",
        "CANCELLED": "cancelled",
        "CANCELED": "cancelled",
        "REJECTED": "rejected",
        "EXPIRED": "expired",
        "PARTIALLY_FILLED": "open",
        "CREATED": "pending",
        "ERROR": "rejected",
    }
    return mapping.get(vooi_status.upper(), vooi_status.lower())
