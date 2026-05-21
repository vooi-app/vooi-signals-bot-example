"""
Post-fill TP + SL placement.
Per spec §8.7 v1.5.

Triggered by the SSE listener when an entry order transitions to status='filled'.
Places SL (reduce-only, trigger=sl) and TP (reduce-only, trigger=tp) as two
separate orders. Updates the position to status='open' on success.

Aster cannot accept TP/SL inline with the entry order ("Aster exchange does
not support bracket orders"); the post-fill approach works uniformly across
hyperliquid / lighter / aster.
"""
import asyncio
import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

import structlog
from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from bot.alerts import send_naked_position_alert
from bot.config import settings
from bot.db import session_scope
from bot.models import Order, Position, Signal
from bot.orders import (
    make_client_order_id,
    place_trigger_with_verification,
)
from bot.resolver import (
    MarketDecimalsUnavailable,
    get_price_decimals,
    get_size_decimals,
)
from bot.streamer import emit_event
from bot.tp_calculator import (
    compute_sl_price_from_pct,
    compute_tp_price_with_fallback,
    opposite_side,
    round_price,
    round_size,
)
from bot.vooi_client import get_vooi_client

log = structlog.get_logger(__name__)

# Per-position lock to make on_entry_filled idempotent under concurrent SSE
# frames + reconciler reruns.
_position_locks: dict[int, asyncio.Lock] = {}


def _lock_for(position_id: int) -> asyncio.Lock:
    lock = _position_locks.get(position_id)
    if lock is None:
        lock = asyncio.Lock()
        _position_locks[position_id] = lock
    return lock


async def on_entry_filled(entry_order_id: int, avg_entry_price: Optional[Decimal] = None) -> None:
    """
    Entry order just transitioned to FILLED. Place SL + TP for the linked
    position (which must be in status='open_pending_tp_sl').
    """
    lock = _lock_for(entry_order_id)
    async with lock:
        try:
            await _place_tp_sl(entry_order_id, avg_entry_price)
        except Exception as e:
            log.error(
                "post_fill_placer_error",
                entry_order_id=entry_order_id,
                error=str(e),
            )


async def _ensure_position_for_fill(
    session: AsyncSession,
    entry_order: Order,
    avg_entry_price: Optional[Decimal],
) -> Optional[Position]:
    """
    Create the positions row for a just-filled entry order, or return the
    existing one. Per the new lifecycle (spec §8.5 v1.5 / 2026-05-13 round 2)
    positions are created at fill time, not at entry-limit placement.
    """
    pos_result = await session.execute(
        select(Position).where(Position.entry_order_id == entry_order.id)
    )
    position = pos_result.scalar_one_or_none()
    if position is not None:
        return position

    # Pull initial TP/SL targets from the signal — these were computed by
    # place_entry_order and need to be replayed against the confirmed fill.
    tp_initial: Optional[Decimal] = None
    sl_initial: Optional[Decimal] = None
    signal_row: Optional[Signal] = None
    if entry_order.signal_id is not None:
        sig_result = await session.execute(
            select(Signal).where(Signal.id == entry_order.signal_id)
        )
        signal_row = sig_result.scalar_one_or_none()

    entry_price = (
        avg_entry_price
        if (avg_entry_price is not None and avg_entry_price > 0)
        else entry_order.price
    )
    size = entry_order.filled_size or entry_order.size

    side = entry_order.side
    leverage = entry_order.leverage or settings.default_leverage

    # Recompute targets relative to the confirmed entry. SL prefers the signal's
    # explicit stop_loss; TP always uses the configured calculator.
    tp_initial = compute_tp_price_with_fallback(
        avg_entry_price=entry_price,
        side=side,
        leverage=leverage,
        exchange=entry_order.exchange,
    )
    if (
        settings.use_signal_sl
        and signal_row is not None
        and signal_row.stop_loss is not None
    ):
        sl_initial = Decimal(str(signal_row.stop_loss))
    else:
        sl_initial = compute_sl_price_from_pct(entry_price, side, leverage)

    now = datetime.now(timezone.utc)
    position = Position(
        signal_id=entry_order.signal_id,
        entry_order_id=entry_order.id,
        exchange=entry_order.exchange,
        symbol=entry_order.symbol,
        side=side,
        entry_price=entry_price,
        size=size,
        leverage=leverage,
        margin_mode=settings.default_margin_mode,
        status="open_pending_tp_sl",
        status_updated_at=now,
        opened_at=now,
        tp_price_initial=tp_initial,
        sl_price_initial=sl_initial,
        sl_price_current=sl_initial,
    )
    session.add(position)
    await session.flush()

    log.info(
        "position_created_on_fill",
        position_id=position.id,
        entry_order_id=entry_order.id,
        exchange=position.exchange,
        symbol=position.symbol,
        entry_price=str(entry_price),
        size=str(size),
    )
    return position


async def _place_tp_sl(entry_order_id: int, avg_entry_price: Optional[Decimal]) -> None:
    async with session_scope() as session:
        order_result = await session.execute(
            select(Order).where(Order.id == entry_order_id)
        )
        entry_order = order_result.scalar_one_or_none()
        if entry_order is None:
            log.warning("post_fill_no_entry_order", entry_order_id=entry_order_id)
            return

        position = await _ensure_position_for_fill(session, entry_order, avg_entry_price)
        if position is None:
            return

        if position.status != "open_pending_tp_sl":
            # Already handled (post-fill placer ran, breakeven moved SL, or
            # position closed before we got here). Idempotency guard.
            log.debug(
                "post_fill_skip_position_status",
                position_id=position.id,
                status=position.status,
            )
            return

        # Update entry_price to confirmed fill price if SSE supplied it.
        if avg_entry_price is not None and avg_entry_price > 0:
            position.entry_price = avg_entry_price

        try:
            price_decimals = await get_price_decimals(position.symbol, position.exchange)
            size_decimals = await get_size_decimals(position.symbol, position.exchange)
        except MarketDecimalsUnavailable as e:
            log.error(
                "post_fill_decimals_unavailable",
                position_id=position.id,
                error=str(e),
            )
            # Without decimals we cannot safely place TP/SL. Flip status to
            # 'open' so sl_safety_check fires ERROR_NAKED_POSITION.
            position.status = "open"
            position.status_updated_at = datetime.now(timezone.utc)
            await session.flush()
            await send_naked_position_alert(position.id, position.symbol, position.exchange)
            return

        # (Re)compute targets relative to the confirmed entry price.
        side = position.side
        exit_side = opposite_side(side)
        leverage = position.leverage

        # Reuse already-stored initial targets if they exist (computed at
        # entry placement time relative to provisional entry); otherwise
        # recompute against confirmed avgEntryPrice.
        if position.tp_price_initial is not None:
            tp_price = position.tp_price_initial
        else:
            tp_price = compute_tp_price_with_fallback(
                avg_entry_price=position.entry_price,
                side=side,
                leverage=leverage,
                exchange=position.exchange,
            )
        if position.sl_price_initial is not None:
            sl_price = position.sl_price_initial
        else:
            sl_price = compute_sl_price_from_pct(position.entry_price, side, leverage)

        tp_price_rounded = round_price(tp_price, price_decimals, exit_side)
        sl_price_rounded = round_price(sl_price, price_decimals, exit_side)
        size_rounded = round_size(position.size, size_decimals)

        client = get_vooi_client()

        # ---------- SL ----------
        sl_client_oid = make_client_order_id(
            position.signal_id or 0, position.exchange, suffix="sl"
        )
        sl_order = Order(
            signal_id=position.signal_id,
            client_order_id=sl_client_oid,
            order_type="stopLoss",
            exchange=position.exchange,
            symbol=position.symbol,
            side=exit_side,
            status="submitting",
            trigger_price=sl_price_rounded,
            size=size_rounded,
            reduce_only=True,
        )
        session.add(sl_order)
        await session.flush()

        sl_placed = await place_trigger_with_verification(
            client=client,
            position=position,
            order_row=sl_order,
            trigger_type="sl",
            trigger_price=sl_price_rounded,
            size=size_rounded,
            client_order_id=sl_client_oid,
        )

        # ---------- TP ----------
        tp_client_oid = make_client_order_id(
            position.signal_id or 0, position.exchange, suffix="tp"
        )
        tp_order = Order(
            signal_id=position.signal_id,
            client_order_id=tp_client_oid,
            order_type="takeProfit",
            exchange=position.exchange,
            symbol=position.symbol,
            side=exit_side,
            status="submitting",
            trigger_price=tp_price_rounded,
            size=size_rounded,
            reduce_only=True,
        )
        session.add(tp_order)
        await session.flush()

        tp_placed = await place_trigger_with_verification(
            client=client,
            position=position,
            order_row=tp_order,
            trigger_type="tp",
            trigger_price=tp_price_rounded,
            size=size_rounded,
            client_order_id=tp_client_oid,
        )

        # Wire up position regardless of partial failure — sl_safety_check
        # will detect a missing SL within 30s and fire ERROR_NAKED_POSITION
        # (per spec §8.7 "Failure handling").
        now = datetime.now(timezone.utc)
        if sl_placed:
            position.sl_order_id = sl_order.id
            position.sl_price_initial = sl_price_rounded
            position.sl_price_current = sl_price_rounded
        if tp_placed:
            position.tp_order_id = tp_order.id
            position.tp_price_initial = tp_price_rounded

        position.status = "open"
        position.status_updated_at = now
        position.last_synced_at = now
        await session.flush()

        # Register with the SSE-driven breakeven evaluator. The supervisor
        # reconciles every 10s, so a missed register here is self-healing,
        # but registering inline minimises trigger latency to the next tick.
        try:
            from bot.breakeven_watcher import register_breakeven_trigger
            register_breakeven_trigger(position)
        except Exception as e:
            log.warning(
                "breakeven_register_failed",
                position_id=position.id,
                error=str(e),
            )

        await emit_event(
            "POST_FILL_TP_SL_PLACED",
            position_id=position.id,
            signal_id=position.signal_id,
            exchange=position.exchange,
            symbol=position.symbol,
            message=(
                f"sl={'OK' if sl_placed else 'FAIL'}@{sl_price_rounded} "
                f"tp={'OK' if tp_placed else 'FAIL'}@{tp_price_rounded} "
                f"size={size_rounded}"
            ),
        )

        if not sl_placed:
            log.error(
                "post_fill_sl_failed_NAKED",
                position_id=position.id,
                symbol=position.symbol,
            )
            await emit_event(
                "ERROR_NAKED_POSITION",
                level="ERROR",
                position_id=position.id,
                exchange=position.exchange,
                symbol=position.symbol,
                message=(
                    f"Post-fill SL placement failed for position {position.id} "
                    f"{position.symbol}. Position is naked!"
                ),
            )
            await send_naked_position_alert(
                position.id, position.symbol, position.exchange
            )

        if not tp_placed:
            log.error(
                "post_fill_tp_failed",
                position_id=position.id,
                symbol=position.symbol,
            )
            await emit_event(
                "ERROR_NO_TP",
                level="ERROR",
                position_id=position.id,
                exchange=position.exchange,
                symbol=position.symbol,
                message=(
                    f"Post-fill TP placement failed for position {position.id} "
                    f"{position.symbol}. tp_safety_watchdog will retry every 30s."
                ),
            )


# Trigger placement + verification moved to bot.orders.place_trigger_with_verification
# (shared with breakeven_watcher / lighter_sl_watchdog).
