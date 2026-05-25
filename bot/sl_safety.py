"""
SL safety check — runs every 30s to detect naked positions and hung watcher.
Per spec §12, §8.8.5 sl_safety_check additions.

Also hosts lighter_sl_watchdog_task: lighter occasionally drops our SL
trigger orders for reasons we cannot observe from our side (TTL / auto-prune
/ funding events). The watchdog re-places the SL when this happens, with a
per-position rate limit so we don't get banned on a pathological loop.
"""
import asyncio
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional

import structlog
from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from bot.alerts import (
    send_emergency_close_alert,
    send_naked_position_alert,
    send_watcher_hung_alert,
)
from bot.config import settings
from bot.db import session_scope
from bot.models import Order, Position
from bot.orders import (
    TriggerWouldImmediatelyFireError,
    emergency_market_close,
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

_WATCHER_HEARTBEAT_THRESHOLD_SEC = 30
_CHECK_INTERVAL_SEC = 30
_LIGHTER_WATCHDOG_INTERVAL_SEC = 30
_LIGHTER_WATCHDOG_MAX_ATTEMPTS_PER_HOUR = 3
_TP_WATCHDOG_INTERVAL_SEC = 30
_TP_WATCHDOG_MAX_ATTEMPTS_PER_HOUR = 6

# In-memory rate-limit: position_id → list[datetime] of replacement attempts.
# Resets on bot restart; that's acceptable — restart implies fresh slate.
_lighter_replacement_attempts: dict[int, list[datetime]] = {}
_tp_replacement_attempts: dict[int, list[datetime]] = {}


async def sl_safety_check_task() -> None:
    """
    Main SL safety check loop. Runs every 30s.
    Checks:
    1. Each open position has an active SL order
    2. tp_breakeven_watcher heartbeat is fresh (<30s)
    """
    while True:
        try:
            await asyncio.sleep(_CHECK_INTERVAL_SEC)
            await sl_safety_check_run()
        except asyncio.CancelledError:
            log.info("sl_safety_check_cancelled")
            break
        except Exception as e:
            log.error("sl_safety_check_error", error=str(e))


async def sl_safety_check_run() -> None:
    """Single SL safety check pass."""
    async with session_scope() as session:
        # 1. Naked position check
        await check_naked_positions(session)

        # 2. Watcher heartbeat check
        await check_watcher_heartbeat()


async def check_naked_positions(session: AsyncSession) -> None:
    """
    For each open position: verify active SL and TP orders exist.
    SL missing → emit ERROR_NAKED_POSITION (immediate risk).
    TP missing → emit ERROR_NO_TP (no immediate risk, but profit ceiling lost).
    """
    result = await session.execute(
        select(Position).where(Position.status == "open")
    )
    open_positions = result.scalars().all()

    for pos in open_positions:
        # ---- SL ----
        has_active_sl = False
        if pos.sl_order_id:
            sl_result = await session.execute(
                select(Order).where(
                    and_(
                        Order.id == pos.sl_order_id,
                        Order.status.in_(["pending", "open"]),
                    )
                )
            )
            has_active_sl = sl_result.scalar_one_or_none() is not None
        elif pos.sl_price_current is not None:
            # Atomic bracket: SL submitted with entry; no separate orderId yet.
            has_active_sl = True

        if not has_active_sl:
            log.error(
                "ERROR_NAKED_POSITION",
                position_id=pos.id,
                symbol=pos.symbol,
                exchange=pos.exchange,
                sl_order_id=pos.sl_order_id,
            )
            await emit_event(
                "ERROR_NAKED_POSITION",
                level="ERROR",
                position_id=pos.id,
                exchange=pos.exchange,
                symbol=pos.symbol,
                message=(
                    f"Position {pos.id} {pos.symbol} on {pos.exchange} "
                    f"has no active SL order! Immediate action required."
                ),
            )
            await send_naked_position_alert(pos.id, pos.symbol, pos.exchange)

        # ---- TP ----
        # Every open position needs a TP, *including* after the SL has been
        # moved to breakeven. BE-SL only protects the downside; without TP
        # the position has no programmed profit-take and must be closed by
        # hand. tp_safety_watchdog re-places when missing.
        has_active_tp = False
        if pos.tp_order_id:
            tp_result = await session.execute(
                select(Order).where(
                    and_(
                        Order.id == pos.tp_order_id,
                        Order.status.in_(["pending", "open"]),
                    )
                )
            )
            has_active_tp = tp_result.scalar_one_or_none() is not None

        if not has_active_tp:
            log.error(
                "ERROR_NO_TP",
                position_id=pos.id,
                symbol=pos.symbol,
                exchange=pos.exchange,
                tp_order_id=pos.tp_order_id,
            )
            await emit_event(
                "ERROR_NO_TP",
                level="ERROR",
                position_id=pos.id,
                exchange=pos.exchange,
                symbol=pos.symbol,
                message=(
                    f"Position {pos.id} {pos.symbol} on {pos.exchange} "
                    f"has no active TP order. tp_safety_watchdog will retry."
                ),
            )


async def check_watcher_heartbeat() -> None:
    """
    Check tp_breakeven_watcher heartbeat.
    If last tick >30s ago → emit ERROR_WATCHER_HUNG and alert.
    Per spec §12 A10.
    """
    from bot.breakeven_watcher import tp_breakeven_watcher_last_tick

    age_sec = time.time() - tp_breakeven_watcher_last_tick

    if tp_breakeven_watcher_last_tick == 0.0:
        # Watcher hasn't started yet — not an error during startup
        return

    if age_sec > _WATCHER_HEARTBEAT_THRESHOLD_SEC:
        log.error(
            "ERROR_WATCHER_HUNG",
            age_sec=age_sec,
            threshold=_WATCHER_HEARTBEAT_THRESHOLD_SEC,
        )
        await emit_event(
            "ERROR_WATCHER_HUNG",
            level="ERROR",
            message=(
                f"tp_breakeven_watcher heartbeat stale for {age_sec:.0f}s "
                f"(threshold: {_WATCHER_HEARTBEAT_THRESHOLD_SEC}s). "
                f"Asyncio task may have crashed."
            ),
        )
        await send_watcher_hung_alert(age_sec)


# -----------------------------------------------------------------------------
# Lighter SL watchdog
# -----------------------------------------------------------------------------
def _record_lighter_attempt(position_id: int) -> int:
    """Append a replacement attempt timestamp and return count in last hour."""
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=1)
    history = _lighter_replacement_attempts.setdefault(position_id, [])
    history.append(now)
    # Prune older entries
    history[:] = [t for t in history if t > cutoff]
    return len(history)


def _lighter_attempts_in_last_hour(position_id: int) -> int:
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=1)
    history = _lighter_replacement_attempts.get(position_id, [])
    return sum(1 for t in history if t > cutoff)


async def lighter_sl_watchdog_task() -> None:
    """
    Every 30s: for each open lighter position, verify an active stopLoss
    exists on the exchange. If not — re-place it via the verified helper.

    Rate-limited at _LIGHTER_WATCHDOG_MAX_ATTEMPTS_PER_HOUR per position.
    After the cap, fires NAKED alert and stops re-trying (avoids spamming
    a lighter API that may be rejecting for a structural reason).
    """
    while True:
        try:
            await asyncio.sleep(_LIGHTER_WATCHDOG_INTERVAL_SEC)
            await lighter_sl_watchdog_run()
        except asyncio.CancelledError:
            log.info("lighter_sl_watchdog_cancelled")
            break
        except Exception as e:
            log.error("lighter_sl_watchdog_error", error=str(e))


async def lighter_sl_watchdog_run() -> None:
    """Single pass: detect missing SL on lighter, re-place where possible."""
    client = get_vooi_client()

    # Fetch live exchange state for lighter only (cheaper than full reconcile).
    try:
        open_orders = await client.get(
            "/exchange/open-orders", params={"exchanges": "lighter"}
        )
    except Exception as e:
        log.warning("lighter_watchdog_open_orders_failed", error=str(e))
        return

    if not isinstance(open_orders, list):
        return

    # Build set of symbols that currently have an active stopLoss on lighter.
    symbols_with_sl: set[str] = set()
    for o in open_orders:
        if not isinstance(o, dict):
            continue
        if (o.get("type") or "").lower() != "stoploss":
            continue
        sym = (o.get("baseSymbol") or o.get("asset") or "").upper()
        if sym:
            symbols_with_sl.add(sym)

    async with session_scope() as session:
        result = await session.execute(
            select(Position).where(
                and_(
                    Position.exchange == "lighter",
                    Position.status == "open",
                )
            )
        )
        lighter_positions = result.scalars().all()

        for pos in lighter_positions:
            if pos.symbol.upper() in symbols_with_sl:
                continue  # SL is present on exchange — nothing to do

            attempts = _lighter_attempts_in_last_hour(pos.id)
            if attempts >= _LIGHTER_WATCHDOG_MAX_ATTEMPTS_PER_HOUR:
                log.error(
                    "lighter_sl_watchdog_rate_limited",
                    position_id=pos.id,
                    symbol=pos.symbol,
                    attempts=attempts,
                )
                # Surface as NAKED so the operator notices.
                await emit_event(
                    "ERROR_NAKED_POSITION",
                    level="ERROR",
                    position_id=pos.id,
                    exchange=pos.exchange,
                    symbol=pos.symbol,
                    message=(
                        f"Lighter watchdog exhausted {attempts} SL replacements "
                        f"in the last hour for position {pos.id} {pos.symbol}. "
                        f"Manual intervention required."
                    ),
                )
                await send_naked_position_alert(pos.id, pos.symbol, pos.exchange)
                continue

            await _replace_lighter_sl(session, client, pos)


async def _replace_lighter_sl(
    session: AsyncSession,
    client,
    pos: Position,
) -> None:
    """Compose + place a fresh stopLoss for this lighter position."""
    try:
        price_decimals = await get_price_decimals(pos.symbol, pos.exchange)
        size_decimals = await get_size_decimals(pos.symbol, pos.exchange)
    except MarketDecimalsUnavailable as e:
        log.error(
            "lighter_sl_watchdog_decimals_unavailable",
            position_id=pos.id, symbol=pos.symbol, error=str(e),
        )
        return

    sl_price_raw: Optional[Decimal] = pos.sl_price_current or pos.sl_price_initial
    if sl_price_raw is None:
        sl_price_raw = compute_sl_price_from_pct(
            pos.entry_price, pos.side, pos.leverage
        )

    exit_side = opposite_side(pos.side)
    sl_price = round_price(Decimal(str(sl_price_raw)), price_decimals, exit_side)
    size_rounded = round_size(pos.size, size_decimals)

    coid = make_client_order_id(pos.signal_id or 0, pos.exchange, suffix="wdsl")
    new_sl = Order(
        signal_id=pos.signal_id,
        client_order_id=coid,
        order_type="stopLoss",
        exchange=pos.exchange,
        symbol=pos.symbol,
        side=exit_side,
        status="submitting",
        trigger_price=sl_price,
        size=size_rounded,
        reduce_only=True,
    )
    session.add(new_sl)
    await session.flush()

    attempt_count = _record_lighter_attempt(pos.id)
    log.warning(
        "lighter_sl_watchdog_replacing",
        position_id=pos.id,
        symbol=pos.symbol,
        attempt=attempt_count,
        sl_price=str(sl_price),
    )

    try:
        placed = await place_trigger_with_verification(
            client=client,
            position=pos,
            order_row=new_sl,
            trigger_type="sl",
            trigger_price=sl_price,
            size=size_rounded,
            client_order_id=coid,
        )
    except TriggerWouldImmediatelyFireError as e:
        await _emergency_close_naked_position(
            session=session,
            client=client,
            pos=pos,
            reason="sl_immediate_trigger_lighter_watchdog",
            response_body=e.response_body,
        )
        return

    if placed:
        pos.sl_order_id = new_sl.id
        pos.sl_price_current = sl_price
        pos.last_synced_at = datetime.now(timezone.utc)
        await session.flush()
        log.info(
            "lighter_sl_watchdog_replaced",
            position_id=pos.id,
            symbol=pos.symbol,
            new_sl_order_id=new_sl.id,
            vooi_order_id=new_sl.vooi_order_id,
        )
        await emit_event(
            "LIGHTER_SL_REPLACED",
            position_id=pos.id,
            exchange=pos.exchange,
            symbol=pos.symbol,
            message=(
                f"Lighter dropped SL for position {pos.id} {pos.symbol}; "
                f"replaced at {sl_price}."
            ),
        )
    else:
        log.error(
            "lighter_sl_watchdog_replace_failed",
            position_id=pos.id,
            symbol=pos.symbol,
            attempt=attempt_count,
        )


# -----------------------------------------------------------------------------
# TP safety watchdog (cross-exchange)
#
# Mirrors the lighter SL watchdog pattern but for take-profit orders. The
# common failure mode is: VOOI returns 503 during the post-fill TP POST and
# the bot gives up after 3 immediate retries; the position then lives without
# a profit ceiling. This task picks up such positions and keeps retrying with
# a per-position hourly cap.
# -----------------------------------------------------------------------------
def _record_tp_attempt(position_id: int) -> int:
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=1)
    history = _tp_replacement_attempts.setdefault(position_id, [])
    history.append(now)
    history[:] = [t for t in history if t > cutoff]
    return len(history)


def _tp_attempts_in_last_hour(position_id: int) -> int:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=1)
    history = _tp_replacement_attempts.get(position_id, [])
    return sum(1 for t in history if t > cutoff)


async def tp_safety_watchdog_task() -> None:
    """
    Every 30s: scan open positions; if any has no active TP (excluding those
    where SL has already been moved to breakeven), recompute the TP price
    and place it. Rate-limited per position.
    """
    while True:
        try:
            await asyncio.sleep(_TP_WATCHDOG_INTERVAL_SEC)
            await tp_safety_watchdog_run()
        except asyncio.CancelledError:
            log.info("tp_safety_watchdog_cancelled")
            break
        except Exception as e:
            log.error("tp_safety_watchdog_error", error=str(e))


async def tp_safety_watchdog_run() -> None:
    client = get_vooi_client()

    async with session_scope() as session:
        result = await session.execute(
            select(Position).where(Position.status == "open")
        )
        open_positions = list(result.scalars())

        for pos in open_positions:
            # Note: we deliberately do NOT skip positions where BE-SL has
            # already fired. BE-SL caps the loss at ~0 but leaves no upside
            # ceiling — the bot would sit until manual close. TP must be
            # re-placed regardless of BE status.
            tp_active = False
            if pos.tp_order_id:
                tp_result = await session.execute(
                    select(Order).where(
                        and_(
                            Order.id == pos.tp_order_id,
                            Order.status.in_(["pending", "open"]),
                        )
                    )
                )
                tp_active = tp_result.scalar_one_or_none() is not None

            if tp_active:
                continue

            attempts = _tp_attempts_in_last_hour(pos.id)
            if attempts >= _TP_WATCHDOG_MAX_ATTEMPTS_PER_HOUR:
                log.error(
                    "tp_safety_watchdog_rate_limited",
                    position_id=pos.id,
                    symbol=pos.symbol,
                    attempts=attempts,
                )
                await emit_event(
                    "ERROR_NO_TP",
                    level="ERROR",
                    position_id=pos.id,
                    exchange=pos.exchange,
                    symbol=pos.symbol,
                    message=(
                        f"tp_safety_watchdog exhausted {attempts} TP placement "
                        f"attempts in the last hour for position {pos.id} "
                        f"{pos.symbol}. Manual intervention required."
                    ),
                )
                continue

            await _replace_tp(session, client, pos)


async def _replace_tp(
    session: AsyncSession,
    client,
    pos: Position,
) -> None:
    """Compute fresh TP for pos and place it via the verified helper."""
    try:
        price_decimals = await get_price_decimals(pos.symbol, pos.exchange)
        size_decimals = await get_size_decimals(pos.symbol, pos.exchange)
    except MarketDecimalsUnavailable as e:
        log.error(
            "tp_safety_watchdog_decimals_unavailable",
            position_id=pos.id, symbol=pos.symbol, error=str(e),
        )
        return

    # Reuse stored target if it exists; otherwise recompute.
    tp_price_raw: Optional[Decimal] = pos.tp_price_initial
    if tp_price_raw is None:
        tp_price_raw = compute_tp_price_with_fallback(
            avg_entry_price=pos.entry_price,
            side=pos.side,
            leverage=pos.leverage,
            exchange=pos.exchange,
        )

    exit_side = opposite_side(pos.side)
    tp_price = round_price(Decimal(str(tp_price_raw)), price_decimals, exit_side)
    size_rounded = round_size(pos.size, size_decimals)

    coid = make_client_order_id(pos.signal_id or 0, pos.exchange, suffix="wdtp")
    new_tp = Order(
        signal_id=pos.signal_id,
        client_order_id=coid,
        order_type="takeProfit",
        exchange=pos.exchange,
        symbol=pos.symbol,
        side=exit_side,
        status="submitting",
        trigger_price=tp_price,
        size=size_rounded,
        reduce_only=True,
    )
    session.add(new_tp)
    await session.flush()

    attempt_count = _record_tp_attempt(pos.id)
    log.warning(
        "tp_safety_watchdog_replacing",
        position_id=pos.id,
        symbol=pos.symbol,
        attempt=attempt_count,
        tp_price=str(tp_price),
    )

    try:
        placed = await place_trigger_with_verification(
            client=client,
            position=pos,
            order_row=new_tp,
            trigger_type="tp",
            trigger_price=tp_price,
            size=size_rounded,
            client_order_id=coid,
        )
    except TriggerWouldImmediatelyFireError as e:
        # Market has moved past our profit target while we were not watching.
        # Take the profit at market instead of re-trying a stale trigger price
        # every 30s forever.
        await _emergency_close_take_profit(
            session=session,
            client=client,
            pos=pos,
            reason="tp_immediate_trigger_watchdog",
            response_body=e.response_body,
        )
        return

    if placed:
        pos.tp_order_id = new_tp.id
        pos.tp_price_initial = tp_price
        pos.last_synced_at = datetime.now(timezone.utc)
        await session.flush()
        log.info(
            "tp_safety_watchdog_replaced",
            position_id=pos.id,
            symbol=pos.symbol,
            new_tp_order_id=new_tp.id,
            vooi_order_id=new_tp.vooi_order_id,
        )
        await emit_event(
            "TP_REPLACED_BY_WATCHDOG",
            position_id=pos.id,
            exchange=pos.exchange,
            symbol=pos.symbol,
            message=(
                f"TP placement was missing for position {pos.id} {pos.symbol}; "
                f"replaced at {tp_price}."
            ),
        )
    else:
        log.error(
            "tp_safety_watchdog_replace_failed",
            position_id=pos.id,
            symbol=pos.symbol,
            attempt=attempt_count,
        )


async def _emergency_close_naked_position(
    *,
    session: AsyncSession,
    client,
    pos: Position,
    reason: str,
    response_body: str = "",
) -> None:
    """
    Watchdog couldn't place SL because the trigger would fire immediately —
    market is already past our intended stop. Dump the position via aggressive
    limit-IOC reduce-only, mark closed_emergency, and shout at the operator.

    Mirrors `post_fill_placer._emergency_close_after_sl_fail` but operates on
    an already-open position (sl_safety runs after post-fill has succeeded
    at one point and TP/SL are tracked).
    """
    log.error(
        "watchdog_sl_would_immediately_fire",
        position_id=pos.id,
        exchange=pos.exchange,
        symbol=pos.symbol,
        response=response_body[:200],
    )
    await emit_event(
        "ERROR_NAKED_POSITION",
        level="ERROR",
        position_id=pos.id,
        exchange=pos.exchange,
        symbol=pos.symbol,
        message=(
            f"SL replacement rejected (trigger would fire immediately) for "
            f"position {pos.id} {pos.symbol}. Emergency market-close engaged."
        ),
    )

    vooi_order_id: Optional[str] = None
    try:
        vooi_order_id = await emergency_market_close(client, pos, reason=reason)
    except Exception as e:
        log.error(
            "emergency_close_watchdog_failed",
            position_id=pos.id,
            exchange=pos.exchange,
            symbol=pos.symbol,
            error=str(e),
        )
        await send_naked_position_alert(pos.id, pos.symbol, pos.exchange)
        return

    now = datetime.now(timezone.utc)
    pos.status = "closed_emergency"
    pos.close_reason = reason
    pos.closed_at = now
    pos.status_updated_at = now
    await session.flush()

    await emit_event(
        "EMERGENCY_MARKET_CLOSE",
        level="ERROR",
        position_id=pos.id,
        exchange=pos.exchange,
        symbol=pos.symbol,
        message=(
            f"Emergency limit-IOC close submitted (reason={reason}, "
            f"vooi_order_id={vooi_order_id})."
        ),
    )
    await send_emergency_close_alert(
        pos.id, pos.symbol, pos.exchange,
        reason=reason,
        vooi_order_id=vooi_order_id,
    )


async def _emergency_close_take_profit(
    *,
    session: AsyncSession,
    client,
    pos: Position,
    reason: str,
    response_body: str = "",
) -> None:
    """
    Watchdog tried to place a fresh TP and got -2021 — price has moved past
    the profit target. Take the win at market via aggressive limit-IOC and
    cancel the existing SL so it doesn't sit unattached after the close.
    """
    log.warning(
        "tp_watchdog_would_immediately_fire",
        position_id=pos.id,
        exchange=pos.exchange,
        symbol=pos.symbol,
        response=response_body[:200],
    )

    # Cancel the live SL (if any) — reduceOnly safeguards a stray fill, but
    # an orphan SL on a closed position triggers startup_cleanup noise next
    # restart, so clean it up here.
    if pos.sl_order_id:
        sl_res = await session.execute(select(Order).where(Order.id == pos.sl_order_id))
        sl_o = sl_res.scalar_one_or_none()
        if sl_o and sl_o.vooi_order_id and sl_o.status in ("pending", "open", "submitting"):
            try:
                await client.delete(
                    "/exchange/orders",
                    json_body={
                        "exchange": pos.exchange,
                        "asset": pos.symbol,
                        "orderId": sl_o.vooi_order_id,
                    },
                )
                sl_o.status = "cancelled"
                sl_o.cancelled_at = datetime.now(timezone.utc)
            except Exception as e:
                log.warning(
                    "tp_watchdog_sl_cancel_failed",
                    position_id=pos.id,
                    sl_order_id=sl_o.id,
                    error=str(e),
                )

    vooi_order_id: Optional[str] = None
    try:
        vooi_order_id = await emergency_market_close(client, pos, reason=reason)
    except Exception as e:
        log.error(
            "emergency_close_tp_watchdog_failed",
            position_id=pos.id,
            exchange=pos.exchange,
            symbol=pos.symbol,
            error=str(e),
        )
        return

    now = datetime.now(timezone.utc)
    pos.status = "closed_emergency"
    pos.close_reason = reason
    pos.closed_at = now
    pos.status_updated_at = now
    await session.flush()

    await emit_event(
        "EMERGENCY_MARKET_CLOSE",
        level="WARNING",
        position_id=pos.id,
        exchange=pos.exchange,
        symbol=pos.symbol,
        message=(
            f"Emergency limit-IOC close submitted (reason={reason}, "
            f"vooi_order_id={vooi_order_id})."
        ),
    )
    await send_emergency_close_alert(
        pos.id, pos.symbol, pos.exchange,
        reason=reason,
        vooi_order_id=vooi_order_id,
    )
