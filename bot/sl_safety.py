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

from bot.alerts import send_naked_position_alert, send_watcher_hung_alert
from bot.config import settings
from bot.db import session_scope
from bot.models import Order, Position
from bot.orders import make_client_order_id, place_trigger_with_verification
from bot.resolver import (
    MarketDecimalsUnavailable,
    get_price_decimals,
    get_size_decimals,
)
from bot.streamer import emit_event
from bot.tp_calculator import (
    compute_sl_price_from_pct,
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

# In-memory rate-limit: position_id → list[datetime] of replacement attempts.
# Resets on bot restart; that's acceptable — restart implies fresh slate.
_lighter_replacement_attempts: dict[int, list[datetime]] = {}


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
    For each open position: verify active SL exists.
    If not → emit ERROR_NAKED_POSITION and send Telegram alert.
    """
    result = await session.execute(
        select(Position).where(Position.status == "open")
    )
    open_positions = result.scalars().all()

    for pos in open_positions:
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
            sl_order = sl_result.scalar_one_or_none()
            has_active_sl = sl_order is not None
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

    broker_id = settings.get_broker_id(pos.exchange)
    broker_fee_bps = settings.get_broker_fee_bps(pos.exchange)

    placed = await place_trigger_with_verification(
        client=client,
        position=pos,
        order_row=new_sl,
        trigger_type="sl",
        trigger_price=sl_price,
        size=size_rounded,
        broker_id=broker_id,
        broker_fee_bps=broker_fee_bps,
        client_order_id=coid,
    )

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
