"""
TP breakeven watcher.
Monitors open position prices every 2s.
When price crosses BREAKEVEN_TRIGGER_PCT favorably, moves SL to breakeven+buffer.
Per spec §8.8, §8.8.5.
"""
import asyncio
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

import structlog
from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from bot.alerts import send_naked_position_alert
from bot.config import settings
from bot.db import session_scope
from bot.models import Order, Position
from bot.orders import make_client_order_id, place_trigger_with_verification
from bot.resolver import get_price_decimals, get_size_decimals
from bot.sse_listener import price_cache, price_cache_updated_at
from bot.streamer import emit_event
from bot.tp_calculator import (
    compute_breakeven_sl_price,
    opposite_side,
    round_price,
    round_size,
)
from bot.vooi_client import get_vooi_client

log = structlog.get_logger(__name__)

# Heartbeat timestamp — checked by sl_safety_check
# Must be declared at module level and updated each iteration
tp_breakeven_watcher_last_tick: float = 0.0


async def _seed_price_cache_from_db() -> None:
    """
    BUG-07: On startup, seed price_cache with entry_price for all open positions.
    Prevents breakeven watcher from silently skipping positions that have no SSE
    price yet (e.g. after bot restart). Entry price is the last known price.
    """
    async with session_scope() as session:
        result = await session.execute(
            select(Position).where(Position.status == "open")
        )
        positions = result.scalars().all()

    for pos in positions:
        key = (pos.exchange.lower(), pos.symbol.upper())
        if key not in price_cache:
            price_cache[key] = pos.entry_price
            # Don't set price_cache_updated_at — age will be > staleness threshold,
            # which triggers REST fallback on first check. That's correct.

    if positions:
        log.info("price_cache_seeded_from_db", count=len(positions))


async def tp_breakeven_watcher_task() -> None:
    """
    Main breakeven watcher loop.
    Checks all open positions every 2s for breakeven trigger.
    Updates tp_breakeven_watcher_last_tick heartbeat each iteration.
    """
    global tp_breakeven_watcher_last_tick

    await _seed_price_cache_from_db()

    while True:
        try:
            tp_breakeven_watcher_last_tick = time.time()  # heartbeat

            async with session_scope() as session:
                # Get open positions that haven't hit breakeven yet
                result = await session.execute(
                    select(Position).where(
                        and_(
                            Position.status == "open",
                            Position.sl_moved_to_be_at.is_(None),
                        )
                    )
                )
                positions = result.scalars().all()

            for pos in positions:
                try:
                    await _check_position_breakeven(pos)
                except Exception as e:
                    log.error(
                        "breakeven_check_error",
                        position_id=pos.id,
                        error=str(e),
                    )

        except asyncio.CancelledError:
            log.info("breakeven_watcher_cancelled")
            break
        except Exception as e:
            log.error("breakeven_watcher_loop_error", error=str(e))

        await asyncio.sleep(2)


async def _check_position_breakeven(pos: Position) -> None:
    """Check if a single position should trigger breakeven SL move."""
    key = (pos.exchange.lower(), pos.symbol.upper())
    cache_age = time.time() - price_cache_updated_at.get(key, 0)

    # Get current price
    cur_price: Optional[Decimal] = None

    if cache_age > settings.sse_price_staleness_threshold_sec:
        # SSE stale — fallback to REST
        log.debug(
            "price_cache_stale_fallback",
            exchange=pos.exchange,
            symbol=pos.symbol,
            cache_age=cache_age,
        )
        client = get_vooi_client()
        try:
            raw_price = await client.get_current_price(pos.symbol, pos.exchange)
            cur_price = Decimal(str(raw_price))
            # Update the cache with REST-fetched price
            price_cache[key] = cur_price
            price_cache_updated_at[key] = time.time()
        except Exception as e:
            log.warning(
                "rest_price_fetch_failed",
                exchange=pos.exchange,
                symbol=pos.symbol,
                error=str(e),
            )
            return  # Skip this position this tick
    else:
        cur_price = price_cache.get(key)
        if cur_price is None:
            return

    E = pos.entry_price
    # BREAKEVEN_TRIGGER_PCT is denominated in % of margin (collateral);
    # divide by leverage to convert to % of price.
    trigger_pct = (
        Decimal(str(settings.breakeven_trigger_pct))
        / Decimal("100")
        / Decimal(str(pos.leverage))
    )

    should_trigger = False
    if pos.side == "buy" and cur_price >= E * (Decimal("1") + trigger_pct):
        should_trigger = True
    elif pos.side == "sell" and cur_price <= E * (Decimal("1") - trigger_pct):
        should_trigger = True

    if should_trigger:
        log.info(
            "breakeven_trigger_detected",
            position_id=pos.id,
            symbol=pos.symbol,
            entry=str(E),
            current=str(cur_price),
            trigger_pct=str(trigger_pct),
        )
        await move_sl_to_breakeven(pos)


async def move_sl_to_breakeven(position: Position) -> None:
    """
    Move SL to breakeven+buffer.
    Steps per spec §8.8:
    1. Idempotency guard: sl_moved_to_be_at IS NOT NULL → return
    2. Compute breakeven SL price
    3. Cancel existing SL order
    4. Place new SL (reduce-only)
    5. Handle race (404 on cancel → position already closed)
    6. If new SL fails after retry → send ERROR_NAKED_POSITION alert
    7. Update position (sl_order_id, sl_price_current, sl_moved_to_be_at)
    8. Emit SL_BREAKEVEN
    """
    async with session_scope() as session:
        # Reload position with fresh data
        result = await session.execute(
            select(Position).where(Position.id == position.id)
        )
        pos = result.scalar_one_or_none()

        if pos is None:
            return

        # Idempotency guard
        if pos.sl_moved_to_be_at is not None:
            log.debug("breakeven_already_moved", position_id=pos.id)
            return

        # Compute breakeven SL price
        from bot.config import settings as cfg
        exit_taker_bps = cfg.get_fee_fallback_bps(pos.exchange)
        be_price = compute_breakeven_sl_price(
            entry_price=pos.entry_price,
            side=pos.side,
            exit_taker_bps=exit_taker_bps,
            exchange=pos.exchange,
        )

        price_decimals = await get_price_decimals(pos.symbol, pos.exchange)
        size_decimals = await get_size_decimals(pos.symbol, pos.exchange)

        be_price_rounded = round_price(be_price, price_decimals, opposite_side(pos.side))
        size_rounded = round_size(pos.size, size_decimals)

        client = get_vooi_client()
        broker_id = cfg.get_broker_id(pos.exchange)

        # Cancel existing SL order. Per Swagger, DELETE /exchange/orders takes
        # {exchange, asset, orderId} and returns {status: "ok"} on success.
        # If we can't confirm success, we MUST abort instead of leaving two
        # SL orders on the book (the live one still at the original trigger).
        if pos.sl_order_id:
            sl_order_result = await session.execute(
                select(Order).where(Order.id == pos.sl_order_id)
            )
            sl_order = sl_order_result.scalar_one_or_none()

            if sl_order and sl_order.vooi_order_id:
                try:
                    cancel_response = await client.delete(
                        "/exchange/orders",
                        json_body={
                            "exchange": pos.exchange,
                            "asset": pos.symbol,
                            "orderId": sl_order.vooi_order_id,
                        },
                    )
                except Exception as e:
                    error_str = str(e).lower()
                    if "404" in error_str or "not found" in error_str:
                        # Position already closed — race condition
                        log.warning(
                            "breakeven_cancel_404_position_closed",
                            position_id=pos.id,
                        )
                        return
                    log.error(
                        "breakeven_sl_cancel_failed_ABORT",
                        position_id=pos.id,
                        sl_order_id=sl_order.id,
                        vooi_order_id=sl_order.vooi_order_id,
                        error=str(e),
                    )
                    # ABORT: do not place a new SL when we can't confirm the
                    # old one was cancelled — we would end up with two SLs.
                    return

                # Cancel API returned 2xx. Confirm the response shape is success.
                cancel_ok = (
                    isinstance(cancel_response, dict)
                    and str(cancel_response.get("status", "")).lower() in ("ok", "success", "cancelled", "canceled")
                )
                if not cancel_ok:
                    log.error(
                        "breakeven_sl_cancel_unexpected_response_ABORT",
                        position_id=pos.id,
                        sl_order_id=sl_order.id,
                        vooi_order_id=sl_order.vooi_order_id,
                        response=str(cancel_response)[:200],
                    )
                    return

                sl_order.status = "cancelled"
                sl_order.cancelled_at = datetime.now(timezone.utc)
                log.info(
                    "breakeven_sl_cancelled",
                    position_id=pos.id,
                    old_sl_order_id=sl_order.id,
                    vooi_order_id=sl_order.vooi_order_id,
                )

        # Place new breakeven SL
        # Hyperliquid rejects non-hex clientOrderIds; delegate format choice
        # to make_client_order_id (issues 0x+32hex for HL, sigbot-... for others).
        be_sl_client_oid = make_client_order_id(
            pos.signal_id or 0, pos.exchange, suffix="besl"
        )
        new_sl_order = Order(
            signal_id=pos.signal_id,
            client_order_id=be_sl_client_oid,
            order_type="stopLoss",
            exchange=pos.exchange,
            symbol=pos.symbol,
            side=opposite_side(pos.side),
            status="submitting",
            trigger_price=be_price_rounded,
            size=size_rounded,
            reduce_only=True,
        )
        session.add(new_sl_order)
        await session.flush()

        # Place + verify via the shared helper (covers lighter clientOrderId=null
        # silent-NAKED and httpx-hang via internal asyncio.wait_for timeout).
        sl_placed = await place_trigger_with_verification(
            client=client,
            position=pos,
            order_row=new_sl_order,
            trigger_type="sl",
            trigger_price=be_price_rounded,
            size=size_rounded,
            broker_id=broker_id,
            broker_fee_bps=cfg.get_broker_fee_bps(pos.exchange),
            client_order_id=be_sl_client_oid,
        )

        if not sl_placed:
            log.error(
                "breakeven_sl_exhausted_ERROR_NAKED_POSITION",
                position_id=pos.id,
            )
            await send_naked_position_alert(pos.id, pos.symbol, pos.exchange)
            await emit_event(
                "ERROR_NAKED_POSITION",
                level="ERROR",
                position_id=pos.id,
                exchange=pos.exchange,
                symbol=pos.symbol,
                message=(
                    f"Breakeven SL placement failed after 3 attempts. "
                    f"Position {pos.id} {pos.symbol} naked!"
                ),
            )
            return

        await session.flush()

        # Update position
        pos.sl_order_id = new_sl_order.id
        pos.sl_price_current = be_price_rounded
        pos.sl_moved_to_be_at = datetime.now(timezone.utc)
        pos.last_synced_at = datetime.now(timezone.utc)
        await session.flush()

        log.info(
            "breakeven_sl_moved",
            position_id=pos.id,
            symbol=pos.symbol,
            be_price=str(be_price_rounded),
            old_sl=str(pos.sl_price_initial),
        )

        await emit_event(
            "SL_BREAKEVEN",
            position_id=pos.id,
            signal_id=pos.signal_id,
            exchange=pos.exchange,
            symbol=pos.symbol,
            message=(
                f"{pos.side} BE SL moved to {be_price_rounded}  "
                f"pos={pos.id}"
            ),
        )


async def simulate_breakeven(position_id: int, dry_run: bool = True) -> None:
    """
    Simulate breakeven trigger for testing.
    Per spec §10, §16:
    1. Get open position
    2. Forcibly set price_cache to entry_price × 1.03
    3. Wait for watcher to trigger (next 2s tick)
    4. Restore actual price after
    """
    async with session_scope() as session:
        result = await session.execute(
            select(Position).where(
                and_(
                    Position.id == position_id,
                    Position.status == "open",
                )
            )
        )
        pos = result.scalar_one_or_none()

    if pos is None:
        log.error("simulate_breakeven_position_not_found", position_id=position_id)
        raise ValueError(f"Position {position_id} not found or not open")

    key = (pos.exchange.lower(), pos.symbol.upper())
    original_price = price_cache.get(key)

    # Simulate 3% favorable move
    if pos.side == "buy":
        simulated_price = pos.entry_price * Decimal("1.03")
    else:
        simulated_price = pos.entry_price * Decimal("0.97")

    log.info(
        "simulate_breakeven",
        position_id=position_id,
        simulated_price=str(simulated_price),
        dry_run=dry_run,
    )

    if not dry_run:
        price_cache[key] = simulated_price
        price_cache_updated_at[key] = time.time()

        # Wait for watcher to detect and trigger
        await asyncio.sleep(3)

        # Restore original price
        if original_price is not None:
            price_cache[key] = original_price
        else:
            price_cache.pop(key, None)
    else:
        log.info(
            "simulate_breakeven_dry_run",
            position_id=position_id,
            would_set_price=str(simulated_price),
        )
