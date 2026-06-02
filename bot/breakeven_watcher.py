"""
TP breakeven — event-driven via SSE marketPrice frames.

The hot path is `evaluate_breakeven_trigger`, called from sse_listener on each
incoming price tick. A lightweight supervisor task runs every 10s to:
  - update the heartbeat read by sl_safety_check,
  - reconcile the in-memory trigger_index with the DB (catches positions we
    might have missed via SSE-driven registration),
  - rescue positions whose SSE feed has gone stale (>30s) by batch-quoting
    REST and re-evaluating.

Per spec §8.8.
"""
import asyncio
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

import structlog
from sqlalchemy import and_, select

from bot.alerts import send_emergency_close_alert, send_naked_position_alert
from bot.config import settings
from bot.db import session_scope
from bot.models import Order, Position
from bot.orders import (
    TriggerWouldImmediatelyFireError,
    emergency_market_close,
    make_client_order_id,
    place_trigger_with_verification,
)
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

# -----------------------------------------------------------------------------
# Heartbeat — read by sl_safety_check.check_watcher_heartbeat
# -----------------------------------------------------------------------------
tp_breakeven_watcher_last_tick: float = 0.0

_SUPERVISOR_INTERVAL_SEC = 10

# Clearance the current price must have over the breakeven SL before we move
# it, as a fraction of price. Keeps the BE move off the immediate-trigger
# boundary so a tiny retrace between guard and placement doesn't reject it.
_BREAKEVEN_REQUOTE_BUFFER_PCT = Decimal("0.001")  # 0.1% of price


# -----------------------------------------------------------------------------
# In-memory trigger index
#
# Source of truth is the `positions` table. This index is a derived cache so
# the SSE hot path doesn't have to SELECT on every price tick. The supervisor
# reconciles it back to the DB every 10s, so a missed register/unregister is
# self-healing within one cycle.
# -----------------------------------------------------------------------------
@dataclass
class _BreakevenTrigger:
    position_id: int
    exchange: str      # lowercased
    symbol: str        # uppercased
    side: str
    entry_price: Decimal
    leverage: int
    trigger_pct: Decimal  # as % of price (margin_pct / leverage / 100)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    fired: bool = False


# (exchange, symbol) -> list[_BreakevenTrigger]
# A list (not a single item) because the same symbol can be open on multiple
# exchanges; in practice the bot rejects duplicates so this is usually len ≤ 1.
trigger_index: dict[tuple[str, str], list[_BreakevenTrigger]] = {}


def _compute_trigger_pct(leverage: int) -> Decimal:
    """BREAKEVEN_TRIGGER_PCT is % of margin; ÷ leverage → % of price."""
    return (
        Decimal(str(settings.breakeven_trigger_pct))
        / Decimal("100")
        / Decimal(str(leverage))
    )


def register_breakeven_trigger(position: Position) -> None:
    """
    Add a position to the in-memory trigger index. Idempotent — if a trigger
    for the same position_id already exists, it is replaced.
    """
    if position.status != "open":
        return
    if position.sl_moved_to_be_at is not None:
        return  # BE already locked in — nothing left to trigger

    key = (position.exchange.lower(), position.symbol.upper())
    trigger = _BreakevenTrigger(
        position_id=position.id,
        exchange=key[0],
        symbol=key[1],
        side=position.side,
        entry_price=position.entry_price,
        leverage=position.leverage,
        trigger_pct=_compute_trigger_pct(position.leverage),
    )
    triggers = trigger_index.setdefault(key, [])
    # Replace any existing entry for this position to keep entry_price fresh.
    triggers[:] = [t for t in triggers if t.position_id != position.id]
    triggers.append(trigger)
    log.debug(
        "breakeven_trigger_registered",
        position_id=position.id,
        exchange=key[0],
        symbol=key[1],
        trigger_pct=str(trigger.trigger_pct),
    )


def unregister_breakeven_trigger(position_id: int) -> None:
    """Remove all entries for a position. Safe to call multiple times."""
    removed = False
    for key in list(trigger_index.keys()):
        before = len(trigger_index[key])
        trigger_index[key] = [t for t in trigger_index[key] if t.position_id != position_id]
        if not trigger_index[key]:
            del trigger_index[key]
        if before != len(trigger_index.get(key, [])):
            removed = True
    if removed:
        log.debug("breakeven_trigger_unregistered", position_id=position_id)


async def evaluate_breakeven_trigger(
    exchange: str, symbol: str, price: Decimal
) -> None:
    """
    Hot path. Called from sse_listener.on_market_price_frame on every tick.

    Looks up the (exchange, symbol) bucket in the trigger index and, for each
    un-fired trigger whose threshold is crossed, schedules a fire-and-forget
    breakeven move. Per-trigger asyncio.Lock + fired flag guarantee a single
    BE move per position even under a torrent of price ticks.

    Does not block on HTTP — heavy work runs in a separate task so subsequent
    marketPrice frames keep flowing.
    """
    key = (exchange.lower(), symbol.upper())
    triggers = trigger_index.get(key)
    if not triggers:
        return

    for trigger in list(triggers):
        if trigger.fired:
            continue
        if trigger.side == "buy":
            crossed = price >= trigger.entry_price * (Decimal("1") + trigger.trigger_pct)
        elif trigger.side == "sell":
            crossed = price <= trigger.entry_price * (Decimal("1") - trigger.trigger_pct)
        else:
            continue
        if not crossed:
            continue
        if trigger.lock.locked():
            continue  # already firing; another task will handle it
        asyncio.create_task(_fire_breakeven_safely(trigger, price))


async def _fire_breakeven_safely(trigger: _BreakevenTrigger, observed_price: Decimal) -> None:
    """Acquire the trigger lock and run the BE move. Idempotent on retry."""
    async with trigger.lock:
        if trigger.fired:
            return

        log.info(
            "breakeven_trigger_detected",
            position_id=trigger.position_id,
            exchange=trigger.exchange,
            symbol=trigger.symbol,
            entry=str(trigger.entry_price),
            current=str(observed_price),
            trigger_pct=str(trigger.trigger_pct),
        )

        try:
            await move_sl_to_breakeven(trigger.position_id, observed_price=observed_price)
        except Exception as e:
            log.error(
                "breakeven_fire_error",
                position_id=trigger.position_id,
                error=str(e),
            )
            return  # leave fired=False → retry on next price

        # Verify the move actually persisted. move_sl_to_breakeven swallows
        # internal failures (cancel failed, NAKED, etc.) and returns silently;
        # we only mark fired once the DB shows sl_moved_to_be_at is set.
        async with session_scope() as session:
            result = await session.execute(
                select(Position).where(Position.id == trigger.position_id)
            )
            pos = result.scalar_one_or_none()
            if pos is not None and pos.sl_moved_to_be_at is not None:
                trigger.fired = True
                unregister_breakeven_trigger(trigger.position_id)
            else:
                log.warning(
                    "breakeven_move_did_not_persist",
                    position_id=trigger.position_id,
                )


# -----------------------------------------------------------------------------
# Startup seeding
# -----------------------------------------------------------------------------
async def _seed_price_cache_from_db() -> None:
    """
    On startup, seed price_cache with entry_price for all open positions.
    Prevents breakeven evaluation from silently skipping positions before
    the first SSE tick arrives (e.g. low-liquidity symbols).
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
            # Intentionally don't set price_cache_updated_at — age > staleness
            # threshold so the rescue path will REST-quote on first cycle.

    if positions:
        log.info("price_cache_seeded_from_db", count=len(positions))


async def _seed_trigger_index_from_db() -> None:
    """Populate trigger_index with all open positions whose BE hasn't fired."""
    async with session_scope() as session:
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
        register_breakeven_trigger(pos)

    log.info(
        "breakeven_trigger_index_seeded",
        count=sum(len(v) for v in trigger_index.values()),
    )


# -----------------------------------------------------------------------------
# Supervisor — runs every 10s
#   1. heartbeat
#   2. reconcile trigger_index with DB (add missing / drop closed)
#   3. rescue positions whose SSE price feed is stale
# -----------------------------------------------------------------------------
async def tp_breakeven_supervisor_task() -> None:
    """
    Replaces the v1.5 2-second polling loop. The hot path is now SSE-driven
    (see `evaluate_breakeven_trigger`). This task only does maintenance.
    """
    global tp_breakeven_watcher_last_tick

    await _seed_price_cache_from_db()
    await _seed_trigger_index_from_db()

    while True:
        try:
            tp_breakeven_watcher_last_tick = time.time()
            await _reconcile_trigger_index()
            await _rescue_stale_sse_positions()
        except asyncio.CancelledError:
            log.info("breakeven_supervisor_cancelled")
            break
        except Exception as e:
            log.error("breakeven_supervisor_error", error=str(e))

        await asyncio.sleep(_SUPERVISOR_INTERVAL_SEC)


async def _reconcile_trigger_index() -> None:
    """Sync in-memory index with DB. Self-healing for missed register calls."""
    async with session_scope() as session:
        result = await session.execute(
            select(Position).where(
                and_(
                    Position.status == "open",
                    Position.sl_moved_to_be_at.is_(None),
                )
            )
        )
        positions = list(result.scalars())

    eligible_ids = {p.id for p in positions}

    # Drop entries for positions that are no longer eligible (closed or BE-fired).
    for key in list(trigger_index.keys()):
        trigger_index[key] = [t for t in trigger_index[key] if t.position_id in eligible_ids]
        if not trigger_index[key]:
            del trigger_index[key]

    # Add entries for any positions missing from the index.
    indexed = {t.position_id for ts in trigger_index.values() for t in ts}
    for pos in positions:
        if pos.id not in indexed:
            register_breakeven_trigger(pos)


async def _rescue_stale_sse_positions() -> None:
    """
    For positions whose SSE marketPrice hasn't ticked within the staleness
    window, batch-fetch REST quotes and feed them back into the evaluator.
    This is the only place that hits /exchange/quotes from breakeven logic;
    with healthy SSE it makes zero requests.
    """
    if not trigger_index:
        return

    cutoff = time.time() - settings.sse_price_staleness_threshold_sec
    stale_keys: set[tuple[str, str]] = set()

    for key, triggers in trigger_index.items():
        if not triggers:
            continue
        last = price_cache_updated_at.get(key, 0)
        if last < cutoff:
            stale_keys.add(key)

    if not stale_keys:
        return

    client = get_vooi_client()
    for exchange, symbol in stale_keys:
        try:
            raw_price = await client.get_current_price(symbol, exchange)
            price = Decimal(str(raw_price))
        except Exception as e:
            log.debug(
                "breakeven_rescue_quote_failed",
                exchange=exchange,
                symbol=symbol,
                error=str(e),
            )
            continue
        price_cache[(exchange, symbol)] = price
        price_cache_updated_at[(exchange, symbol)] = time.time()
        await evaluate_breakeven_trigger(exchange, symbol, price)


# -----------------------------------------------------------------------------
# Breakeven move
# -----------------------------------------------------------------------------
async def move_sl_to_breakeven(
    position_id: int, observed_price: Optional[Decimal] = None
) -> None:
    """
    Cancel the live SL and place a new one at entry + safety buffer.

    Steps per spec §8.8:
    1. Idempotency guard: sl_moved_to_be_at IS NOT NULL → return.
    2. Compute breakeven SL price.
    2b. Re-quote guard: if the BE-SL would immediately trigger at the current
        market (price retraced back through breakeven after the trigger spike),
        DEFER — keep the live protective SL untouched and let the evaluator
        retry on a later tick. Prevents the cancel-then-dump race that flattened
        positions at scratch on volatile venues (aster -2021).
    3. Cancel existing SL order (abort if cancel cannot be confirmed).
    4. Place new SL (reduce-only) via the verified-trigger helper.
    5. On race (404 on cancel) → position already closed.
    6. If new SL placement fails → ERROR_NAKED_POSITION alert; do NOT set
       sl_moved_to_be_at, so the SSE evaluator will retry on the next price.
    7. On success, update position (sl_order_id, sl_price_current,
       sl_moved_to_be_at) and emit SL_BREAKEVEN.

    `observed_price` is the SSE tick that fired the trigger; passed through to
    avoid an extra REST quote. Falls back to a REST quote if not supplied.
    """
    async with session_scope() as session:
        result = await session.execute(
            select(Position).where(Position.id == position_id)
        )
        pos = result.scalar_one_or_none()

        if pos is None:
            return

        if pos.sl_moved_to_be_at is not None:
            log.debug("breakeven_already_moved", position_id=pos.id)
            return

        from bot.config import settings as cfg
        exit_taker_bps = cfg.get_fee_fallback_bps(pos.exchange)
        be_price = compute_breakeven_sl_price(
            entry_price=pos.entry_price,
            side=pos.side,
            exit_taker_bps=exit_taker_bps,
        )

        price_decimals = await get_price_decimals(pos.symbol, pos.exchange)
        size_decimals = await get_size_decimals(pos.symbol, pos.exchange)

        be_price_rounded = round_price(be_price, price_decimals, opposite_side(pos.side))
        size_rounded = round_size(pos.size, size_decimals)

        client = get_vooi_client()

        # Re-quote guard (step 2b): a BE move that would immediately fire means
        # price has retraced back through breakeven since the trigger spike.
        # Cancelling the live SL to place a doomed BE-SL leaves us naked and
        # forces a scratch dump. Instead defer: keep the protective SL in place
        # and let the evaluator retry once price clears breakeven for real.
        current_price = observed_price
        if current_price is None:
            try:
                current_price = Decimal(
                    str(await client.get_current_price(pos.symbol, pos.exchange))
                )
            except Exception as e:
                log.debug(
                    "breakeven_requote_guard_price_unavailable",
                    position_id=pos.id, error=str(e),
                )
                current_price = None

        if current_price is not None:
            buffer = _BREAKEVEN_REQUOTE_BUFFER_PCT
            if pos.side == "buy":
                clear = current_price > be_price_rounded * (Decimal("1") + buffer)
            else:
                clear = current_price < be_price_rounded * (Decimal("1") - buffer)
            if not clear:
                log.info(
                    "breakeven_move_deferred_would_immediately_fire",
                    position_id=pos.id,
                    exchange=pos.exchange,
                    symbol=pos.symbol,
                    be_price=str(be_price_rounded),
                    current=str(current_price),
                )
                return  # protective SL untouched; sl_moved_to_be_at stays NULL → retry

        # Cancel existing SL order. We must confirm cancel before placing a
        # new SL — two SLs on the book would double-close on the first hit.
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
                    return

                cancel_ok = (
                    isinstance(cancel_response, dict)
                    and str(cancel_response.get("status", "")).lower()
                    in ("ok", "success", "cancelled", "canceled")
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

        # Place new breakeven SL via the verified helper.
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

        try:
            sl_placed = await place_trigger_with_verification(
                client=client,
                position=pos,
                order_row=new_sl_order,
                trigger_type="sl",
                trigger_price=be_price_rounded,
                size=size_rounded,
                client_order_id=be_sl_client_oid,
            )
        except TriggerWouldImmediatelyFireError as e:
            # Residual race: price retraced through breakeven in the window
            # between our confirmed cancel and the re-place, so the BE-SL would
            # fire immediately. We're momentarily naked. DON'T scratch-dump the
            # position (the old behaviour). After a retrace toward entry the
            # ORIGINAL (wider) stop is comfortably clear of market, so restore
            # protection there and leave sl_moved_to_be_at NULL → BE retries on
            # a later tick. Emergency close is now only the last resort if even
            # the original level would immediately fire (price genuinely past
            # the protective stop too).
            log.warning(
                "breakeven_sl_would_immediately_fire_restoring",
                position_id=pos.id,
                exchange=pos.exchange,
                symbol=pos.symbol,
                be_price=str(be_price_rounded),
                response=e.response_body[:200],
            )

            restored = False
            restore_price = None
            if pos.sl_price_initial is not None:
                restore_price = round_price(
                    Decimal(str(pos.sl_price_initial)),
                    price_decimals,
                    opposite_side(pos.side),
                )
                restore_coid = make_client_order_id(
                    pos.signal_id or 0, pos.exchange, suffix="rsl"
                )
                restore_order = Order(
                    signal_id=pos.signal_id,
                    client_order_id=restore_coid,
                    order_type="stopLoss",
                    exchange=pos.exchange,
                    symbol=pos.symbol,
                    side=opposite_side(pos.side),
                    status="submitting",
                    trigger_price=restore_price,
                    size=size_rounded,
                    reduce_only=True,
                )
                session.add(restore_order)
                await session.flush()
                try:
                    restored = await place_trigger_with_verification(
                        client=client,
                        position=pos,
                        order_row=restore_order,
                        trigger_type="sl",
                        trigger_price=restore_price,
                        size=size_rounded,
                        client_order_id=restore_coid,
                    )
                except TriggerWouldImmediatelyFireError:
                    restored = False

            if restored:
                pos.sl_order_id = restore_order.id
                pos.sl_price_current = restore_price
                pos.last_synced_at = datetime.now(timezone.utc)
                await session.flush()
                log.info(
                    "breakeven_protection_restored",
                    position_id=pos.id,
                    symbol=pos.symbol,
                    sl_price=str(restore_price),
                )
                await emit_event(
                    "SL_RESTORED",
                    level="WARNING",
                    position_id=pos.id,
                    signal_id=pos.signal_id,
                    exchange=pos.exchange,
                    symbol=pos.symbol,
                    message=(
                        f"BE move deferred (would fire immediately); protective "
                        f"SL restored @ {restore_price} for pos={pos.id}. "
                        f"Will retry breakeven."
                    ),
                )
                return  # sl_moved_to_be_at stays NULL → BE retried next tick

            # Could not restore protection either → genuine emergency.
            log.error(
                "breakeven_sl_restore_failed_emergency",
                position_id=pos.id,
                exchange=pos.exchange,
                symbol=pos.symbol,
                be_price=str(be_price_rounded),
            )
            await emit_event(
                "ERROR_NAKED_POSITION",
                level="ERROR",
                position_id=pos.id,
                exchange=pos.exchange,
                symbol=pos.symbol,
                message=(
                    f"Breakeven SL replacement rejected (trigger would fire "
                    f"immediately) for position {pos.id} {pos.symbol} and "
                    f"protective SL could not be restored. Emergency "
                    f"market-close engaged."
                ),
            )

            vooi_order_id: Optional[str] = None
            try:
                vooi_order_id = await emergency_market_close(
                    client, pos, reason="sl_immediate_trigger_breakeven_move",
                )
            except Exception as ex:
                log.error(
                    "emergency_close_breakeven_failed",
                    position_id=pos.id,
                    exchange=pos.exchange,
                    symbol=pos.symbol,
                    error=str(ex),
                )
                await send_naked_position_alert(pos.id, pos.symbol, pos.exchange)
                return

            now = datetime.now(timezone.utc)
            pos.status = "closed_emergency"
            pos.close_reason = "sl_immediate_trigger_breakeven_move"
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
                    f"Emergency limit-IOC close submitted "
                    f"(reason=sl_immediate_trigger_breakeven_move, "
                    f"vooi_order_id={vooi_order_id})."
                ),
            )
            await send_emergency_close_alert(
                pos.id, pos.symbol, pos.exchange,
                reason="sl_immediate_trigger_breakeven_move",
                vooi_order_id=vooi_order_id,
            )
            return

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
                    f"Breakeven move failed for position {pos.id} "
                    f"{pos.symbol}: new SL not verified on exchange."
                ),
            )
            return

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
    Simulate breakeven trigger for testing (CLI: `bot simulate-breakeven`).
    1. Look up position.
    2. Compute the price that would cross the threshold (3% favorable move).
    3. If --no-dry-run: inject into price_cache and call evaluate directly,
       which fires the BE move via the standard path.
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

    if dry_run:
        log.info(
            "simulate_breakeven_dry_run",
            position_id=position_id,
            would_set_price=str(simulated_price),
        )
        return

    key = (pos.exchange.lower(), pos.symbol.upper())
    original_price = price_cache.get(key)
    price_cache[key] = simulated_price
    price_cache_updated_at[key] = time.time()

    # Re-evaluate immediately — synchronous path back into the same logic
    # that the SSE handler would use.
    await evaluate_breakeven_trigger(pos.exchange, pos.symbol, simulated_price)

    # Restore so subsequent SSE frames are not biased by the synthetic value.
    await asyncio.sleep(1)
    if original_price is not None:
        price_cache[key] = original_price
    else:
        price_cache.pop(key, None)
