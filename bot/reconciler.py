"""
REST reconciler — periodic sync to catch SSE misses.
Runs every RECONCILER_INTERVAL_SEC.
Per spec §9, reconciler task.
"""
import asyncio
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional

import structlog
from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from bot.config import settings
from bot.db import session_scope
from bot.models import Order, Position, Trade
from bot import sse_listener as _sse
from bot.streamer import emit_event
from bot.vooi_client import get_vooi_client

log = structlog.get_logger(__name__)


async def reconciler_task() -> None:
    """Main reconciler loop — runs every RECONCILER_INTERVAL_SEC."""
    while True:
        try:
            await asyncio.sleep(settings.reconciler_interval_sec)
            await reconciler_run()
        except asyncio.CancelledError:
            log.info("reconciler_cancelled")
            break
        except Exception as e:
            log.error("reconciler_error", error=str(e))


async def reconciler_run() -> None:
    """Single reconciler run."""
    await emit_event("RECONCILER_RUN", level="DEBUG", message="Reconciler tick")

    # Read live module-level values (must NOT capture at import time — that
    # was the bug behind the bogus "silence_sec=258000" warnings).
    sse_silence_sec = time.monotonic() - _sse.sse_last_frame_at
    if sse_silence_sec > 120:  # 2 minutes
        log.warning(
            "reconciler_sse_silent",
            silence_sec=sse_silence_sec,
            connected=_sse.sse_is_connected,
        )

    # Fetch exchange state ONCE per tick — used by orders sync, positions
    # sync, and entry-fill detection so they all see the same snapshot.
    exchange_state = await fetch_exchange_state()

    fills_to_dispatch: list[tuple[int, Optional[Decimal]]] = []
    orphans_to_adopt: list[tuple[int, Optional[Decimal]]] = []
    async with session_scope() as session:
        # Always sync orders and positions against real exchange state. SSE is
        # primary for fast updates but the reconciler is the authoritative
        # ground-truth pass — it catches missed fills, phantom positions, and
        # stale pending entry orders even when SSE is healthy.
        fills_to_dispatch = await sync_orders_to_exchange(session, exchange_state)
        orphans_to_adopt = await adopt_orphan_exchange_positions(session, exchange_state)
        await sync_positions_to_exchange(session, exchange_state)
        await cancel_expired_limit_orders(session)

    # Dispatch post-fill placement AFTER the reconciler's transaction commits.
    # Each on_entry_filled opens its own session and writes to the same rows
    # we just updated — running it inside the parent tx caused row-lock
    # deadlocks on the second aster fill (2026-05-18 & -19 silent stalls).
    all_dispatch = list(fills_to_dispatch) + list(orphans_to_adopt)
    if all_dispatch:
        from bot.post_fill_placer import on_entry_filled
        for entry_order_id, avg_price in all_dispatch:
            try:
                await on_entry_filled(entry_order_id, avg_price)
            except Exception as e:
                log.error(
                    "reconciler_post_fill_dispatch_error",
                    entry_order_id=entry_order_id, error=str(e),
                )

    # Recover post-fill placement for positions whose entry filled while
    # SSE was offline (spec §8.7 reconciler note).
    await retry_pending_tp_sl()


async def fetch_exchange_state() -> dict[str, dict]:
    """
    Fetch open-orders + positions from all 3 exchanges in one pass.
    Returns: {exchange: {"open_orders": [...], "positions": [...]}}

    On per-exchange failure, that exchange is left out of the result — the
    sync routines treat a missing exchange as "no data" and leave our rows
    untouched, which is the conservative choice.
    """
    client = get_vooi_client()
    state: dict[str, dict] = {}
    for exchange in ["hyperliquid", "lighter", "aster"]:
        try:
            open_orders = await client.get(
                "/exchange/open-orders", params={"exchanges": exchange}
            )
            positions = await client.get(
                "/exchange/positions", params={"exchanges": exchange}
            )
            state[exchange] = {
                "open_orders": open_orders if isinstance(open_orders, list) else [],
                "positions": positions if isinstance(positions, list) else [],
            }
        except Exception as e:
            log.warning("reconciler_fetch_exchange_failed", exchange=exchange, error=str(e))
    return state


def _exchange_position_for(
    exchange_state: dict[str, dict],
    exchange: str,
    symbol: str,
) -> Optional[dict]:
    """Find a non-zero-size position on the exchange for (exchange, symbol)."""
    data = exchange_state.get(exchange)
    if not data:
        return None
    for pos in data.get("positions", []):
        if not isinstance(pos, dict):
            continue
        pos_sym = str(pos.get("baseSymbol") or pos.get("asset") or pos.get("symbol") or "")
        if pos_sym.upper() != symbol.upper():
            continue
        try:
            size = abs(Decimal(str(pos.get("size") or pos.get("qty") or 0)))
        except Exception:
            size = Decimal("0")
        if size > 0:
            return pos
    return None


async def sync_orders_to_exchange(
    session: AsyncSession,
    exchange_state: dict[str, dict],
) -> list[tuple[int, Optional[Decimal]]]:
    """
    Reconcile our `orders` table against /exchange/open-orders + /exchange/positions.

    For each pending entry order whose exchange we have state for:
      - Still on /exchange/open-orders → leave alone.
      - Gone from open-orders AND a matching exchange position exists → it filled
        (mark filled, capture avg_fill_price + filled_size from the position,
        and dispatch post_fill_placer so SL/TP get placed and the positions row
        gets created).
      - Gone from open-orders AND no exchange position → it was cancelled or
        rejected outside our flow; mark cancelled_reconciler.

    For TP/SL orders (order_type='stopLoss'|'takeProfit'):
      - Gone from open-orders AND no exchange position → mark cancelled_reconciler
        (the position closed, the trigger was consumed/cancelled by the exchange).
    """
    if not exchange_state:
        return []

    result = await session.execute(
        select(Order).where(
            and_(
                Order.exchange.in_(list(exchange_state.keys())),
                Order.status.in_(["pending", "open", "submitting"]),
            )
        )
    )
    db_orders = result.scalars().all()

    # Backfill missing vooi_order_id by clientOrderId. The synchronous
    # post-place lookup misses fast-fill / fast-cancel cases (the order is
    # already off /exchange/open-orders by the time we poll), leaving the row
    # with a NULL orderId. Without this, the row falls through every reconciler
    # tick and the position can run unprotected (CHZ/SNX 2026-05-15 incident).
    from bot.orders import lookup_vooi_order_id_by_client_id
    client = get_vooi_client()
    for order in db_orders:
        if order.vooi_order_id or not order.client_order_id:
            continue
        if order.exchange not in exchange_state:
            continue
        try:
            backfilled = await lookup_vooi_order_id_by_client_id(
                client, order.exchange, order.client_order_id,
                attempts=1,
            )
        except Exception as e:
            log.warning(
                "reconciler_backfill_orderid_error",
                order_id=order.id, error=str(e),
            )
            continue
        if backfilled:
            order.vooi_order_id = backfilled
            log.info(
                "reconciler_vooi_order_id_backfilled",
                order_id=order.id,
                client_order_id=order.client_order_id,
                vooi_order_id=backfilled,
                order_type=order.order_type,
            )

    fills_to_dispatch: list[tuple[int, Optional[Decimal]]] = []

    for order in db_orders:
        ex_data = exchange_state.get(order.exchange)
        if not ex_data:
            continue
        open_vooi_ids = {
            str(o.get("orderId") or o.get("id"))
            for o in ex_data.get("open_orders", [])
            if isinstance(o, dict) and (o.get("orderId") or o.get("id"))
        }
        if not order.vooi_order_id:
            continue
        if order.vooi_order_id in open_vooi_ids:
            continue

        # Order is no longer on the exchange. Decide between filled vs cancelled
        # based on whether a position now exists for this (exchange, symbol).
        ex_pos = _exchange_position_for(exchange_state, order.exchange, order.symbol)

        if order.order_type == "entry" and ex_pos is not None:
            now = datetime.now(timezone.utc)
            avg_price_raw = ex_pos.get("entryPrice")
            size_raw = ex_pos.get("size")
            if order.status != "filled":
                order.status = "filled"
                order.filled_at = now
                if avg_price_raw is not None:
                    order.avg_fill_price = Decimal(str(avg_price_raw))
                if size_raw is not None:
                    try:
                        order.filled_size = abs(Decimal(str(size_raw)))
                    except Exception:
                        pass
                log.info(
                    "reconciler_entry_fill_detected",
                    order_id=order.id,
                    vooi_order_id=order.vooi_order_id,
                    exchange=order.exchange,
                    symbol=order.symbol,
                    avg_fill_price=str(order.avg_fill_price) if order.avg_fill_price else None,
                )
                fills_to_dispatch.append((order.id, order.avg_fill_price))
        else:
            # Either a non-entry order, or an entry with no exchange position
            # → treat as cancelled.
            log.info(
                "reconciler_order_disappeared",
                order_id=order.id,
                order_type=order.order_type,
                vooi_order_id=order.vooi_order_id,
            )
            order.status = "cancelled_reconciler"
            order.cancelled_at = datetime.now(timezone.utc)

    await session.flush()

    # Return fills for the caller to dispatch AFTER the parent session_scope
    # exits (so the entry-row commit completes before post_fill_placer opens
    # its own session — without this, the second aster fill in a tick stalls
    # forever in row-lock contention against the still-open parent tx).
    return fills_to_dispatch


async def adopt_orphan_exchange_positions(
    session: AsyncSession,
    exchange_state: dict[str, dict],
) -> list[tuple[int, Optional[Decimal]]]:
    """
    Defensive sweep that catches missed entry-fill transitions starting from
    *exchange* state rather than from our orders table.

    For every non-zero exchange position on every exchange we have state for:
      - If a positions row already exists in 'open' / 'open_pending_tp_sl' /
        'closed_emergency' for (exchange, symbol) → already tracked, skip.
      - Otherwise locate a tracked entry order (status in pending/submitting/
        filled-but-no-position) for (exchange, symbol, side) with size within
        5%. Mark it filled (if not already), and return its id so the caller
        dispatches `on_entry_filled` — which creates the position row and
        places SL/TP.

    Why this exists alongside `sync_orders_to_exchange`: the orders-out path
    requires `vooi_order_id` to be known and the position to actually appear
    in `/exchange/positions`. Edge cases (vooi_order_id mismatch after a
    rejected-then-replaced flow, brief race between fill and position
    materialisation, post_fill_placer crash before position row commits) can
    leave a live exchange position with no DB row. Walking the live positions
    list closes that loop.
    """
    if not exchange_state:
        return []

    adoptions: list[tuple[int, Optional[Decimal]]] = []
    now = datetime.now(timezone.utc)

    for exchange, data in exchange_state.items():
        for ex_pos in data.get("positions", []) or []:
            if not isinstance(ex_pos, dict):
                continue
            symbol = str(
                ex_pos.get("baseSymbol") or ex_pos.get("asset")
                or ex_pos.get("symbol") or ""
            ).upper()
            if not symbol:
                continue
            try:
                size_raw = Decimal(str(ex_pos.get("size") or ex_pos.get("qty") or 0))
            except Exception:
                continue
            if size_raw == 0:
                continue
            ex_size = abs(size_raw)

            # Exchange sometimes reports `side` as long/short, sometimes
            # buy/sell, sometimes only via sign of size. Normalise.
            raw_side = str(ex_pos.get("side") or "").lower()
            if raw_side in ("long", "buy"):
                ex_side = "buy"
            elif raw_side in ("short", "sell"):
                ex_side = "sell"
            else:
                ex_side = "buy" if size_raw > 0 else "sell"

            # Already tracked?
            tracked_q = await session.execute(
                select(Position).where(
                    and_(
                        Position.exchange == exchange,
                        Position.symbol == symbol,
                        Position.status.in_(
                            ["open", "open_pending_tp_sl", "closed_emergency"]
                        ),
                    )
                )
            )
            if tracked_q.scalars().first() is not None:
                continue

            # Find a candidate entry order. Prefer `filled` (no position row
            # yet means post_fill_placer didn't finish); fall back to
            # `pending`/`submitting` (sync_orders_to_exchange should have
            # promoted these already, but here we are).
            entry_q = await session.execute(
                select(Order).where(
                    and_(
                        Order.exchange == exchange,
                        Order.symbol == symbol,
                        Order.side == ex_side,
                        Order.order_type == "entry",
                        Order.status.in_(["filled", "pending", "submitting"]),
                    )
                ).order_by(Order.id.desc())
            )
            candidates = entry_q.scalars().all()

            entry_order: Optional[Order] = None
            for cand in candidates:
                cand_size = cand.size or Decimal("0")
                if cand_size == 0:
                    continue
                drift = abs(cand_size - ex_size) / cand_size
                if drift <= Decimal("0.05"):
                    entry_order = cand
                    break

            if entry_order is None:
                log.warning(
                    "orphan_position_no_candidate_entry",
                    exchange=exchange,
                    symbol=symbol,
                    side=ex_side,
                    size=str(ex_size),
                )
                continue

            avg_price_raw = ex_pos.get("entryPrice")
            avg_price: Optional[Decimal] = None
            if avg_price_raw is not None:
                try:
                    avg_price = Decimal(str(avg_price_raw))
                except Exception:
                    avg_price = None

            if entry_order.status != "filled":
                entry_order.status = "filled"
                entry_order.filled_at = now
                entry_order.filled_size = ex_size
                if avg_price is not None and avg_price > 0:
                    entry_order.avg_fill_price = avg_price

            log.warning(
                "orphan_position_adopting",
                exchange=exchange,
                symbol=symbol,
                side=ex_side,
                size=str(ex_size),
                entry_order_id=entry_order.id,
                vooi_order_id=entry_order.vooi_order_id,
            )
            adoptions.append((entry_order.id, avg_price))

    await session.flush()
    return adoptions


async def sync_positions_to_exchange(
    session: AsyncSession,
    exchange_state: dict[str, dict],
) -> None:
    """
    Reconcile our `positions` table against /exchange/positions.

    For each row with status IN ('open', 'open_pending_tp_sl'):
      - Matching exchange position with non-zero size →
          * Update entry_price / liquidation_price.
          * If size or entry_price differs by >1% → log a `position_drift`
            warning and overwrite with exchange values.
      - No matching exchange position →
          * If a pending entry order is still on the exchange order book → leave
            it (we're waiting for the limit to fill).
          * Otherwise mark closed with close_reason='phantom_no_exchange_position'.
    """
    if not exchange_state:
        return

    # 'closed_emergency' rows enter here too so that _try_close_from_history
    # can backfill close_price / realized_pnl_usd from the IOC reduce-only
    # fill we just sent.
    result = await session.execute(
        select(Position).where(
            and_(
                Position.exchange.in_(list(exchange_state.keys())),
                or_(
                    Position.status.in_(["open", "open_pending_tp_sl"]),
                    and_(
                        Position.status == "closed_emergency",
                        Position.realized_pnl_usd.is_(None),
                    ),
                ),
            )
        )
    )
    rows = result.scalars().all()
    if not rows:
        return

    now = datetime.now(timezone.utc)
    one_pct = Decimal("0.01")

    for pos in rows:
        ex_pos = _exchange_position_for(exchange_state, pos.exchange, pos.symbol)
        if ex_pos is not None:
            try:
                ex_entry = Decimal(str(ex_pos.get("entryPrice") or "0"))
                ex_size = abs(Decimal(str(ex_pos.get("size") or "0")))
            except Exception:
                continue
            if ex_entry > 0 and pos.entry_price and pos.entry_price > 0:
                price_drift = abs(ex_entry - pos.entry_price) / pos.entry_price
                size_drift = (
                    abs(ex_size - pos.size) / pos.size
                    if pos.size and pos.size > 0 else Decimal("0")
                )
                if price_drift > one_pct or size_drift > one_pct:
                    log.warning(
                        "position_drift",
                        position_id=pos.id,
                        exchange=pos.exchange,
                        symbol=pos.symbol,
                        our_entry=str(pos.entry_price),
                        ex_entry=str(ex_entry),
                        our_size=str(pos.size),
                        ex_size=str(ex_size),
                    )
                    pos.entry_price = ex_entry
                    pos.size = ex_size
            if ex_pos.get("liquidationPrice"):
                pos.liquidation_price = Decimal(str(ex_pos["liquidationPrice"]))
            pos.last_synced_at = now
            continue

        # No exchange position. Is there a still-pending entry order on the
        # exchange order book? If yes, leave the row alone (this is the brief
        # window before a fill is detected).
        ex_data = exchange_state.get(pos.exchange, {})
        open_vooi_ids = {
            str(o.get("orderId") or o.get("id"))
            for o in ex_data.get("open_orders", [])
            if isinstance(o, dict) and (o.get("orderId") or o.get("id"))
        }
        entry_still_open = False
        if pos.entry_order_id:
            er = await session.execute(
                select(Order).where(Order.id == pos.entry_order_id)
            )
            entry_order = er.scalar_one_or_none()
            if (
                entry_order is not None
                and entry_order.vooi_order_id
                and entry_order.vooi_order_id in open_vooi_ids
            ):
                entry_still_open = True

        if entry_still_open:
            continue

        # Position vanished from exchange. Before declaring it phantom, walk
        # /exchange/trades — TP/SL may have triggered between ticks and the
        # position simply finished its lifecycle.
        try:
            reconciled = await _try_close_from_history(session, pos)
        except Exception as e:
            log.warning(
                "reconciler_history_close_error",
                position_id=pos.id, error=str(e),
            )
            reconciled = False
        if reconciled:
            continue

        # Grace period: don't declare phantom on a single missed tick. A
        # transient API hiccup or a fill caught between reconciler passes
        # would otherwise flip a real position to closed_manual. Require
        # the position to be absent from /exchange/positions for at least
        # 3 reconciler cycles (using last_synced_at as the anchor — only
        # updated when the position IS observed on exchange).
        grace_sec = max(180, 3 * settings.reconciler_interval_sec)
        if pos.last_synced_at is not None:
            missing_age_sec = (now - pos.last_synced_at).total_seconds()
        else:
            missing_age_sec = (now - pos.opened_at).total_seconds() if pos.opened_at else grace_sec + 1
        if missing_age_sec < grace_sec:
            log.info(
                "phantom_grace_period",
                position_id=pos.id,
                exchange=pos.exchange,
                symbol=pos.symbol,
                missing_age_sec=int(missing_age_sec),
                grace_sec=grace_sec,
            )
            continue

        entry_vooi_id = None
        if pos.entry_order_id:
            er = await session.execute(
                select(Order).where(Order.id == pos.entry_order_id)
            )
            eo = er.scalar_one_or_none()
            if eo is not None:
                entry_vooi_id = eo.vooi_order_id
        log.warning(
            "phantom_position_detected",
            position_id=pos.id,
            exchange=pos.exchange,
            symbol=pos.symbol,
            status=pos.status,
            entry_order_id=pos.entry_order_id,
            entry_vooi_order_id=entry_vooi_id,
            opened_at=pos.opened_at.isoformat() if pos.opened_at else None,
            last_synced_at=pos.last_synced_at.isoformat() if pos.last_synced_at else None,
            missing_age_sec=int(missing_age_sec),
        )
        # Don't downgrade an already-recorded emergency exit to 'closed_manual'
        # just because the IOC fill hasn't surfaced in /exchange/trades yet.
        if pos.status != "closed_emergency":
            pos.status = "closed_manual"
            pos.close_reason = "phantom_no_exchange_position"
        pos.closed_at = now
        pos.status_updated_at = now

    await session.flush()


async def _try_close_from_history(
    session: AsyncSession,
    position: Position,
) -> bool:
    """
    Reconcile a position that's gone from /exchange/positions by walking
    /exchange/trades. If we can match the entry order's vooi_order_id and a
    subsequent opposite-side fill covering its size, write close_price /
    realized_pnl_usd / fees_paid_usd / close_reason and insert a `trades`
    row. Returns True on success (caller skips the phantom branch).

    No-op when:
      - the position has no entry_order_id or the entry order has no
        vooi_order_id (we have nothing to anchor on),
      - VOOI doesn't return the entry trade (entry never actually filled),
      - we can't find enough opposite-side fills after entry to cover size.
    """
    entry_vooi_id: Optional[str] = None
    entry_order: Optional[Order] = None
    if position.entry_order_id:
        er = await session.execute(
            select(Order).where(Order.id == position.entry_order_id)
        )
        entry_order = er.scalar_one_or_none()
        if entry_order is not None and entry_order.vooi_order_id:
            entry_vooi_id = str(entry_order.vooi_order_id)
    if not entry_vooi_id:
        return False

    client = get_vooi_client()
    try:
        resp = await client.get(
            "/exchange/trades",
            params={"exchanges": position.exchange, "symbol": position.symbol},
        )
    except Exception as e:
        log.warning(
            "reconciler_history_fetch_failed",
            position_id=position.id,
            symbol=position.symbol,
            error=str(e),
        )
        return False
    items = resp.get("items", []) if isinstance(resp, dict) else []
    if not items:
        return False

    def parse_ts(s: str) -> datetime:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))

    # 1. Locate the entry trade.
    entry_trade: Optional[dict] = None
    for t in items:
        if str(t.get("orderId")) == entry_vooi_id:
            entry_trade = t
            break
    if entry_trade is None:
        return False

    try:
        entry_ts = parse_ts(str(entry_trade.get("createdAt") or ""))
    except Exception:
        return False
    entry_side = str(entry_trade.get("side", "")).lower()
    opposite_side = "sell" if entry_side == "buy" else "buy"

    target_size = Decimal(str(position.size or 0))
    if target_size <= 0:
        return False

    # 2. Collect opposite-side fills after the entry until we cover size.
    #    Items are newest-first per VOOI convention.
    close_trades: list[dict] = []
    closed_size = Decimal("0")
    tolerance = Decimal("0.0000001")
    for t in items:
        if str(t.get("orderId")) == entry_vooi_id:
            continue
        try:
            ts = parse_ts(str(t.get("createdAt") or ""))
        except Exception:
            continue
        if ts <= entry_ts:
            continue
        if str(t.get("side", "")).lower() != opposite_side:
            continue
        try:
            sz = abs(Decimal(str(t.get("size") or 0)))
        except Exception:
            continue
        if sz <= 0:
            continue
        close_trades.append(t)
        closed_size += sz
        if closed_size + tolerance >= target_size:
            break
    if not close_trades or closed_size <= 0:
        return False

    # 3. Aggregate.
    total_notional = Decimal("0")
    total_close_fee = Decimal("0")
    total_realized = Decimal("0")
    close_order_ids: set[str] = set()
    latest_close_ts: Optional[datetime] = None
    for t in close_trades:
        try:
            price = Decimal(str(t.get("price") or 0))
            sz = abs(Decimal(str(t.get("size") or 0)))
            fee = Decimal(str(t.get("fee") or 0))
            rpnl = Decimal(str(t.get("realizedPnl") or 0))
            ts = parse_ts(str(t.get("createdAt") or ""))
        except Exception:
            continue
        total_notional += price * sz
        total_close_fee += fee
        total_realized += rpnl
        oid = str(t.get("orderId") or "")
        if oid:
            close_order_ids.add(oid)
        if latest_close_ts is None or ts > latest_close_ts:
            latest_close_ts = ts
    if closed_size <= 0 or latest_close_ts is None:
        return False
    avg_close = total_notional / closed_size

    try:
        entry_fee = Decimal(str(entry_trade.get("fee") or 0))
    except Exception:
        entry_fee = Decimal("0")
    total_fees = entry_fee + total_close_fee

    # 4. Classify close_reason via our orders table.
    close_reason = "closed_manual"
    sl_vooi: Optional[str] = None
    tp_vooi: Optional[str] = None
    if position.sl_order_id:
        r = await session.execute(select(Order).where(Order.id == position.sl_order_id))
        o = r.scalar_one_or_none()
        if o and o.vooi_order_id:
            sl_vooi = str(o.vooi_order_id)
    if position.tp_order_id:
        r = await session.execute(select(Order).where(Order.id == position.tp_order_id))
        o = r.scalar_one_or_none()
        if o and o.vooi_order_id:
            tp_vooi = str(o.vooi_order_id)
    if tp_vooi and tp_vooi in close_order_ids:
        close_reason = "closed_tp"
    elif sl_vooi and sl_vooi in close_order_ids:
        close_reason = "closed_sl"
    elif position.status == "closed_emergency":
        # An emergency-close IOC fill is neither SL nor TP — keep the existing
        # marker so reports/audits can distinguish panic exits.
        close_reason = position.close_reason or "closed_emergency"

    # 5. Write back. Preserve `closed_emergency` status & close_reason if the
    # row was flatted via emergency_market_close — we only need the trade
    # backfill (close_price / pnl / fees) for reports.
    now = datetime.now(timezone.utc)
    if position.status != "closed_emergency":
        position.status = close_reason
        position.close_reason = close_reason
    position.close_price = avg_close
    position.realized_pnl_usd = total_realized
    position.fees_paid_usd = total_fees
    position.closed_at = latest_close_ts
    position.status_updated_at = now

    session.add(Trade(
        position_id=position.id,
        exchange=position.exchange,
        symbol=position.symbol,
        side=position.side,
        entry_price=position.entry_price,
        exit_price=avg_close,
        size=position.size,
        leverage=position.leverage or 0,
        realized_pnl_usd=total_realized,
        fees_paid_usd=total_fees,
        funding_paid_usd=Decimal("0"),
        close_reason=close_reason,
        opened_at=position.opened_at,
        closed_at=latest_close_ts,
    ))

    log.info(
        "reconciler_closed_from_history",
        position_id=position.id,
        exchange=position.exchange,
        symbol=position.symbol,
        close_price=str(avg_close),
        realized_pnl=str(total_realized),
        fees=str(total_fees),
        close_reason=close_reason,
        close_order_ids=list(close_order_ids),
    )
    await emit_event(
        "POSITION_RECONCILED",
        position_id=position.id,
        exchange=position.exchange,
        symbol=position.symbol,
        message=(
            f"{position.side} {close_reason} @ {avg_close} "
            f"pnl={total_realized} fees={total_fees}"
        ),
    )
    return True


async def retry_pending_tp_sl() -> None:
    """
    Re-trigger post-fill TP+SL placement for positions stuck in
    status='open_pending_tp_sl' with a filled entry order. This recovers
    from cases where the SSE FILLED frame was missed (bot offline / SSE
    silent) — per spec §8.7 reconciler note.
    """
    async with session_scope() as session:
        result = await session.execute(
            select(Position, Order)
            .join(Order, Position.entry_order_id == Order.id)
            .where(
                and_(
                    Position.status == "open_pending_tp_sl",
                    Order.status == "filled",
                )
            )
        )
        rows = result.all()

    if not rows:
        return

    from bot.post_fill_placer import on_entry_filled
    for position, entry_order in rows:
        log.warning(
            "reconciler_replaying_post_fill",
            position_id=position.id,
            entry_order_id=entry_order.id,
        )
        avg = entry_order.avg_fill_price
        await on_entry_filled(entry_order.id, avg)


async def cancel_expired_limit_orders(session: AsyncSession) -> None:
    """
    Cancel entry limit orders older than LIMIT_ORDER_TTL_HOURS.
    Per spec: auto-cancel stale unfilled orders.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=settings.limit_order_ttl_hours)

    result = await session.execute(
        select(Order).where(
            and_(
                Order.order_type == "entry",
                # BUG-08: include 'submitting' — requests that never got a response
                Order.status.in_(["submitting", "pending", "open"]),
                Order.created_at < cutoff,
            )
        )
    )
    expired_orders = result.scalars().all()

    client = get_vooi_client()

    for order in expired_orders:
        log.info(
            "reconciler_cancelling_expired_order",
            order_id=order.id,
            vooi_order_id=order.vooi_order_id,
            created_at=str(order.created_at),
        )

        if order.vooi_order_id:
            try:
                await client.delete(
                    "/exchange/orders",
                    json_body={
                        "exchange": order.exchange,
                        "asset": order.symbol,
                        "orderId": order.vooi_order_id,
                    },
                )
            except Exception as e:
                log.warning(
                    "reconciler_cancel_failed",
                    order_id=order.id,
                    error=str(e),
                )

        order.status = "expired"
        order.cancelled_at = datetime.now(timezone.utc)

    await session.flush()
