"""
One-shot startup cleanup, per 2026-05-13 round-2 fixes.

(1) Phantom positions: for each `positions` row with status IN ('open',
    'open_pending_tp_sl'), check the real exchange. If no matching exchange
    position exists AND no live entry limit on the order book, mark the row
    as `closed_manual` with close_reason='phantom_no_exchange_position'.

(2) Dangling orders: any open order on an exchange whose symbol has no live
    exchange position is an orphan (e.g. a size-0 conditional SL/TP trigger
    left over from a previous-era bracket attach). Cancel via DELETE
    /exchange/orders.

Both passes are idempotent — safe to run on every startup.
"""
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

import structlog
from sqlalchemy import and_, select

from bot.db import session_scope
from bot.models import Order, Position
from bot.vooi_client import get_vooi_client

log = structlog.get_logger(__name__)

EXCHANGES = ["hyperliquid", "lighter", "aster"]


async def _fetch_exchange_state() -> dict[str, dict]:
    client = get_vooi_client()
    state: dict[str, dict] = {}
    for exchange in EXCHANGES:
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
            log.warning("startup_cleanup_fetch_failed", exchange=exchange, error=str(e))
    return state


def _live_position_symbols(positions: list[dict]) -> set[str]:
    out: set[str] = set()
    for p in positions:
        if not isinstance(p, dict):
            continue
        try:
            size = abs(Decimal(str(p.get("size") or 0)))
        except Exception:
            size = Decimal("0")
        if size <= 0:
            continue
        sym = str(p.get("baseSymbol") or p.get("asset") or p.get("symbol") or "").upper()
        if sym:
            out.add(sym)
    return out


async def cleanup_phantom_positions(state: dict[str, dict]) -> int:
    """Mark positions phantom when no exchange position backs them."""
    if not state:
        return 0

    cleaned = 0
    async with session_scope() as session:
        result = await session.execute(
            select(Position).where(
                Position.status.in_(["open", "open_pending_tp_sl"]),
            )
        )
        positions = result.scalars().all()
        now = datetime.now(timezone.utc)

        for pos in positions:
            ex_data = state.get(pos.exchange)
            if not ex_data:
                continue  # exchange unreachable — leave it for the next run
            live_syms = _live_position_symbols(ex_data["positions"])
            if pos.symbol.upper() in live_syms:
                continue

            # No live position. Is the entry limit still on the book?
            open_vooi_ids = {
                str(o.get("orderId") or o.get("id"))
                for o in ex_data["open_orders"]
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

            # Try trade-history reconciliation first — a TP/SL fill between
            # the bot stopping and starting again should be recorded, not
            # mislabelled phantom.
            try:
                from bot.reconciler import _try_close_from_history
                reconciled = await _try_close_from_history(session, pos)
            except Exception as e:
                log.warning(
                    "startup_cleanup_history_close_error",
                    position_id=pos.id, error=str(e),
                )
                reconciled = False
            if reconciled:
                continue

            log.warning(
                "startup_cleanup_phantom_position",
                position_id=pos.id,
                exchange=pos.exchange,
                symbol=pos.symbol,
                status=pos.status,
            )
            pos.status = "closed_manual"
            pos.close_reason = "phantom_no_exchange_position"
            pos.closed_at = now
            pos.status_updated_at = now
            cleaned += 1
    return cleaned


async def _is_tracked_by_db(
    exchange: str,
    order_id: str,
    symbol: str,
    side: Optional[str],
    price: Optional[str],
    size: Optional[str],
    *,
    tol_bps: Decimal = Decimal("10"),
) -> bool:
    """
    Is this exchange order one of ours that's actively pending?

    Match by `vooi_order_id` (preferred) or by
    (exchange, symbol, side, price ±10bps, size ±10bps) for legacy rows
    placed before fix #1 (round-3) started persisting the orderId.

    Pending entry limits waiting for fill must be spared by the dangling-
    order sweep — otherwise every restart cancels live orders the bot
    placed and is waiting on.
    """
    async with session_scope() as session:
        # Fast path: orderId match.
        if order_id:
            r = await session.execute(
                select(Order).where(
                    Order.vooi_order_id == order_id,
                    Order.status.in_(["pending", "open", "submitting"]),
                )
            )
            if r.scalar_one_or_none() is not None:
                return True

        # Legacy path: match by tuple. Pre-fix orders have vooi_order_id IS NULL.
        s_upper = (symbol or "").upper()
        s_side = (side or "").lower()
        if not s_upper or not s_side:
            return False

        r = await session.execute(
            select(Order).where(
                Order.exchange == exchange,
                Order.symbol == s_upper,
                Order.side == s_side,
                Order.status.in_(["pending", "open", "submitting"]),
            )
        )
        candidates = r.scalars().all()
        if not candidates:
            return False

        try:
            o_price = Decimal(str(price or 0))
            o_size = Decimal(str(size or 0))
        except Exception:
            return False

        for c in candidates:
            try:
                c_price = Decimal(c.price) if c.price is not None else Decimal("0")
                c_size = Decimal(c.size) if c.size is not None else Decimal("0")
            except Exception:
                continue
            if c_price <= 0 or c_size <= 0:
                continue
            if abs(o_price - c_price) / c_price * Decimal("10000") > tol_bps:
                continue
            if abs(o_size - c_size) / c_size * Decimal("10000") > tol_bps:
                continue
            # Backfill vooi_order_id for the round-3 self-tracking work.
            if order_id and not c.vooi_order_id:
                c.vooi_order_id = order_id
            return True
        return False


async def cancel_dangling_orders(state: dict[str, dict]) -> int:
    """
    Cancel any open order on an exchange whose symbol has no live exchange
    position AND no matching pending row in our orders table. Catches the
    TAO bracket legs (entry limit + size-0 SL/TP triggers) left behind by
    the pre-fix-era bracket attach, while sparing live entry limits the
    bot is currently waiting to fill.
    """
    if not state:
        return 0

    client = get_vooi_client()
    cancelled = 0
    for exchange, data in state.items():
        live_syms = _live_position_symbols(data["positions"])
        for order in data["open_orders"]:
            if not isinstance(order, dict):
                continue
            order_id = str(order.get("orderId") or order.get("id") or "")
            symbol = str(
                order.get("baseSymbol") or order.get("asset") or order.get("symbol") or ""
            ).upper()
            if not order_id or not symbol:
                continue
            if symbol in live_syms:
                continue  # legitimate live-position bracket — leave alone

            tracked = await _is_tracked_by_db(
                exchange=exchange,
                order_id=order_id,
                symbol=symbol,
                side=order.get("side"),
                price=order.get("price"),
                size=order.get("size"),
            )
            if tracked:
                log.info(
                    "startup_cleanup_spared_tracked_order",
                    exchange=exchange,
                    symbol=symbol,
                    order_id=order_id,
                    order_type=order.get("type"),
                )
                continue

            log.warning(
                "startup_cleanup_dangling_order",
                exchange=exchange,
                symbol=symbol,
                order_id=order_id,
                order_type=order.get("type"),
                trigger_price=order.get("triggerPrice"),
            )
            try:
                resp = await client.delete(
                    "/exchange/orders",
                    json_body={"exchange": exchange, "asset": symbol, "orderId": order_id},
                )
                ok = isinstance(resp, dict) and str(resp.get("status", "")).lower() in (
                    "ok", "success", "cancelled", "canceled",
                )
                if ok:
                    cancelled += 1
                else:
                    log.warning(
                        "startup_cleanup_cancel_unexpected_response",
                        exchange=exchange,
                        order_id=order_id,
                        response=str(resp)[:200],
                    )
            except Exception as e:
                log.warning(
                    "startup_cleanup_cancel_failed",
                    exchange=exchange,
                    order_id=order_id,
                    error=str(e),
                )

            # Mark our DB row, if we tracked this order, as cancelled.
            async with session_scope() as session:
                r = await session.execute(
                    select(Order).where(Order.vooi_order_id == order_id)
                )
                db_order = r.scalar_one_or_none()
                if db_order is not None and db_order.status in (
                    "pending", "open", "submitting"
                ):
                    db_order.status = "cancelled_reconciler"
                    db_order.cancelled_at = datetime.now(timezone.utc)
    return cancelled


async def run_startup_cleanup() -> None:
    """Entry point — called once at bot startup before the long-running tasks."""
    log.info("startup_cleanup_begin")
    state = await _fetch_exchange_state()
    if not state:
        log.warning("startup_cleanup_no_exchange_state")
        return
    phantom = await cleanup_phantom_positions(state)
    dangling = await cancel_dangling_orders(state)
    log.info(
        "startup_cleanup_done",
        phantom_positions=phantom,
        dangling_orders_cancelled=dangling,
    )
