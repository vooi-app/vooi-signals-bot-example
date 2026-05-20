"""
One-off: recompute TP for every open position and replace the live trigger order.

Used after TP_OVERHEAD_FLOOR_PCT semantics changed from "% of price" to "% of
margin" (divide by leverage). Run with the main bot stopped, otherwise the
SSE listener / breakeven_watcher race on the same orders.

    .venv/bin/python -m scripts.reset_tp           # dry run
    .venv/bin/python -m scripts.reset_tp --apply   # actually cancel+replace
"""
import asyncio
import json
import sys
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

import structlog
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from bot.config import settings
from bot.db import dispose_engine, session_scope
from bot.models import Order, Position
from bot.orders import make_client_order_id, place_trigger_with_verification
from bot.resolver import get_price_decimals, get_size_decimals
from bot.streamer import emit_event
from bot.tp_calculator import (
    compute_tp_price_with_fallback,
    opposite_side,
    round_price,
    round_size,
)
from bot.vooi_client import get_vooi_client

log = structlog.get_logger(__name__)


async def _reset_one(position: Position, *, apply: bool) -> None:
    side = position.side
    exit_side = opposite_side(side)

    price_decimals = await get_price_decimals(position.symbol, position.exchange)
    size_decimals = await get_size_decimals(position.symbol, position.exchange)

    new_tp_raw = compute_tp_price_with_fallback(
        avg_entry_price=position.entry_price,
        side=side,
        leverage=position.leverage,
        exchange=position.exchange,
    )
    new_tp = round_price(new_tp_raw, price_decimals, exit_side)
    size = round_size(position.size, size_decimals)

    old_tp = position.tp_price_initial
    print(
        f"  pos {position.id} {position.exchange} {position.symbol} {side} "
        f"entry={position.entry_price} lev={position.leverage}  "
        f"TP {old_tp} -> {new_tp}  size={size}"
    )

    if old_tp is not None and abs(new_tp - old_tp) / old_tp < Decimal("0.0001"):
        print("    skip: change < 1 bps")
        return

    if not apply:
        return

    client = get_vooi_client()

    # 1) Cancel old TP order on the exchange
    async with session_scope() as session:
        old_order = await session.get(Order, position.tp_order_id)
        if old_order is None or not old_order.vooi_order_id:
            print(f"    ERROR: no old TP order/vooi_order_id for position {position.id}")
            return
        old_vooi_id = old_order.vooi_order_id

    try:
        cancel_resp = await client.delete(
            "/exchange/orders",
            json_body={
                "exchange": position.exchange,
                "asset": position.symbol,
                "orderId": old_vooi_id,
            },
        )
        ok = isinstance(cancel_resp, dict) and str(cancel_resp.get("status", "")).lower() in (
            "ok", "success", "cancelled", "canceled",
        )
        if not ok:
            print(f"    WARN: cancel response unexpected: {str(cancel_resp)[:200]}")
    except Exception as e:
        print(f"    ERROR cancelling old TP: {e}")
        return

    # 2) Place new TP via the same verified path the bot uses
    async with session_scope() as session:
        position_db = await session.get(Position, position.id)
        old_order = await session.get(Order, position_db.tp_order_id)
        if old_order is not None:
            old_order.status = "cancelled"
            old_order.cancelled_at = datetime.now(timezone.utc)

        new_client_oid = make_client_order_id(
            position_db.signal_id or 0, position_db.exchange, suffix="tp-reset"
        )
        new_order = Order(
            signal_id=position_db.signal_id,
            client_order_id=new_client_oid,
            order_type="takeProfit",
            exchange=position_db.exchange,
            symbol=position_db.symbol,
            side=exit_side,
            status="submitting",
            trigger_price=new_tp,
            size=size,
            reduce_only=True,
        )
        session.add(new_order)
        await session.flush()

        broker_id = settings.get_broker_id(position_db.exchange)
        broker_fee_bps = settings.get_broker_fee_bps(position_db.exchange)

        placed = await place_trigger_with_verification(
            client=client,
            position=position_db,
            order_row=new_order,
            trigger_type="tp",
            trigger_price=new_tp,
            size=size,
            broker_id=broker_id,
            broker_fee_bps=broker_fee_bps,
            client_order_id=new_client_oid,
        )

        if placed:
            position_db.tp_order_id = new_order.id
            position_db.tp_price_initial = new_tp
            position_db.last_synced_at = datetime.now(timezone.utc)
            print(f"    OK: new TP placed @ {new_tp}, vooi_order_id={new_order.vooi_order_id}")
            await emit_event(
                "TP_RESET_AFTER_FLOOR_CHANGE",
                position_id=position_db.id,
                signal_id=position_db.signal_id,
                exchange=position_db.exchange,
                symbol=position_db.symbol,
                message=f"old_tp={old_tp} new_tp={new_tp}",
            )
        else:
            print(f"    ERROR: new TP not verified on exchange — position NAKED on TP side")


async def main(apply: bool) -> None:
    async with session_scope() as session:
        result = await session.execute(
            select(Position)
            .where(Position.status.in_(["open", "open_pending_tp_sl"]))
            .where(Position.tp_order_id.is_not(None))
            .order_by(Position.id)
        )
        positions = list(result.scalars())

    if not positions:
        print("No open positions with TP orders. Nothing to do.")
        return

    print(f"Found {len(positions)} open position(s) {'(APPLY)' if apply else '(DRY-RUN)'}:")
    for p in positions:
        try:
            await _reset_one(p, apply=apply)
        except Exception as e:
            print(f"  pos {p.id}: FAILED with {type(e).__name__}: {e}")

    await dispose_engine()


if __name__ == "__main__":
    apply = "--apply" in sys.argv
    asyncio.run(main(apply=apply))
