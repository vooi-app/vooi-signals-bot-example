"""
Startup cleanup: phantom positions get marked closed, and dangling orders
on the exchange book (no live position) get cancelled via DELETE
/exchange/orders.
"""
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import bot.startup_cleanup as su


class _FakeSession:
    def __init__(self, positions=None, orders=None):
        self._positions = positions or []
        self._orders = orders or []
        self._call = 0

    def add(self, obj):
        pass

    async def flush(self):
        pass

    async def execute(self, *args, **kwargs):
        self._call += 1
        r = MagicMock()
        if self._call == 1:
            # 1st: select positions (scalars().all())
            r.scalars = MagicMock(
                return_value=MagicMock(all=MagicMock(return_value=self._positions))
            )
            return r
        # subsequent: select Order by id → return None (no entry order tracked)
        r.scalar_one_or_none = MagicMock(return_value=None)
        return r


class _Scope:
    def __init__(self, s):
        self._s = s

    def __call__(self):
        return self

    async def __aenter__(self):
        return self._s

    async def __aexit__(self, *exc):
        return False


def _phantom_position(symbol="TAO"):
    pos = MagicMock()
    pos.id = 1
    pos.status = "open"
    pos.exchange = "lighter"
    pos.symbol = symbol
    pos.entry_order_id = None
    pos.entry_price = Decimal("100")
    pos.size = Decimal("1")
    pos.close_reason = None
    pos.closed_at = None
    pos.status_updated_at = None
    return pos


@pytest.mark.asyncio
async def test_cleanup_phantom_position_marks_closed():
    phantom = _phantom_position()
    state = {"lighter": {"open_orders": [], "positions": []}}

    session = _FakeSession(positions=[phantom])
    with patch.object(su, "session_scope", _Scope(session)):
        n = await su.cleanup_phantom_positions(state)

    assert n == 1
    assert phantom.status == "closed_manual"
    assert phantom.close_reason == "phantom_no_exchange_position"
    assert phantom.closed_at is not None


@pytest.mark.asyncio
async def test_cleanup_skips_when_exchange_position_exists():
    phantom = _phantom_position(symbol="BTC")
    state = {"lighter": {
        "open_orders": [],
        "positions": [{"baseSymbol": "BTC", "size": "0.5", "entryPrice": "65000"}],
    }}

    session = _FakeSession(positions=[phantom])
    with patch.object(su, "session_scope", _Scope(session)):
        n = await su.cleanup_phantom_positions(state)

    assert n == 0
    assert phantom.status == "open"
    assert phantom.close_reason is None


@pytest.mark.asyncio
async def test_cancel_dangling_orders_calls_delete():
    """Open order with no live position → cancel via DELETE /exchange/orders."""
    state = {
        "lighter": {
            "open_orders": [
                {"orderId": "v-orphan-1", "baseSymbol": "TAO", "type": "limit"},
                {"orderId": "v-orphan-2", "baseSymbol": "TAO", "type": "stopLoss",
                 "triggerPrice": "270"},
            ],
            "positions": [],
        },
        "hyperliquid": {
            "open_orders": [
                {"orderId": "v-real", "baseSymbol": "BTC", "type": "stopLoss"},
            ],
            "positions": [{"baseSymbol": "BTC", "size": "0.01", "entryPrice": "65000"}],
        },
    }

    client = MagicMock()
    client.delete = AsyncMock(return_value={"status": "ok"})

    session = _FakeSession()
    with patch.object(su, "get_vooi_client", return_value=client), \
         patch.object(su, "session_scope", _Scope(session)), \
         patch.object(su, "_is_tracked_by_db", AsyncMock(return_value=False)):
        n = await su.cancel_dangling_orders(state)

    # 2 TAO orders cancelled, BTC SL is kept because there's a live BTC position.
    assert n == 2
    assert client.delete.await_count == 2
    cancelled_ids = {c.kwargs["json_body"]["orderId"] for c in client.delete.await_args_list}
    assert cancelled_ids == {"v-orphan-1", "v-orphan-2"}


@pytest.mark.asyncio
async def test_cancel_dangling_orders_spares_tracked_db_order():
    """A pending entry-limit waiting for fill must NOT be cancelled on restart.

    Match path: tuple of (exchange, symbol, side, price ±10bps, size ±10bps).
    Covers legacy rows with vooi_order_id IS NULL (pre-fix-#1 round-3).
    """
    state = {
        "hyperliquid": {
            "open_orders": [
                {"orderId": "live-comp-42", "baseSymbol": "COMP", "side": "buy",
                 "price": "21.81", "size": "1.14", "type": "limit",
                 "createdAt": "2026-05-14T04:57:51Z"},
            ],
            "positions": [],
        },
    }

    client = MagicMock()
    client.delete = AsyncMock(return_value={"status": "ok"})

    session = _FakeSession()
    with patch.object(su, "get_vooi_client", return_value=client), \
         patch.object(su, "session_scope", _Scope(session)), \
         patch.object(su, "_is_tracked_by_db", AsyncMock(return_value=True)):
        n = await su.cancel_dangling_orders(state)

    assert n == 0
    client.delete.assert_not_awaited()
