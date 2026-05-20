"""
Bug #5 (round 2): the new position lifecycle.

`place_entry_order` MUST NOT create a `positions` row. The position only
exists after the entry order fills — at that point `post_fill_placer.
_ensure_position_for_fill` creates it (or finds the legacy row).

A position row created at entry-placement time is semantically wrong: it
claims a position exists on the exchange when really only a limit order is
on the book.
"""
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from bot.models import Order, Position
from bot.orders import MarketSettings, place_entry_order
from bot.router import QuoteResult


def _signal():
    sig = MagicMock()
    sig.id = 1
    sig.side = "buy"
    sig.entry_prices_json = "[100.0]"
    sig.leverage = 5
    sig.stop_loss = None
    sig.skip_reason = None
    return sig


def _market_settings():
    return MarketSettings(
        current_leverage=5,
        current_margin_mode="cross",
        base_decimals=2,
        price_decimals=2,
        min_notional_usd=Decimal("10"),
    )


@pytest.mark.asyncio
async def test_place_entry_order_does_not_create_position_row():
    """No Position row may be `session.add()`'d during entry placement."""
    session = MagicMock()
    session.add = MagicMock()
    session.flush = AsyncMock()

    client = MagicMock()
    client.post = AsyncMock(return_value={"orderId": "VOOI-1"})
    client.get = AsyncMock(return_value={"availableMargin": "10000"})

    quote = QuoteResult(
        exchange="aster", symbol_normalized="HIGH",
        fees_bps=Decimal("3.5"), slippage_bps=Decimal("5"),
        total_cost_bps=Decimal("8.5"),
    )

    with patch("bot.orders.get_vooi_client", return_value=client), \
         patch("bot.orders.get_market_settings", AsyncMock(return_value=_market_settings())), \
         patch("bot.orders.get_max_leverage", AsyncMock(return_value=None)), \
         patch("bot.orders.pre_trade_setup", AsyncMock()), \
         patch("bot.orders.get_quotes", AsyncMock(return_value=None)), \
         patch("bot.orders.compute_position_size", AsyncMock(return_value=Decimal("100"))):
        result = await place_entry_order(
            session=session,
            signal=_signal(),
            exchange="aster",
            symbol_normalized="HIGH",
            quote=quote,
        )

    # Return type changed: now Order, not (Order, Position).
    assert isinstance(result, Order)

    # No Position was added.
    added_types = [type(c.args[0]).__name__ for c in session.add.call_args_list]
    assert "Position" not in added_types, (
        f"Position row created at entry placement (forbidden): {added_types}"
    )
    assert "Order" in added_types


@pytest.mark.asyncio
async def test_post_fill_placer_creates_position_when_missing():
    """When the entry fills and no Position exists, post_fill_placer creates one."""
    import bot.post_fill_placer as ppf

    entry_order = MagicMock(spec=Order)
    entry_order.id = 99
    entry_order.signal_id = 7
    entry_order.exchange = "lighter"
    entry_order.symbol = "TAO"
    entry_order.side = "buy"
    entry_order.price = Decimal("300")
    entry_order.size = Decimal("0.5")
    entry_order.filled_size = Decimal("0.5")
    entry_order.leverage = 5
    entry_order.avg_fill_price = None

    class _FakeSession:
        def __init__(self):
            self.added = []
            self._position_after_add = None
            self._call_count = 0
            self._entry_order = entry_order

        def add(self, obj):
            self.added.append(obj)
            if not getattr(obj, "id", None):
                obj.id = 5
            self._position_after_add = obj

        async def flush(self):
            return None

        async def execute(self, *args, **kwargs):
            self._call_count += 1
            r = MagicMock()
            # 1st call: lookup Order by id → return entry_order
            # 2nd call: lookup Position by entry_order_id → return None initially
            # 3rd call: lookup Signal by id → return None (no signal_row)
            if self._call_count == 1:
                r.scalar_one_or_none = MagicMock(return_value=self._entry_order)
            elif self._call_count == 2:
                r.scalar_one_or_none = MagicMock(return_value=self._position_after_add)
            else:
                r.scalar_one_or_none = MagicMock(return_value=None)
            return r

    fake_session = _FakeSession()

    class _Scope:
        def __init__(self, s): self._s = s
        def __call__(self): return self
        async def __aenter__(self): return self._s
        async def __aexit__(self, *exc): return False

    client = MagicMock()
    client.post = AsyncMock(return_value={"orderId": "VOOI-X"})

    with patch.object(ppf, "session_scope", _Scope(fake_session)), \
         patch.object(ppf, "get_vooi_client", return_value=client), \
         patch.object(ppf, "get_price_decimals", AsyncMock(return_value=2)), \
         patch.object(ppf, "get_size_decimals", AsyncMock(return_value=2)):
        await ppf.on_entry_filled(entry_order_id=99, avg_entry_price=Decimal("300.5"))

    positions_added = [o for o in fake_session.added if isinstance(o, Position)]
    assert len(positions_added) == 1, (
        f"expected 1 Position row added on fill; added: {[type(o).__name__ for o in fake_session.added]}"
    )
    new_pos = positions_added[0]
    assert new_pos.exchange == "lighter"
    assert new_pos.symbol == "TAO"
    assert new_pos.entry_price == Decimal("300.5")
    assert new_pos.size == Decimal("0.5")
    assert new_pos.leverage == 5
