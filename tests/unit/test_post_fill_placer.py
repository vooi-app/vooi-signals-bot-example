"""
Tests for post_fill_placer.

Per spec §8.7 v1.5: when an entry order transitions to FILLED, place two
separate reduce-only trigger orders (SL: type='sl', TP: type='tp'). Then
flip position.status from 'open_pending_tp_sl' to 'open'.

Key behaviours to lock in:
  - SL and TP are placed as TWO separate POST /exchange/orders calls.
  - Each call uses `trigger`, not `stopLoss` / `takeProfit`, and is reduceOnly.
  - Idempotent: a position whose status is not 'open_pending_tp_sl' is skipped.
"""
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import bot.post_fill_placer as ppf


@pytest.fixture(autouse=True)
def _clear_locks():
    ppf._position_locks.clear()
    yield
    ppf._position_locks.clear()


class _DummySession:
    def __init__(self, position):
        self._position = position
        self.added = []
        self.flushes = 0

    def add(self, obj):
        self.added.append(obj)
        # Give the added order a synthetic id so position.sl_order_id wiring works.
        if not hasattr(obj, "id") or obj.id is None:
            obj.id = 1000 + len(self.added)

    async def flush(self):
        self.flushes += 1

    async def execute(self, *_args, **_kwargs):
        scalar_one_or_none = MagicMock(return_value=self._position)
        result = MagicMock()
        result.scalar_one_or_none = scalar_one_or_none
        return result


class _ScopeFactory:
    def __init__(self, session):
        self._session = session

    def __call__(self):
        return self

    async def __aenter__(self):
        return self._session

    async def __aexit__(self, *exc):
        return False


def make_position(status="open_pending_tp_sl"):
    p = MagicMock()
    p.id = 5
    p.signal_id = 42
    p.entry_order_id = 100
    p.exchange = "aster"
    p.symbol = "HIGH"
    p.side = "buy"
    p.entry_price = Decimal("0.19")
    p.size = Decimal("133.18")
    p.leverage = 5
    p.status = status
    p.tp_price_initial = Decimal("0.21")
    p.sl_price_initial = Decimal("0.17")
    p.sl_order_id = None
    p.tp_order_id = None
    return p


async def test_places_two_trigger_orders():
    position = make_position()
    session = _DummySession(position)

    mock_client = MagicMock()
    mock_client.post = AsyncMock(return_value={"orderId": "VOOI-X"})

    with patch.object(ppf, "session_scope", _ScopeFactory(session)), \
         patch.object(ppf, "get_vooi_client", return_value=mock_client), \
         patch.object(ppf, "get_price_decimals", AsyncMock(return_value=2)), \
         patch.object(ppf, "get_size_decimals", AsyncMock(return_value=2)):
        await ppf.on_entry_filled(entry_order_id=100, avg_entry_price=Decimal("0.20"))

    # Exactly two POSTs to /exchange/orders (SL + TP)
    order_calls = [c for c in mock_client.post.await_args_list if c.args[0] == "/exchange/orders"]
    assert len(order_calls) == 2, f"expected 2 calls (SL+TP), got {len(order_calls)}"

    bodies = [c.args[1] for c in order_calls]
    triggers = [b["trigger"]["type"] for b in bodies]
    assert set(triggers) == {"sl", "tp"}

    # Both must be reduce-only and target the opposite side ("sell" for a long).
    for body in bodies:
        assert body["reduceOnly"] is True
        assert body["side"] == "sell"
        assert "stopLoss" not in body
        assert "takeProfit" not in body
        assert "price" not in body  # trigger orders are stop/take only


async def test_skips_when_status_already_open():
    """Idempotency: if position.status != 'open_pending_tp_sl' we must do nothing."""
    position = make_position(status="open")  # already handled
    session = _DummySession(position)

    mock_client = MagicMock()
    mock_client.post = AsyncMock()

    with patch.object(ppf, "session_scope", _ScopeFactory(session)), \
         patch.object(ppf, "get_vooi_client", return_value=mock_client), \
         patch.object(ppf, "get_price_decimals", AsyncMock(return_value=2)), \
         patch.object(ppf, "get_size_decimals", AsyncMock(return_value=2)):
        await ppf.on_entry_filled(entry_order_id=100)

    mock_client.post.assert_not_called()


async def test_status_flipped_to_open_after_success():
    position = make_position()
    session = _DummySession(position)

    mock_client = MagicMock()
    mock_client.post = AsyncMock(return_value={"orderId": "VOOI-X"})

    with patch.object(ppf, "session_scope", _ScopeFactory(session)), \
         patch.object(ppf, "get_vooi_client", return_value=mock_client), \
         patch.object(ppf, "get_price_decimals", AsyncMock(return_value=2)), \
         patch.object(ppf, "get_size_decimals", AsyncMock(return_value=2)):
        await ppf.on_entry_filled(entry_order_id=100, avg_entry_price=Decimal("0.20"))

    assert position.status == "open"
    # Provisional entry_price updated to the confirmed fill price.
    assert position.entry_price == Decimal("0.20")
    # Position now points to the two new orders.
    assert position.sl_order_id is not None
    assert position.tp_order_id is not None
