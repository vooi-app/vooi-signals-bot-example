"""
Bug #7 (round 2): the breakeven cancel-replace MUST verify the cancel
succeeded before placing a new SL. Otherwise we leave two SL orders on the
exchange book — the old one at the original trigger, plus the new one at
breakeven.

Per Swagger, DELETE /exchange/orders returns {status: "ok"} on success.
"""
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from bot.models import Order


def _make_position(id=1, side="buy", entry_price="100", sl_order_id=5):
    from bot.models import Position
    pos = MagicMock(spec=Position)
    pos.id = id
    pos.side = side
    pos.entry_price = Decimal(entry_price)
    pos.leverage = 5
    pos.status = "open"
    pos.sl_moved_to_be_at = None
    pos.signal_id = 1
    pos.exchange = "hyperliquid"
    pos.symbol = "BTC"
    pos.size = Decimal("0.1")
    pos.sl_order_id = sl_order_id
    pos.sl_price_initial = Decimal("93")
    pos.sl_price_current = Decimal("93")
    return pos


def _make_sl_order(vooi_id="v-old-sl"):
    sl = MagicMock(spec=Order)
    sl.id = 5
    sl.vooi_order_id = vooi_id
    sl.status = "pending"
    return sl


@pytest.mark.asyncio
async def test_cancel_unexpected_response_aborts_replacement():
    """If DELETE response is not {status: 'ok'}, do NOT place a new SL."""
    pos = _make_position()

    with (
        patch("bot.breakeven_watcher.session_scope") as mock_scope,
        patch("bot.breakeven_watcher.get_vooi_client") as mock_get_client,
        patch("bot.breakeven_watcher.get_price_decimals", AsyncMock(return_value=2)),
        patch("bot.breakeven_watcher.get_size_decimals", AsyncMock(return_value=4)),
        patch("bot.breakeven_watcher.settings") as mock_settings,
    ):
        mock_settings.get_broker_fee_bps = MagicMock(return_value="15")
        mock_settings.get_broker_id = MagicMock(return_value="broker-1")
        mock_settings.get_fee_fallback_bps = MagicMock(return_value=Decimal("4.5"))

        mock_session = AsyncMock()
        mock_scope.return_value.__aenter__ = AsyncMock(return_value=mock_session)
        mock_scope.return_value.__aexit__ = AsyncMock(return_value=False)
        results = [_make_position(), _make_sl_order()]
        idx = [0]

        def se(*a, **kw):
            r = MagicMock()
            r.scalar_one_or_none = MagicMock(return_value=results[min(idx[0], len(results) - 1)])
            idx[0] += 1
            return r

        mock_session.execute = AsyncMock(side_effect=se)

        client = MagicMock()
        # Returns 2xx but body says "error" — must not place a new SL.
        client.delete = AsyncMock(return_value={"status": "error", "message": "no order"})
        client.post = AsyncMock()
        mock_get_client.return_value = client

        from bot.breakeven_watcher import move_sl_to_breakeven
        await move_sl_to_breakeven(pos)

        # No POST /exchange/orders was issued for the replacement SL.
        client.post.assert_not_called()
        # The new SL Order row was not added either.
        mock_session.add.assert_not_called()


@pytest.mark.asyncio
async def test_cancel_exception_aborts_replacement():
    """Non-404 cancel exception → abort (avoid double-SL)."""
    pos = _make_position()

    with (
        patch("bot.breakeven_watcher.session_scope") as mock_scope,
        patch("bot.breakeven_watcher.get_vooi_client") as mock_get_client,
        patch("bot.breakeven_watcher.get_price_decimals", AsyncMock(return_value=2)),
        patch("bot.breakeven_watcher.get_size_decimals", AsyncMock(return_value=4)),
        patch("bot.breakeven_watcher.settings") as mock_settings,
    ):
        mock_settings.get_broker_fee_bps = MagicMock(return_value="15")
        mock_settings.get_broker_id = MagicMock(return_value="broker-1")
        mock_settings.get_fee_fallback_bps = MagicMock(return_value=Decimal("4.5"))

        mock_session = AsyncMock()
        mock_scope.return_value.__aenter__ = AsyncMock(return_value=mock_session)
        mock_scope.return_value.__aexit__ = AsyncMock(return_value=False)
        results = [_make_position(), _make_sl_order()]
        idx = [0]

        def se(*a, **kw):
            r = MagicMock()
            r.scalar_one_or_none = MagicMock(return_value=results[min(idx[0], len(results) - 1)])
            idx[0] += 1
            return r

        mock_session.execute = AsyncMock(side_effect=se)

        client = MagicMock()
        client.delete = AsyncMock(side_effect=Exception("connect timeout"))
        client.post = AsyncMock()
        mock_get_client.return_value = client

        from bot.breakeven_watcher import move_sl_to_breakeven
        await move_sl_to_breakeven(pos)

        client.post.assert_not_called()
        mock_session.add.assert_not_called()


@pytest.mark.asyncio
async def test_cancel_ok_proceeds_to_place_new_sl():
    """{status: 'ok'} → cancel confirmed → proceed to place new SL."""
    pos = _make_position()

    with (
        patch("bot.breakeven_watcher.session_scope") as mock_scope,
        patch("bot.breakeven_watcher.get_vooi_client") as mock_get_client,
        patch("bot.breakeven_watcher.get_price_decimals", AsyncMock(return_value=2)),
        patch("bot.breakeven_watcher.get_size_decimals", AsyncMock(return_value=4)),
        patch("bot.breakeven_watcher.settings") as mock_settings,
    ):
        mock_settings.get_broker_fee_bps = MagicMock(return_value="15")
        mock_settings.get_broker_id = MagicMock(return_value="broker-1")
        mock_settings.get_fee_fallback_bps = MagicMock(return_value=Decimal("4.5"))

        mock_session = AsyncMock()
        mock_scope.return_value.__aenter__ = AsyncMock(return_value=mock_session)
        mock_scope.return_value.__aexit__ = AsyncMock(return_value=False)
        results = [_make_position(), _make_sl_order()]
        idx = [0]

        def se(*a, **kw):
            r = MagicMock()
            r.scalar_one_or_none = MagicMock(return_value=results[min(idx[0], len(results) - 1)])
            idx[0] += 1
            return r

        mock_session.execute = AsyncMock(side_effect=se)

        client = MagicMock()
        client.delete = AsyncMock(return_value={"status": "ok"})
        client.post = AsyncMock(return_value={"orderId": "new-sl"})
        mock_get_client.return_value = client

        from bot.breakeven_watcher import move_sl_to_breakeven
        await move_sl_to_breakeven(pos)

        client.post.assert_called()  # new SL was placed
