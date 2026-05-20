"""
Unit tests for reconciler.
Per QA TESTS-04.
"""
import decimal
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

decimal.getcontext().prec = 28


def make_position(id=1, status="open", exchange="hyperliquid", symbol="BTC",
                  entry_order_id=10, opened_at=None, status_updated_at=None):
    from bot.models import Position
    pos = MagicMock(spec=Position)
    pos.id = id
    pos.status = status
    pos.exchange = exchange
    pos.symbol = symbol
    pos.entry_order_id = entry_order_id
    pos.opened_at = opened_at or datetime.now(timezone.utc) - timedelta(seconds=120)
    pos.status_updated_at = status_updated_at
    pos.close_reason = None
    pos.closed_at = None
    return pos


def make_order(id=10, vooi_order_id="v-100", status="pending", order_type="entry",
               avg_fill_price=None, filled_size=None):
    from bot.models import Order
    order = MagicMock(spec=Order)
    order.id = id
    order.vooi_order_id = vooi_order_id
    order.status = status
    order.order_type = order_type
    order.avg_fill_price = avg_fill_price
    order.filled_size = filled_size
    order.cancelled_at = None
    order.exchange = "hyperliquid"
    order.symbol = "BTC"
    return order


class TestCancelExpiredLimitOrders:
    """Tests for cancel_expired_limit_orders."""

    @pytest.mark.asyncio
    async def test_sets_status_expired_and_cancels_via_api(self):
        """Expired orders are cancelled via VOOI API and marked 'expired' in DB."""
        expired_order = make_order(
            id=5, vooi_order_id="v-old",
            status="pending", order_type="entry",
        )
        # No associated position
        no_pos_result = MagicMock()
        no_pos_result.scalar_one_or_none = MagicMock(return_value=None)

        mock_session = AsyncMock()
        orders_result = MagicMock()
        orders_result.scalars = MagicMock(
            return_value=MagicMock(all=MagicMock(return_value=[expired_order]))
        )
        mock_session.execute = AsyncMock(side_effect=[orders_result, no_pos_result])

        mock_client = MagicMock()
        mock_client.delete = AsyncMock(return_value={})

        with (
            patch("bot.reconciler.get_vooi_client", return_value=mock_client),
            patch("bot.reconciler.settings") as mock_settings,
        ):
            mock_settings.limit_order_ttl_hours = 24

            from bot.reconciler import cancel_expired_limit_orders
            await cancel_expired_limit_orders(mock_session)

        mock_client.delete.assert_awaited_once()
        assert expired_order.status == "expired"

    @pytest.mark.asyncio
    async def test_submitting_orders_also_cancelled(self):
        """BUG-08: 'submitting' orders also get cancelled when expired."""
        stuck_order = make_order(id=6, vooi_order_id=None, status="submitting")
        no_pos_result = MagicMock()
        no_pos_result.scalar_one_or_none = MagicMock(return_value=None)

        mock_session = AsyncMock()
        orders_result = MagicMock()
        orders_result.scalars = MagicMock(
            return_value=MagicMock(all=MagicMock(return_value=[stuck_order]))
        )
        mock_session.execute = AsyncMock(side_effect=[orders_result, no_pos_result])

        mock_client = MagicMock()
        mock_client.delete = AsyncMock(return_value={})

        with (
            patch("bot.reconciler.get_vooi_client", return_value=mock_client),
            patch("bot.reconciler.settings") as mock_settings,
        ):
            mock_settings.limit_order_ttl_hours = 24

            from bot.reconciler import cancel_expired_limit_orders
            await cancel_expired_limit_orders(mock_session)

        # No vooi_order_id → delete not called, but status still set to expired
        mock_client.delete.assert_not_awaited()
        assert stuck_order.status == "expired"


class TestSyncOrdersToExchange:
    """Tests for sync_orders_to_exchange — entry-fill detection + cancelled detection."""

    @pytest.mark.asyncio
    async def test_disappeared_order_no_position_marked_cancelled(self):
        """Entry order gone from open-orders AND no exchange position → cancelled_reconciler."""
        db_order = make_order(id=7, vooi_order_id="v-gone", status="pending")
        db_order.exchange = "hyperliquid"
        db_order.symbol = "BTC"

        mock_session = AsyncMock()
        db_orders_result = MagicMock()
        db_orders_result.scalars = MagicMock(
            return_value=MagicMock(all=MagicMock(return_value=[db_order]))
        )
        mock_session.execute = AsyncMock(return_value=db_orders_result)
        mock_session.flush = AsyncMock()

        from bot.reconciler import sync_orders_to_exchange
        await sync_orders_to_exchange(
            mock_session,
            {"hyperliquid": {"open_orders": [], "positions": []}},
        )

        assert db_order.status == "cancelled_reconciler"
        assert db_order.cancelled_at is not None

    @pytest.mark.asyncio
    async def test_disappeared_entry_with_position_marked_filled(self):
        """Entry order gone AND exchange position exists → mark filled and dispatch placer."""
        from decimal import Decimal as D
        db_order = make_order(id=8, vooi_order_id="v-fill", status="pending", order_type="entry")
        db_order.exchange = "lighter"
        db_order.symbol = "TAO"
        db_order.avg_fill_price = None
        db_order.filled_size = None
        db_order.filled_at = None

        mock_session = AsyncMock()
        db_orders_result = MagicMock()
        db_orders_result.scalars = MagicMock(
            return_value=MagicMock(all=MagicMock(return_value=[db_order]))
        )
        mock_session.execute = AsyncMock(return_value=db_orders_result)
        mock_session.flush = AsyncMock()

        state = {
            "lighter": {
                "open_orders": [],
                "positions": [
                    {"baseSymbol": "TAO", "size": "1.5", "entryPrice": "300.0", "side": "buy"}
                ],
            }
        }

        with patch("bot.reconciler.session_scope") as scope_mock:
            from bot.reconciler import sync_orders_to_exchange
            # post_fill_placer.on_entry_filled is called outside session — patch it
            with patch("bot.post_fill_placer.on_entry_filled", AsyncMock()) as mock_placer:
                await sync_orders_to_exchange(mock_session, state)

        assert db_order.status == "filled"
        assert db_order.avg_fill_price == D("300.0")
        assert db_order.filled_size == D("1.5")
        assert db_order.filled_at is not None
        mock_placer.assert_awaited_once_with(8, D("300.0"))


class TestSyncPositionsToExchange:
    """Tests for sync_positions_to_exchange — phantom-position detection."""

    @pytest.mark.asyncio
    async def test_phantom_position_marked_closed(self):
        """Position in DB but no exchange position AND no live entry → phantom."""
        pos = make_position(id=99, status="open", exchange="lighter", symbol="TAO")
        pos.entry_price = None
        pos.size = None
        pos.entry_order_id = None  # no entry order to check

        mock_session = AsyncMock()
        pos_result = MagicMock()
        pos_result.scalars = MagicMock(return_value=MagicMock(all=MagicMock(return_value=[pos])))
        mock_session.execute = AsyncMock(return_value=pos_result)
        mock_session.flush = AsyncMock()

        from bot.reconciler import sync_positions_to_exchange
        await sync_positions_to_exchange(
            mock_session,
            {"lighter": {"open_orders": [], "positions": []}},
        )

        assert pos.status == "closed_manual"
        assert pos.close_reason == "phantom_no_exchange_position"
        assert pos.closed_at is not None

    @pytest.mark.asyncio
    async def test_live_position_left_alone(self):
        """Position with a matching exchange position → keep open, update fields."""
        from decimal import Decimal as D
        pos = make_position(id=100, status="open", exchange="hyperliquid", symbol="BTC")
        pos.entry_price = D("65000")
        pos.size = D("0.01")
        pos.liquidation_price = None
        pos.entry_order_id = None

        mock_session = AsyncMock()
        pos_result = MagicMock()
        pos_result.scalars = MagicMock(return_value=MagicMock(all=MagicMock(return_value=[pos])))
        mock_session.execute = AsyncMock(return_value=pos_result)
        mock_session.flush = AsyncMock()

        from bot.reconciler import sync_positions_to_exchange
        await sync_positions_to_exchange(
            mock_session,
            {"hyperliquid": {
                "open_orders": [],
                "positions": [{
                    "baseSymbol": "BTC",
                    "size": "0.01",
                    "entryPrice": "65000",
                    "liquidationPrice": "60000",
                }],
            }},
        )

        assert pos.status == "open"
        assert pos.liquidation_price == D("60000")
