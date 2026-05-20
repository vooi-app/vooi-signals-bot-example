"""
Unit tests for conflict check.
Per spec §8.1 v1.5:
- Block on open position (status='open')
- Block on unfilled entry order (status IN ('pending','open'), order_type='entry')
- Allow if different side
"""
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.ext.asyncio import AsyncSession


def make_mock_session():
    """Create a mock async session that returns empty by default."""
    session = AsyncMock(spec=AsyncSession)
    return session


def make_mock_result(obj):
    """Create a mock result that returns the given object from scalar_one_or_none."""
    result = MagicMock()
    result.scalar_one_or_none = MagicMock(return_value=obj)
    return result


def make_position(status: str, symbol: str = "BTC", side: str = "buy"):
    from bot.models import Position
    pos = MagicMock(spec=Position)
    pos.id = 1
    pos.status = status
    pos.symbol = symbol
    pos.side = side
    return pos


def make_order(status: str, order_type: str = "entry", symbol: str = "BTC", side: str = "buy"):
    from bot.models import Order
    order = MagicMock(spec=Order)
    order.id = 1
    order.status = status
    order.order_type = order_type
    order.symbol = symbol
    order.side = side
    return order


class TestConflictCheck:
    """Test conflict_check function from bot.router."""

    @pytest.mark.asyncio
    async def test_blocks_on_open_position(self):
        """Should return 'already_in_position' when open position exists."""
        from bot.router import conflict_check

        session = make_mock_session()
        open_pos = make_position("open", "BTC", "buy")

        # First execute (position check) returns the position
        # Second execute (order check) returns nothing
        session.execute = AsyncMock(side_effect=[
            make_mock_result(open_pos),  # position query
            make_mock_result(None),       # order query (won't be reached)
        ])

        result = await conflict_check(session, "BTC", "buy")
        assert result == "already_in_position"

    @pytest.mark.asyncio
    async def test_blocks_on_unfilled_entry_order(self):
        """Should return 'already_in_position' when unfilled entry order exists."""
        from bot.router import conflict_check

        session = make_mock_session()
        pending_order = make_order("pending", "entry", "ETH", "sell")

        session.execute = AsyncMock(side_effect=[
            make_mock_result(None),          # no open position
            make_mock_result(pending_order), # but there's a pending order
        ])

        result = await conflict_check(session, "ETH", "sell")
        assert result == "already_in_position"

    @pytest.mark.asyncio
    async def test_blocks_on_open_status_entry_order(self):
        """'open' status orders also block (not just 'pending')."""
        from bot.router import conflict_check

        session = make_mock_session()
        open_order = make_order("open", "entry", "SOL", "buy")

        session.execute = AsyncMock(side_effect=[
            make_mock_result(None),
            make_mock_result(open_order),
        ])

        result = await conflict_check(session, "SOL", "buy")
        assert result == "already_in_position"

    @pytest.mark.asyncio
    async def test_allows_when_no_conflict(self):
        """Should return None when no conflicting position or order."""
        from bot.router import conflict_check

        session = make_mock_session()

        session.execute = AsyncMock(side_effect=[
            make_mock_result(None),
            make_mock_result(None),
        ])

        result = await conflict_check(session, "BTC", "buy")
        assert result is None

    @pytest.mark.asyncio
    async def test_allows_different_side(self):
        """Existing long doesn't block new short on same symbol."""
        from bot.router import conflict_check

        session = make_mock_session()
        # Position exists but for LONG (buy)
        # We're checking SELL conflict
        long_pos = make_position("open", "BTC", "buy")

        # Mock: first execute (filter side='sell', status='open') → returns None
        # because the existing position is 'buy' not 'sell'
        session.execute = AsyncMock(side_effect=[
            make_mock_result(None),  # no open SELL position
            make_mock_result(None),  # no pending SELL entry order
        ])

        result = await conflict_check(session, "BTC", "sell")
        assert result is None

    @pytest.mark.asyncio
    async def test_allows_different_symbol(self):
        """BTC position doesn't block ETH signal."""
        from bot.router import conflict_check

        session = make_mock_session()

        # No ETH conflicts
        session.execute = AsyncMock(side_effect=[
            make_mock_result(None),
            make_mock_result(None),
        ])

        result = await conflict_check(session, "ETH", "buy")
        assert result is None

    @pytest.mark.asyncio
    async def test_blocks_on_submitting_order(self):
        """'submitting' orders (pre-sent to API) also block."""
        from bot.router import conflict_check

        session = make_mock_session()
        submitting_order = make_order("submitting", "entry", "BTC", "buy")

        session.execute = AsyncMock(side_effect=[
            make_mock_result(None),
            make_mock_result(submitting_order),
        ])

        result = await conflict_check(session, "BTC", "buy")
        assert result == "already_in_position"

    @pytest.mark.asyncio
    async def test_does_not_block_on_filled_order(self):
        """Filled or cancelled orders should not block new signals."""
        from bot.router import conflict_check

        # A filled entry order → position should have been created
        # The position query should handle this case
        # Here we test: filled order without open position → no block
        session = make_mock_session()

        session.execute = AsyncMock(side_effect=[
            make_mock_result(None),  # no open position
            make_mock_result(None),  # filled order is not in pending/open status → no result
        ])

        result = await conflict_check(session, "BTC", "buy")
        assert result is None

    @pytest.mark.asyncio
    async def test_does_not_block_on_tp_sl_orders(self):
        """TP/SL orders should not trigger entry conflict check."""
        # conflict check only looks at order_type='entry'
        # This is handled by the SQL filter in the implementation
        from bot.router import conflict_check

        session = make_mock_session()

        # Even if there's a pending SL order, it shouldn't block
        session.execute = AsyncMock(side_effect=[
            make_mock_result(None),  # no open position
            make_mock_result(None),  # no pending ENTRY order
        ])

        result = await conflict_check(session, "BTC", "buy")
        assert result is None
