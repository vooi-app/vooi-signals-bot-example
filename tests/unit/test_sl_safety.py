"""
Unit tests for sl_safety_check.
Per spec §12 / QA TESTS-03.
"""
import time
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


def make_position(id=1, status="open", exchange="hyperliquid", symbol="BTC", sl_order_id=None, sl_price_current=None):
    from bot.models import Position
    pos = MagicMock(spec=Position)
    pos.id = id
    pos.status = status
    pos.exchange = exchange
    pos.symbol = symbol
    pos.sl_order_id = sl_order_id
    pos.sl_price_current = sl_price_current
    return pos


def make_sl_order(id=1, status="pending"):
    from bot.models import Order
    order = MagicMock(spec=Order)
    order.id = id
    order.status = status
    return order


class TestCheckNakedPositions:
    """Tests for check_naked_positions."""

    @pytest.mark.asyncio
    async def test_detects_position_with_no_sl_order_id(self):
        """Position with sl_order_id=None is naked — triggers alert."""
        pos = make_position(sl_order_id=None)

        with (
            patch("bot.sl_safety.send_naked_position_alert") as mock_alert,
            patch("bot.sl_safety.emit_event", AsyncMock()),
        ):
            mock_session = AsyncMock()
            result = MagicMock()
            result.scalars = MagicMock(return_value=MagicMock(all=MagicMock(return_value=[pos])))
            mock_session.execute = AsyncMock(return_value=result)

            mock_alert.return_value = None

            from bot.sl_safety import check_naked_positions
            await check_naked_positions(mock_session)

            mock_alert.assert_awaited_once_with(pos.id, pos.symbol, pos.exchange)

    @pytest.mark.asyncio
    async def test_detects_cancelled_sl_order(self):
        """Position whose SL order is cancelled is naked."""
        pos = make_position(sl_order_id=5)
        cancelled_sl = make_sl_order(id=5, status="cancelled")

        with (
            patch("bot.sl_safety.send_naked_position_alert") as mock_alert,
            patch("bot.sl_safety.emit_event", AsyncMock()),
        ):
            mock_session = AsyncMock()

            positions_result = MagicMock()
            positions_result.scalars = MagicMock(
                return_value=MagicMock(all=MagicMock(return_value=[pos]))
            )
            # SL order lookup returns nothing (cancelled status not in 'pending','open')
            sl_result = MagicMock()
            sl_result.scalar_one_or_none = MagicMock(return_value=None)

            mock_session.execute = AsyncMock(side_effect=[positions_result, sl_result])
            mock_alert.return_value = None

            from bot.sl_safety import check_naked_positions
            await check_naked_positions(mock_session)

            mock_alert.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_healthy_position_no_alert(self):
        """Position with active SL order does not trigger alert."""
        pos = make_position(sl_order_id=5)
        active_sl = make_sl_order(id=5, status="pending")

        with (
            patch("bot.sl_safety.send_naked_position_alert") as mock_alert,
            patch("bot.sl_safety.emit_event", AsyncMock()),
        ):
            mock_session = AsyncMock()

            positions_result = MagicMock()
            positions_result.scalars = MagicMock(
                return_value=MagicMock(all=MagicMock(return_value=[pos]))
            )
            sl_result = MagicMock()
            sl_result.scalar_one_or_none = MagicMock(return_value=active_sl)

            mock_session.execute = AsyncMock(side_effect=[positions_result, sl_result])

            from bot.sl_safety import check_naked_positions
            await check_naked_positions(mock_session)

            mock_alert.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_no_open_positions_no_alerts(self):
        """No open positions → no alerts sent."""
        with patch("bot.sl_safety.send_naked_position_alert") as mock_alert:
            mock_session = AsyncMock()
            result = MagicMock()
            result.scalars = MagicMock(return_value=MagicMock(all=MagicMock(return_value=[])))
            mock_session.execute = AsyncMock(return_value=result)

            from bot.sl_safety import check_naked_positions
            await check_naked_positions(mock_session)

            mock_alert.assert_not_awaited()


class TestCheckWatcherHeartbeat:
    """Tests for check_watcher_heartbeat."""

    @pytest.mark.asyncio
    async def test_watcher_heartbeat_not_started_no_alert(self):
        """If last_tick == 0.0 (not started), no alert — startup grace period."""
        with (
            patch("bot.breakeven_watcher.tp_breakeven_watcher_last_tick", 0.0),
            patch("bot.sl_safety.emit_event", AsyncMock()) as mock_emit,
            patch("bot.sl_safety.send_watcher_hung_alert", AsyncMock()) as mock_alert,
        ):
            from bot.sl_safety import check_watcher_heartbeat
            await check_watcher_heartbeat()

            mock_emit.assert_not_awaited()
            mock_alert.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_watcher_heartbeat_stale_sends_alert(self):
        """Heartbeat older than 30s triggers alert."""
        stale_time = time.time() - 60  # 60s ago

        with (
            patch("bot.breakeven_watcher.tp_breakeven_watcher_last_tick", stale_time),
            patch("bot.sl_safety.emit_event", AsyncMock()) as mock_emit,
            patch("bot.sl_safety.send_watcher_hung_alert", AsyncMock()) as mock_alert,
        ):
            from bot.sl_safety import check_watcher_heartbeat
            await check_watcher_heartbeat()

            mock_emit.assert_awaited_once()
            mock_alert.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_watcher_heartbeat_fresh_no_alert(self):
        """Fresh heartbeat (< 30s) does not trigger alert."""
        fresh_time = time.time() - 5  # 5s ago

        with (
            patch("bot.breakeven_watcher.tp_breakeven_watcher_last_tick", fresh_time),
            patch("bot.sl_safety.emit_event", AsyncMock()) as mock_emit,
            patch("bot.sl_safety.send_watcher_hung_alert", AsyncMock()) as mock_alert,
        ):
            from bot.sl_safety import check_watcher_heartbeat
            await check_watcher_heartbeat()

            mock_emit.assert_not_awaited()
            mock_alert.assert_not_awaited()
