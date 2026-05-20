"""
Unit tests for breakeven_watcher.
Tests trigger logic, idempotency guard, cancel-404 race, and price cache seed.
"""
import decimal
import time
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

decimal.getcontext().prec = 28


def make_position(
    id=1, side="buy", entry_price="65000", leverage=10,
    status="open", sl_moved_to_be_at=None, signal_id=1,
    exchange="hyperliquid", symbol="BTC", size="0.01", sl_order_id=None,
):
    from bot.models import Position
    pos = MagicMock(spec=Position)
    pos.id = id
    pos.side = side
    pos.entry_price = Decimal(entry_price)
    pos.leverage = leverage
    pos.status = status
    pos.sl_moved_to_be_at = sl_moved_to_be_at
    pos.signal_id = signal_id
    pos.exchange = exchange
    pos.symbol = symbol
    pos.size = Decimal(size)
    pos.sl_order_id = sl_order_id
    pos.sl_price_initial = Decimal(entry_price) * Decimal("0.93")
    return pos


class TestCheckPositionBreakeven:
    """Tests for _check_position_breakeven."""

    @pytest.mark.asyncio
    async def test_buy_triggers_when_price_above_threshold(self):
        """Long position triggers breakeven when price > entry * (1 + trigger_pct)."""
        pos = make_position(side="buy", entry_price="65000")

        with (
            patch("bot.breakeven_watcher.price_cache", {("hyperliquid", "BTC"): Decimal("67000")}),
            patch("bot.breakeven_watcher.price_cache_updated_at", {("hyperliquid", "BTC"): time.time()}),
            patch("bot.breakeven_watcher.move_sl_to_breakeven", AsyncMock()) as mock_move,
            patch("bot.breakeven_watcher.settings") as mock_settings,
        ):
            mock_settings.sse_price_staleness_threshold_sec = 30
            mock_settings.breakeven_trigger_pct = Decimal("2")  # 2% trigger

            from bot.breakeven_watcher import _check_position_breakeven
            await _check_position_breakeven(pos)

            # 65000 * 1.02 = 66300, cur_price=67000 > 66300 → trigger
            mock_move.assert_awaited_once_with(pos)

    @pytest.mark.asyncio
    async def test_sell_triggers_when_price_below_threshold(self):
        """Short position triggers breakeven when price < entry * (1 - trigger_pct)."""
        pos = make_position(side="sell", entry_price="65000")

        with (
            patch("bot.breakeven_watcher.price_cache", {("hyperliquid", "BTC"): Decimal("63000")}),
            patch("bot.breakeven_watcher.price_cache_updated_at", {("hyperliquid", "BTC"): time.time()}),
            patch("bot.breakeven_watcher.move_sl_to_breakeven", AsyncMock()) as mock_move,
            patch("bot.breakeven_watcher.settings") as mock_settings,
        ):
            mock_settings.sse_price_staleness_threshold_sec = 30
            mock_settings.breakeven_trigger_pct = Decimal("2")

            from bot.breakeven_watcher import _check_position_breakeven
            await _check_position_breakeven(pos)

            # 65000 * 0.98 = 63700, cur_price=63000 < 63700 → trigger
            mock_move.assert_awaited_once_with(pos)

    @pytest.mark.asyncio
    async def test_no_trigger_when_price_not_moved_enough(self):
        """Does not trigger when price hasn't moved enough."""
        pos = make_position(side="buy", entry_price="65000")

        with (
            patch("bot.breakeven_watcher.price_cache", {("hyperliquid", "BTC"): Decimal("65500")}),
            patch("bot.breakeven_watcher.price_cache_updated_at", {("hyperliquid", "BTC"): time.time()}),
            patch("bot.breakeven_watcher.move_sl_to_breakeven", AsyncMock()) as mock_move,
            patch("bot.breakeven_watcher.settings") as mock_settings,
        ):
            mock_settings.sse_price_staleness_threshold_sec = 30
            mock_settings.breakeven_trigger_pct = Decimal("2")

            from bot.breakeven_watcher import _check_position_breakeven
            await _check_position_breakeven(pos)

            mock_move.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_no_price_in_cache_skips(self):
        """Position is skipped when price cache has no entry."""
        pos = make_position(side="buy", entry_price="65000")

        with (
            patch("bot.breakeven_watcher.price_cache", {}),
            patch("bot.breakeven_watcher.price_cache_updated_at", {}),
            patch("bot.breakeven_watcher.move_sl_to_breakeven", AsyncMock()) as mock_move,
            patch("bot.breakeven_watcher.settings") as mock_settings,
        ):
            mock_settings.sse_price_staleness_threshold_sec = 30
            mock_settings.breakeven_trigger_pct = Decimal("2")

            from bot.breakeven_watcher import _check_position_breakeven
            await _check_position_breakeven(pos)

            mock_move.assert_not_awaited()


class TestMoveSlToBreakeven:
    """Tests for move_sl_to_breakeven."""

    @pytest.mark.asyncio
    async def test_idempotency_guard_skips_if_already_moved(self):
        """If sl_moved_to_be_at is set, function exits without placing order."""
        from datetime import datetime, timezone
        pos = make_position(sl_moved_to_be_at=datetime.now(timezone.utc))

        with patch("bot.breakeven_watcher.session_scope") as mock_scope:
            mock_session = AsyncMock()
            mock_scope.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_scope.return_value.__aexit__ = AsyncMock(return_value=False)

            result = MagicMock()
            result.scalar_one_or_none = MagicMock(return_value=pos)
            mock_session.execute = AsyncMock(return_value=result)

            from bot.breakeven_watcher import move_sl_to_breakeven
            await move_sl_to_breakeven(pos)

            # Session should not have placed any order
            mock_session.add.assert_not_called()

    @pytest.mark.asyncio
    async def test_cancel_404_returns_early(self):
        """If cancel returns 404, position was already closed — return without placing new SL."""
        pos = make_position(sl_order_id=5)

        from bot.models import Order
        sl_order = MagicMock(spec=Order)
        sl_order.id = 5
        sl_order.vooi_order_id = "v-old-sl"
        sl_order.status = "pending"

        with (
            patch("bot.breakeven_watcher.session_scope") as mock_scope,
            patch("bot.breakeven_watcher.get_vooi_client") as mock_get_client,
            patch("bot.breakeven_watcher.get_price_decimals", AsyncMock(return_value=2)),
            patch("bot.breakeven_watcher.get_size_decimals", AsyncMock(return_value=4)),
            patch("bot.breakeven_watcher.settings") as mock_settings,
        ):
            mock_settings.breakeven_trigger_pct = Decimal("2")
            mock_settings.get_broker_fee_bps = MagicMock(return_value="15")
            mock_settings.get_fee_fallback_bps = MagicMock(return_value=Decimal("4.5"))
            mock_settings.get_broker_id = MagicMock(return_value="broker-1")

            mock_session = AsyncMock()
            mock_scope.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_scope.return_value.__aexit__ = AsyncMock(return_value=False)

            pos_fresh = make_position(sl_moved_to_be_at=None, sl_order_id=5)
            results = [pos_fresh, sl_order]
            idx = [0]
            def se(*a, **kw):
                r = MagicMock()
                r.scalar_one_or_none = MagicMock(return_value=results[min(idx[0], len(results)-1)])
                idx[0] += 1
                return r
            mock_session.execute = AsyncMock(side_effect=se)

            mock_client = MagicMock()
            mock_client.delete = AsyncMock(side_effect=Exception("404 Not Found"))
            mock_get_client.return_value = mock_client

            from bot.breakeven_watcher import move_sl_to_breakeven
            await move_sl_to_breakeven(pos)

            # Should not have added a new SL order (returned after 404)
            mock_session.add.assert_not_called()


class TestSeedPriceCache:
    """Tests for _seed_price_cache_from_db."""

    @pytest.mark.asyncio
    async def test_seeds_entry_price_for_open_positions(self):
        """BUG-07: open positions without cache entry get seeded from entry_price."""
        pos1 = make_position(id=1, exchange="hyperliquid", symbol="BTC", entry_price="65000")
        pos2 = make_position(id=2, exchange="lighter", symbol="ETH", entry_price="3000")

        cache = {}

        with (
            patch("bot.breakeven_watcher.session_scope") as mock_scope,
            patch("bot.breakeven_watcher.price_cache", cache),
        ):
            mock_session = AsyncMock()
            mock_scope.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_scope.return_value.__aexit__ = AsyncMock(return_value=False)

            result = MagicMock()
            result.scalars = MagicMock(return_value=MagicMock(all=MagicMock(return_value=[pos1, pos2])))
            mock_session.execute = AsyncMock(return_value=result)

            from bot.breakeven_watcher import _seed_price_cache_from_db
            await _seed_price_cache_from_db()

        assert cache.get(("hyperliquid", "BTC")) == Decimal("65000")
        assert cache.get(("lighter", "ETH")) == Decimal("3000")

    @pytest.mark.asyncio
    async def test_does_not_overwrite_existing_cache_entry(self):
        """Existing cache entries (from live SSE) must not be overwritten."""
        pos = make_position(id=1, exchange="hyperliquid", symbol="BTC", entry_price="65000")
        cache = {("hyperliquid", "BTC"): Decimal("66000")}  # live price

        with (
            patch("bot.breakeven_watcher.session_scope") as mock_scope,
            patch("bot.breakeven_watcher.price_cache", cache),
        ):
            mock_session = AsyncMock()
            mock_scope.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_scope.return_value.__aexit__ = AsyncMock(return_value=False)

            result = MagicMock()
            result.scalars = MagicMock(return_value=MagicMock(all=MagicMock(return_value=[pos])))
            mock_session.execute = AsyncMock(return_value=result)

            from bot.breakeven_watcher import _seed_price_cache_from_db
            await _seed_price_cache_from_db()

        assert cache[("hyperliquid", "BTC")] == Decimal("66000")
