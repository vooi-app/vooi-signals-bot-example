"""
Unit tests for position sizing logic.
Tests notional formula, MAX_POSITION_SIZE_USD cap, min notional check.
Per spec §8.3.
"""
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


def get_settings_mock(
    position_size_pct: Decimal = Decimal("5"),
    max_position_size_usd_arg: Decimal = Decimal("1000"),
    min_notional: Decimal = Decimal("10"),
):
    class MockSettings:
        default_position_size_pct = position_size_pct
        max_position_size_usd = max_position_size_usd_arg
        min_profit_pct_of_collateral = Decimal("5")
        funding_cost_buffer_bps = Decimal("5")
        vooi_broker_fee_bps_hyperliquid = "15"
        vooi_broker_fee_bps_lighter = "150"
        vooi_broker_fee_bps_aster = "1.5"
        default_sl_pct = Decimal("7")
        fee_fallback_taker_bps_hyperliquid = Decimal("4.5")
        fee_fallback_taker_bps_lighter = Decimal("0.0")
        fee_fallback_taker_bps_aster = Decimal("3.5")

        def get_min_notional_usd(self, exchange: str) -> Decimal:
            return min_notional

        def get_broker_id(self, exchange: str) -> str:
            return "0xbroker"

        def get_fee_fallback_bps(self, exchange: str) -> Decimal:
            return Decimal("4.5")

        def get_broker_fee_bps(self, exchange: str) -> str:
            return {
                "hyperliquid": "15",
                "lighter": "150",
                "aster": "1.5",
            }[exchange.lower()]

    return MockSettings()


class TestPositionSizing:
    """Test position size computation from spec §8.3."""

    @pytest.mark.asyncio
    async def test_basic_notional_formula(self):
        """notional = available_margin × pct% × leverage."""
        # $10,000 margin × 5% × 5x = $2,500 notional
        # but capped at $1,000
        mock_settings = get_settings_mock()
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value={"availableMargin": "10000"})

        with patch("bot.orders.settings", mock_settings), \
             patch("bot.orders.get_vooi_client", return_value=mock_client):
            from bot.orders import compute_position_size
            result = await compute_position_size(mock_client, "hyperliquid", 5)

        # 10000 * 5% * 5 = 2500, but capped at 1000
        assert result == Decimal("1000")

    @pytest.mark.asyncio
    async def test_max_position_size_cap(self):
        """notional must not exceed MAX_POSITION_SIZE_USD."""
        mock_settings = get_settings_mock(
            position_size_pct=Decimal("20"),
            max_position_size_usd_arg=Decimal("1000"),
        )
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value={"availableMargin": "50000"})

        with patch("bot.orders.settings", mock_settings), \
             patch("bot.orders.get_vooi_client", return_value=mock_client):
            from bot.orders import compute_position_size
            result = await compute_position_size(mock_client, "hyperliquid", 10)

        # 50000 * 20% * 10 = 100000, capped at 1000
        assert result == Decimal("1000")

    @pytest.mark.asyncio
    async def test_notional_below_max_cap(self):
        """Small account: notional stays below cap."""
        mock_settings = get_settings_mock(
            position_size_pct=Decimal("5"),
            max_position_size_usd_arg=Decimal("1000"),
        )
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value={"availableMargin": "1000"})

        with patch("bot.orders.settings", mock_settings), \
             patch("bot.orders.get_vooi_client", return_value=mock_client):
            from bot.orders import compute_position_size
            result = await compute_position_size(mock_client, "hyperliquid", 5)

        # 1000 * 5% * 5 = 250 → no cap needed
        assert result == Decimal("250")

    @pytest.mark.asyncio
    async def test_zero_margin_returns_zero(self):
        """Zero available margin returns zero notional."""
        mock_settings = get_settings_mock()
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value={"availableMargin": "0"})

        with patch("bot.orders.settings", mock_settings), \
             patch("bot.orders.get_vooi_client", return_value=mock_client):
            from bot.orders import compute_position_size
            result = await compute_position_size(mock_client, "hyperliquid", 5)

        assert result == Decimal("0")

    @pytest.mark.asyncio
    async def test_api_failure_returns_zero(self):
        """API failure returns zero (conservative)."""
        mock_settings = get_settings_mock()
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=Exception("API timeout"))

        with patch("bot.orders.settings", mock_settings), \
             patch("bot.orders.get_vooi_client", return_value=mock_client):
            from bot.orders import compute_position_size
            result = await compute_position_size(mock_client, "hyperliquid", 5)

        assert result == Decimal("0")

    def test_leverage_multiplier_effect(self):
        """Higher leverage → larger notional (up to cap)."""
        # With $200 margin and 5% size:
        # 5x: 200 * 5% * 5 = 50
        # 10x: 200 * 5% * 10 = 100
        margin = Decimal("200")
        pct = Decimal("5")
        cap = Decimal("1000")

        notional_5x = min(margin * pct / 100 * 5, cap)
        notional_10x = min(margin * pct / 100 * 10, cap)

        assert notional_5x == Decimal("50")
        assert notional_10x == Decimal("100")
        assert notional_10x > notional_5x

    def test_size_from_notional_and_price(self):
        """Size = notional / entry_price, rounded down."""
        from bot.tp_calculator import round_size

        notional = Decimal("1000")
        entry_price = Decimal("65000")
        size = notional / entry_price
        size_rounded = round_size(size, 4)

        assert size_rounded == Decimal("0.0153")
        assert size_rounded * entry_price <= notional  # Never over-size

    def test_actual_notional_vs_min_notional(self):
        """Actual notional must be >= min_notional for the order to proceed."""
        from bot.tp_calculator import round_size

        size = round_size(Decimal("1000") / Decimal("65000"), 4)
        actual_notional = size * Decimal("65000")
        min_notional = Decimal("10")

        assert actual_notional >= min_notional, (
            f"actual={actual_notional} below min={min_notional}"
        )

    def test_tiny_account_below_min_notional(self):
        """Very small account may fall below min notional."""
        margin = Decimal("10")  # $10 total margin
        pct = Decimal("5")
        leverage = 2
        entry_price = Decimal("65000")
        cap = Decimal("1000")

        notional = min(margin * pct / 100 * leverage, cap)  # = $1
        size = notional / entry_price  # tiny

        from bot.tp_calculator import round_size
        size_rounded = round_size(size, 4)
        actual_notional = size_rounded * entry_price

        min_notional = Decimal("10")
        # Should be detected and skipped
        assert actual_notional < min_notional
