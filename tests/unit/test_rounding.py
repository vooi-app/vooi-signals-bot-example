"""
Unit tests for round_price and round_size.
Edge cases: very small values, exact boundaries, different decimal counts.
"""
from decimal import Decimal

import pytest

import unittest.mock as mock


def get_settings_mock():
    class MockSettings:
        min_profit_pct_of_collateral = Decimal("5")
        funding_cost_buffer_bps = Decimal("5")
        vooi_broker_fee_bps_hyperliquid = "15"
        vooi_broker_fee_bps_lighter = "150"
        vooi_broker_fee_bps_aster = "1.5"
        default_sl_pct = Decimal("7")
        fee_fallback_taker_bps_hyperliquid = Decimal("4.5")
        fee_fallback_taker_bps_lighter = Decimal("0.0")
        fee_fallback_taker_bps_aster = Decimal("3.5")

        def get_broker_fee_bps(self, exchange: str) -> str:
            mapping = {
                "hyperliquid": self.vooi_broker_fee_bps_hyperliquid,
                "lighter": self.vooi_broker_fee_bps_lighter,
                "aster": self.vooi_broker_fee_bps_aster,
            }
            return mapping[exchange.lower()]
    return MockSettings()


mock_settings = get_settings_mock()

with mock.patch("bot.config.settings", mock_settings):
    from bot.tp_calculator import round_price, round_size


class TestRoundPrice:
    """Test round_price function."""

    def test_buy_rounds_down(self):
        """For buy orders, round price DOWN (pay less)."""
        # 65643.567 with 2 decimals → 65643.56 (not 65643.57)
        result = round_price(Decimal("65643.567"), 2, "buy")
        assert result == Decimal("65643.56")

    def test_sell_rounds_up(self):
        """For sell orders, round price UP (receive more)."""
        # 65643.561 with 2 decimals → 65643.57 (not 65643.56)
        result = round_price(Decimal("65643.561"), 2, "sell")
        assert result == Decimal("65643.57")

    def test_buy_already_exact(self):
        """Exact value should not change."""
        result = round_price(Decimal("65000.00"), 2, "buy")
        assert result == Decimal("65000.00")

    def test_sell_already_exact(self):
        """Exact value should not change."""
        result = round_price(Decimal("65000.00"), 2, "sell")
        assert result == Decimal("65000.00")

    def test_zero_decimals_buy(self):
        """Round to 0 decimal places for buy."""
        result = round_price(Decimal("65999.9"), 0, "buy")
        assert result == Decimal("65999")

    def test_zero_decimals_sell(self):
        """Round to 0 decimal places for sell."""
        result = round_price(Decimal("65000.1"), 0, "sell")
        assert result == Decimal("65001")

    def test_four_decimals_buy(self):
        """Round to 4 decimal places for buy."""
        # 0.14201 with 4 decimals → 0.1420 (down)
        result = round_price(Decimal("0.14201"), 4, "buy")
        assert result == Decimal("0.1420")

    def test_four_decimals_sell(self):
        """Round to 4 decimal places for sell (short TP rounds up)."""
        # 0.14199 with 4 decimals → 0.1420 (up)
        result = round_price(Decimal("0.14199"), 4, "sell")
        assert result == Decimal("0.1420")

    def test_small_price_six_decimals_buy(self):
        """Very small prices (SHIB-like) with 6 decimals for buy."""
        result = round_price(Decimal("0.00001234567"), 6, "buy")
        assert result == Decimal("0.000012")

    def test_small_price_six_decimals_sell(self):
        """Very small prices (SHIB-like) with 6 decimals for sell."""
        result = round_price(Decimal("0.00001200001"), 6, "sell")
        assert result == Decimal("0.000013")

    def test_large_price_no_decimals_buy(self):
        """Large round price for buy."""
        result = round_price(Decimal("1000000.7"), 0, "buy")
        assert result == Decimal("1000000")

    def test_buy_does_not_round_up(self):
        """Buy must never round up — would pay more than calculated."""
        original = Decimal("100.005")
        result = round_price(original, 2, "buy")
        assert result <= original

    def test_sell_does_not_round_down(self):
        """Sell must never round down — would receive less than calculated."""
        original = Decimal("100.005")
        result = round_price(original, 2, "sell")
        assert result >= original

    def test_buy_and_sell_bracket_original(self):
        """buy_rounded <= original <= sell_rounded."""
        price = Decimal("65123.456789")
        buy_r = round_price(price, 2, "buy")
        sell_r = round_price(price, 2, "sell")
        assert buy_r <= price <= sell_r


class TestRoundSize:
    """Test round_size function."""

    def test_rounds_down(self):
        """Size always rounds DOWN — never over-allocate."""
        result = round_size(Decimal("0.12399"), 4)
        assert result == Decimal("0.1239")

    def test_rounds_down_not_up(self):
        """Even 0.12395 with 4 decimals rounds to 0.1239 (DOWN)."""
        result = round_size(Decimal("0.12395"), 4)
        assert result == Decimal("0.1239")

    def test_zero_decimals(self):
        """Round to 0 decimal places."""
        result = round_size(Decimal("1.9999"), 0)
        assert result == Decimal("1")

    def test_two_decimals(self):
        """Standard 2-decimal rounding."""
        result = round_size(Decimal("1.235"), 2)
        assert result == Decimal("1.23")

    def test_exact_value_unchanged(self):
        """Exact values should be unchanged."""
        result = round_size(Decimal("1.2345"), 4)
        assert result == Decimal("1.2345")

    def test_large_size(self):
        """Large position size rounds correctly."""
        result = round_size(Decimal("12345.6789"), 2)
        assert result == Decimal("12345.67")

    def test_small_size_six_decimals(self):
        """Very small sizes with 6 decimal precision."""
        result = round_size(Decimal("0.0001237"), 6)
        assert result == Decimal("0.000123")

    def test_result_always_lte_input(self):
        """Result must always be ≤ input (never over-size)."""
        for size_str in ["1.9999", "0.1239", "100.001", "0.000001"]:
            size = Decimal(size_str)
            result = round_size(size, 4)
            assert result <= size, f"round_size({size}) = {result} > input"
