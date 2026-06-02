"""
Unit tests for TP calculator.
Validates worked example from spec §8.6.2 and other formulas.
"""
import decimal
from decimal import Decimal

import pytest

# Set precision for all tests
decimal.getcontext().prec = 28


def get_settings_mock():
    """Return a mock settings object for calculator tests."""
    class MockSettings:
        min_profit_pct_of_collateral = Decimal("5")
        funding_cost_buffer_bps = Decimal("5")
        tp_overhead_floor_pct = Decimal("0")
        # Per-exchange builder fees match production .env values.
        vooi_broker_fee_bps_hyperliquid = "15"
        vooi_broker_fee_bps_lighter = "150"
        vooi_broker_fee_bps_aster = "1.5"
        default_sl_pct = Decimal("7")
        fee_fallback_taker_bps_hyperliquid = Decimal("4.5")
        fee_fallback_taker_bps_lighter = Decimal("0.0")
        fee_fallback_taker_bps_aster = Decimal("3.5")
        # Level-widening knobs disabled here so these tests exercise the pure
        # cost-based formula; production defaults differ (see .env.example).
        base_extra_distance_pct = Decimal("0")
        symmetric_tp_sl = False
        min_sl_distance_pct_hyperliquid = Decimal("0")
        min_sl_distance_pct_lighter = Decimal("0")
        min_sl_distance_pct_aster = Decimal("0")

        def get_broker_fee_bps(self, exchange: str) -> str:
            mapping = {
                "hyperliquid": self.vooi_broker_fee_bps_hyperliquid,
                "lighter": self.vooi_broker_fee_bps_lighter,
                "aster": self.vooi_broker_fee_bps_aster,
            }
            return mapping[exchange.lower()]

        def get_min_sl_distance_pct(self, exchange: str) -> Decimal:
            return Decimal("0")
    return MockSettings()


# Patch settings in tp_calculator before import
import unittest.mock as mock
mock_settings = get_settings_mock()

with mock.patch("bot.config.settings", mock_settings):
    from bot.tp_calculator import (
        compute_breakeven_sl_price,
        compute_sl_price_from_pct,
        compute_tp_price,
        exit_fees_bps_round_trip,
        exit_slippage_bps_for_limit,
        round_price,
        round_size,
    )


class TestComputeTpPrice:
    """Test TP price calculation from spec §8.6.1 and §8.6.2."""

    def test_worked_example_spec_8_6_2(self):
        """
        Spec §8.6.2 worked example:
        BTC long, entry 65000, leverage 10
        HL taker fee exit: 4.5 bps + builder 15 bps = 19.5 bps one-way exit
        Round-trip fee overhead: 19.5 × 2 = 39 bps
        Exit slippage: 5 bps (one-way)
        Funding buffer: 5 bps
        Total cost overhead: 49 bps = 0.49%
        Required net gain: 5% / 10 = 0.5% of entry
        TP price: 65000 × 1.0099 ≈ 65643.5
        """
        avg_entry = Decimal("65000")
        side = "buy"
        leverage = 10

        # HL taker: 4.5 bps, builder: 15 bps → one-way exit = 19.5 bps
        # Round-trip = 19.5 * 2 = 39 bps
        exit_fees_rt = Decimal("39")  # (4.5 + 15) * 2
        exit_slippage = Decimal("5")  # one-way

        with mock.patch("bot.tp_calculator.settings", mock_settings):
            tp_price = compute_tp_price(
                avg_entry_price=avg_entry,
                side=side,
                leverage=leverage,
                exit_fees_bps_round_trip=exit_fees_rt,
                exit_slippage_bps=exit_slippage,
            )

        # required_gain = 5/100/10 = 0.005
        # cost_overhead = (39 + 5 + 5) / 10000 = 0.0049
        # total = 0.0099
        # tp = 65000 * 1.0099 = 65643.5
        assert abs(tp_price - Decimal("65643.5")) < Decimal("1.0"), (
            f"Expected ~65643.5, got {tp_price}"
        )

    def test_worked_example_exact_value(self):
        """Verify the math matches exactly."""
        avg_entry = Decimal("65000")
        leverage = 10
        exit_fees_rt = Decimal("39")
        exit_slippage = Decimal("5")

        with mock.patch("bot.tp_calculator.settings", mock_settings):
            tp_long = compute_tp_price(avg_entry, "buy", leverage, exit_fees_rt, exit_slippage)
            tp_short = compute_tp_price(avg_entry, "sell", leverage, exit_fees_rt, exit_slippage)

        # Long: entry * (1 + 0.0099) = 65000 * 1.0099 = 65643.5
        expected_long = Decimal("65000") * Decimal("1.0099")
        assert abs(tp_long - expected_long) < Decimal("0.01")

        # Short: entry * (1 - 0.0099) = 65000 * 0.9901 = 64356.5
        expected_short = Decimal("65000") * Decimal("0.9901")
        assert abs(tp_short - expected_short) < Decimal("0.01")

    def test_long_tp_above_entry(self):
        """Long TP must be strictly above entry price."""
        with mock.patch("bot.tp_calculator.settings", mock_settings):
            tp = compute_tp_price(
                Decimal("50000"), "buy", 5,
                Decimal("30"), Decimal("5")
            )
        assert tp > Decimal("50000")

    def test_short_tp_below_entry(self):
        """Short TP must be strictly below entry price."""
        with mock.patch("bot.tp_calculator.settings", mock_settings):
            tp = compute_tp_price(
                Decimal("50000"), "sell", 5,
                Decimal("30"), Decimal("5")
            )
        assert tp < Decimal("50000")

    def test_higher_leverage_smaller_required_gain(self):
        """Higher leverage → smaller required price move to achieve MIN_PROFIT_PCT."""
        with mock.patch("bot.tp_calculator.settings", mock_settings):
            tp_5x = compute_tp_price(Decimal("65000"), "buy", 5, Decimal("39"), Decimal("5"))
            tp_10x = compute_tp_price(Decimal("65000"), "buy", 10, Decimal("39"), Decimal("5"))
        # 10x leverage means we need less price movement to hit 5% collateral profit
        assert tp_10x < tp_5x

    def test_zero_fees(self):
        """With zero fees, TP just needs to cover MIN_PROFIT_PCT/leverage."""
        with mock.patch("bot.tp_calculator.settings", mock_settings):
            tp = compute_tp_price(
                Decimal("100"), "buy", 10,
                Decimal("0"), Decimal("0")
            )
        # required_gain = 5/100/10 = 0.005 = 0.5%
        # funding = 5/10000 = 0.0005
        # total = 0.0055
        expected = Decimal("100") * Decimal("1.0055")
        assert abs(tp - expected) < Decimal("0.001")

    def test_lighter_zero_fees(self):
        """Lighter has 0 taker fee fallback — should yield lower TP than HL."""
        with mock.patch("bot.tp_calculator.settings", mock_settings):
            # HL: fees = (4.5 + 15) * 2 = 39 bps
            tp_hl = compute_tp_price(Decimal("65000"), "buy", 10, Decimal("39"), Decimal("5"))
            # Lighter: fees = (0 + 15) * 2 = 30 bps
            tp_lighter = compute_tp_price(Decimal("65000"), "buy", 10, Decimal("30"), Decimal("5"))

        assert tp_lighter < tp_hl


class TestExitFeesHelper:
    """Test the exit_fees_bps_round_trip helper."""

    def test_hyperliquid_fees(self):
        """HL builder = 15 bps → (4.5 + 15) * 2 = 39 bps."""
        with mock.patch("bot.tp_calculator.settings", mock_settings):
            result = exit_fees_bps_round_trip("hyperliquid", Decimal("4.5"))
        assert result == Decimal("39")

    def test_lighter_fees(self):
        """Lighter builder = 150 bps → (0 + 150) * 2 = 300 bps."""
        with mock.patch("bot.tp_calculator.settings", mock_settings):
            result = exit_fees_bps_round_trip("lighter", Decimal("0"))
        assert result == Decimal("300")

    def test_aster_fees(self):
        """Aster builder = 1.5 bps → (3.5 + 1.5) * 2 = 10 bps."""
        with mock.patch("bot.tp_calculator.settings", mock_settings):
            result = exit_fees_bps_round_trip("aster", Decimal("3.5"))
        assert result == Decimal("10")


class TestComputeBreakevenSlPrice:
    """Test breakeven SL price calculation."""

    def test_long_be_above_entry(self):
        """Breakeven SL for long must be above entry to cover exit costs."""
        with mock.patch("bot.tp_calculator.settings", mock_settings):
            be = compute_breakeven_sl_price(
                entry_price=Decimal("65000"),
                side="buy",
                exit_taker_bps=Decimal("4.5"),
                exchange="hyperliquid",
            )
        assert be > Decimal("65000"), f"Long BE SL {be} should be above entry 65000"

    def test_short_be_below_entry(self):
        """Breakeven SL for short must be below entry."""
        with mock.patch("bot.tp_calculator.settings", mock_settings):
            be = compute_breakeven_sl_price(
                entry_price=Decimal("65000"),
                side="sell",
                exit_taker_bps=Decimal("4.5"),
                exchange="hyperliquid",
            )
        assert be < Decimal("65000"), f"Short BE SL {be} should be below entry 65000"

    def test_be_covers_fees_long(self):
        """
        BE SL should result in near-zero loss when hit.
        buffer = exit_taker + builder*2 + safety = 4.5 + 30 + 5 = 39.5 bps
        For $65000: buffer_price = 65000 * 1.00395 ≈ 65256.75
        """
        with mock.patch("bot.tp_calculator.settings", mock_settings):
            be = compute_breakeven_sl_price(
                entry_price=Decimal("65000"),
                side="buy",
                exit_taker_bps=Decimal("4.5"),
                builder_bps=Decimal("15"),
                safety_bps=Decimal("5"),
            )
        # buffer_bps = 4.5 + 15*2 + 5 = 39.5 bps
        expected = Decimal("65000") * (Decimal("1") + Decimal("39.5") / Decimal("10000"))
        assert abs(be - expected) < Decimal("0.1")

    def test_be_idempotent(self):
        """Same inputs → same output every time."""
        with mock.patch("bot.tp_calculator.settings", mock_settings):
            be1 = compute_breakeven_sl_price(Decimal("100"), "buy", Decimal("5"), exchange="hyperliquid")
            be2 = compute_breakeven_sl_price(Decimal("100"), "buy", Decimal("5"), exchange="hyperliquid")
        assert be1 == be2


class TestComputeSlPriceFromPct:
    """Test SL price from percentage."""

    def test_long_sl_below_entry(self):
        """Long SL must be below entry."""
        with mock.patch("bot.tp_calculator.settings", mock_settings):
            sl = compute_sl_price_from_pct(Decimal("65000"), "buy")
        assert sl < Decimal("65000")

    def test_long_sl_default_7pct(self):
        """Default 7% SL for long."""
        with mock.patch("bot.tp_calculator.settings", mock_settings):
            sl = compute_sl_price_from_pct(Decimal("65000"), "buy")
        expected = Decimal("65000") * Decimal("0.93")
        assert abs(sl - expected) < Decimal("0.01")

    def test_short_sl_above_entry(self):
        """Short SL must be above entry."""
        with mock.patch("bot.tp_calculator.settings", mock_settings):
            sl = compute_sl_price_from_pct(Decimal("65000"), "sell")
        assert sl > Decimal("65000")

    def test_custom_sl_pct(self):
        """Custom SL percentage overrides default."""
        with mock.patch("bot.tp_calculator.settings", mock_settings):
            sl = compute_sl_price_from_pct(Decimal("100"), "buy", Decimal("5"))
        expected = Decimal("100") * Decimal("0.95")
        assert abs(sl - expected) < Decimal("0.001")
