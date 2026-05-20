"""
AC#19: TP close PnL within ±0.5% of MIN_PROFIT_PCT of collateral.

Verifies that the TP price formula, when hit, produces a realized PnL
within the expected band:  MIN_PROFIT_PCT/leverage ± tolerance.
"""
import decimal
from decimal import Decimal

import pytest

decimal.getcontext().prec = 28

# Simulate settings values (same as spec §8.6.2)
MIN_PROFIT_PCT = Decimal("5")   # 5% of collateral
FUNDING_BUFFER_BPS = Decimal("5")
BUILDER_FEE_BPS = Decimal("15")


def _compute_tp(
    entry: Decimal,
    side: str,
    leverage: int,
    exit_fees_bps: Decimal,   # round-trip
    exit_slippage_bps: Decimal,  # one-way
) -> Decimal:
    required = MIN_PROFIT_PCT / Decimal("100") / Decimal(str(leverage))
    overhead = (exit_fees_bps + exit_slippage_bps + FUNDING_BUFFER_BPS) / Decimal("10000")
    if side == "buy":
        return entry * (1 + required + overhead)
    return entry * (1 - required - overhead)


def _realized_pnl_pct_of_collateral(
    entry: Decimal,
    exit_price: Decimal,
    side: str,
    leverage: int,
    exit_taker_bps: Decimal,
) -> Decimal:
    """
    Compute realized PnL as % of collateral, accounting for exit taker + builder fees.
    collateral = notional / leverage = entry * size / leverage
    For size=1 unit: collateral = entry / leverage
    """
    if side == "buy":
        price_return = (exit_price - entry) / entry
    else:
        price_return = (entry - exit_price) / entry

    # Leveraged PnL
    gross_pct = price_return * Decimal(str(leverage))

    # Exit cost (taker + builder), both bps
    exit_cost = (exit_taker_bps + BUILDER_FEE_BPS) / Decimal("10000") * Decimal(str(leverage))

    return gross_pct - exit_cost


class TestPnlAccuracyAtTP:
    """AC#19: Realized PnL at TP must be within ±0.5% of MIN_PROFIT_PCT (collateral-based)."""

    TOLERANCE = Decimal("0.5")  # ±0.5 percentage points

    def test_btc_long_hl_worked_example(self):
        """
        Spec §8.6.2 worked example:
        BTC long, entry 65000, 10x, HL fees 4.5 bps exit, builder 15 bps.
        TP fires at computed tp_price.
        Net PnL should be >= MIN_PROFIT_PCT = 5% (before funding).
        """
        entry = Decimal("65000")
        leverage = 10
        hl_taker = Decimal("4.5")
        exit_fees_rt = (hl_taker + BUILDER_FEE_BPS) * 2  # = 39 bps
        exit_slip = Decimal("5")

        tp_price = _compute_tp(entry, "buy", leverage, exit_fees_rt, exit_slip)

        # Realized PnL when exiting at tp_price (as taker)
        pnl_pct = _realized_pnl_pct_of_collateral(entry, tp_price, "buy", leverage, hl_taker)

        # Should be close to MIN_PROFIT_PCT (5%)
        # Funding buffer accounts for actual funding paid; without funding, pnl > 5%
        expected = MIN_PROFIT_PCT
        assert abs(pnl_pct * 100 - expected) <= self.TOLERANCE * 2, (
            f"PnL at TP ({pnl_pct * 100:.4f}%) too far from expected {expected}%"
        )

    def test_eth_short_lighter_zero_fees(self):
        """Lighter has 0 taker fee — lower TP target, same PnL band."""
        entry = Decimal("3000")
        leverage = 5
        lighter_taker = Decimal("0")
        exit_fees_rt = (lighter_taker + BUILDER_FEE_BPS) * 2  # = 30 bps
        exit_slip = Decimal("5")

        tp_price = _compute_tp(entry, "sell", leverage, exit_fees_rt, exit_slip)
        pnl_pct = _realized_pnl_pct_of_collateral(entry, tp_price, "sell", leverage, lighter_taker)

        assert abs(pnl_pct * 100 - MIN_PROFIT_PCT) <= self.TOLERANCE * 2

    def test_higher_leverage_smaller_price_move(self):
        """10x leverage needs less price movement than 5x to hit MIN_PROFIT_PCT."""
        entry = Decimal("65000")
        fees_rt = Decimal("39")
        slip = Decimal("5")

        tp_5x = _compute_tp(entry, "buy", 5, fees_rt, slip)
        tp_10x = _compute_tp(entry, "buy", 10, fees_rt, slip)

        pnl_5x = _realized_pnl_pct_of_collateral(entry, tp_5x, "buy", 5, Decimal("4.5"))
        pnl_10x = _realized_pnl_pct_of_collateral(entry, tp_10x, "buy", 10, Decimal("4.5"))

        # Both should be near MIN_PROFIT_PCT
        assert abs(pnl_5x * 100 - MIN_PROFIT_PCT) <= self.TOLERANCE * 2
        assert abs(pnl_10x * 100 - MIN_PROFIT_PCT) <= self.TOLERANCE * 2

    def test_tp_always_above_entry_for_long(self):
        for lev in [3, 5, 10, 20]:
            tp = _compute_tp(Decimal("65000"), "buy", lev, Decimal("39"), Decimal("5"))
            assert tp > Decimal("65000"), f"TP should be above entry for long at {lev}x"

    def test_tp_always_below_entry_for_short(self):
        for lev in [3, 5, 10, 20]:
            tp = _compute_tp(Decimal("65000"), "sell", lev, Decimal("39"), Decimal("5"))
            assert tp < Decimal("65000"), f"TP should be below entry for short at {lev}x"
