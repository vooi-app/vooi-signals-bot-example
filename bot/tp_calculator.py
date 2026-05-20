"""
TP price calculator and rounding utilities.
All arithmetic uses Python Decimal with precision=28.

Formulas from spec §8.6.1 and §8.8.2.
"""
import decimal
from decimal import ROUND_DOWN, ROUND_HALF_UP, ROUND_UP, Decimal
from typing import TYPE_CHECKING, Optional

decimal.getcontext().prec = 28

from bot.config import settings

if TYPE_CHECKING:
    from bot.models import Position


def compute_tp_price(
    avg_entry_price: Decimal,
    side: str,
    leverage: int,
    exit_fees_bps_round_trip: Decimal,
    exit_slippage_bps: Decimal,
) -> Decimal:
    """
    Compute TP price per spec §8.6.1.

    Args:
        avg_entry_price: Confirmed fill price (from SSE avgEntryPrice)
        side: 'buy' or 'sell'
        leverage: Position leverage multiplier
        exit_fees_bps_round_trip: (exit_taker_bps + builder_fee_bps) * 2
        exit_slippage_bps: One-way exit slippage only (limit entry = 0 entry slippage)

    Returns:
        TP price as Decimal

    Both MIN_PROFIT_PCT_OF_COLLATERAL and TP_OVERHEAD_FLOOR_PCT are expressed as
    % of margin (collateral); divide by leverage to convert to % of price.

    Worked example (§8.6.2):
        BTC long, entry=65000, leverage=10
        MIN_PROFIT_PCT_OF_COLLATERAL=5 → required gain 0.5% of price
        TP_OVERHEAD_FLOOR_PCT=2        → floor 0.2% of price
        HL taker fee exit: 4.5 bps + builder 15 bps = 19.5 bps → round-trip 39 bps
        Exit slippage: 5 bps (one-way) + funding 5 bps → 49 bps raw overhead
        49 bps > 20 bps floor → use raw overhead 0.49%
        Total: 0.5% + 0.49% = 0.99% above entry
        TP price: 65000 × 1.0099 ≈ 65643.5
    """
    P = Decimal(str(settings.min_profit_pct_of_collateral))
    L = Decimal(str(leverage))
    fund = Decimal(str(settings.funding_cost_buffer_bps))
    floor_pct = Decimal(str(settings.tp_overhead_floor_pct)) / Decimal("100") / L

    required_gain_pct = P / Decimal("100") / L
    cost_overhead_pct = (exit_fees_bps_round_trip + exit_slippage_bps + fund) / Decimal("10000")
    if cost_overhead_pct < floor_pct:
        cost_overhead_pct = floor_pct
    total_pct = required_gain_pct + cost_overhead_pct

    if side == "buy":
        return avg_entry_price * (Decimal("1") + total_pct)
    else:
        return avg_entry_price * (Decimal("1") - total_pct)


def compute_breakeven_sl_price(
    entry_price: Decimal,
    side: str,
    exit_taker_bps: Decimal,
    builder_bps: Optional[Decimal] = None,
    safety_bps: Decimal = Decimal("5"),
    exchange: Optional[str] = None,
) -> Decimal:
    """
    Compute breakeven SL price — the price at which we exit at net zero.

    Buffer = (exit_taker_bps + builder_bps * 2 + safety_5bps) / 10000
    This covers the cost of exiting without a loss.

    Args:
        entry_price: Confirmed fill price
        side: 'buy' or 'sell'
        exit_taker_bps: Exchange taker fee for exit leg (bps)
        builder_bps: VOOI builder fee (bps). If None, taken from settings for the given exchange.
        safety_bps: Extra safety buffer (bps), default 5
        exchange: Exchange name (required if builder_bps is None)

    Returns:
        Breakeven SL price
    """
    if builder_bps is None:
        if exchange is None:
            raise ValueError("Either builder_bps or exchange must be provided")
        builder_bps = settings.get_broker_fee_bps_effective(exchange)

    # Buffer covers: entry fee already paid + exit fee + builder (both ways) + safety
    buffer_bps = exit_taker_bps + builder_bps * Decimal("2") + safety_bps
    buffer_pct = buffer_bps / Decimal("10000")

    if side == "buy":
        return entry_price * (Decimal("1") + buffer_pct)
    else:
        return entry_price * (Decimal("1") - buffer_pct)


def exit_fees_bps_round_trip(exchange: str, fees_bps_from_quote: Decimal) -> Decimal:
    """
    Compute round-trip exit fee overhead.
    = (exchange_fee + builder_fee) * 2
    Factor of 2 accounts for: exit leg (taker) + entry leg (maker, but we use taker conservatively)
    Builder fee is taken per-exchange (Hyperliquid/Lighter/Aster have different rates).
    """
    builder_bps = settings.get_broker_fee_bps_effective(exchange)
    return (fees_bps_from_quote + builder_bps) * Decimal("2")


def exit_slippage_bps_for_limit(slippage_bps_from_quote: Decimal) -> Decimal:
    """
    For limit orders: only exit slippage applies (entry has zero slippage).
    Returns one-way exit slippage from quote.
    """
    return slippage_bps_from_quote


def get_fallback_fees_bps(exchange: str) -> Decimal:
    """Return fallback taker fee bps when quotes are unavailable."""
    return settings.get_fee_fallback_bps(exchange)


def compute_tp_price_with_fallback(
    avg_entry_price: Decimal,
    side: str,
    leverage: int,
    exchange: str,
    quote_fees_bps: Optional[Decimal] = None,
    quote_slippage_bps: Optional[Decimal] = None,
) -> Decimal:
    """
    Compute TP price with automatic fallback when quotes are unavailable.
    Uses conservative one-way exit slippage of 5 bps when no quote.
    """
    if quote_fees_bps is not None:
        fees_rt = exit_fees_bps_round_trip(exchange, quote_fees_bps)
    else:
        fallback = get_fallback_fees_bps(exchange)
        fees_rt = exit_fees_bps_round_trip(exchange, fallback)

    if quote_slippage_bps is not None:
        slippage = exit_slippage_bps_for_limit(quote_slippage_bps)
    else:
        slippage = Decimal("5")  # conservative one-way fallback per spec §8.6.3

    return compute_tp_price(
        avg_entry_price=avg_entry_price,
        side=side,
        leverage=leverage,
        exit_fees_bps_round_trip=fees_rt,
        exit_slippage_bps=slippage,
    )


def compute_sl_price_from_pct(
    entry_price: Decimal,
    side: str,
    leverage: int,
    sl_pct: Optional[Decimal] = None,
) -> Decimal:
    """
    Compute SL price from a collateral-loss target.

    `DEFAULT_SL_PCT` is interpreted as % of *collateral* (mirroring TP, which
    targets MIN_PROFIT_PCT_OF_COLLATERAL). The corresponding price distance
    is `sl_pct / leverage` — e.g. SL=7%, lev=5 → 1.4% from entry; lev=10 →
    0.7% from entry.
    """
    pct = sl_pct if sl_pct is not None else Decimal(str(settings.default_sl_pct))
    factor = (pct / Decimal("100")) / Decimal(str(leverage))

    if side == "buy":
        return entry_price * (Decimal("1") - factor)
    else:
        return entry_price * (Decimal("1") + factor)


def round_price(price: Decimal, decimals: int, side: str) -> Decimal:
    """
    Round price to given decimal places.
    For buy orders: round DOWN (conservative — don't pay more than calculated)
    For sell orders: round UP (conservative — don't receive less than calculated)
    """
    quantum = Decimal(10) ** -decimals
    if side == "buy":
        return price.quantize(quantum, rounding=ROUND_DOWN)
    else:
        return price.quantize(quantum, rounding=ROUND_UP)


def round_size(size: Decimal, decimals: int) -> Decimal:
    """
    Round position size DOWN to given decimal places.
    Always round down to avoid exceeding available margin.
    """
    quantum = Decimal(10) ** -decimals
    return size.quantize(quantum, rounding=ROUND_DOWN)


def opposite_side(side: str) -> str:
    """Return opposite trading side."""
    if side == "buy":
        return "sell"
    elif side == "sell":
        return "buy"
    raise ValueError(f"Unknown side: {side}")
