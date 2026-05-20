"""
Bug #1 (round 2): place_entry_order must clamp DEFAULT_LEVERAGE to the
per-symbol maxLeverage from /exchange/markets. Lighter caps JTO at 3x and
rejects 5x with HTTP 503 "Invalid leverage: 5 exceeds maxLeverage=3".
"""
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from bot.orders import MarketSettings, place_entry_order
from bot.router import QuoteResult


def _signal(side="buy", entry='[100.0]'):
    sig = MagicMock()
    sig.id = 1
    sig.side = side
    sig.entry_prices_json = entry
    sig.leverage = 5
    sig.stop_loss = None
    sig.skip_reason = None
    return sig


@pytest.fixture
def mock_session():
    s = MagicMock()
    s.add = MagicMock()
    s.flush = AsyncMock()
    return s


def _market_settings():
    return MarketSettings(
        current_leverage=5,
        current_margin_mode="cross",
        base_decimals=2,
        price_decimals=2,
        min_notional_usd=Decimal("10"),
    )


@pytest.mark.asyncio
async def test_leverage_clamped_when_symbol_max_below_default(mock_session):
    client = MagicMock()
    client.post = AsyncMock(return_value={"orderId": "v-1"})
    client.get = AsyncMock(return_value={"availableMargin": "10000"})

    quote = QuoteResult(
        exchange="lighter", symbol_normalized="JTO",
        fees_bps=Decimal("3"), slippage_bps=Decimal("5"),
        total_cost_bps=Decimal("8"),
    )

    pre_setup = AsyncMock()
    with patch("bot.orders.get_vooi_client", return_value=client), \
         patch("bot.orders.get_market_settings", AsyncMock(return_value=_market_settings())), \
         patch("bot.orders.get_max_leverage", AsyncMock(return_value=3)), \
         patch("bot.orders.pre_trade_setup", pre_setup), \
         patch("bot.orders.get_quotes", AsyncMock(return_value=None)), \
         patch("bot.orders.compute_position_size", AsyncMock(return_value=Decimal("100"))):
        await place_entry_order(
            session=mock_session,
            signal=_signal(),
            exchange="lighter",
            symbol_normalized="JTO",
            quote=quote,
        )

    # pre_trade_setup must have been called with the clamped leverage = 3
    assert pre_setup.await_args.args[3] == 3, (
        f"pre_trade_setup called with wrong leverage: {pre_setup.await_args}"
    )

    # The orders row created in the session should carry leverage=3
    order_rows = [o for o in mock_session.add.call_args_list]
    assert order_rows, "no Order row added"
    added_order = order_rows[0].args[0]
    assert added_order.leverage == 3


@pytest.mark.asyncio
async def test_leverage_unchanged_when_symbol_max_above_default(mock_session):
    client = MagicMock()
    client.post = AsyncMock(return_value={"orderId": "v-2"})
    client.get = AsyncMock(return_value={"availableMargin": "10000"})

    quote = QuoteResult(
        exchange="hyperliquid", symbol_normalized="BTC",
        fees_bps=Decimal("3"), slippage_bps=Decimal("5"),
        total_cost_bps=Decimal("8"),
    )

    pre_setup = AsyncMock()
    with patch("bot.orders.get_vooi_client", return_value=client), \
         patch("bot.orders.get_market_settings", AsyncMock(return_value=_market_settings())), \
         patch("bot.orders.get_max_leverage", AsyncMock(return_value=20)), \
         patch("bot.orders.pre_trade_setup", pre_setup), \
         patch("bot.orders.get_quotes", AsyncMock(return_value=None)), \
         patch("bot.orders.compute_position_size", AsyncMock(return_value=Decimal("500"))):
        await place_entry_order(
            session=mock_session,
            signal=_signal(),
            exchange="hyperliquid",
            symbol_normalized="BTC",
            quote=quote,
        )

    assert pre_setup.await_args.args[3] == 5  # default, not clamped


@pytest.mark.asyncio
async def test_leverage_unchanged_when_max_leverage_missing(mock_session):
    """If /exchange/markets does not advertise maxLeverage, fall back to defaults."""
    client = MagicMock()
    client.post = AsyncMock(return_value={"orderId": "v-3"})
    client.get = AsyncMock(return_value={"availableMargin": "10000"})

    quote = QuoteResult(
        exchange="aster", symbol_normalized="ETH",
        fees_bps=Decimal("3"), slippage_bps=Decimal("5"),
        total_cost_bps=Decimal("8"),
    )

    with patch("bot.orders.get_vooi_client", return_value=client), \
         patch("bot.orders.get_market_settings", AsyncMock(return_value=_market_settings())), \
         patch("bot.orders.get_max_leverage", AsyncMock(return_value=None)), \
         patch("bot.orders.pre_trade_setup", AsyncMock()), \
         patch("bot.orders.get_quotes", AsyncMock(return_value=None)), \
         patch("bot.orders.compute_position_size", AsyncMock(return_value=Decimal("100"))):
        await place_entry_order(
            session=mock_session,
            signal=_signal(),
            exchange="aster",
            symbol_normalized="ETH",
            quote=quote,
        )

    order_rows = [o for o in mock_session.add.call_args_list]
    assert order_rows
    assert order_rows[0].args[0].leverage == 5
