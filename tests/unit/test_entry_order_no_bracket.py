"""
Regression test: entry order POST /exchange/orders MUST NOT include
'stopLoss' or 'takeProfit' fields.

Spec v1.5 §8.5: entry placement is bracket-free; TP and SL are placed
post-fill by post_fill_placer. Aster outright rejects bracket fields
("Aster exchange does not support bracket orders").
"""
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from bot.orders import MarketSettings, place_entry_order
from bot.router import QuoteResult


def make_signal(side="buy", entry_prices='[100.0]'):
    sig = MagicMock()
    sig.id = 1
    sig.side = side
    sig.entry_prices_json = entry_prices
    sig.leverage = 5
    sig.stop_loss = None
    sig.skip_reason = None
    return sig


@pytest.fixture
def mock_session():
    sess = MagicMock()
    sess.add = MagicMock()
    sess.flush = AsyncMock()
    return sess


async def test_entry_body_excludes_bracket_fields(mock_session):
    """The body POSTed to /exchange/orders must NOT have stopLoss / takeProfit."""
    mkt = MarketSettings(
        current_leverage=5,
        current_margin_mode="cross",
        base_decimals=2,
        price_decimals=2,
        min_notional_usd=Decimal("10"),
    )
    mock_client = MagicMock()
    mock_client.post = AsyncMock(return_value={"orderId": "VOOI-123"})
    mock_client.get = AsyncMock(return_value={"availableMargin": "10000"})

    quote = QuoteResult(
        exchange="aster",
        symbol_normalized="HIGH",
        fees_bps=Decimal("3.5"),
        slippage_bps=Decimal("5"),
        total_cost_bps=Decimal("8.5"),
    )

    with patch("bot.orders.get_vooi_client", return_value=mock_client), \
         patch("bot.orders.get_market_settings", AsyncMock(return_value=mkt)), \
         patch("bot.orders.pre_trade_setup", AsyncMock(return_value=None)), \
         patch("bot.orders.get_quotes", AsyncMock(return_value=None)), \
         patch("bot.orders.compute_position_size", AsyncMock(return_value=Decimal("100"))):
        await place_entry_order(
            session=mock_session,
            signal=make_signal(side="buy"),
            exchange="aster",
            symbol_normalized="HIGH",
            quote=quote,
            dry_run=False,
        )

    assert mock_client.post.await_count >= 1
    # Find the /exchange/orders call.
    call = next(
        c for c in mock_client.post.await_args_list if c.args and c.args[0] == "/exchange/orders"
    )
    body = call.args[1]
    assert "stopLoss" not in body, f"entry body must not include stopLoss; got: {body}"
    assert "takeProfit" not in body, f"entry body must not include takeProfit; got: {body}"
    # Spec sanity: it IS a limit order with required fields.
    assert body["timeInForce"] == "gtc"
    assert body["price"]
    assert body["clientOrderId"]


async def test_hyperliquid_entry_uses_0x_client_order_id(mock_session):
    """HL entry's clientOrderId must be 0x + 32 hex chars (34 total)."""
    mkt = MarketSettings(
        current_leverage=5,
        current_margin_mode="cross",
        base_decimals=2,
        price_decimals=2,
        min_notional_usd=Decimal("10"),
    )
    mock_client = MagicMock()
    mock_client.post = AsyncMock(return_value={"orderId": "VOOI-x"})
    mock_client.get = AsyncMock(return_value={"availableMargin": "10000"})

    quote = QuoteResult(
        exchange="hyperliquid",
        symbol_normalized="ATOM",
        fees_bps=Decimal("4.5"),
        slippage_bps=Decimal("5"),
        total_cost_bps=Decimal("9.5"),
    )

    with patch("bot.orders.get_vooi_client", return_value=mock_client), \
         patch("bot.orders.get_market_settings", AsyncMock(return_value=mkt)), \
         patch("bot.orders.pre_trade_setup", AsyncMock(return_value=None)), \
         patch("bot.orders.get_quotes", AsyncMock(return_value=None)), \
         patch("bot.orders.compute_position_size", AsyncMock(return_value=Decimal("200"))):
        await place_entry_order(
            session=mock_session,
            signal=make_signal(side="sell"),
            exchange="hyperliquid",
            symbol_normalized="ATOM",
            quote=quote,
            dry_run=False,
        )

    call = next(
        c for c in mock_client.post.await_args_list if c.args and c.args[0] == "/exchange/orders"
    )
    body = call.args[1]
    cid = body["clientOrderId"]
    import re
    assert re.match(r"^0x[0-9a-f]{32}$", cid), f"bad HL clientOrderId: {cid!r}"
