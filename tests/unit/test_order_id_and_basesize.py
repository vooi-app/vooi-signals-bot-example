"""
Round-3 fixes:
  #1 — POST /exchange/orders returns only {"status":"ok"}; we must look up
       orderId via /exchange/open-orders and persist it to orders.vooi_order_id.
  #2 — Use VOOI's quoted baseSize (already lot-size-correct for Lighter et al.)
       instead of a local notional/price divide-and-round that Lighter rejects
       with code 21706 "invalid order base or quote amount".
"""
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from bot.orders import MarketSettings, _lookup_vooi_order_id, place_entry_order
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


# ─── #1: vooi_order_id lookup ─────────────────────────────────────────────

@pytest.mark.asyncio
async def test_lookup_vooi_order_id_matches_by_symbol_side_price_size():
    client = MagicMock()
    client.get = AsyncMock(return_value=[
        {"baseSymbol": "OTHER", "side": "buy", "price": "1.00",
         "size": "1", "orderId": "X", "createdAt": "2026-05-14T04:00:00Z"},
        {"baseSymbol": "COMP", "side": "buy", "price": "21.81",
         "size": "1.14", "orderId": "425053486126",
         "createdAt": "2026-05-14T04:57:51Z"},
    ])
    oid = await _lookup_vooi_order_id(
        client, "hyperliquid", "COMP", "buy",
        Decimal("21.81"), Decimal("1.14"), attempts=1, delay_sec=0,
    )
    assert oid == "425053486126"


@pytest.mark.asyncio
async def test_lookup_vooi_order_id_returns_none_when_no_match():
    client = MagicMock()
    client.get = AsyncMock(return_value=[
        {"baseSymbol": "ETH", "side": "buy", "price": "1.0",
         "size": "1", "orderId": "X", "createdAt": "2026-05-14T04:00:00Z"},
    ])
    oid = await _lookup_vooi_order_id(
        client, "hyperliquid", "COMP", "buy",
        Decimal("21.81"), Decimal("1.14"), attempts=1, delay_sec=0,
    )
    assert oid is None


@pytest.mark.asyncio
async def test_lookup_vooi_order_id_picks_most_recent_when_dup():
    """If two identical orders exist, pick the newer createdAt."""
    client = MagicMock()
    client.get = AsyncMock(return_value=[
        {"baseSymbol": "BTC", "side": "buy", "price": "100",
         "size": "1", "orderId": "OLD",
         "createdAt": "2026-05-14T04:00:00Z"},
        {"baseSymbol": "BTC", "side": "buy", "price": "100",
         "size": "1", "orderId": "NEW",
         "createdAt": "2026-05-14T05:00:00Z"},
    ])
    oid = await _lookup_vooi_order_id(
        client, "hyperliquid", "BTC", "buy",
        Decimal("100"), Decimal("1"), attempts=1, delay_sec=0,
    )
    assert oid == "NEW"


@pytest.mark.asyncio
async def test_lookup_vooi_order_id_tolerates_small_price_drift():
    """Match within 0.1% price tolerance — exchange might round-trip differently."""
    client = MagicMock()
    client.get = AsyncMock(return_value=[
        {"baseSymbol": "BTC", "side": "buy", "price": "100.05",
         "size": "1", "orderId": "OK", "createdAt": "2026-05-14T05:00:00Z"},
    ])
    oid = await _lookup_vooi_order_id(
        client, "hyperliquid", "BTC", "buy",
        Decimal("100.00"), Decimal("1"), attempts=1, delay_sec=0,
    )
    assert oid == "OK"


@pytest.mark.asyncio
async def test_place_entry_order_persists_recovered_vooi_order_id(mock_session):
    """End-to-end: POST returns {"status":"ok"}, lookup fills vooi_order_id."""
    client = MagicMock()
    client.post = AsyncMock(return_value={"status": "ok"})
    client.get = AsyncMock(return_value=[
        {"baseSymbol": "BTC", "side": "buy", "price": "100",
         "size": "1.00", "orderId": "RECOVERED-42",
         "createdAt": "2026-05-14T05:00:00Z"},
    ])

    quote = QuoteResult(
        exchange="hyperliquid", symbol_normalized="BTC",
        fees_bps=Decimal("3"), slippage_bps=Decimal("5"),
        total_cost_bps=Decimal("8"),
    )

    with patch("bot.orders.get_vooi_client", return_value=client), \
         patch("bot.orders.get_market_settings",
               AsyncMock(return_value=_market_settings())), \
         patch("bot.orders.get_max_leverage", AsyncMock(return_value=20)), \
         patch("bot.orders.pre_trade_setup", AsyncMock()), \
         patch("bot.orders.get_quotes", AsyncMock(return_value=None)), \
         patch("bot.orders.compute_position_size",
               AsyncMock(return_value=Decimal("100"))):
        await place_entry_order(
            session=mock_session, signal=_signal(),
            exchange="hyperliquid", symbol_normalized="BTC", quote=quote,
        )

    added = [c.args[0] for c in mock_session.add.call_args_list]
    assert added, "no Order row added"
    assert added[0].vooi_order_id == "RECOVERED-42"


# ─── #2: VOOI-quantized baseSize ──────────────────────────────────────────

@pytest.mark.asyncio
async def test_place_entry_uses_basesize_from_quote_not_local_divide(mock_session):
    """When /exchange/quotes returns baseSize, use it verbatim (lot-size-safe)."""
    client = MagicMock()
    client.post = AsyncMock(return_value={"status": "ok"})
    # open-orders match: the size MUST be the VOOI baseSize, not local divide
    client.get = AsyncMock(return_value=[
        {"baseSymbol": "ZEC", "side": "sell", "price": "100",
         "size": "0.15", "orderId": "QUOTED",
         "createdAt": "2026-05-14T05:00:00Z"},
    ])

    quote = QuoteResult(
        exchange="lighter", symbol_normalized="ZEC",
        fees_bps=Decimal("0"), slippage_bps=Decimal("1.4"),
        total_cost_bps=Decimal("1.4"),
    )

    # If we did local divide: 50 / 100 = 0.5, rounded to 0.5 at baseDecimals=2.
    # With VOOI quote returning 0.15, that's what should hit the wire.
    mock_quote_resp = {"baseSize": "0.15", "averageExecutionPrice": "100"}

    with patch("bot.orders.get_vooi_client", return_value=client), \
         patch("bot.orders.get_market_settings",
               AsyncMock(return_value=_market_settings())), \
         patch("bot.orders.get_max_leverage", AsyncMock(return_value=10)), \
         patch("bot.orders.pre_trade_setup", AsyncMock()), \
         patch("bot.orders.get_quotes",
               AsyncMock(return_value=mock_quote_resp)), \
         patch("bot.orders.compute_position_size",
               AsyncMock(return_value=Decimal("50"))):
        await place_entry_order(
            session=mock_session, signal=_signal(side="sell"),
            exchange="lighter", symbol_normalized="ZEC", quote=quote,
        )

    call = next(c for c in client.post.await_args_list
                if c.args and c.args[0] == "/exchange/orders")
    assert call.args[1]["size"] == "0.15", (
        f"size must come from VOOI quote, got {call.args[1]['size']!r}"
    )


@pytest.mark.asyncio
async def test_place_entry_falls_back_to_local_compute_when_no_basesize(mock_session):
    """If quote endpoint returns no baseSize, divide-and-round still works."""
    client = MagicMock()
    client.post = AsyncMock(return_value={"status": "ok"})
    client.get = AsyncMock(return_value=[])  # lookup misses — fine

    quote = QuoteResult(
        exchange="hyperliquid", symbol_normalized="BTC",
        fees_bps=Decimal("4.5"), slippage_bps=Decimal("5"),
        total_cost_bps=Decimal("9.5"),
    )

    with patch("bot.orders.get_vooi_client", return_value=client), \
         patch("bot.orders.get_market_settings",
               AsyncMock(return_value=_market_settings())), \
         patch("bot.orders.get_max_leverage", AsyncMock(return_value=20)), \
         patch("bot.orders.pre_trade_setup", AsyncMock()), \
         patch("bot.orders.get_quotes", AsyncMock(return_value=None)), \
         patch("bot.orders.compute_position_size",
               AsyncMock(return_value=Decimal("100"))):
        await place_entry_order(
            session=mock_session, signal=_signal(),
            exchange="hyperliquid", symbol_normalized="BTC", quote=quote,
        )

    call = next(c for c in client.post.await_args_list
                if c.args and c.args[0] == "/exchange/orders")
    # 100 / 100 = 1.0 at base_decimals=2 → "1.00"
    assert call.args[1]["size"] == "1.00"
