"""
Tests for the markets resolver cache behaviour.

Bug: previously, a single timeout on GET /exchange/markets would replace the
good cached market data with an empty dict and advance the TTL timestamp,
locking in the empty cache for 5 minutes. Subsequent `get_size_decimals`
calls then fell back to a default of 4 dp and the exchange rejected orders
with "Size '357.1428' has too many decimals".

After the fix:
  - Empty / failing fetches MUST NOT overwrite a good cache.
  - get_size_decimals / get_price_decimals MUST raise rather than return
    a hard-coded default.
"""
import time
from unittest.mock import AsyncMock, patch

import pytest

import bot.resolver as resolver_module
from bot.resolver import (
    MarketDecimalsUnavailable,
    get_market_info,
    get_price_decimals,
    get_size_decimals,
    invalidate_cache,
)


@pytest.fixture(autouse=True)
def _clean_cache():
    invalidate_cache()
    yield
    invalidate_cache()


async def test_decimals_unavailable_raises():
    """No market entry → raise rather than silently default."""
    with patch.object(
        resolver_module,
        "_fetch_markets",
        AsyncMock(return_value={"OTHER": {"baseSymbol": "OTHER", "baseDecimals": 3, "priceDecimals": 2}}),
    ):
        with pytest.raises(MarketDecimalsUnavailable):
            await get_size_decimals("STRK", "hyperliquid")
        with pytest.raises(MarketDecimalsUnavailable):
            await get_price_decimals("STRK", "hyperliquid")


async def test_resolves_from_basedecimals():
    """When the markets endpoint returns baseDecimals, that's the size precision."""
    markets = {"STRK": {"baseSymbol": "STRK", "baseDecimals": 1, "priceDecimals": 5}}
    with patch.object(resolver_module, "_fetch_markets", AsyncMock(return_value=markets)):
        assert await get_size_decimals("STRK", "hyperliquid") == 1
        assert await get_price_decimals("STRK", "hyperliquid") == 5


async def test_fetch_failure_keeps_previous_cache():
    """A transient failure must NOT blank a good cache."""
    good = {"STRK": {"baseSymbol": "STRK", "baseDecimals": 1, "priceDecimals": 5}}

    # 1st call populates the cache.
    with patch.object(resolver_module, "_fetch_markets", AsyncMock(return_value=good)):
        assert await get_size_decimals("STRK", "hyperliquid") == 1

    # Force the TTL to expire so the next call attempts a refresh.
    resolver_module._cache_updated_at["hyperliquid"] = 0.0

    # 2nd call: fetch raises (timeout). Cache must NOT be cleared.
    with patch.object(
        resolver_module,
        "_fetch_markets",
        AsyncMock(side_effect=TimeoutError("simulated")),
    ):
        # Still resolvable because we kept the previous data.
        assert await get_size_decimals("STRK", "hyperliquid") == 1
        assert await get_price_decimals("STRK", "hyperliquid") == 5


async def test_fetch_returns_empty_keeps_previous_cache():
    """An empty payload must NOT blank a good cache either."""
    good = {"STRK": {"baseSymbol": "STRK", "baseDecimals": 1, "priceDecimals": 5}}
    with patch.object(resolver_module, "_fetch_markets", AsyncMock(return_value=good)):
        assert await get_size_decimals("STRK", "hyperliquid") == 1

    resolver_module._cache_updated_at["hyperliquid"] = 0.0

    with patch.object(resolver_module, "_fetch_markets", AsyncMock(return_value={})):
        assert await get_size_decimals("STRK", "hyperliquid") == 1
