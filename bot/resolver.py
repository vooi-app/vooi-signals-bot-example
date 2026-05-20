"""
Symbol resolver — maps signal symbols to VOOI exchange assets.
Caches GET /exchange/markets for 5 minutes to avoid excessive API calls.
"""
import asyncio
import time
from typing import Optional

import structlog

from bot.vooi_client import get_vooi_client

log = structlog.get_logger(__name__)

_CACHE_TTL_SEC = 300  # 5 minutes

# Cache structure: {exchange: {normalized_symbol: market_data}}
_markets_cache: dict[str, dict[str, dict]] = {}
_cache_updated_at: dict[str, float] = {}
_cache_lock = asyncio.Lock()

# Known exchanges
KNOWN_EXCHANGES = ["hyperliquid", "lighter", "aster"]


async def _fetch_markets(exchange: str) -> dict[str, dict]:
    """
    Fetch markets for a single exchange from VOOI API.
    Raises on transport failure — caller decides whether to retain the
    previous cache (we MUST NOT replace good data with an empty dict, otherwise
    `get_size_decimals` silently falls back to a default that produces orders
    the exchange rejects, e.g. "Size '357.1428' has too many decimals").
    """
    client = get_vooi_client()
    data = await client.get("/exchange/markets", params={"exchanges": exchange})
    markets: dict[str, dict] = {}

    if isinstance(data, list):
        items = data
    elif isinstance(data, dict) and "markets" in data:
        items = data["markets"]
    else:
        items = []

    for item in items:
        # VOOI returns baseSymbol; older field aliases kept as fallback.
        asset = (
            item.get("baseSymbol")
            or item.get("asset")
            or item.get("symbol")
            or item.get("name", "")
        )
        if asset:
            markets[asset.upper()] = item
            # Also expose the bare base, in case the asset string is "BTC-PERP" or "BTC/USDT".
            base = asset.split("-")[0].split("/")[0].upper()
            if base not in markets:
                markets[base] = item

    return markets


async def _refresh_cache_if_needed(exchange: str) -> None:
    """
    Refresh markets cache if stale, under lock.

    On fetch failure: keep the previous (possibly stale) cache rather than
    blanking it. An expired cache that still has correct decimals is far safer
    than an empty cache that silently routes to the wrong default precision.
    """
    async with _cache_lock:
        age = time.monotonic() - _cache_updated_at.get(exchange, 0)
        if age <= _CACHE_TTL_SEC:
            return

        log.debug("markets_cache_refresh", exchange=exchange)
        try:
            markets = await _fetch_markets(exchange)
        except Exception as e:
            log.warning(
                "markets_fetch_failed_keep_stale_cache",
                exchange=exchange,
                error=str(e),
                have_previous=bool(_markets_cache.get(exchange)),
            )
            return  # leave previous cache + timestamp untouched

        if not markets:
            # Endpoint returned an empty/unexpected payload. Do not poison the cache.
            log.warning("markets_fetch_empty_keep_stale_cache", exchange=exchange)
            return

        _markets_cache[exchange] = markets
        _cache_updated_at[exchange] = time.monotonic()


async def normalize_symbol(symbol: str, exchange: str) -> Optional[str]:
    """
    Resolve a signal symbol to the exchange-specific asset name.
    Returns None if the symbol is not found on this exchange.

    Examples:
        normalize_symbol("BTC", "hyperliquid") → "BTC"
        normalize_symbol("bitcoin", "aster") → "BTC" (if mapped)
    """
    await _refresh_cache_if_needed(exchange)
    markets = _markets_cache.get(exchange, {})

    # Try exact match (case-insensitive)
    upper = symbol.upper()
    if upper in markets:
        market = markets[upper]
        return market.get("baseSymbol") or market.get("asset") or market.get("symbol") or upper

    # Try with common suffixes
    for suffix in ["-PERP", "USDT", "-USD", "USD", "-USDC"]:
        candidate = upper + suffix
        if candidate in markets:
            market = markets[candidate]
            return market.get("baseSymbol") or market.get("asset") or market.get("symbol") or candidate

    # Try stripping suffixes from the input
    stripped = upper.replace("-PERP", "").replace("USDT", "").replace("-USD", "").replace("USD", "").replace("-USDC", "")
    if stripped != upper and stripped in markets:
        market = markets[stripped]
        return market.get("baseSymbol") or market.get("asset") or market.get("symbol") or stripped

    return None


async def get_available_exchanges(symbol: str) -> list[str]:
    """
    Return list of exchanges where this symbol is available.
    Refreshes all exchange market caches.
    """
    available = []
    for exchange in KNOWN_EXCHANGES:
        resolved = await normalize_symbol(symbol, exchange)
        if resolved is not None:
            available.append(exchange)
    return available


async def get_market_info(symbol: str, exchange: str) -> Optional[dict]:
    """Get full market info dict for symbol on exchange."""
    await _refresh_cache_if_needed(exchange)
    markets = _markets_cache.get(exchange, {})

    upper = symbol.upper()
    if upper in markets:
        return markets[upper]

    for suffix in ["-PERP", "USDT", "-USD", "USD"]:
        candidate = upper + suffix
        if candidate in markets:
            return markets[candidate]

    return None


class MarketDecimalsUnavailable(RuntimeError):
    """Raised when we cannot resolve decimals for a market — we refuse to
    silently guess a precision that the exchange will reject."""


async def get_price_decimals(symbol: str, exchange: str) -> int:
    """
    Get price decimal places for this market (for rounding).

    Raises MarketDecimalsUnavailable if the market cannot be located, so
    callers either re-route or abort placement instead of submitting an order
    with a wrong precision that VOOI / the exchange will reject.
    """
    info = await get_market_info(symbol, exchange)
    if info:
        for field in ("priceDecimals", "pricePrecision", "tickSize"):
            if field in info:
                val = info[field]
                if field == "tickSize":
                    from decimal import Decimal
                    d = Decimal(str(val))
                    return abs(d.as_tuple().exponent)
                return int(val)
    raise MarketDecimalsUnavailable(
        f"priceDecimals unavailable for {symbol} on {exchange}"
    )


async def get_max_leverage(symbol: str, exchange: str) -> Optional[int]:
    """
    Return the per-symbol maxLeverage advertised by `/exchange/markets`.

    Lighter caps some symbols at 3x (e.g. JTO) while DEFAULT_LEVERAGE=5,
    which the exchange rejects with 503 "Invalid leverage: 5 exceeds
    maxLeverage=3". Callers MUST clamp the requested leverage against this
    before POSTing /exchange/leverage.

    Returns None when the market table is missing the symbol — callers should
    fall back to settings.max_leverage in that case.
    """
    info = await get_market_info(symbol, exchange)
    if info and "maxLeverage" in info:
        try:
            return int(info["maxLeverage"])
        except (TypeError, ValueError):
            return None
    return None


async def get_size_decimals(symbol: str, exchange: str) -> int:
    """
    Get size decimal places for this market (for rounding).

    Raises MarketDecimalsUnavailable rather than defaulting to a guess —
    a wrong default (e.g. 4 dp for a market that wants 1 dp) produces orders
    the exchange rejects with HTTP 503 "Size has too many decimals".
    """
    info = await get_market_info(symbol, exchange)
    if info:
        for field in ("sizeDecimals", "sizePrecision", "lotSize", "baseDecimals"):
            if field in info:
                val = info[field]
                if field == "lotSize":
                    from decimal import Decimal
                    d = Decimal(str(val))
                    return abs(d.as_tuple().exponent)
                return int(val)
    raise MarketDecimalsUnavailable(
        f"sizeDecimals unavailable for {symbol} on {exchange}"
    )


def invalidate_cache(exchange: Optional[str] = None) -> None:
    """Force cache invalidation (useful for tests)."""
    if exchange:
        _cache_updated_at.pop(exchange, None)
        _markets_cache.pop(exchange, None)
    else:
        _cache_updated_at.clear()
        _markets_cache.clear()
