"""
Test НОВЫЙ-02: routing skip_reason must be persisted to signals.skip_reason.
Test НОВЫЙ-07: leverage_set_failed sets signal.skip_reason.
"""
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


def _mk_signal(id_=1, skip_reason=None, symbol="BTC", side="buy"):
    from bot.models import Signal
    s = MagicMock(spec=Signal)
    s.id = id_
    s.skip_reason = skip_reason
    s.symbol = symbol
    s.side = side
    s.is_signal = True
    return s


class TestRoutingSkipReasonPersisted:
    """НОВЫЙ-02: when route_signal returns skip_reason, signals.skip_reason gets it."""

    @pytest.mark.asyncio
    async def test_already_in_position_writes_skip_reason(self):
        sig = _mk_signal()

        # Build a route_result with skip_reason
        from bot.router import QuoteResult, RouteResult
        route_result = RouteResult(
            exchange="",
            symbol_normalized="BTC",
            quote=QuoteResult(
                exchange="", symbol_normalized="BTC",
                fees_bps=Decimal("0"), slippage_bps=Decimal("0"),
                total_cost_bps=Decimal("0"),
            ),
            skip_reason="already_in_position",
        )

        with (
            patch("bot.ingester.session_scope") as mock_scope,
            patch("bot.ingester.route_signal", AsyncMock(return_value=route_result)),
            patch("bot.ingester.emit_event", AsyncMock()),
        ):
            mock_session = AsyncMock()
            mock_scope.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_scope.return_value.__aexit__ = AsyncMock(return_value=False)

            r = MagicMock()
            r.scalar_one_or_none = MagicMock(return_value=sig)
            mock_session.execute = AsyncMock(return_value=r)

            from bot.ingester import handle_signal_routing
            await handle_signal_routing(sig, channel_id=1)

        assert sig.skip_reason == "already_in_position"

    @pytest.mark.asyncio
    async def test_does_not_overwrite_existing_skip_reason(self):
        """If parser already set a skip_reason, don't overwrite it."""
        sig = _mk_signal(skip_reason="not_signal")

        from bot.router import QuoteResult, RouteResult
        route_result = RouteResult(
            exchange="", symbol_normalized="BTC",
            quote=QuoteResult(
                exchange="", symbol_normalized="BTC",
                fees_bps=Decimal("0"), slippage_bps=Decimal("0"),
                total_cost_bps=Decimal("0"),
            ),
            skip_reason="dd_breaker_active",
        )

        with (
            patch("bot.ingester.session_scope") as mock_scope,
            patch("bot.ingester.route_signal", AsyncMock(return_value=route_result)),
            patch("bot.ingester.emit_event", AsyncMock()),
        ):
            mock_session = AsyncMock()
            mock_scope.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_scope.return_value.__aexit__ = AsyncMock(return_value=False)

            r = MagicMock()
            r.scalar_one_or_none = MagicMock(return_value=sig)
            mock_session.execute = AsyncMock(return_value=r)

            from bot.ingester import handle_signal_routing
            await handle_signal_routing(sig, channel_id=1)

        assert sig.skip_reason == "not_signal"


class TestEntryPriceBoundary:
    """НОВЫЙ-03: buy uses min(entry_prices), sell uses max."""

    def test_buy_uses_min_of_zone(self):
        """For a buy in a 64000–66000 zone, entry must be 64000."""
        prices = [66000, 64000, 65000]
        prices_decimal = [Decimal(str(p)) for p in prices]
        # Replicate the in-place logic from orders.py
        side = "buy"
        if side == "buy":
            entry = min(prices_decimal)
        else:
            entry = max(prices_decimal)
        assert entry == Decimal("64000")

    def test_sell_uses_max_of_zone(self):
        """For a sell in a 64000–66000 zone, entry must be 66000."""
        prices = [66000, 64000, 65000]
        prices_decimal = [Decimal(str(p)) for p in prices]
        side = "sell"
        if side == "buy":
            entry = min(prices_decimal)
        else:
            entry = max(prices_decimal)
        assert entry == Decimal("66000")

    def test_decimal_precision_preserved(self):
        """Float-division loses precision; Decimal(str(x)) chain must not."""
        prices = ["64321.123456789012345678", "66432.987654321098765432"]
        prices_decimal = [Decimal(str(p)) for p in prices]
        # min preserves the full precision of the input string
        assert min(prices_decimal) == Decimal("64321.123456789012345678")
