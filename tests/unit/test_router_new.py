"""
Unit tests for router fixes from code-review.md:
- НОВЫЙ-01: DD breaker auto-resets on a new UTC day
- НОВЫЙ-04: rate limit checks query the orders table (no in-memory state)
"""
import decimal
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

decimal.getcontext().prec = 28


def _mk_runtime_state(value: str, updated_at: datetime):
    from bot.models import RuntimeState
    s = MagicMock(spec=RuntimeState)
    s.key = "dd_breaker_active"
    s.value = value
    s.updated_at = updated_at
    return s


class TestDdBreakerAutoReset:
    """НОВЫЙ-01: breaker set yesterday must auto-reset; set today must still block."""

    @pytest.mark.asyncio
    async def test_breaker_set_yesterday_auto_resets(self):
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        dd_state = _mk_runtime_state("true", yesterday)

        mock_session = AsyncMock()
        # 1st execute: dd_state lookup. 2nd/3rd: rate limit counts → 0
        results = [dd_state]
        idx = [0]

        def execute_side_effect(*args, **kwargs):
            r = MagicMock()
            if idx[0] == 0:
                r.scalar_one_or_none = MagicMock(return_value=results[0])
            else:
                # rate limit count queries
                r.scalar = MagicMock(return_value=0)
            idx[0] += 1
            return r

        mock_session.execute = AsyncMock(side_effect=execute_side_effect)

        with patch("bot.router.settings") as mock_settings:
            mock_settings.max_placements_per_hour_global = 100
            mock_settings.max_placements_per_hour_per_channel = 10

            from bot.router import check_risk_gates
            result = await check_risk_gates(mock_session, channel_id=1, symbol="BTC")

        # Auto-reset: returned None (gate clear), value flipped to "false"
        assert result is None
        assert dd_state.value == "false"

    @pytest.mark.asyncio
    async def test_breaker_set_today_still_blocks(self):
        today = datetime.now(timezone.utc)
        dd_state = _mk_runtime_state("true", today)

        mock_session = AsyncMock()
        result_obj = MagicMock()
        result_obj.scalar_one_or_none = MagicMock(return_value=dd_state)
        mock_session.execute = AsyncMock(return_value=result_obj)

        from bot.router import check_risk_gates
        result = await check_risk_gates(mock_session, channel_id=1, symbol="BTC")

        assert result == "dd_breaker_active"
        assert dd_state.value == "true"  # not auto-reset

    @pytest.mark.asyncio
    async def test_no_breaker_passes_through(self):
        mock_session = AsyncMock()
        idx = [0]

        def se(*a, **kw):
            r = MagicMock()
            if idx[0] == 0:
                r.scalar_one_or_none = MagicMock(return_value=None)
            else:
                r.scalar = MagicMock(return_value=0)
            idx[0] += 1
            return r

        mock_session.execute = AsyncMock(side_effect=se)

        with patch("bot.router.settings") as mock_settings:
            mock_settings.max_placements_per_hour_global = 100
            mock_settings.max_placements_per_hour_per_channel = 10

            from bot.router import check_risk_gates
            result = await check_risk_gates(mock_session, channel_id=1, symbol="BTC")
        assert result is None


class TestDbBackedRateLimits:
    """НОВЫЙ-04: rate limit reads from orders table, survives restarts."""

    @pytest.mark.asyncio
    async def test_global_limit_enforced_from_db_count(self):
        mock_session = AsyncMock()
        idx = [0]

        def se(*a, **kw):
            r = MagicMock()
            if idx[0] == 0:
                # No DD breaker
                r.scalar_one_or_none = MagicMock(return_value=None)
            elif idx[0] == 1:
                # Global count = 100 (at limit)
                r.scalar = MagicMock(return_value=100)
            else:
                r.scalar = MagicMock(return_value=0)
            idx[0] += 1
            return r

        mock_session.execute = AsyncMock(side_effect=se)

        with patch("bot.router.settings") as mock_settings:
            mock_settings.max_placements_per_hour_global = 100
            mock_settings.max_placements_per_hour_per_channel = 10

            from bot.router import check_risk_gates
            result = await check_risk_gates(mock_session, channel_id=1, symbol="BTC")

        assert result == "rate_limit_global"

    @pytest.mark.asyncio
    async def test_per_channel_limit_enforced(self):
        mock_session = AsyncMock()
        idx = [0]

        def se(*a, **kw):
            r = MagicMock()
            if idx[0] == 0:
                r.scalar_one_or_none = MagicMock(return_value=None)  # no breaker
            elif idx[0] == 1:
                r.scalar = MagicMock(return_value=5)  # global ok
            else:
                r.scalar = MagicMock(return_value=10)  # per-channel at limit
            idx[0] += 1
            return r

        mock_session.execute = AsyncMock(side_effect=se)

        with patch("bot.router.settings") as mock_settings:
            mock_settings.max_placements_per_hour_global = 100
            mock_settings.max_placements_per_hour_per_channel = 10

            from bot.router import check_risk_gates
            result = await check_risk_gates(mock_session, channel_id=1, symbol="BTC")

        assert result == "rate_limit_per_channel"


class TestRecordPlacementNoOp:
    """record_placement is a backwards-compat no-op now."""

    def test_record_placement_returns_none(self):
        from bot.router import record_placement
        assert record_placement(channel_id=42) is None
