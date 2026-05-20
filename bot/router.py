"""
Signal routing: conflict check, risk gates, exchange selection.
Per spec §8.0, §8.1, §8.2, §8.3 v1.5.
"""
import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional

import structlog
from sqlalchemy import and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from bot.config import settings
from bot.db import session_scope
from bot.models import Order, Position, RuntimeState, Signal
from bot.resolver import KNOWN_EXCHANGES, get_available_exchanges, normalize_symbol
from bot.vooi_client import get_vooi_client

log = structlog.get_logger(__name__)

# НОВЫЙ-09: keep strong reference to the DD-alert task so GC can't collect it mid-flight
_pending_alert_tasks: set[asyncio.Task] = set()


@dataclass
class QuoteResult:
    exchange: str
    symbol_normalized: str
    fees_bps: Decimal
    slippage_bps: Decimal
    total_cost_bps: Decimal
    raw_quote: Optional[dict] = None


@dataclass
class RouteResult:
    exchange: str
    symbol_normalized: str
    quote: QuoteResult
    skip_reason: Optional[str] = None


# НОВЫЙ-04: rate limits are now sourced from the orders table (DB), not in-memory.
# In-memory tracking lost state on restart, breaking the per-hour limit guarantee.


async def conflict_check(
    session: AsyncSession,
    symbol: str,
    side: str,
) -> Optional[str]:
    """
    Check for existing open position or pending entry order for this symbol+side.
    Returns skip_reason string if conflict found, None if clear.

    Per spec §8.1 v1.5:
    - Block on existing open POSITION (status='open')
    - Block on existing UNFILLED entry order (status IN ('pending','open'), order_type='entry')
    """
    # Treat 'open' AND 'open_pending_tp_sl' as conflicts — the position row
    # exists in both, and a second signal must not stack a duplicate entry
    # while we're still placing TP/SL for the first.
    pos_result = await session.execute(
        select(Position).where(
            and_(
                Position.symbol == symbol,
                Position.side == side,
                Position.status.in_(["open", "open_pending_tp_sl"]),
            )
        ).limit(1)
    )
    existing_position = pos_result.scalar_one_or_none()
    if existing_position:
        log.info(
            "conflict_blocked_position",
            symbol=symbol,
            side=side,
            position_id=existing_position.id,
            status=existing_position.status,
        )
        return "already_in_position"

    # Check for unfilled entry orders
    order_result = await session.execute(
        select(Order).where(
            and_(
                Order.symbol == symbol,
                Order.side == side,
                Order.order_type == "entry",
                Order.status.in_(["pending", "open", "submitting"]),
            )
        ).limit(1)
    )
    existing_order = order_result.scalar_one_or_none()
    if existing_order:
        log.info(
            "conflict_blocked_order",
            symbol=symbol,
            side=side,
            order_id=existing_order.id,
            status=existing_order.status,
        )
        return "already_in_position"

    return None


async def check_risk_gates(
    session: AsyncSession,
    channel_id: int,
    symbol: str,
) -> Optional[str]:
    """
    Pre-flight risk gate checks.
    Returns skip_reason if any gate fails, None if all pass.

    Gates:
    1. Daily drawdown circuit breaker
    2. Global placement rate limit (per hour)
    3. Per-channel placement rate limit (per hour)
    """
    # 1. Drawdown circuit breaker
    # НОВЫЙ-01: auto-reset breaker that was set on a previous UTC day.
    # Spec §8.0: "pause until 00:00 UTC". Without this, a once-tripped breaker
    # would silently freeze the bot indefinitely.
    result = await session.execute(
        select(RuntimeState).where(RuntimeState.key == "dd_breaker_active")
    )
    dd_state = result.scalar_one_or_none()
    if dd_state and dd_state.value == "true":
        today_utc = datetime.now(timezone.utc).date()
        last_set_utc = dd_state.updated_at.astimezone(timezone.utc).date() if dd_state.updated_at else today_utc
        if last_set_utc < today_utc:
            log.info(
                "dd_breaker_auto_reset",
                last_set_utc=str(last_set_utc),
                today_utc=str(today_utc),
            )
            dd_state.value = "false"
            await session.flush()
            # fall through to rate-limit checks
        else:
            log.warning("risk_gate_dd_breaker", symbol=symbol)
            return "dd_breaker_active"

    # НОВЫЙ-04: count placements from orders table (survives restarts).
    # Spec §8.0: placements_last_hour := count(orders WHERE submitted_at > NOW() - 1h).
    hour_ago = datetime.now(timezone.utc) - timedelta(hours=1)

    # 2. Global rate limit
    global_count_result = await session.execute(
        select(func.count(Order.id)).where(
            and_(
                Order.order_type == "entry",
                Order.created_at > hour_ago,
            )
        )
    )
    global_count = global_count_result.scalar() or 0
    if global_count >= settings.max_placements_per_hour_global:
        log.warning(
            "risk_gate_global_rate_limit",
            count=global_count,
            limit=settings.max_placements_per_hour_global,
        )
        return "rate_limit_global"

    # 3. Per-channel rate limit (Order → Signal → channel_id via subquery)
    channel_count_result = await session.execute(
        select(func.count(Order.id)).where(
            and_(
                Order.order_type == "entry",
                Order.created_at > hour_ago,
                Order.signal_id.in_(
                    select(Signal.id).where(Signal.channel_id == channel_id)
                ),
            )
        )
    )
    channel_count = channel_count_result.scalar() or 0
    if channel_count >= settings.max_placements_per_hour_per_channel:
        log.warning(
            "risk_gate_channel_rate_limit",
            channel_id=channel_id,
            count=channel_count,
            limit=settings.max_placements_per_hour_per_channel,
        )
        return "rate_limit_per_channel"

    return None


def record_placement(channel_id: int) -> None:
    """
    No-op kept for backward compatibility with callers in ingester.
    НОВЫЙ-04: rate limit state now lives in the orders table; the Order row
    inserted by place_entry_order is the placement record.
    """
    return None


async def get_quotes(
    symbol: str,
    exchange: str,
    side: str,
    quote_size: str,
    leverage: int,
) -> Optional[dict]:
    """Fetch quote from VOOI API for fee and slippage data."""
    client = get_vooi_client()
    try:
        data = await client.get(
            "/exchange/quotes",
            params={
                "asset": symbol,
                "exchanges": exchange,
                "side": side,
                "quoteSize": str(quote_size),
                "leverage": str(leverage),
            },
        )
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict) and item.get("exchange") == exchange:
                    return item.get("quote")
            if data and isinstance(data[0], dict):
                return data[0].get("quote")
        return None
    except Exception as e:
        log.warning("get_quotes_failed", exchange=exchange, symbol=symbol, error=str(e))
        return None


async def select_exchange(
    symbol: str,
    candidates: list[str],
    side: str,
    quote_size: str,
    leverage: int,
) -> Optional[QuoteResult]:
    """
    Select cheapest exchange from candidates based on total cost (fees + slippage).
    Per spec §8.2: min(feesBps + slippageBps).
    Returns QuoteResult for the best exchange, or None if no candidates available.
    """
    best: Optional[QuoteResult] = None

    for exchange in candidates:
        resolved = await normalize_symbol(symbol, exchange)
        if resolved is None:
            log.debug("symbol_not_available", symbol=symbol, exchange=exchange)
            continue

        quote_data = await get_quotes(resolved, exchange, side, quote_size, leverage)

        if quote_data is not None and isinstance(quote_data, dict):
            fees_bps = Decimal(str(quote_data.get("feesBps", 0)))
            slippage_bps = Decimal(str(quote_data.get("slippageBps", 0)))
        else:
            fees_bps = settings.get_fee_fallback_bps(exchange)
            slippage_bps = Decimal("5")
            quote_data = None

        total = fees_bps + slippage_bps
        candidate = QuoteResult(
            exchange=exchange,
            symbol_normalized=resolved,
            fees_bps=fees_bps,
            slippage_bps=slippage_bps,
            total_cost_bps=total,
            raw_quote=quote_data,
        )

        if best is None or total < best.total_cost_bps:
            best = candidate

    return best


async def route_signal(
    session: AsyncSession,
    signal: Signal,
    channel_id: int,
) -> RouteResult:
    """
    Full routing pipeline for a parsed signal:
    1. Validate signal has entry prices (market orders forbidden in v1)
    2. Conflict check
    3. Risk gates
    4. Find available exchanges
    5. Per-exchange conflict check (filter candidates)
    6. Select cheapest exchange
    Returns RouteResult with skip_reason if any step fails.
    """
    import json

    symbol = signal.symbol or ""
    side = signal.side or ""

    # 1. Market order guard — no entry price = no trade in v1
    entry_prices_raw = signal.entry_prices_json
    try:
        entry_prices = json.loads(entry_prices_raw) if entry_prices_raw else []
    except Exception:
        entry_prices = []

    if not entry_prices:
        log.info("signal_skipped_no_entry_price", signal_id=signal.id, symbol=symbol)
        return RouteResult(
            exchange="",
            symbol_normalized=symbol,
            quote=QuoteResult(
                exchange="",
                symbol_normalized=symbol,
                fees_bps=Decimal("0"),
                slippage_bps=Decimal("0"),
                total_cost_bps=Decimal("0"),
            ),
            skip_reason="no_entry_price",
        )

    # 2. Conflict check (symbol+side level — before exchange selection)
    conflict = await conflict_check(session, symbol, side)
    if conflict:
        return RouteResult(
            exchange="",
            symbol_normalized=symbol,
            quote=QuoteResult(
                exchange="",
                symbol_normalized=symbol,
                fees_bps=Decimal("0"),
                slippage_bps=Decimal("0"),
                total_cost_bps=Decimal("0"),
            ),
            skip_reason=conflict,
        )

    # 3. Risk gates
    gate = await check_risk_gates(session, channel_id, symbol)
    if gate:
        return RouteResult(
            exchange="",
            symbol_normalized=symbol,
            quote=QuoteResult(
                exchange="",
                symbol_normalized=symbol,
                fees_bps=Decimal("0"),
                slippage_bps=Decimal("0"),
                total_cost_bps=Decimal("0"),
            ),
            skip_reason=gate,
        )

    # 4. Find exchanges where symbol is available
    available_exchanges = await get_available_exchanges(symbol)
    if not available_exchanges:
        log.warning("signal_no_available_exchange", signal_id=signal.id, symbol=symbol)
        return RouteResult(
            exchange="",
            symbol_normalized=symbol,
            quote=QuoteResult(
                exchange="",
                symbol_normalized=symbol,
                fees_bps=Decimal("0"),
                slippage_bps=Decimal("0"),
                total_cost_bps=Decimal("0"),
            ),
            skip_reason="symbol_not_found",
        )

    # 5. Per-exchange conflict filter — one asset = one exchange.
    #
    #    Exclude any exchange that already has either:
    #      (a) an open position for this symbol (any side), or
    #      (b) a pending entry order for this symbol (any side).
    #
    #    The opposite-side case is what bit us on 2026-05-15: BILL sell was
    #    pending on aster, the next BILL buy signal went there too because
    #    we only filtered on positions. New rule: a second signal for the
    #    same symbol must land on a *different* exchange, or it gets skipped
    #    if none is available.
    filtered_candidates = []
    for exchange in available_exchanges:
        pos_exists = await session.execute(
            select(Position).where(
                and_(
                    Position.exchange == exchange,
                    Position.symbol == symbol,
                    Position.status.in_(["open", "open_pending_tp_sl"]),
                )
            ).limit(1)
        )
        if pos_exists.scalar_one_or_none() is not None:
            log.info(
                "exchange_excluded_existing_position",
                symbol=symbol, exchange=exchange,
            )
            continue
        ord_exists = await session.execute(
            select(Order).where(
                and_(
                    Order.exchange == exchange,
                    Order.symbol == symbol,
                    Order.order_type == "entry",
                    Order.status.in_(["pending", "open", "submitting"]),
                )
            ).limit(1)
        )
        if ord_exists.scalar_one_or_none() is not None:
            log.info(
                "exchange_excluded_existing_order",
                symbol=symbol, exchange=exchange,
            )
            continue
        filtered_candidates.append(exchange)

    if not filtered_candidates:
        return RouteResult(
            exchange="",
            symbol_normalized=symbol,
            quote=QuoteResult(
                exchange="",
                symbol_normalized=symbol,
                fees_bps=Decimal("0"),
                slippage_bps=Decimal("0"),
                total_cost_bps=Decimal("0"),
            ),
            skip_reason="symbol_active_on_all_available_exchanges",
        )

    # 6. Select cheapest exchange
    eff_leverage = signal.leverage or settings.default_leverage
    eff_leverage = min(eff_leverage, settings.max_leverage)
    quote_size_for_probe = str(settings.max_position_size_usd)
    best_quote = await select_exchange(
        symbol,
        filtered_candidates,
        side=side,
        quote_size=quote_size_for_probe,
        leverage=eff_leverage,
    )
    if best_quote is None:
        return RouteResult(
            exchange="",
            symbol_normalized=symbol,
            quote=QuoteResult(
                exchange="",
                symbol_normalized=symbol,
                fees_bps=Decimal("0"),
                slippage_bps=Decimal("0"),
                total_cost_bps=Decimal("0"),
            ),
            skip_reason="no_quote_available",
        )

    log.info(
        "signal_routed",
        signal_id=signal.id,
        symbol=symbol,
        side=side,
        exchange=best_quote.exchange,
        fees_bps=str(best_quote.fees_bps),
        slippage_bps=str(best_quote.slippage_bps),
    )

    return RouteResult(
        exchange=best_quote.exchange,
        symbol_normalized=best_quote.symbol_normalized,
        quote=best_quote,
    )


async def update_dd_state(session: AsyncSession) -> None:
    """
    Update daily drawdown state after position close.
    If DD exceeds DAILY_DD_PCT_PAUSE, activate circuit breaker.
    """
    from sqlalchemy import text
    from datetime import date

    today = date.today()

    # Sum realized PnL for today's closed positions
    result = await session.execute(
        select(func.sum(Position.realized_pnl_usd)).where(
            and_(
                Position.status.in_(["closed_tp", "closed_sl", "closed_breakeven", "closed_manual", "liquidated"]),
                func.date(Position.closed_at) == today,
            )
        )
    )
    total_pnl = result.scalar_one_or_none() or Decimal("0")

    # Get account equity from runtime state
    equity_result = await session.execute(
        select(RuntimeState).where(RuntimeState.key == "account_equity_usd")
    )
    equity_state = equity_result.scalar_one_or_none()

    if equity_state and equity_state.value:
        equity = Decimal(equity_state.value)
        if equity > 0 and total_pnl < 0:
            dd_pct = abs(total_pnl) / equity * 100
            if dd_pct >= settings.daily_dd_pct_pause:
                log.warning(
                    "dd_breaker_triggered",
                    dd_pct=str(dd_pct),
                    threshold=str(settings.daily_dd_pct_pause),
                )
                # Activate circuit breaker
                state = await session.execute(
                    select(RuntimeState).where(RuntimeState.key == "dd_breaker_active")
                )
                existing = state.scalar_one_or_none()
                if existing:
                    existing.value = "true"
                else:
                    session.add(RuntimeState(key="dd_breaker_active", value="true"))

                from bot.alerts import send_dd_breaker_alert
                # НОВЫЙ-09: keep strong reference; bare create_task() can be GC'd
                task = asyncio.create_task(send_dd_breaker_alert(float(dd_pct)))
                _pending_alert_tasks.add(task)
                task.add_done_callback(_pending_alert_tasks.discard)
