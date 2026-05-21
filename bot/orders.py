"""
Order placement module.
Handles: pre-trade setup (leverage/margin), entry-only limit order placement.
Per spec §8.3, §8.4, §8.5 v1.5.
"""
import asyncio
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from bot.config import settings
from bot.db import session_scope
from bot.models import Order, Position, Signal
from bot.resolver import (
    MarketDecimalsUnavailable,
    get_max_leverage,
    get_price_decimals,
    get_size_decimals,
    normalize_symbol,
)
from bot.router import QuoteResult, get_quotes
from bot.streamer import emit_event
from bot.tp_calculator import (
    opposite_side,
    round_price,
    round_size,
)
from bot.vooi_client import VooiClient, get_vooi_client


async def lookup_vooi_order_id_by_client_id(
    client: VooiClient,
    exchange: str,
    client_order_id: str,
    *,
    attempts: int = 3,
    delay_sec: float = 0.4,
    page_size: int = 100,
) -> Optional[str]:
    """
    Exact-match lookup of our placed order's exchange-side orderId by the
    clientOrderId we sent. VOOI's POST /exchange/orders returns only
    {"status":"ok"}, so we have to recover the orderId from a follow-up
    query. /exchange/open-orders works for resting orders but misses fast
    fills that left the book instantly; /exchange/orders (full history)
    includes both — and it always echoes our clientOrderId verbatim, which
    gives us an exact match with no tolerances.
    """
    if not client_order_id:
        return None
    for attempt in range(1, attempts + 1):
        items: list = []
        try:
            r = await client.get(
                "/exchange/orders", params={"exchanges": exchange}
            )
            if isinstance(r, dict):
                items = r.get("items") or []
        except Exception as e:
            log.warning(
                "vooi_order_id_clientid_lookup_failed",
                exchange=exchange,
                client_order_id=client_order_id,
                attempt=attempt,
                error=str(e),
            )
        for x in items[:page_size]:
            if not isinstance(x, dict):
                continue
            if str(x.get("clientOrderId") or "") == client_order_id:
                oid = str(x.get("orderId") or x.get("id") or "").strip()
                if oid:
                    return oid
        if attempt < attempts:
            await asyncio.sleep(delay_sec)
    return None


async def _lookup_vooi_order_id(
    client: VooiClient,
    exchange: str,
    symbol: str,
    side: str,
    price: Decimal,
    size: Decimal,
    *,
    attempts: int = 3,
    delay_sec: float = 0.4,
    price_tol_bps: Decimal = Decimal("10"),  # 0.10%
    size_tol_bps: Decimal = Decimal("10"),
) -> Optional[str]:
    """
    POST /exchange/orders only returns {"status":"ok"}; recover the orderId
    by polling /exchange/open-orders and matching (symbol, side, price, size).
    Picks the most recent createdAt to disambiguate identical re-entries.
    """
    s_upper = symbol.upper()
    s_side = side.lower()
    for attempt in range(1, attempts + 1):
        try:
            open_orders = await client.get(
                "/exchange/open-orders", params={"exchanges": exchange}
            )
        except Exception as e:
            log.warning(
                "vooi_order_id_lookup_http_failed",
                exchange=exchange,
                symbol=symbol,
                attempt=attempt,
                error=str(e),
            )
            open_orders = []

        if isinstance(open_orders, list):
            best: Optional[dict] = None
            best_created = ""
            for o in open_orders:
                if not isinstance(o, dict):
                    continue
                if (o.get("baseSymbol") or o.get("asset") or "").upper() != s_upper:
                    continue
                if (o.get("side") or "").lower() != s_side:
                    continue
                try:
                    o_price = Decimal(str(o.get("price") or 0))
                    o_size = Decimal(str(o.get("size") or 0))
                except Exception:
                    continue
                if price > 0 and abs(o_price - price) / price * Decimal("10000") > price_tol_bps:
                    continue
                if size > 0 and abs(o_size - size) / size * Decimal("10000") > size_tol_bps:
                    continue
                created = str(o.get("createdAt") or "")
                if created >= best_created:
                    best_created = created
                    best = o
            if best is not None:
                oid = str(best.get("orderId") or best.get("id") or "").strip()
                return oid or None

        if attempt < attempts:
            await asyncio.sleep(delay_sec)

    return None


async def verify_trigger_on_exchange(
    client: VooiClient,
    *,
    exchange: str,
    symbol: str,
    side: str,
    trigger_price: Decimal,
    size: Decimal,
    trigger_type: str,
    attempts: int = 3,
    delay_sec: float = 0.5,
) -> Optional[str]:
    """
    Look up a freshly-placed trigger order on the exchange by matching
    (baseSymbol, side, triggerPrice, size, type). Needed because lighter
    returns clientOrderId=null in /exchange/open-orders, which breaks the
    clientOrderId-based recovery path — without this, we cannot tell whether
    a POST that returned {"status":"ok"} actually resulted in an order on
    the exchange (the silent-NAKED bug).
    """
    s_upper = symbol.upper()
    side_l = side.lower()
    api_type = {"sl": "stopLoss", "tp": "takeProfit"}.get(trigger_type, trigger_type)

    for attempt in range(1, attempts + 1):
        open_orders: list = []
        try:
            r = await client.get(
                "/exchange/open-orders", params={"exchanges": exchange}
            )
            if isinstance(r, list):
                open_orders = r
        except Exception as e:
            log.warning(
                "verify_trigger_open_orders_failed",
                exchange=exchange, symbol=symbol, attempt=attempt, error=str(e),
            )

        for o in open_orders:
            if not isinstance(o, dict):
                continue
            if (o.get("baseSymbol") or o.get("asset") or "").upper() != s_upper:
                continue
            if (o.get("side") or "").lower() != side_l:
                continue
            if (o.get("type") or "").lower() != api_type.lower():
                continue
            try:
                o_trig = Decimal(str(o.get("triggerPrice") or 0))
                o_size = Decimal(str(o.get("size") or 0))
            except Exception:
                continue
            if trigger_price > 0 and abs(o_trig - trigger_price) / trigger_price * Decimal("10000") > Decimal("10"):
                continue
            if size > 0 and abs(o_size - size) / size * Decimal("10000") > Decimal("10"):
                continue
            oid = str(o.get("orderId") or o.get("id") or "").strip()
            if oid:
                return oid

        if attempt < attempts:
            await asyncio.sleep(delay_sec)

    return None


async def place_trigger_with_verification(
    client: VooiClient,
    position: Position,
    order_row: Order,
    *,
    trigger_type: str,
    trigger_price: Decimal,
    size: Decimal,
    client_order_id: str,
    timeout_sec: float = 20.0,
) -> bool:
    """
    Place a SL or TP trigger order on VOOI and verify it actually landed on
    the exchange before reporting success.

    On POST returning {"status":"ok"} without a recoverable orderId, falls
    back to clientOrderId lookup, then attribute match (asset/side/triggerPrice
    /size/type). If still nothing found — order is marked 'rejected' and the
    function returns False so the caller can fire NAKED handling. This
    prevents the lighter silent-NAKED class of bug.

    Updates `order_row.status`, `order_row.vooi_order_id`, and `order_row.raw_response`
    in place; caller owns the session/commit.

    Builder/broker metadata is no longer attached client-side — VOOI assigns
    it server-side based on the API key.
    """
    body = {
        "exchange": position.exchange,
        "asset": position.symbol,
        "side": opposite_side(position.side),
        "size": str(size),
        "reduceOnly": True,
        "trigger": {"price": str(trigger_price), "type": trigger_type},
        "clientOrderId": client_order_id,
    }

    for attempt in range(1, 4):
        try:
            response = await asyncio.wait_for(
                client.post("/exchange/orders", body),
                timeout=timeout_sec,
            )
        except Exception as e:
            log.warning(
                "trigger_post_attempt_failed",
                position_id=position.id,
                trigger_type=trigger_type,
                attempt=attempt,
                error=str(e),
            )
            if attempt < 3:
                await asyncio.sleep(1.0)
            continue

        # POST didn't raise. Try to recover the exchange-side orderId.
        vooi_order_id: Optional[str] = None
        if isinstance(response, dict):
            vooi_order_id = (
                str(response.get("orderId") or response.get("id") or "") or None
            )
        if vooi_order_id is None:
            vooi_order_id = await lookup_vooi_order_id_by_client_id(
                client, position.exchange, client_order_id,
            )
        if vooi_order_id is None:
            vooi_order_id = await verify_trigger_on_exchange(
                client,
                exchange=position.exchange,
                symbol=position.symbol,
                side=opposite_side(position.side),
                trigger_price=trigger_price,
                size=size,
                trigger_type=trigger_type,
            )

        if vooi_order_id is None:
            order_row.status = "rejected"
            order_row.raw_response = json.dumps(response, default=str)
            log.error(
                "trigger_unverified_NAKED",
                position_id=position.id,
                trigger_type=trigger_type,
                exchange=position.exchange,
                symbol=position.symbol,
                client_order_id=client_order_id,
                response=str(response)[:200],
            )
            return False

        order_row.vooi_order_id = vooi_order_id
        order_row.status = "pending"
        order_row.raw_response = json.dumps(response, default=str)
        log.info(
            "trigger_placed_verified",
            position_id=position.id,
            order_id=order_row.id,
            trigger_type=trigger_type,
            price=str(trigger_price),
            vooi_order_id=vooi_order_id,
        )
        return True

    order_row.status = "rejected"
    log.error(
        "trigger_post_exhausted",
        position_id=position.id,
        trigger_type=trigger_type,
    )
    return False


def make_client_order_id(signal_id: int, exchange: str, suffix: str = "") -> str:
    """
    Build a clientOrderId acceptable to the target exchange.

    Hyperliquid requires a 128-bit hex string with "0x" prefix (34 chars total:
    "0x" + 32 hex chars). uuid4().hex is exactly 32 hex chars — we use it as-is
    and prepend "0x". The signal_id / suffix are embedded for diagnostics only
    for non-hyperliquid exchanges; on HL the id stays in DB/logs only.

    Lighter / Aster accept arbitrary strings, so we keep the descriptive
    "sigbot-{signal_id}-{...}" form for grep-ability.
    """
    if exchange.lower() == "hyperliquid":
        return "0x" + uuid.uuid4().hex  # exactly 34 chars total
    # Lighter / Aster: human-readable id with optional purpose suffix
    parts = [f"sigbot-{signal_id}"]
    if suffix:
        parts.append(suffix)
    parts.append(uuid.uuid4().hex[:8])
    return "-".join(parts)

log = structlog.get_logger(__name__)


@dataclass
class MarketSettings:
    """Market settings from VOOI API."""
    current_leverage: Optional[int]
    current_margin_mode: Optional[str]
    base_decimals: int
    price_decimals: int
    min_notional_usd: Decimal


async def get_market_settings(
    client: VooiClient,
    exchange: str,
    symbol: str,
) -> MarketSettings:
    """
    Fetch current market settings for leverage/margin AND resolve decimals.

    GET /exchange/market-settings returns only {asset, exchange, leverage,
    marginMode} — decimal precision lives on /exchange/markets and is served
    by the resolver cache. We therefore look up decimals via the resolver,
    propagating MarketDecimalsUnavailable so the caller can abort placement
    rather than guessing a precision the exchange will reject.
    """
    current_leverage: Optional[int] = None
    current_margin_mode: Optional[str] = None
    try:
        data = await client.get(
            "/exchange/market-settings",
            params={"exchange": exchange, "asset": symbol},
        )
        if isinstance(data, dict):
            current_leverage = data.get("leverage")
            current_margin_mode = data.get("marginMode")
    except Exception as e:
        # Non-fatal: pre_trade_setup will simply re-set leverage / margin.
        log.warning(
            "market_settings_fetch_failed",
            exchange=exchange,
            symbol=symbol,
            error=str(e),
        )

    return MarketSettings(
        current_leverage=current_leverage,
        current_margin_mode=current_margin_mode,
        base_decimals=await get_size_decimals(symbol, exchange),
        price_decimals=await get_price_decimals(symbol, exchange),
        min_notional_usd=settings.get_min_notional_usd(exchange),
    )


async def pre_trade_setup(
    client: VooiClient,
    exchange: str,
    symbol: str,
    target_leverage: int,
    target_margin_mode: str,
) -> None:
    """
    Set leverage and margin mode if different from current.
    Per spec §8.4: GET market-settings → POST leverage → POST margin-mode.
    """
    mkt = await get_market_settings(client, exchange, symbol)

    # Set leverage if needed
    if mkt.current_leverage != target_leverage:
        try:
            await client.post(
                "/exchange/leverage",
                {
                    "exchange": exchange,
                    "asset": symbol,
                    "leverage": target_leverage,
                },
            )
            log.info(
                "leverage_set",
                exchange=exchange,
                symbol=symbol,
                leverage=target_leverage,
            )
        except Exception as e:
            log.error("leverage_set_failed", exchange=exchange, symbol=symbol, error=str(e))
            raise

    # Set margin mode if needed
    if mkt.current_margin_mode != target_margin_mode:
        try:
            await client.post(
                "/exchange/margin-mode",
                {
                    "exchange": exchange,
                    "asset": symbol,
                    "marginMode": target_margin_mode,
                },
            )
            log.info(
                "margin_mode_set",
                exchange=exchange,
                symbol=symbol,
                margin_mode=target_margin_mode,
            )
        except Exception as e:
            log.warning(
                "margin_mode_set_failed",
                exchange=exchange,
                symbol=symbol,
                error=str(e),
            )
            # Non-fatal — some exchanges don't support this


async def compute_position_size(
    client: VooiClient,
    exchange: str,
    leverage: int,
) -> Decimal:
    """
    Compute position notional in USD.
    notional = availableMargin × DEFAULT_POSITION_SIZE_PCT% × leverage
    Capped at MAX_POSITION_SIZE_USD.
    Per spec §8.3.
    """
    available_margin = Decimal("0")

    try:
        data = await client.get("/exchange/accounts", params={"exchanges": exchange})
        account: dict = {}
        if isinstance(data, list) and data:
            account = data[0] if isinstance(data[0], dict) else {}
        elif isinstance(data, dict):
            account = data
        for key in ("availableMargin", "availableBalance", "freeBalance", "available"):
            if key in account:
                available_margin = Decimal(str(account[key]))
                break
    except Exception as e:
        log.warning("account_fetch_failed", exchange=exchange, error=str(e))
        # Use a conservative fallback — operator should fix their API key
        available_margin = Decimal("0")

    if available_margin <= 0:
        log.warning("zero_available_margin", exchange=exchange)
        return Decimal("0")

    notional = (
        available_margin
        * settings.default_position_size_pct
        / Decimal("100")
        * Decimal(str(leverage))
    )

    # Apply absolute cap
    notional = min(notional, settings.max_position_size_usd)

    return notional


async def place_entry_order(
    session: AsyncSession,
    signal: Signal,
    exchange: str,
    symbol_normalized: str,
    quote: QuoteResult,
    leverage: Optional[int] = None,
    margin_mode: Optional[str] = None,
    dry_run: bool = False,
) -> Optional["Order"]:
    """
    Place an entry limit order. No `positions` row is created here — the
    position only exists once the entry fills (see post_fill_placer.
    _ensure_position_for_fill). Returns the Order on success, None on skip.
    """
    import json as json_mod

    # Parse signal fields
    try:
        entry_prices = json_mod.loads(signal.entry_prices_json or "[]")
    except Exception:
        entry_prices = []

    if not entry_prices:
        log.info("order_skip_no_entry_price", signal_id=signal.id)
        return None

    requested_leverage = leverage or signal.leverage or settings.default_leverage
    eff_leverage = min(requested_leverage, settings.max_leverage)
    eff_margin_mode = margin_mode or settings.default_margin_mode

    client = get_vooi_client()

    # Fetch market decimals. If the resolver cannot supply them (markets
    # endpoint timed out and we have no prior cache) we MUST skip rather than
    # fall back to a default precision that the exchange will reject.
    try:
        mkt = await get_market_settings(client, exchange, symbol_normalized)
    except MarketDecimalsUnavailable as e:
        log.error(
            "order_skip_market_decimals_unavailable",
            signal_id=signal.id,
            exchange=exchange,
            symbol=symbol_normalized,
            error=str(e),
        )
        signal.skip_reason = "market_decimals_unavailable"
        await session.flush()
        return None

    # Clamp leverage against the per-symbol cap from /exchange/markets.
    # Lighter rejects 5x on JTO with HTTP 503 "Invalid leverage: 5 exceeds
    # maxLeverage=3"; without this clamp the entire signal is wasted.
    sym_max_lev = await get_max_leverage(symbol_normalized, exchange)
    if sym_max_lev is not None and sym_max_lev > 0:
        clamped = min(eff_leverage, sym_max_lev)
        if clamped != eff_leverage:
            log.info(
                "leverage_clamped_to_symbol_max",
                signal_id=signal.id,
                exchange=exchange,
                symbol=symbol_normalized,
                requested=eff_leverage,
                symbol_max=sym_max_lev,
                effective=clamped,
            )
        eff_leverage = clamped

    # НОВЫЙ-03: pick the boundary of the entry zone, not the midpoint (spec §8.3.3).
    # buy → lowest price (best fill chance for a maker buy), sell → highest price.
    # Convert each element via Decimal(str(...)) before any arithmetic to avoid
    # float-division precision loss in the midpoint formula we used previously.
    prices_decimal = [Decimal(str(p)) for p in entry_prices]
    if (signal.side or "buy") == "buy":
        entry_price = min(prices_decimal)
    else:
        entry_price = max(prices_decimal)
    entry_price_rounded = round_price(entry_price, mkt.price_decimals, signal.side or "buy")

    # Compute position size
    notional = await compute_position_size(client, exchange, eff_leverage)
    if notional <= 0:
        log.warning("order_skip_zero_notional", signal_id=signal.id)
        return None

    # Size: prefer VOOI's quoted baseSize over a local divide-and-round.
    # /exchange/quotes returns a baseSize already quantized to the exchange's
    # exact lot rules (Lighter in particular rejects locally-rounded sizes
    # with code 21706 "invalid order base or quote amount" even when our
    # baseDecimals match — its on-chain encoding has stricter granularity
    # than the decimals field advertises).
    authoritative_size: Optional[Decimal] = None
    try:
        size_quote = await get_quotes(
            symbol_normalized,
            exchange,
            signal.side or "buy",
            str(notional),
            eff_leverage,
        )
        if isinstance(size_quote, dict):
            raw_base_size = str(size_quote.get("baseSize") or "").strip()
            if raw_base_size:
                authoritative_size = Decimal(raw_base_size)
    except Exception as e:
        log.warning(
            "quote_basesize_fetch_failed",
            signal_id=signal.id,
            exchange=exchange,
            symbol=symbol_normalized,
            error=str(e),
        )

    if authoritative_size is not None and authoritative_size > 0:
        size_rounded = authoritative_size
    else:
        size = notional / entry_price_rounded
        size_rounded = round_size(size, mkt.base_decimals)

    # Min notional check
    actual_notional = size_rounded * entry_price_rounded
    if actual_notional < mkt.min_notional_usd:
        log.warning(
            "order_skip_below_min_notional",
            signal_id=signal.id,
            notional=str(actual_notional),
            minimum=str(mkt.min_notional_usd),
        )
        return None

    side = signal.side or "buy"

    # Early-reject signals whose stop_loss is non-positive (post_fill_placer
    # would otherwise fall back to compute_sl_price_from_pct, masking the bug).
    if signal.stop_loss is not None and Decimal(str(signal.stop_loss)) <= 0:
        log.error(
            "invalid_sl_price",
            signal_id=signal.id,
            sl_price=str(signal.stop_loss),
        )
        return None

    if not dry_run:
        # Pre-trade setup
        try:
            await pre_trade_setup(client, exchange, symbol_normalized, eff_leverage, eff_margin_mode)
        except Exception as e:
            log.error("pre_trade_setup_failed", error=str(e), exchange=exchange)
            # НОВЫЙ-07: persist skip_reason so the signal row reflects why we didn't trade (AC#3)
            signal.skip_reason = "leverage_set_failed"
            await session.flush()
            return None

    # Pre-insert order (before sending request — per spec A11)
    # Hyperliquid requires 0x-prefixed 32-hex-char clientOrderId; other exchanges
    # accept arbitrary strings. See make_client_order_id().
    client_order_id = make_client_order_id(signal.id, exchange)

    order = Order(
        signal_id=signal.id,
        client_order_id=client_order_id,
        order_type="entry",
        exchange=exchange,
        symbol=symbol_normalized,
        side=signal.side,
        status="submitting",
        price=entry_price_rounded,
        size=size_rounded,
        leverage=eff_leverage,
        reduce_only=False,
    )
    session.add(order)
    await session.flush()  # Get order.id

    if dry_run:
        log.info(
            "dry_run_entry_order",
            signal_id=signal.id,
            exchange=exchange,
            symbol=symbol_normalized,
            side=signal.side,
            price=str(entry_price_rounded),
            size=str(size_rounded),
            notional=str(actual_notional),
            client_order_id=client_order_id,
        )
        await emit_event(
            "DRY_RUN_ENTRY",
            signal_id=signal.id,
            exchange=exchange,
            symbol=symbol_normalized,
            message=(
                f"{signal.side} limit {size_rounded} @{entry_price_rounded} "
                f"notional={actual_notional:.2f} USD"
            ),
        )
        return None

    # Place the order — entry only, NO stopLoss / takeProfit.
    # Per spec §8.5 v1.5: TP and SL are placed post-fill by post_fill_placer.
    # Aster explicitly rejects bracket fields ("Aster exchange does not support
    # bracket orders") and Hyperliquid / Lighter work fine without them.
    order_body = {
        "exchange": exchange,
        "asset": symbol_normalized,
        "side": signal.side,
        "size": str(size_rounded),
        "price": str(entry_price_rounded),
        "timeInForce": "gtc",
        "clientOrderId": client_order_id,
    }

    try:
        response = await client.post("/exchange/orders", order_body)
        vooi_order_id = None
        if isinstance(response, dict):
            vooi_order_id = str(
                response.get("orderId")
                or response.get("id")
                or response.get("clientOrderId")
                or ""
            )
            if not vooi_order_id:
                vooi_order_id = None

        # VOOI's POST returns only {"status":"ok"} — recover the orderId via
        # a follow-up query so we can later track / cancel by our DB row
        # (startup_cleanup, reconciler, breakeven_watcher all match by
        # vooi_order_id; without it we operate blind). Prefer the
        # clientOrderId-based lookup over the open-orders tuple match —
        # it's exact and survives fast fills that left the book before we
        # got around to polling.
        if vooi_order_id is None:
            vooi_order_id = await lookup_vooi_order_id_by_client_id(
                client, exchange, client_order_id,
            )
        if vooi_order_id is None:
            vooi_order_id = await _lookup_vooi_order_id(
                client,
                exchange,
                symbol_normalized,
                signal.side or "",
                entry_price_rounded,
                size_rounded,
            )
        if vooi_order_id is None:
            log.warning(
                "vooi_order_id_lookup_missed",
                signal_id=signal.id,
                exchange=exchange,
                symbol=symbol_normalized,
                client_order_id=client_order_id,
            )

        order.vooi_order_id = vooi_order_id
        order.status = "pending"
        order.raw_response = json_mod.dumps(response, default=str)
        await session.flush()

        log.info(
            "entry_order_placed",
            signal_id=signal.id,
            order_id=order.id,
            exchange=exchange,
            symbol=symbol_normalized,
            side=signal.side,
            price=str(entry_price_rounded),
            size=str(size_rounded),
            leverage=eff_leverage,
            vooi_order_id=vooi_order_id,
        )

        await emit_event(
            "ENTRY_PLACED",
            signal_id=signal.id,
            order_id=order.id,
            exchange=exchange,
            symbol=symbol_normalized,
            message=(
                f"{signal.side} limit {size_rounded} @{entry_price_rounded}  "
                f"signal_id={signal.id} (pending fill)"
            ),
        )

        return order

    except Exception as e:
        log.error(
            "entry_order_failed",
            signal_id=signal.id,
            exchange=exchange,
            symbol=symbol_normalized,
            error=str(e),
        )
        order.status = "rejected"
        await session.flush()
        return None
