"""
LLM signal parser.
Uses OpenAI (or compatible API) to extract structured signal data from Telegram messages.
Per spec §6, Stage 2.
"""
import json
from datetime import datetime, timedelta, timezone
from typing import Optional

import structlog
from openai import AsyncOpenAI
from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from bot.alerts import send_signal_checkpoint_alert
from bot.config import settings
from bot.db import session_scope
from bot.models import Message, Signal

log = structlog.get_logger(__name__)

# OpenAI client (lazy-initialized)
_openai_client: Optional[AsyncOpenAI] = None


def get_openai_client() -> AsyncOpenAI:
    global _openai_client
    if _openai_client is None:
        _openai_client = AsyncOpenAI(api_key=settings.llm_api_key)
    return _openai_client


SYSTEM_PROMPT = """You are a cryptocurrency trading signal parser.
Your task is to extract structured trading signal information from Telegram channel messages.

Extract the following fields if present:
- symbol: The cryptocurrency ticker (e.g., "BTC", "ETH", "SOL"). Normalize to base symbol without chain suffix.
- side: "buy" (for LONG signals) or "sell" (for SHORT signals)
- entry_prices: List of entry price levels. Can be a range [lower, upper] or single price [price].
- take_profits: List of take-profit target prices.
- stop_loss: Stop-loss price (single value or null if not specified).
- leverage: Leverage multiplier as integer (e.g., 10 for 10x), or null if not specified.
- chain: The blockchain/exchange hint if mentioned (e.g., "binance", "bybit"), or null.

Rules:
1. If the message is NOT a trading signal (news, analysis, random text), set is_signal=false.
2. Entry "zone" or "range" = extract both boundary prices as entry_prices list.
3. Multiple TP targets = list all in order.
4. If leverage mentioned as "5x" or "5X" or "lev 5" → extract as integer 5.
5. Prices must be numbers, not text. Ignore percentage-based targets.
6. For "BUY" / "LONG" / "🟢" → side = "buy". For "SELL" / "SHORT" / "🔴" → side = "sell".
7. If critical fields (symbol, side, entry_prices) are missing → is_signal=false.

Output ONLY valid JSON in this exact format:
{
  "is_signal": true,
  "symbol": "BTC",
  "side": "buy",
  "entry_prices": [65000, 65500],
  "take_profits": [70000, 75000],
  "stop_loss": 63000,
  "leverage": 10,
  "chain": null
}

Or for non-signals:
{
  "is_signal": false,
  "symbol": null,
  "side": null,
  "entry_prices": [],
  "take_profits": [],
  "stop_loss": null,
  "leverage": null,
  "chain": null
}"""


async def parse_signal(
    message_text: str,
    session: Optional[AsyncSession] = None,
) -> Optional[dict]:
    """
    Parse a message text using LLM.
    Returns structured signal dict or None on error.

    Dict keys: is_signal, symbol, side, entry_prices, take_profits, stop_loss, leverage, chain
    """
    client = get_openai_client()

    try:
        response = await client.chat.completions.create(
            model=settings.llm_model,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": message_text},
            ],
            response_format={"type": "json_object"},
            temperature=0.0,
            max_tokens=512,
        )

        content = response.choices[0].message.content
        if not content:
            log.warning("llm_empty_response")
            return None

        result = json.loads(content)
        return result

    except Exception as e:
        log.error("llm_parse_error", error=str(e), text_preview=message_text[:100])
        return None


async def is_duplicate_signal(
    session: AsyncSession,
    symbol: str,
    side: str,
    entry_prices: list[float],
) -> bool:
    """
    Check for duplicate signal: same symbol+side+entry_price within 6h window.
    Per spec §6 deduplication logic.
    """
    if not entry_prices:
        return False

    window_start = datetime.now(timezone.utc) - timedelta(hours=6)
    first_entry = str(entry_prices[0])

    result = await session.execute(
        select(Signal).where(
            and_(
                Signal.symbol == symbol,
                Signal.side == side,
                Signal.parsed_at >= window_start,
                Signal.is_signal == True,
            )
        )
    )
    existing_signals = result.scalars().all()

    for sig in existing_signals:
        try:
            existing_entries = json.loads(sig.entry_prices_json or "[]")
            if existing_entries and abs(float(existing_entries[0]) - float(entry_prices[0])) < 1.0:
                return True
        except Exception:
            continue

    return False


async def process_message_for_signal(
    message: Message,
    channel_username: Optional[str] = None,
) -> Optional[Signal]:
    """
    Full pipeline: parse message → deduplicate → persist signal → checkpoint alert.
    Returns Signal record (even if is_signal=False), or None on critical error.
    """
    if not message.raw_text or not message.raw_text.strip():
        return None

    # Parse with LLM
    parsed = await parse_signal(message.raw_text)
    if parsed is None:
        return None

    async with session_scope() as session:
        is_signal = bool(parsed.get("is_signal", False))
        symbol = parsed.get("symbol")
        side = parsed.get("side")
        entry_prices = parsed.get("entry_prices") or []
        take_profits = parsed.get("take_profits") or []
        stop_loss = parsed.get("stop_loss")
        leverage = parsed.get("leverage")
        chain = parsed.get("chain")

        skip_reason: Optional[str] = None

        if is_signal and symbol and side:
            # Deduplication check
            is_dup = await is_duplicate_signal(session, symbol, side, entry_prices)
            if is_dup:
                log.info(
                    "signal_duplicate_skipped",
                    symbol=symbol,
                    side=side,
                    message_id=message.id,
                )
                skip_reason = "duplicate_signal"
                is_signal = False

            # Market order guard per spec A1
            if is_signal and not entry_prices:
                skip_reason = "no_entry_price"
                is_signal = False

        signal = Signal(
            message_id=message.id,
            channel_id=message.channel_id,
            is_signal=is_signal,
            symbol=symbol,
            side=side,
            entry_prices_json=json.dumps(entry_prices) if entry_prices else None,
            take_profits_json=json.dumps(take_profits) if take_profits else None,
            stop_loss=stop_loss,
            leverage=leverage,
            chain=chain,
            skip_reason=skip_reason,
            prompt_version=settings.signal_parser_prompt_version,
        )
        session.add(signal)

        # Mark message as processed
        result = await session.execute(
            select(Message).where(Message.id == message.id)
        )
        msg = result.scalar_one_or_none()
        if msg:
            msg.processed = True

        await session.flush()
        signal_id = signal.id

        log.info(
            "signal_parsed",
            message_id=message.id,
            signal_id=signal_id,
            is_signal=is_signal,
            symbol=symbol,
            side=side,
            entry_prices=entry_prices,
            stop_loss=stop_loss,
            skip_reason=skip_reason,
        )

    # Checkpoint: send Telegram notification (if configured)
    if is_signal and symbol and side:
        await send_signal_checkpoint_alert(
            signal_id=signal_id,
            symbol=symbol,
            side=side,
            entry_prices=entry_prices,
            stop_loss=stop_loss,
            channel=channel_username or f"channel_{message.channel_id}",
        )

    return signal
