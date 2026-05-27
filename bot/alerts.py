"""
Outbound Telegram alerts to operator.
Uses plain Telegram Bot API (HTTP, not Telethon).
If ALERT_TELEGRAM_BOT_TOKEN is empty, alerts are only logged.
"""
from typing import Optional

import structlog
import httpx

from bot.config import settings

log = structlog.get_logger(__name__)

TELEGRAM_SEND_URL = "https://api.telegram.org/bot{token}/sendMessage"


async def send_alert(message: str, parse_mode: str = "HTML") -> bool:
    """
    Send a Telegram message to the configured alert chat.
    Returns True on success, False on failure.
    If bot token is empty, only logs and returns False.
    """
    if not settings.alert_telegram_bot_token:
        log.info("alert_suppressed_no_token", message=message[:200])
        return False

    if not settings.alert_telegram_chat_id:
        log.warning("alert_suppressed_no_chat_id", message=message[:200])
        return False

    url = TELEGRAM_SEND_URL.format(token=settings.alert_telegram_bot_token)
    payload = {
        "chat_id": settings.alert_telegram_chat_id,
        "text": message,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True,
    }

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(url, json=payload)
            if resp.status_code == 200:
                log.debug("alert_sent", chat_id=settings.alert_telegram_chat_id)
                return True
            else:
                log.warning(
                    "alert_failed",
                    status=resp.status_code,
                    body=resp.text[:200],
                )
                return False
    except Exception as e:
        log.error("alert_exception", error=str(e))
        return False


async def send_signal_checkpoint_alert(
    signal_id: int,
    symbol: str,
    side: str,
    entry_prices: list[float],
    stop_loss: float | None,
    channel: str,
) -> None:
    """
    Send signal checkpoint notification (Stage 2 — no money involved).
    Called after each successfully parsed signal if SIGNAL_CHECKPOINT_NOTIFY_TELEGRAM=true.
    """
    if not settings.signal_checkpoint_notify_telegram:
        return

    side_emoji = "🟢" if side == "buy" else "🔴"
    entry_str = ", ".join(str(p) for p in entry_prices) if entry_prices else "—"
    sl_str = str(stop_loss) if stop_loss else "—"

    text = (
        f"{side_emoji} <b>Signal parsed</b> (signal_id={signal_id})\n"
        f"Symbol: <b>{symbol}</b>\n"
        f"Side: <b>{side.upper()}</b>\n"
        f"Entry: {entry_str}\n"
        f"SL: {sl_str}\n"
        f"Channel: {channel}\n"
        f"<i>Checkpoint — no order placed</i>"
    )
    await send_alert(text)


async def send_naked_position_alert(position_id: int, symbol: str, exchange: str) -> None:
    """Alert for ERROR_NAKED_POSITION — position has no active SL."""
    text = (
        f"🚨 <b>ERROR_NAKED_POSITION</b>\n"
        f"Position {position_id}: <b>{symbol}</b> on {exchange}\n"
        f"No active SL order found. Manual intervention required!"
    )
    await send_alert(text)


async def send_orphan_naked_alert(
    symbol: str, exchange: str, side: str, size: str
) -> None:
    """Alert for a live exchange position the bot cannot attribute to any
    tracked entry order — so there is no position row and no SL/TP. This is
    the most dangerous untracked state: real exposure the bot is blind to."""
    text = (
        f"🚨 <b>ERROR_NAKED_POSITION (untracked orphan)</b>\n"
        f"<b>{symbol}</b> {side} {size} on {exchange}\n"
        f"Live position on the exchange with NO matching entry order — bot "
        f"cannot place SL/TP for it. Close it manually or check the key."
    )
    await send_alert(text)


async def send_emergency_close_alert(
    position_id: int,
    symbol: str,
    exchange: str,
    reason: str,
    *,
    vooi_order_id: Optional[str] = None,
) -> None:
    """Alert when the bot panics out of a position via aggressive limit-IOC."""
    oid_line = f"\nClose order: <code>{vooi_order_id}</code>" if vooi_order_id else ""
    text = (
        f"🆘 <b>EMERGENCY_MARKET_CLOSE</b>\n"
        f"Position {position_id}: <b>{symbol}</b> on {exchange}\n"
        f"Reason: {reason}\n"
        f"SL trigger would fire immediately — bot dumped via aggressive "
        f"limit-IOC reduce-only.{oid_line}"
    )
    await send_alert(text)


async def send_watcher_hung_alert(age_sec: float) -> None:
    """Alert for ERROR_WATCHER_HUNG — breakeven watcher heartbeat stale."""
    text = (
        f"⚠️ <b>ERROR_WATCHER_HUNG</b>\n"
        f"tp_breakeven_watcher heartbeat stale for {age_sec:.0f}s (threshold: 30s)\n"
        f"The asyncio task may have crashed. Inspect process logs."
    )
    await send_alert(text)


async def send_dd_breaker_alert(dd_pct: float) -> None:
    """Alert when daily drawdown circuit breaker is triggered."""
    text = (
        f"🛑 <b>DD_BREAKER</b>\n"
        f"Daily drawdown {dd_pct:.1f}% exceeded threshold "
        f"{settings.daily_dd_pct_pause}%.\n"
        f"New trade placement is <b>paused</b>. Reset manually after reviewing."
    )
    await send_alert(text)
