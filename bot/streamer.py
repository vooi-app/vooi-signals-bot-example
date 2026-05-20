"""
Console event streamer using rich.
Formats structured events per spec §11:
  [timestamp UTC] LEVEL  EVENT_TYPE  details
"""
import asyncio
import json
from datetime import datetime, timezone
from typing import Any, Optional

import structlog
from rich.console import Console

from bot.db import session_scope
from bot.models import Event

_console = Console(highlight=False)
log = structlog.get_logger(__name__)

# Level → rich color
_LEVEL_COLORS = {
    "INFO": "bold green",
    "WARN": "bold yellow",
    "WARNING": "bold yellow",
    "ERROR": "bold red",
    "DEBUG": "dim",
}

# Event type → rich color
_EVENT_COLORS = {
    "SIGNAL_PARSED": "cyan",
    "SIGNAL_SKIPPED": "yellow",
    "ENTRY_PLACED": "green",
    "ENTRY_FILLED": "bold green",
    "TP_SL_PLACED": "green",
    "SL_BREAKEVEN": "bold cyan",
    "POSITION_CLOSED": "bold white",
    "ERROR_NAKED_POSITION": "bold red",
    "ERROR_WATCHER_HUNG": "bold red",
    "DD_BREAKER": "bold red",
    "SSE_RECONNECT": "yellow",
    "RECONCILER_RUN": "dim",
}


async def emit_event(
    event_type: str,
    level: str = "INFO",
    signal_id: Optional[int] = None,
    order_id: Optional[int] = None,
    position_id: Optional[int] = None,
    exchange: Optional[str] = None,
    symbol: Optional[str] = None,
    message: Optional[str] = None,
    persist: bool = True,
    **kwargs: Any,
) -> None:
    """
    Emit a structured event to console and optionally persist to DB.

    Example output:
      [2024-01-15 14:23:01 UTC] INFO   ENTRY_PLACED    BTC long limit 0.0123 @65000  signal_id=87
    """
    ts = datetime.now(timezone.utc)
    ts_str = ts.strftime("%Y-%m-%d %H:%M:%S UTC")

    level_color = _LEVEL_COLORS.get(level.upper(), "white")
    event_color = _EVENT_COLORS.get(event_type, "white")

    # Build detail string from kwargs
    detail_parts = []
    if message:
        detail_parts.append(message)
    for k, v in kwargs.items():
        if v is not None:
            detail_parts.append(f"{k}={v}")

    detail_str = "  ".join(detail_parts)

    # Format: [timestamp] LEVEL   EVENT_TYPE   details
    formatted = (
        f"[dim]\\[{ts_str}][/dim] "
        f"[{level_color}]{level:<6}[/{level_color}] "
        f"[{event_color}]{event_type:<20}[/{event_color}] "
        f"{detail_str}"
    )
    _console.print(formatted)

    # Persist to DB
    if persist:
        try:
            data_json: Optional[str] = None
            if kwargs:
                data_json = json.dumps(kwargs, default=str)

            async with session_scope() as session:
                ev = Event(
                    event_type=event_type,
                    level=level.upper(),
                    signal_id=signal_id,
                    order_id=order_id,
                    position_id=position_id,
                    exchange=exchange,
                    symbol=symbol,
                    message=message or detail_str[:500],
                    data_json=data_json,
                    created_at=ts,
                )
                session.add(ev)
        except Exception as e:
            log.warning("event_persist_failed", error=str(e), event_type=event_type)


def emit_event_sync(event_type: str, level: str = "INFO", message: Optional[str] = None, **kwargs: Any) -> None:
    """Synchronous console-only emit (no DB, no async). Used in CLI status commands."""
    ts = datetime.now(timezone.utc)
    ts_str = ts.strftime("%Y-%m-%d %H:%M:%S UTC")

    level_color = _LEVEL_COLORS.get(level.upper(), "white")
    event_color = _EVENT_COLORS.get(event_type, "white")

    detail_parts = []
    if message:
        detail_parts.append(message)
    for k, v in kwargs.items():
        if v is not None:
            detail_parts.append(f"{k}={v}")

    detail_str = "  ".join(detail_parts)
    formatted = (
        f"[dim]\\[{ts_str}][/dim] "
        f"[{level_color}]{level:<6}[/{level_color}] "
        f"[{event_color}]{event_type:<20}[/{event_color}] "
        f"{detail_str}"
    )
    _console.print(formatted)
