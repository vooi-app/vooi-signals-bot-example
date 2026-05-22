"""
CLI report functions — format positions, PnL, orders, and errors for display.
"""
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional

import structlog
from rich.console import Console
from rich.table import Table
from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from bot.db import session_scope
from bot.models import Order, Position, Signal, VooiError

log = structlog.get_logger(__name__)
console = Console()


def _parse_since(since: str) -> datetime:
    """Parse '1h', '24h', '7d', 'week' to datetime."""
    since = since.lower().strip()
    now = datetime.now(timezone.utc)

    if since in ("1h", "1hour"):
        return now - timedelta(hours=1)
    elif since in ("24h", "1d"):
        return now - timedelta(hours=24)
    elif since in ("48h", "2d"):
        return now - timedelta(hours=48)
    elif since in ("7d", "week", "1w"):
        return now - timedelta(days=7)
    elif since in ("30d", "month"):
        return now - timedelta(days=30)
    else:
        # Try to parse as hours
        if since.endswith("h"):
            try:
                return now - timedelta(hours=int(since[:-1]))
            except ValueError:
                pass
        if since.endswith("d"):
            try:
                return now - timedelta(days=int(since[:-1]))
            except ValueError:
                pass
    return now - timedelta(hours=24)


async def get_positions_report(status_filter: Optional[str] = None) -> Table:
    """Format open positions with sl_state and to_tp_pct columns."""
    table = Table(title="Positions", show_header=True, header_style="bold magenta")
    table.add_column("ID", style="dim")
    table.add_column("Exchange")
    table.add_column("Symbol")
    table.add_column("Side")
    table.add_column("Entry")
    table.add_column("Size")
    table.add_column("Lev")
    table.add_column("Status")
    table.add_column("SL State")
    table.add_column("To TP %")
    table.add_column("SL Price")
    table.add_column("TP Price")
    table.add_column("Opened At")

    async with session_scope() as session:
        query = select(Position)
        if status_filter:
            query = query.where(Position.status == status_filter)
        else:
            query = query.where(Position.status == "open")
        query = query.order_by(Position.opened_at.desc())

        result = await session.execute(query)
        positions = result.scalars().all()

        for pos in positions:
            sl_state = "—"
            to_tp_pct_str = "—"

            if pos.status == "open":
                sl_state = "BE" if pos.sl_moved_to_be_at else "fixed"

                # Compute to_tp_pct from cache or stored TP
                if pos.tp_price_initial and pos.entry_price:
                    if pos.side == "buy":
                        to_tp = (pos.tp_price_initial - pos.entry_price) / pos.entry_price * 100
                    else:
                        to_tp = (pos.entry_price - pos.tp_price_initial) / pos.entry_price * 100
                    to_tp_pct_str = f"{to_tp:.2f}%"

            table.add_row(
                str(pos.id),
                pos.exchange,
                pos.symbol,
                pos.side,
                str(pos.entry_price),
                str(pos.size),
                str(pos.leverage),
                pos.status,
                sl_state,
                to_tp_pct_str,
                str(pos.sl_price_current or pos.sl_price_initial or "—"),
                str(pos.tp_price_initial or "—"),
                pos.opened_at.strftime("%Y-%m-%d %H:%M") if pos.opened_at else "—",
            )

    return table


async def get_pnl_report(period: str = "week") -> Table:
    """Summarize realized PnL for closed positions in the given period."""
    table = Table(title=f"PnL Report ({period})", show_header=True, header_style="bold cyan")
    table.add_column("Exchange")
    table.add_column("Symbol")
    table.add_column("Side")
    table.add_column("Close Reason")
    table.add_column("Realized PnL")
    table.add_column("Fees Paid")
    table.add_column("Closed At")

    since = _parse_since(period)
    total_pnl = Decimal("0")
    total_fees = Decimal("0")

    async with session_scope() as session:
        result = await session.execute(
            select(Position).where(
                and_(
                    Position.status.in_(
                        ["closed_tp", "closed_sl", "closed_breakeven", "closed_manual", "closed_emergency", "liquidated"]
                    ),
                    Position.closed_at >= since,
                )
            ).order_by(Position.closed_at.desc())
        )
        positions = result.scalars().all()

        for pos in positions:
            pnl = pos.realized_pnl_usd or Decimal("0")
            fees = pos.fees_paid_usd or Decimal("0")
            total_pnl += pnl
            total_fees += fees

            pnl_str = f"${pnl:.2f}"
            pnl_style = "green" if pnl >= 0 else "red"

            table.add_row(
                pos.exchange,
                pos.symbol,
                pos.side,
                pos.close_reason or pos.status,
                f"[{pnl_style}]{pnl_str}[/{pnl_style}]",
                f"${fees:.2f}",
                pos.closed_at.strftime("%Y-%m-%d %H:%M") if pos.closed_at else "—",
            )

        # Summary row
        summary_style = "green" if total_pnl >= 0 else "red"
        table.add_section()
        table.add_row(
            "", "", "", "[bold]TOTAL[/bold]",
            f"[bold {summary_style}]${total_pnl:.2f}[/bold {summary_style}]",
            f"[bold]${total_fees:.2f}[/bold]",
            "",
        )

    return table


async def get_orders_report(since: str = "24h") -> Table:
    """Show recent orders."""
    table = Table(title=f"Orders (since {since})", show_header=True, header_style="bold yellow")
    table.add_column("ID", style="dim")
    table.add_column("Exchange")
    table.add_column("Symbol")
    table.add_column("Side")
    table.add_column("Type")
    table.add_column("Status")
    table.add_column("Price")
    table.add_column("Size")
    table.add_column("Created At")

    since_dt = _parse_since(since)

    async with session_scope() as session:
        result = await session.execute(
            select(Order).where(
                Order.created_at >= since_dt
            ).order_by(Order.created_at.desc()).limit(200)
        )
        orders = result.scalars().all()

        for order in orders:
            price = str(order.price or order.trigger_price or "—")
            status_style = {
                "filled": "green",
                "pending": "yellow",
                "open": "yellow",
                "cancelled": "dim",
                "rejected": "red",
                "expired": "dim",
                "submitting": "blue",
            }.get(order.status, "white")

            table.add_row(
                str(order.id),
                order.exchange,
                order.symbol,
                order.side,
                order.order_type,
                f"[{status_style}]{order.status}[/{status_style}]",
                price,
                str(order.size or "—"),
                order.created_at.strftime("%Y-%m-%d %H:%M") if order.created_at else "—",
            )

    return table


async def get_vooi_errors_report(limit: int = 50) -> Table:
    """Show recent VOOI API errors."""
    table = Table(title="VOOI API Errors", show_header=True, header_style="bold red")
    table.add_column("ID", style="dim")
    table.add_column("Error Kind")
    table.add_column("Method")
    table.add_column("Path")
    table.add_column("Status")
    table.add_column("Detail")
    table.add_column("Reported")
    table.add_column("Created At")

    async with session_scope() as session:
        result = await session.execute(
            select(VooiError).order_by(VooiError.created_at.desc()).limit(limit)
        )
        errors = result.scalars().all()

        for error in errors:
            reported_str = "✓" if error.reported else "—"
            table.add_row(
                str(error.id),
                error.error_kind,
                error.method,
                error.path,
                str(error.response_status or "—"),
                (error.error_detail or "")[:60],
                reported_str,
                error.created_at.strftime("%Y-%m-%d %H:%M") if error.created_at else "—",
            )

    return table


async def export_vooi_errors_jsonl(output_path: str, unreported_only: bool = True) -> int:
    """Export VOOI errors to JSONL file. Returns count of exported records."""
    import aiofiles

    async with session_scope() as session:
        query = select(VooiError)
        if unreported_only:
            query = query.where(VooiError.reported == False)
        query = query.order_by(VooiError.created_at)

        result = await session.execute(query)
        errors = result.scalars().all()

        count = 0
        async with aiofiles.open(output_path, "w", encoding="utf-8") as f:
            for error in errors:
                record = {
                    "id": error.id,
                    "correlation_id": error.correlation_id,
                    "method": error.method,
                    "path": error.path,
                    "error_kind": error.error_kind,
                    "error_detail": error.error_detail,
                    "response_status": error.response_status,
                    "duration_ms": error.duration_ms,
                    "reported": error.reported,
                    "created_at": error.created_at.isoformat() if error.created_at else None,
                }
                await f.write(json.dumps(record) + "\n")
                count += 1

        return count


async def mark_errors_reported(error_ids: list[int]) -> int:
    """Mark specified error IDs as reported. Returns count updated."""
    async with session_scope() as session:
        result = await session.execute(
            select(VooiError).where(VooiError.id.in_(error_ids))
        )
        errors = result.scalars().all()
        for error in errors:
            error.reported = True
        await session.flush()
        return len(errors)


async def prune_vooi_errors(days: int = 30) -> int:
    """Delete old error records. Returns count deleted."""
    from sqlalchemy import delete

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    async with session_scope() as session:
        result = await session.execute(
            select(func.count(VooiError.id)).where(
                and_(
                    VooiError.created_at < cutoff,
                    VooiError.reported == True,
                )
            )
        )
        count = result.scalar_one()
        await session.execute(
            delete(VooiError).where(
                and_(
                    VooiError.created_at < cutoff,
                    VooiError.reported == True,
                )
            )
        )
    return count


async def get_signals_report(since: str = "1h") -> Table:
    """Show recent parsed signals."""
    table = Table(title=f"Signals (since {since})", show_header=True, header_style="bold cyan")
    table.add_column("ID", style="dim")
    table.add_column("Symbol")
    table.add_column("Side")
    table.add_column("Entry Prices")
    table.add_column("Stop Loss")
    table.add_column("Leverage")
    table.add_column("Is Signal")
    table.add_column("Skip Reason")
    table.add_column("Prompt Ver")
    table.add_column("Parsed At")

    since_dt = _parse_since(since)

    async with session_scope() as session:
        result = await session.execute(
            select(Signal).where(
                Signal.parsed_at >= since_dt
            ).order_by(Signal.parsed_at.desc()).limit(100)
        )
        signals = result.scalars().all()

        for sig in signals:
            is_sig = "✓" if sig.is_signal else "✗"
            sig_style = "green" if sig.is_signal else "dim"

            try:
                entry_prices = json.loads(sig.entry_prices_json or "[]")
                entry_str = ", ".join(str(p) for p in entry_prices)
            except Exception:
                entry_str = sig.entry_prices_json or "—"

            table.add_row(
                str(sig.id),
                sig.symbol or "—",
                sig.side or "—",
                entry_str or "—",
                str(sig.stop_loss or "—"),
                str(sig.leverage or "—"),
                f"[{sig_style}]{is_sig}[/{sig_style}]",
                sig.skip_reason or "—",
                sig.prompt_version or "—",
                sig.parsed_at.strftime("%Y-%m-%d %H:%M") if sig.parsed_at else "—",
            )

    return table


async def get_messages_report(since: str = "1h") -> Table:
    """Show recent raw messages."""
    table = Table(title=f"Messages (since {since})", show_header=True, header_style="bold white")
    table.add_column("ID", style="dim")
    table.add_column("Channel ID")
    table.add_column("Telegram ID")
    table.add_column("Processed")
    table.add_column("Preview")
    table.add_column("Received At")

    since_dt = _parse_since(since)

    from bot.models import Message

    async with session_scope() as session:
        from sqlalchemy import select

        result = await session.execute(
            select(Message).where(
                Message.received_at >= since_dt
            ).order_by(Message.received_at.desc()).limit(100)
        )
        messages = result.scalars().all()

        for msg in messages:
            processed_str = "✓" if msg.processed else "—"
            preview = (msg.raw_text or "")[:60].replace("\n", " ")
            table.add_row(
                str(msg.id),
                str(msg.channel_id),
                str(msg.telegram_message_id),
                processed_str,
                preview,
                msg.received_at.strftime("%Y-%m-%d %H:%M") if msg.received_at else "—",
            )

    return table
