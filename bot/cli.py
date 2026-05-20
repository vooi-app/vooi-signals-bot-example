"""
CLI entry point using Typer.
All bot commands per spec §10.
"""
import asyncio
import json
import os
import sys
from datetime import datetime, timezone
from typing import Optional

import structlog
import typer
from rich.console import Console
from rich.table import Table

from bot.config import settings

app = typer.Typer(
    name="bot",
    help="VOOI Signal Bot — automated crypto trading bot",
    add_completion=False,
)
console = Console()
log = structlog.get_logger(__name__)

channels_app = typer.Typer(help="Manage Telegram channels")
vooi_errors_app = typer.Typer(help="Manage VOOI API error log")

app.add_typer(channels_app, name="channels")
app.add_typer(vooi_errors_app, name="vooi-errors")


def setup_logging() -> None:
    """Configure structlog for console + file output."""
    import logging
    import structlog

    os.makedirs(os.path.dirname(settings.log_file_path), exist_ok=True)

    logging.basicConfig(
        format="%(message)s",
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
    )

    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
    )


# ---------------------------------------------------------------------------
# bot run
# ---------------------------------------------------------------------------
@app.command()
def run() -> None:
    """Start all bot tasks (ingester, SSE, reconciler, watchers)."""
    setup_logging()
    console.print("[bold green]Starting VOOI Signal Bot...[/bold green]")
    asyncio.run(_run_all_tasks())


async def _run_all_tasks() -> None:
    """Start all six concurrent async tasks."""
    from bot.db import dispose_engine
    from bot.ingester import ingester_task
    from bot.reconciler import reconciler_task
    from bot.sse_listener import sse_listener_task
    from bot.breakeven_watcher import tp_breakeven_watcher_task
    from bot.sl_safety import lighter_sl_watchdog_task, sl_safety_check_task
    from bot.startup_cleanup import run_startup_cleanup

    # Run alembic migrations on startup
    await _run_migrations()

    # One-shot phantom-position + dangling-order cleanup.
    try:
        await run_startup_cleanup()
    except Exception as e:
        console.print(f"[yellow]Startup cleanup failed (non-fatal): {e}[/yellow]")

    tasks = [
        asyncio.create_task(ingester_task(), name="ingester"),
        asyncio.create_task(sse_listener_task(), name="sse_listener"),
        asyncio.create_task(reconciler_task(), name="reconciler"),
        asyncio.create_task(tp_breakeven_watcher_task(), name="breakeven_watcher"),
        asyncio.create_task(sl_safety_check_task(), name="sl_safety"),
        asyncio.create_task(lighter_sl_watchdog_task(), name="lighter_sl_watchdog"),
    ]

    console.print(f"[green]All {len(tasks)} tasks started.[/green]")

    try:
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
        for task in done:
            if task.exception():
                console.print(
                    f"[bold red]Task {task.get_name()} failed: {task.exception()}[/bold red]"
                )
    except KeyboardInterrupt:
        console.print("\n[yellow]Shutting down...[/yellow]")
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await dispose_engine()
        console.print("[green]Shutdown complete.[/green]")


async def _run_migrations() -> None:
    """Run alembic migrations on startup."""
    try:
        import subprocess
        alembic_bin = os.path.join(os.path.dirname(sys.executable), "alembic")
        result = subprocess.run(
            [alembic_bin if os.path.exists(alembic_bin) else "alembic", "upgrade", "head"],
            capture_output=True,
            text=True,
            cwd=os.path.dirname(os.path.dirname(__file__)),
        )
        if result.returncode == 0:
            console.print("[green]Database migrations: OK[/green]")
        else:
            console.print(f"[red]Migration warning: {result.stderr}[/red]")
    except Exception as e:
        console.print(f"[yellow]Could not run migrations: {e}[/yellow]")


# ---------------------------------------------------------------------------
# bot status
# ---------------------------------------------------------------------------
@app.command()
def status() -> None:
    """Healthcheck: VOOI connectivity, clock skew, exchange modes."""
    setup_logging()
    asyncio.run(_status_check())


async def _status_check() -> None:
    from bot.vooi_client import VooiClient
    import time

    console.print("[bold]VOOI Signal Bot — Status Check[/bold]")
    console.print("=" * 50)

    all_ok = True

    async with VooiClient() as client:
        # 1. VOOI connectivity + clock skew
        try:
            start = time.monotonic()
            server_data = await client.get("/time")
            elapsed = time.monotonic() - start

            server_ts = server_data.get("timestamp") or server_data.get("serverTime", 0)
            local_ts = int(time.time() * 1000)
            skew_ms = abs(local_ts - int(server_ts))

            console.print(f"[green]✓[/green] VOOI API connected (latency: {elapsed*1000:.0f}ms)")
            if skew_ms < 5000:
                console.print(f"[green]✓[/green] Clock skew: {skew_ms}ms (acceptable)")
            else:
                console.print(f"[yellow]⚠[/yellow] Clock skew: {skew_ms}ms (may cause issues)")
                all_ok = False
        except Exception as e:
            console.print(f"[red]✗[/red] VOOI API connection failed: {e}")
            all_ok = False

        # 2. Exchange-specific checks
        for exchange in ["hyperliquid", "lighter", "aster"]:
            try:
                data = await client.get("/exchange/markets", params={"exchanges": exchange})
                count = len(data) if isinstance(data, list) else len(data.get("markets", []))
                console.print(f"[green]✓[/green] {exchange}: {count} markets available")
            except Exception as e:
                console.print(f"[yellow]⚠[/yellow] {exchange}: {e}")

        # 3. Database
        try:
            from bot.db import engine
            from sqlalchemy import text
            async with engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
            console.print("[green]✓[/green] Database connected")
        except Exception as e:
            console.print(f"[red]✗[/red] Database error: {e}")
            all_ok = False

    console.print("=" * 50)
    if all_ok:
        console.print("[bold green]All checks passed.[/bold green]")
    else:
        console.print("[bold yellow]Some checks failed — review above.[/bold yellow]")
        raise typer.Exit(code=1)


# ---------------------------------------------------------------------------
# bot vooi-check
# ---------------------------------------------------------------------------
@app.command(name="vooi-check")
def vooi_check() -> None:
    """Detailed VOOI API parameter verification."""
    setup_logging()
    asyncio.run(_vooi_check())


async def _vooi_check() -> None:
    from bot.vooi_client import VooiClient

    console.print("[bold]VOOI API Parameter Check[/bold]")

    async with VooiClient() as client:
        # Check API key is valid
        try:
            data = await client.get("/exchange/accounts", params={"exchanges": "hyperliquid"})
            account = data[0] if isinstance(data, list) and data else {}
            console.print("[green]✓[/green] API key valid")
            if "availableMargin" in account or "totalBalance" in account:
                balance = account.get("totalBalance", "N/A")
                available = account.get("availableMargin", "N/A")
                console.print(f"  Total balance: {balance}")
                console.print(f"  Available margin: {available}")
        except Exception as e:
            console.print(f"[red]✗[/red] API key check failed: {e}")

        # Check broker IDs and per-exchange builder fees
        for exchange in ["hyperliquid", "lighter", "aster"]:
            broker_id = settings.get_broker_id(exchange)
            fee = settings.get_broker_fee_bps(exchange)
            console.print(f"  Broker ({exchange}): id={broker_id}  feeBps={fee}")

        console.print(f"  Default leverage: {settings.default_leverage}x")
        console.print(f"  Max position size: ${settings.max_position_size_usd}")


# ---------------------------------------------------------------------------
# bot tg-login
# ---------------------------------------------------------------------------
@app.command(name="tg-login")
def tg_login() -> None:
    """Interactive Telethon Telegram login (creates session file)."""
    asyncio.run(_tg_login())


async def _tg_login() -> None:
    from telethon import TelegramClient

    console.print("[bold]Telegram Login[/bold]")
    console.print(f"Session file: {settings.telegram_session_name}.session")

    client = TelegramClient(
        settings.telegram_session_name,
        settings.telegram_api_id,
        settings.telegram_api_hash,
    )
    await client.start()
    me = await client.get_me()
    console.print(f"[green]✓[/green] Logged in as: {me.username or me.first_name} (ID: {me.id})")
    await client.disconnect()


# ---------------------------------------------------------------------------
# channels add/list/remove
# ---------------------------------------------------------------------------
@channels_app.command("add")
def channels_add(
    channel: str = typer.Argument(help="Channel username or ID (e.g. @example_signals or 1234567890)"),
) -> None:
    """Add a Telegram channel to monitor."""
    asyncio.run(_channels_add(channel))


async def _channels_add(channel_str: str) -> None:
    from bot.ingester import get_telegram_client

    client = get_telegram_client()
    await client.start()

    try:
        entity = await client.get_entity(channel_str)
        tg_id = entity.id
        username = getattr(entity, "username", None)
        title = getattr(entity, "title", None)

        from bot.db import session_scope
        from bot.models import Channel
        from sqlalchemy import select

        async with session_scope() as session:
            result = await session.execute(
                select(Channel).where(Channel.telegram_id == tg_id)
            )
            existing = result.scalar_one_or_none()

            if existing:
                existing.is_active = True
                console.print(f"[yellow]Channel already exists, re-activated: {title or username}[/yellow]")
            else:
                channel = Channel(
                    telegram_id=tg_id,
                    username=username,
                    title=title,
                    is_active=True,
                )
                session.add(channel)
                console.print(f"[green]✓[/green] Channel added: {title or username} (ID: {tg_id})")

    except Exception as e:
        console.print(f"[red]✗[/red] Failed to add channel: {e}")
        raise typer.Exit(1)
    finally:
        await client.disconnect()


@channels_app.command("list")
def channels_list() -> None:
    """List monitored channels."""
    asyncio.run(_channels_list())


async def _channels_list() -> None:
    from bot.db import session_scope
    from bot.models import Channel
    from sqlalchemy import select

    table = Table(title="Monitored Channels")
    table.add_column("ID")
    table.add_column("Telegram ID")
    table.add_column("Username")
    table.add_column("Title")
    table.add_column("Active")

    async with session_scope() as session:
        result = await session.execute(select(Channel).order_by(Channel.added_at))
        channels = result.scalars().all()
        for ch in channels:
            table.add_row(
                str(ch.id),
                str(ch.telegram_id),
                ch.username or "—",
                ch.title or "—",
                "✓" if ch.is_active else "✗",
            )

    console.print(table)


@channels_app.command("remove")
def channels_remove(
    channel_id: int = typer.Argument(help="DB channel ID to deactivate"),
) -> None:
    """Deactivate a channel (stop monitoring)."""
    asyncio.run(_channels_remove(channel_id))


async def _channels_remove(channel_id: int) -> None:
    from bot.db import session_scope
    from bot.models import Channel
    from sqlalchemy import select

    async with session_scope() as session:
        result = await session.execute(select(Channel).where(Channel.id == channel_id))
        ch = result.scalar_one_or_none()
        if ch:
            ch.is_active = False
            console.print(f"[yellow]Channel {ch.title or ch.username} deactivated.[/yellow]")
        else:
            console.print(f"[red]Channel {channel_id} not found.[/red]")


# ---------------------------------------------------------------------------
# bot signals
# ---------------------------------------------------------------------------
@app.command()
def signals(
    since: str = typer.Option("1h", help="Time window, e.g. '1h', '24h', '7d'"),
) -> None:
    """Show recent parsed signals."""
    setup_logging()
    asyncio.run(_show_signals(since))


async def _show_signals(since: str) -> None:
    from bot.reports import get_signals_report
    table = await get_signals_report(since)
    console.print(table)


# ---------------------------------------------------------------------------
# bot messages
# ---------------------------------------------------------------------------
@app.command()
def messages(
    since: str = typer.Option("1h", help="Time window"),
) -> None:
    """Show recent raw Telegram messages."""
    setup_logging()
    asyncio.run(_show_messages(since))


async def _show_messages(since: str) -> None:
    from bot.reports import get_messages_report
    table = await get_messages_report(since)
    console.print(table)


# ---------------------------------------------------------------------------
# bot positions
# ---------------------------------------------------------------------------
@app.command()
def positions() -> None:
    """Show open positions with SL state and to-TP %."""
    setup_logging()
    asyncio.run(_show_positions())


async def _show_positions() -> None:
    from bot.reports import get_positions_report
    table = await get_positions_report()
    console.print(table)


# ---------------------------------------------------------------------------
# bot pnl
# ---------------------------------------------------------------------------
@app.command()
def pnl(
    period: str = typer.Option("week", help="Period: '24h', 'week', '30d'"),
) -> None:
    """Show PnL report for closed positions."""
    setup_logging()
    asyncio.run(_show_pnl(period))


async def _show_pnl(period: str) -> None:
    from bot.reports import get_pnl_report
    table = await get_pnl_report(period)
    console.print(table)


# ---------------------------------------------------------------------------
# bot orders
# ---------------------------------------------------------------------------
@app.command()
def orders(
    since: str = typer.Option("24h", help="Time window"),
) -> None:
    """Show recent orders."""
    setup_logging()
    asyncio.run(_show_orders(since))


async def _show_orders(since: str) -> None:
    from bot.reports import get_orders_report
    table = await get_orders_report(since)
    console.print(table)


# ---------------------------------------------------------------------------
# bot signal-dryrun
# ---------------------------------------------------------------------------
@app.command(name="signal-dryrun")
def signal_dryrun(
    message_id: int = typer.Option(..., "--message-id", help="DB message ID to dry-run"),
) -> None:
    """Dry-run signal routing on an existing message (no orders placed)."""
    setup_logging()
    asyncio.run(_signal_dryrun(message_id))


async def _signal_dryrun(message_id: int) -> None:
    from bot.db import session_scope
    from bot.models import Message, Signal
    from sqlalchemy import select

    async with session_scope() as session:
        result = await session.execute(select(Message).where(Message.id == message_id))
        message = result.scalar_one_or_none()

    if not message:
        console.print(f"[red]Message {message_id} not found.[/red]")
        raise typer.Exit(1)

    console.print(f"[bold]Dry-run signal routing for message {message_id}[/bold]")
    console.print(f"Text: {message.raw_text[:200]}")
    console.print()

    # Parse
    from bot.parser import parse_signal
    parsed = await parse_signal(message.raw_text or "")
    if not parsed:
        console.print("[red]LLM parsing failed.[/red]")
        return

    console.print(f"[cyan]Parsed result:[/cyan] {json.dumps(parsed, indent=2)}")

    if not parsed.get("is_signal"):
        console.print("[yellow]Not a trading signal.[/yellow]")
        return

    symbol = parsed.get("symbol", "")
    side = parsed.get("side", "")

    # Route
    from bot.router import route_signal
    from bot.models import Signal as SignalModel

    # Create temporary signal object for routing
    sig = SignalModel(
        message_id=message.id,
        channel_id=message.channel_id,
        is_signal=True,
        symbol=symbol,
        side=side,
        entry_prices_json=json.dumps(parsed.get("entry_prices", [])),
        take_profits_json=json.dumps(parsed.get("take_profits", [])),
        stop_loss=parsed.get("stop_loss"),
        leverage=parsed.get("leverage"),
        prompt_version=settings.signal_parser_prompt_version,
        skip_reason=None,
    )

    async with session_scope() as session:
        route_result = await route_signal(session, sig, message.channel_id)

    console.print()
    console.print("[bold]Routing result:[/bold]")
    if route_result.skip_reason:
        console.print(f"  [red]SKIP: {route_result.skip_reason}[/red]")
        return

    console.print(f"  Exchange: [green]{route_result.exchange}[/green]")
    console.print(f"  Symbol normalized: {route_result.symbol_normalized}")
    console.print(f"  Fees: {route_result.quote.fees_bps} bps")
    console.print(f"  Slippage: {route_result.quote.slippage_bps} bps")

    # Compute TP
    from decimal import Decimal
    from bot.tp_calculator import compute_tp_price_with_fallback, compute_sl_price_from_pct

    entry_prices = parsed.get("entry_prices", [])
    if entry_prices:
        avg_entry = Decimal(str(sum(entry_prices) / len(entry_prices)))
        leverage = parsed.get("leverage") or settings.default_leverage

        tp_price = compute_tp_price_with_fallback(
            avg_entry_price=avg_entry,
            side=side,
            leverage=leverage,
            exchange=route_result.exchange,
            quote_fees_bps=route_result.quote.fees_bps,
            quote_slippage_bps=route_result.quote.slippage_bps,
        )

        sl = parsed.get("stop_loss")
        sl_price = (
            Decimal(str(sl)) if sl
            else compute_sl_price_from_pct(avg_entry, side, leverage)
        )

        console.print(f"  Entry: {avg_entry}")
        console.print(f"  TP price: [green]{tp_price:.4f}[/green]")
        console.print(f"  SL price: [red]{sl_price:.4f}[/red]")
        console.print(f"  Leverage: {leverage}x")

    console.print()
    console.print("[bold yellow]DRY RUN — no orders placed.[/bold yellow]")


# ---------------------------------------------------------------------------
# vooi-errors subcommands
# ---------------------------------------------------------------------------
@vooi_errors_app.command("list")
def vooi_errors_list(
    limit: int = typer.Option(50, help="Max records to show"),
) -> None:
    """List recent VOOI API errors."""
    asyncio.run(_vooi_errors_list(limit))


async def _vooi_errors_list(limit: int) -> None:
    from bot.reports import get_vooi_errors_report
    table = await get_vooi_errors_report(limit)
    console.print(table)


@vooi_errors_app.command("export")
def vooi_errors_export(
    output: str = typer.Option("vooi-errors.jsonl", help="Output file path"),
    all_records: bool = typer.Option(False, "--all", help="Export all (not just unreported)"),
) -> None:
    """Export VOOI errors to JSONL file."""
    asyncio.run(_vooi_errors_export(output, not all_records))


async def _vooi_errors_export(output: str, unreported_only: bool) -> None:
    from bot.reports import export_vooi_errors_jsonl
    count = await export_vooi_errors_jsonl(output, unreported_only)
    console.print(f"[green]Exported {count} errors to {output}[/green]")


@vooi_errors_app.command("mark-reported")
def vooi_errors_mark_reported(
    ids: str = typer.Argument(help="Comma-separated error IDs to mark as reported"),
) -> None:
    """Mark errors as reported."""
    asyncio.run(_vooi_errors_mark_reported(ids))


async def _vooi_errors_mark_reported(ids_str: str) -> None:
    from bot.reports import mark_errors_reported
    ids = [int(i.strip()) for i in ids_str.split(",") if i.strip()]
    count = await mark_errors_reported(ids)
    console.print(f"[green]Marked {count} errors as reported.[/green]")


@vooi_errors_app.command("prune")
def vooi_errors_prune(
    days: int = typer.Option(30, help="Delete reported errors older than N days"),
) -> None:
    """Delete old reported errors."""
    asyncio.run(_vooi_errors_prune(days))


async def _vooi_errors_prune(days: int) -> None:
    from bot.reports import prune_vooi_errors
    count = await prune_vooi_errors(days)
    console.print(f"[green]Pruned {count} old error records.[/green]")


# ---------------------------------------------------------------------------
# bot simulate-breakeven
# ---------------------------------------------------------------------------
@app.command(name="simulate-breakeven")
def simulate_breakeven(
    position_id: int = typer.Option(..., "--position-id", help="Position ID to simulate"),
    dry_run: bool = typer.Option(True, "--dry-run/--real", help="Dry run (default) or real"),
) -> None:
    """
    Simulate breakeven trigger for testing.
    Forcibly sets price cache to entry+3% and waits for watcher to trigger.
    """
    setup_logging()
    asyncio.run(_simulate_breakeven(position_id, dry_run))


async def _simulate_breakeven(position_id: int, dry_run: bool) -> None:
    from bot.breakeven_watcher import simulate_breakeven as sim_be

    if not dry_run:
        console.print(
            "[bold yellow]Running REAL breakeven simulation — will cancel+replace SL![/bold yellow]"
        )
        confirmed = typer.confirm("Are you sure?")
        if not confirmed:
            console.print("Aborted.")
            return

    try:
        await sim_be(position_id, dry_run=dry_run)
        console.print(f"[green]Breakeven simulation completed for position {position_id}.[/green]")
    except ValueError as e:
        console.print(f"[red]{e}[/red]")
        raise typer.Exit(1)


@app.command()
def healthcheck() -> None:
    """QUALITY-03: Verify DB connectivity and config are valid (used by Docker HEALTHCHECK)."""
    setup_logging()
    asyncio.run(_healthcheck())


async def _healthcheck() -> None:
    from sqlalchemy import text

    from bot.db import engine

    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        console.print("OK")
    except Exception as e:
        console.print(f"[red]UNHEALTHY: {e}[/red]")
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
