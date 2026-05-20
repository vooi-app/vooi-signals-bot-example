"""
Telegram signal ingester.
Primary: Telethon real-time event listener.
Fallback: polling loop every TELEGRAM_POLL_INTERVAL_SEC.
Deduplication by (channel_id, message_id).
Per spec §5 v1.5.
"""
import asyncio
from datetime import datetime, timezone
from typing import Optional

import structlog
from sqlalchemy import and_, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from telethon import TelegramClient, events
from telethon.tl.types import Channel as TelegramChannel
from telethon.tl.types import Message as TelegramMessage

from bot.config import settings
from bot.db import session_scope
from bot.models import Channel, Message, Signal
from bot.parser import process_message_for_signal
from bot.router import route_signal
from bot.orders import place_entry_order
from bot.router import record_placement
from bot.streamer import emit_event

log = structlog.get_logger(__name__)

# Global Telethon client (initialized in ingester_task)
_tg_client: Optional[TelegramClient] = None

# Track last polled message ID per channel: {channel_id: message_id}
_last_seen_message_id: dict[int, int] = {}


def get_telegram_client() -> TelegramClient:
    """Get or create the Telethon client."""
    global _tg_client
    if _tg_client is None:
        _tg_client = TelegramClient(
            settings.telegram_session_name,
            settings.telegram_api_id,
            settings.telegram_api_hash,
        )
    return _tg_client


async def get_active_channel_ids() -> list[int]:
    """Fetch active channel Telegram IDs from DB."""
    async with session_scope() as session:
        result = await session.execute(
            select(Channel.telegram_id).where(Channel.is_active == True)
        )
        return [row[0] for row in result.all()]


async def process_telegram_message(
    tg_message: TelegramMessage,
    channel_telegram_id: int,
    channel_username: Optional[str] = None,
) -> Optional[Signal]:
    """
    Process a single Telegram message:
    1. Deduplicate by (channel_id, message_id)
    2. Save to messages table
    3. Parse with LLM
    4. Route and place order if valid signal
    """
    message_id = tg_message.id
    text = tg_message.message or tg_message.text or ""

    if not text:
        return None

    async with session_scope() as session:
        # Find channel
        channel_result = await session.execute(
            select(Channel).where(Channel.telegram_id == channel_telegram_id)
        )
        channel = channel_result.scalar_one_or_none()
        if channel is None:
            return None

        # Atomic dedup: INSERT ... ON CONFLICT DO NOTHING avoids the race
        # between SELECT+INSERT when Telethon re-delivers a message (e.g. after
        # `Got difference for channel`). RETURNING id lets us skip parsing
        # when the row already existed.
        stmt = (
            pg_insert(Message)
            .values(
                channel_id=channel.id,
                telegram_message_id=message_id,
                raw_text=text,
                received_at=datetime.now(timezone.utc),
                processed=False,
            )
            .on_conflict_do_nothing(index_elements=["channel_id", "telegram_message_id"])
            .returning(Message.id)
        )
        ins_result = await session.execute(stmt)
        inserted_id = ins_result.scalar_one_or_none()
        if inserted_id is None:
            log.debug(
                "message_duplicate_skipped",
                channel_id=channel.id,
                telegram_message_id=message_id,
            )
            return None

        msg_id = inserted_id
        channel_id = channel.id
        channel_uname = channel.username or channel_username

        log.info(
            "message_received",
            channel=channel_uname,
            message_id=message_id,
            text_preview=text[:80],
        )

    # Load message for parser (outside session to avoid holding connection)
    async with session_scope() as session:
        result = await session.execute(select(Message).where(Message.id == msg_id))
        message = result.scalar_one_or_none()

    if message is None:
        return None

    # Parse with LLM
    signal = await process_message_for_signal(message, channel_uname)

    if signal and signal.is_signal:
        await emit_event(
            "SIGNAL_PARSED",
            signal_id=signal.id,
            symbol=signal.symbol,
            message=(
                f"{signal.symbol} {signal.side} "
                f"entry={signal.entry_prices_json} "
                f"SL={signal.stop_loss} "
                f"channel={channel_uname}  signal_id={signal.id}"
            ),
        )

        # Route and place order
        if not signal.skip_reason:
            await handle_signal_routing(signal, channel_id)
    elif signal:
        await emit_event(
            "SIGNAL_SKIPPED",
            signal_id=signal.id,
            message=f"skip_reason={signal.skip_reason or 'not_signal'}",
        )

    return signal


async def handle_signal_routing(signal: Signal, channel_id: int) -> None:
    """Route signal and place entry order if routing succeeds."""
    async with session_scope() as session:
        route_result = await route_signal(session, signal, channel_id)

        if route_result.skip_reason:
            # НОВЫЙ-02: persist routing skip_reason on the signal row so `bot signals`
            # and downstream reports can show why a signal was rejected (AC#3).
            # Parser-level skips already write skip_reason; routing-level didn't.
            sig_result = await session.execute(
                select(Signal).where(Signal.id == signal.id)
            )
            sig_row = sig_result.scalar_one_or_none()
            if sig_row is not None and not sig_row.skip_reason:
                sig_row.skip_reason = route_result.skip_reason

            log.info(
                "signal_routing_skipped",
                signal_id=signal.id,
                skip_reason=route_result.skip_reason,
            )
            await emit_event(
                "SIGNAL_SKIPPED",
                signal_id=signal.id,
                message=f"routing skip_reason={route_result.skip_reason}",
            )
            return

        # Place entry order
        order = await place_entry_order(
            session=session,
            signal=signal,
            exchange=route_result.exchange,
            symbol_normalized=route_result.symbol_normalized,
            quote=route_result.quote,
        )

        if order is not None:
            record_placement(channel_id)


async def ingester_task() -> None:
    """
    Main ingester task.
    Starts Telethon client, registers real-time listener,
    and runs polling loop in parallel.
    """
    client = get_telegram_client()

    try:
        await client.start()
        log.info("telegram_client_started")

        # Register real-time listener (primary mode)
        channel_ids = await get_active_channel_ids()

        @client.on(events.NewMessage(chats=channel_ids if channel_ids else None))
        async def on_new_message(event: events.NewMessage.Event) -> None:
            try:
                tg_message = event.message
                chat = await event.get_chat()
                channel_tg_id = chat.id

                # Handle channels not in our list (safety check)
                if channel_ids and channel_tg_id not in channel_ids:
                    return

                username = getattr(chat, "username", None)
                await process_telegram_message(tg_message, channel_tg_id, username)
            except Exception as e:
                log.error("on_new_message_error", error=str(e))

        log.info(
            "telethon_listener_registered",
            channel_count=len(channel_ids),
        )

        # Run polling loop as concurrent task (fallback)
        polling_task = asyncio.create_task(ingester_polling_loop(client))

        # Keep client running
        await client.run_until_disconnected()

        polling_task.cancel()
        try:
            await polling_task
        except asyncio.CancelledError:
            pass

    except asyncio.CancelledError:
        log.info("ingester_cancelled")
    finally:
        global _tg_client
        if client.is_connected():
            await client.disconnect()
        _tg_client = None  # БАГ-03: reset so get_telegram_client() creates fresh client on reconnect


async def ingester_polling_loop(client: TelegramClient) -> None:
    """
    Polling fallback — runs every TELEGRAM_POLL_INTERVAL_SEC.
    Catches messages missed during reconnect or event handler downtime.
    """
    while True:
        try:
            await asyncio.sleep(settings.telegram_poll_interval_sec)
            await poll_and_process_new_messages(client)
        except asyncio.CancelledError:
            break
        except Exception as e:
            log.error("polling_loop_error", error=str(e))


async def poll_and_process_new_messages(client: TelegramClient) -> None:
    """Poll all active channels for new messages since last seen."""
    channel_ids = await get_active_channel_ids()

    async with session_scope() as session:
        channels_result = await session.execute(
            select(Channel).where(Channel.is_active == True)
        )
        channels = channels_result.scalars().all()

    for channel in channels:
        try:
            min_id = _last_seen_message_id.get(channel.telegram_id, 0)

            async for tg_message in client.iter_messages(
                channel.telegram_id,
                min_id=min_id,
                limit=50,
            ):
                if isinstance(tg_message, TelegramMessage):
                    await process_telegram_message(
                        tg_message,
                        channel.telegram_id,
                        channel.username,
                    )
                    if tg_message.id > _last_seen_message_id.get(channel.telegram_id, 0):
                        _last_seen_message_id[channel.telegram_id] = tg_message.id

        except Exception as e:
            log.warning(
                "poll_channel_error",
                channel_id=channel.id,
                telegram_id=channel.telegram_id,
                error=str(e),
            )
