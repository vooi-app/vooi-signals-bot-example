"""
Bug #3 (round 2): the SSE silence counter inherited from a previous run
should be reset to "now" when a new SSE connection opens, so the reconciler
doesn't keep emitting `reconciler_sse_silent` warnings for a stream that's
healthy but quiet.
"""
import asyncio
import time
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import bot.sse_listener as sse_mod


class _FakeEventSource:
    def __init__(self, events=None):
        self._events = events or []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def aiter_sse(self):
        for e in self._events:
            yield e


@asynccontextmanager
async def _fake_client(*args, **kwargs):
    yield MagicMock()


@pytest.mark.asyncio
async def test_sse_connect_resets_silence_counter(monkeypatch):
    # Simulate stale module-global from previous run
    sse_mod.sse_last_event_at = 0.0
    stale_age_before = time.monotonic() - sse_mod.sse_last_event_at
    assert stale_age_before > 60  # cannot be normal silence

    @asynccontextmanager
    async def _fake_httpx_client(*args, **kwargs):
        yield MagicMock()

    monkeypatch.setattr(sse_mod, "httpx", MagicMock(AsyncClient=lambda *a, **kw: _fake_httpx_client(), Timeout=lambda *a, **kw: None))

    def _fake_aconnect_sse(client, method, path):
        # No events streamed — just a successful connect, exit immediately.
        return _FakeEventSource(events=[])

    monkeypatch.setattr(sse_mod, "aconnect_sse", _fake_aconnect_sse)

    await sse_mod._run_sse_connection()

    # After connect, silence counter must have been reset.
    silence_after = time.monotonic() - sse_mod.sse_last_event_at
    assert silence_after < 5, (
        f"sse_last_event_at not reset on connect: silence={silence_after}s"
    )
