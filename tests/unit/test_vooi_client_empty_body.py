"""
Unit tests for VooiClient empty-body handling.

VOOI returns HTTP 201 with empty body for endpoints like POST /exchange/leverage
and POST /exchange/margin-mode. The client must treat that as success and
return an empty dict, NOT raise "Expecting value: line 1 column 1 (char 0)".
"""
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from bot.vooi_client import VooiClient


@pytest.fixture
def patched_audit():
    with patch("bot.vooi_client.insert_api_call_log", AsyncMock()) as m_call, \
         patch("bot.vooi_client.insert_error_log", AsyncMock()) as m_err:
        yield m_call, m_err


async def _fake_response(status_code: int, text: str, content: bytes = None):
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.text = text
    resp.content = content if content is not None else text.encode("utf-8")
    resp.headers = {}
    resp.json = MagicMock(side_effect=lambda: __import__("json").loads(text))
    resp.raise_for_status = MagicMock()
    return resp


async def test_empty_body_201_returns_empty_dict(patched_audit):
    """POST /exchange/leverage returns 201 + empty body → client returns {}."""
    client = VooiClient()
    await client._ensure_client()
    fake_resp = await _fake_response(201, "")

    with patch.object(client._client, "request", AsyncMock(return_value=fake_resp)):
        result = await client.post(
            "/exchange/leverage",
            {"exchange": "lighter", "asset": "DOGE", "leverage": 5},
        )

    assert result == {}

    # No error log should have been written.
    _, mock_err_log = patched_audit
    mock_err_log.assert_not_called()


async def test_empty_body_200_returns_empty_dict(patched_audit):
    """200 OK with empty body is also treated as success."""
    client = VooiClient()
    await client._ensure_client()
    fake_resp = await _fake_response(200, "")

    with patch.object(client._client, "request", AsyncMock(return_value=fake_resp)):
        result = await client.post("/exchange/margin-mode", {"x": 1})

    assert result == {}


async def test_whitespace_only_body_returns_empty_dict(patched_audit):
    """A body of just whitespace must also be treated as empty success."""
    client = VooiClient()
    await client._ensure_client()
    fake_resp = await _fake_response(200, "  \n  ")

    with patch.object(client._client, "request", AsyncMock(return_value=fake_resp)):
        result = await client.post("/exchange/leverage", {"x": 1})

    assert result == {}


async def test_json_body_still_parsed(patched_audit):
    """A real JSON body still gets parsed as before — regression guard."""
    client = VooiClient()
    await client._ensure_client()
    fake_resp = await _fake_response(200, '{"status":"ok","orderId":"abc"}')

    with patch.object(client._client, "request", AsyncMock(return_value=fake_resp)):
        result = await client.post("/exchange/orders", {"x": 1})

    assert result == {"status": "ok", "orderId": "abc"}
