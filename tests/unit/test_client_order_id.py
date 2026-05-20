"""
Tests for make_client_order_id.

Hyperliquid rejects orders whose clientOrderId is not a 128-bit hex string
prefixed with "0x" (34 chars total). The bot's prior id format
"sigbot-{signal_id}-{8 hex chars}" produced 503 errors.
"""
import re

import pytest

from bot.orders import make_client_order_id


HEX_34 = re.compile(r"^0x[0-9a-f]{32}$")


def test_hyperliquid_id_is_0x_prefixed_32_hex():
    cid = make_client_order_id(signal_id=281, exchange="hyperliquid")
    assert HEX_34.match(cid) is not None, f"bad HL clientOrderId: {cid!r}"
    assert len(cid) == 34


def test_hyperliquid_ids_unique():
    a = make_client_order_id(123, "hyperliquid")
    b = make_client_order_id(123, "hyperliquid")
    assert a != b


def test_hyperliquid_id_case_insensitive_exchange():
    cid = make_client_order_id(signal_id=1, exchange="Hyperliquid")
    assert HEX_34.match(cid) is not None


def test_lighter_id_keeps_descriptive_form():
    cid = make_client_order_id(signal_id=42, exchange="lighter")
    assert cid.startswith("sigbot-42-")
    # No "0x" prefix forced
    assert not cid.startswith("0x")


def test_aster_id_keeps_descriptive_form():
    cid = make_client_order_id(signal_id=99, exchange="aster")
    assert cid.startswith("sigbot-99-")


def test_suffix_embedded_for_non_hyperliquid():
    cid_sl = make_client_order_id(signal_id=7, exchange="lighter", suffix="sl")
    cid_tp = make_client_order_id(signal_id=7, exchange="lighter", suffix="tp")
    assert "-sl-" in cid_sl
    assert "-tp-" in cid_tp


def test_suffix_ignored_for_hyperliquid():
    # HL ids must be 34 chars total regardless of suffix.
    cid = make_client_order_id(signal_id=7, exchange="hyperliquid", suffix="sl")
    assert HEX_34.match(cid) is not None
