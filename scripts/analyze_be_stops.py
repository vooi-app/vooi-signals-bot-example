"""Post-mortem: did BE-stopped positions reach TP1 anyway?

For each position closed at the moved-to-BE stop loss, query Binance Futures
1m klines for the 24h window after close and report whether price reached
the originally-declared TP1.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal

import httpx

BINANCE_FUTURES_KLINES = "https://fapi.binance.com/fapi/v1/klines"


@dataclass
class Position:
    pid: int
    symbol: str
    side: str            # 'buy' / 'sell'
    entry: Decimal
    tp1: Decimal         # tp_price_initial OR first of take_profits
    close_price: Decimal
    opened_ts: int
    be_moved_ts: int
    closed_ts: int


POSITIONS: list[Position] = [
    Position(77, "LINK", "sell", Decimal("9.521"),   Decimal("9.46444"), Decimal("9.51824"), 1779773879, 1779774628, 1779775172),
    Position(67, "AVAX", "sell", Decimal("9.309"),   Decimal("9.2535"),  Decimal("9.3041"),  1779626555, 1779629905, 1779630027),
    Position(64, "SOL",  "sell", Decimal("86.757"),  Decimal("83.7366"), Decimal("86.721"),  1779569916, 1779570867, 1779571217),
    Position(58, "JTO",  "buy",  Decimal("0.5043"),  Decimal("0.522473"),Decimal("0.50488"), 1779470781, 1779470796, 1779470855),
    Position(51, "GMT",  "sell", Decimal("0.01038"), Decimal("0.01017"), Decimal("0.010369"),1779376967, 1779380352, 1779380438),
    Position(45, "JTO",  "sell", Decimal("0.55"),    Decimal("0.525"),   Decimal("0.54997"), 1779338842, 1779338889, 1779339086),
    Position(44, "BTC",  "sell", Decimal("77900"),   Decimal("77121"),   Decimal("77821"),   1779324648, 1779326802, 1779326801),
    Position(38, "BTC",  "buy",  Decimal("77200"),   Decimal("80000"),   Decimal("77243.7"), 1779263660, 1779302397, 1779315003),
    Position(17, "UB",   "buy",  Decimal("0.2045"),  Decimal("0.218"),   Decimal("0.2025"),  1778912580, 1778912659, 1778913437),
]


async def fetch_klines(
    client: httpx.AsyncClient, symbol: str, start_ms: int, end_ms: int
) -> list[list]:
    """Fetch 1m klines paged across the full window (Binance caps 1500 per req)."""
    out: list[list] = []
    cur = start_ms
    while cur < end_ms:
        params = {
            "symbol": f"{symbol}USDT",
            "interval": "1m",
            "startTime": cur,
            "endTime": end_ms,
            "limit": 1500,
        }
        r = await client.get(BINANCE_FUTURES_KLINES, params=params, timeout=20)
        if r.status_code != 200:
            return out  # likely "Invalid symbol"
        rows = r.json()
        if not rows:
            break
        out.extend(rows)
        last_close = int(rows[-1][6])
        if last_close <= cur:
            break
        cur = last_close + 1
    return out


def analyze(pos: Position, klines: list[list]) -> dict:
    """Return: did TP1 ever get hit after closed_ts, peak favorable price, time-to-TP."""
    closed_ms = pos.closed_ts * 1000
    tp1 = pos.tp1
    side = pos.side

    # Best price = lowest low (for sell) or highest high (for buy) after BE close.
    best_price: Decimal | None = None
    tp_hit_ts: int | None = None
    tp_hit_within_24h = False

    for k in klines:
        open_ms = int(k[0])
        high = Decimal(k[2])
        low = Decimal(k[3])
        # Only consider candles that opened at-or-after the BE close.
        if open_ms < closed_ms:
            continue

        # Track best favorable price.
        if side == "sell":
            if best_price is None or low < best_price:
                best_price = low
            if tp_hit_ts is None and low <= tp1:
                tp_hit_ts = open_ms
        else:  # buy
            if best_price is None or high > best_price:
                best_price = high
            if tp_hit_ts is None and high >= tp1:
                tp_hit_ts = open_ms

    if tp_hit_ts is not None:
        tp_hit_within_24h = (tp_hit_ts - closed_ms) <= 24 * 3600 * 1000

    return {
        "pid": pos.pid,
        "symbol": pos.symbol,
        "side": pos.side,
        "entry": pos.entry,
        "tp1": tp1,
        "close_price": pos.close_price,
        "best_after_close": best_price,
        "tp_hit": tp_hit_ts is not None,
        "tp_hit_within_24h": tp_hit_within_24h,
        "minutes_to_tp": (
            (tp_hit_ts - closed_ms) // 60000 if tp_hit_ts is not None else None
        ),
        "be_to_close_seconds": pos.closed_ts - pos.be_moved_ts,
        "entry_to_be_seconds": pos.be_moved_ts - pos.opened_ts,
    }


async def main() -> None:
    async with httpx.AsyncClient() as client:
        results = []
        for p in POSITIONS:
            start_ms = (p.closed_ts - 60) * 1000               # 1 min margin
            end_ms = (p.closed_ts + 24 * 3600) * 1000          # +24h
            klines = await fetch_klines(client, p.symbol, start_ms, end_ms)
            if not klines:
                results.append({"pid": p.pid, "symbol": p.symbol, "error": "no klines"})
                continue
            results.append(analyze(p, klines))

    # Print table
    print(
        f"{'pid':>4}  {'sym':>5} {'side':>5} {'entry':>10} {'TP1':>10} "
        f"{'close':>10} {'best_after':>11} {'TP_hit':>7} {'<24h':>5} "
        f"{'min_to_TP':>10} {'BE→close,s':>11} {'entry→BE,s':>11}"
    )
    for r in results:
        if "error" in r:
            print(f"{r['pid']:>4}  {r['symbol']:>5}  -- {r['error']}")
            continue
        best = r["best_after_close"]
        favorable_pct = None
        if best is not None:
            diff = (best - r["entry"]) / r["entry"] * Decimal(100)
            if r["side"] == "sell":
                diff = -diff
            favorable_pct = float(diff)

        print(
            f"{r['pid']:>4}  {r['symbol']:>5} {r['side']:>5} "
            f"{float(r['entry']):>10.6g} {float(r['tp1']):>10.6g} "
            f"{float(r['close_price']):>10.6g} "
            f"{float(best) if best else 0:>11.6g} "
            f"{'YES' if r['tp_hit'] else 'no':>7} "
            f"{'YES' if r['tp_hit_within_24h'] else 'no':>5} "
            f"{r['minutes_to_tp'] if r['minutes_to_tp'] is not None else '-':>10} "
            f"{r['be_to_close_seconds']:>11} {r['entry_to_be_seconds']:>11}"
        )
        if favorable_pct is not None:
            print(
                f"      best favorable post-close: {favorable_pct:+.3f}% "
                f"(TP1 distance from entry: "
                f"{abs(float(r['tp1']) - float(r['entry'])) / float(r['entry']) * 100:.3f}%)"
            )


if __name__ == "__main__":
    asyncio.run(main())
