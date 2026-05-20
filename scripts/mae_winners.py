"""
Maximum Adverse Excursion analysis for TP-closed (winning) positions.

For each historical 'closed_tp' position, pull 1m klines from Binance over
the holding window and compute how deep the position went into the red before
turning around and hitting TP. Output is MAE expressed as % of margin —
the metric you'd use to pick a safe SL distance.

Run:
    .venv/bin/python -m scripts.mae_winners
"""
import asyncio
from decimal import Decimal
from typing import Optional

import httpx
from sqlalchemy import select

from bot.db import dispose_engine, session_scope
from bot.models import Position


BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"


async def _fetch_klines(
    client: httpx.AsyncClient, symbol: str, start_ms: int, end_ms: int
) -> Optional[list[list]]:
    pair = f"{symbol.upper()}USDT"
    out: list[list] = []
    cursor = start_ms
    while cursor < end_ms:
        try:
            r = await client.get(
                BINANCE_KLINES_URL,
                params={
                    "symbol": pair,
                    "interval": "1m",
                    "startTime": cursor,
                    "endTime": end_ms,
                    "limit": 1000,
                },
                timeout=15,
            )
            if r.status_code != 200:
                return None
            batch = r.json()
            if not batch:
                break
            out.extend(batch)
            last_open = batch[-1][0]
            if last_open + 60_000 >= end_ms:
                break
            cursor = last_open + 60_000
        except Exception:
            return None
    return out or None


def _worst_against(klines: list[list], entry: Decimal, side: str) -> Decimal:
    """Return the worst (most adverse) price during the window."""
    worst = entry
    for k in klines:
        high = Decimal(k[2])
        low = Decimal(k[3])
        if side == "buy":
            if low < worst:
                worst = low
        else:
            if high > worst:
                worst = high
    return worst


def _pct_margin(entry: Decimal, worst: Decimal, side: str, leverage: int) -> Decimal:
    price_move_pct = ((worst - entry) / entry) * Decimal("100")
    # Sign convention: positive number = adverse for the trader.
    adverse_pct = -price_move_pct if side == "buy" else price_move_pct
    return adverse_pct * Decimal(str(leverage))


async def main() -> None:
    async with session_scope() as session:
        result = await session.execute(
            select(Position)
            .where(Position.close_reason == "closed_tp")
            .order_by(Position.opened_at)
        )
        positions = list(result.scalars())

    print(f"Analysing {len(positions)} winning (closed_tp) positions.\n")
    print(
        f"{'pos':>4}  {'sym':<6} {'side':<4} {'lev':>3}  "
        f"{'entry':>11}  {'TP px':>11}  {'worst px':>11}  "
        f"{'MAE %price':>10}  {'MAE %margin':>12}  {'held(min)':>9}"
    )
    print("-" * 110)

    rows: list[dict] = []
    async with httpx.AsyncClient() as http:
        for p in positions:
            start_ms = int(p.opened_at.timestamp() * 1000)
            end_ms = int(p.closed_at.timestamp() * 1000) if p.closed_at else start_ms + 86_400_000
            klines = await _fetch_klines(http, p.symbol, start_ms, end_ms)
            if not klines:
                print(
                    f"{p.id:>4}  {p.symbol:<6} {p.side:<4} {p.leverage:>3}  "
                    f"{float(p.entry_price):>11.6g}  {float(p.close_price):>11.6g}  "
                    f"     no-klines-from-binance"
                )
                continue

            worst = _worst_against(klines, p.entry_price, p.side)
            adverse_price_pct = (
                (-(worst - p.entry_price) / p.entry_price * 100)
                if p.side == "buy"
                else ((worst - p.entry_price) / p.entry_price * 100)
            )
            adverse_margin_pct = _pct_margin(p.entry_price, worst, p.side, p.leverage)
            held_min = (p.closed_at - p.opened_at).total_seconds() / 60.0

            rows.append(
                {
                    "id": p.id, "sym": p.symbol, "side": p.side, "lev": p.leverage,
                    "entry": p.entry_price, "tp": p.close_price, "worst": worst,
                    "mae_price_pct": adverse_price_pct, "mae_margin_pct": adverse_margin_pct,
                    "held_min": held_min,
                }
            )

            print(
                f"{p.id:>4}  {p.symbol:<6} {p.side:<4} {p.leverage:>3}  "
                f"{float(p.entry_price):>11.6g}  {float(p.close_price):>11.6g}  "
                f"{float(worst):>11.6g}  "
                f"{float(adverse_price_pct):>9.3f}%  {float(adverse_margin_pct):>11.2f}%  "
                f"{held_min:>9.1f}"
            )

    if rows:
        margin_pcts = [float(r["mae_margin_pct"]) for r in rows]
        margin_pcts.sort()
        worst = max(margin_pcts)
        avg = sum(margin_pcts) / len(margin_pcts)
        median = margin_pcts[len(margin_pcts) // 2]
        p75 = margin_pcts[int(len(margin_pcts) * 0.75)]
        p90 = margin_pcts[int(len(margin_pcts) * 0.90)]
        print("\nMAE in % of margin (adverse) across winners:")
        print(f"  worst : {worst:>6.2f}%")
        print(f"  p90   : {p90:>6.2f}%")
        print(f"  p75   : {p75:>6.2f}%")
        print(f"  median: {median:>6.2f}%")
        print(f"  avg   : {avg:>6.2f}%")
        print("\nInterpretation:")
        print(f"  SL > {p90:.0f}% margin: keeps ~90% of historical winners alive")
        print(f"  SL > {worst:.0f}% margin: would have saved ALL winners we have data for")

    await dispose_engine()


if __name__ == "__main__":
    asyncio.run(main())
