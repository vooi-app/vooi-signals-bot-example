# VOOI Signal Bot

Automated crypto-futures trading bot that ingests trading signals from Telegram channels, parses them with an LLM, and routes orders to perpetual exchanges (Hyperliquid, Lighter, Aster) via the [VOOI Ultra API](https://perps-api.vooi.io).

> ⚠️ **This bot trades with real money.** It will place leveraged perpetual orders against connected exchanges as soon as it parses a valid signal. Read the [Safety](#safety) and [Risk management](#risk-management) sections before pointing it at a funded account. The authors accept no liability for trading losses. See [LICENSE](./LICENSE).

---

## Contents

- [What it does](#what-it-does)
- [Architecture](#architecture)
- [Signal funnel](#signal-funnel)
- [Quickstart](#quickstart)
- [Configuration](#configuration)
- [CLI reference](#cli-reference)
- [Database schema](#database-schema)
- [Risk management](#risk-management)
- [Operations](#operations)
- [Development](#development)
- [Known limitations](#known-limitations)
- [License](#license)

---

## What it does

1. **Listens** to a configured set of Telegram channels via Telethon (real-time + 60s polling fallback).
2. **Parses** each message with an OpenAI-compatible LLM into a structured signal `{symbol, side, entry_prices, take_profits, stop_loss, leverage}` or rejects it as noise.
3. **Routes** valid signals to one of three connected exchanges (Hyperliquid / Lighter / Aster) using live quotes from `/exchange/quotes` to pick the venue with the best execution.
4. **Places** a reduce-only-aware limit entry, then — once filled — places SL and TP triggers via VOOI Ultra API.
5. **Monitors** open positions: moves SL to breakeven on configurable PnL%, runs a reconciler against `/exchange/positions` + `/exchange/open-orders`, and an SL-safety watchdog that fires `ERROR_NAKED_POSITION` if a position is observed without a stop-loss.
6. **Persists** everything (messages → signals → orders → positions → trades) in PostgreSQL so the system survives restarts and can audit historic runs.

### Scope

- ✅ Spot perpetual futures (linear, USD-quoted).
- ✅ Three venues via VOOI: Hyperliquid, Lighter, Aster.
- ❌ Spot trading (no spot endpoints used).
- ❌ Options.
- ❌ Mean-reversion / market-making strategies — entries are driven by external signals only.

---

## Architecture

```
┌──────────────┐    NewMessage    ┌──────────┐    parse     ┌────────┐
│ Telegram     ├─────────────────▶│ ingester ├─────────────▶│ LLM    │
│ channels     │                  └─────┬────┘              │ (GPT)  │
└──────────────┘                        │                   └────┬───┘
                                        ▼ valid signal           │
                                  ┌────────────┐  ◀──────────────┘
                                  │  router    │
                                  └─────┬──────┘
                                        │ quote shop, sizing,
                                        ▼ rate-limit, dedup
                                  ┌────────────┐    POST /exchange/orders
                                  │  orders.py ├──────────────────────────▶ VOOI API ─▶ exchange
                                  └─────┬──────┘
                                        │ entry filled (SSE/reconciler)
                                        ▼
                                  ┌──────────────────┐    POST trigger × 2
                                  │ post_fill_placer ├──────────────────▶  VOOI API
                                  └─────┬────────────┘    (SL + TP)
                                        │
                                        ▼
                          ┌───────────────────────────────┐
                          │ breakeven (SSE-driven)        │  ── price ticks fire evaluator
                          │ breakeven_supervisor (10s)    │  ── heartbeat + reconcile + rescue
                          │ reconciler (60s sync)         │  ── REST positions/orders
                          │ sl_safety_check (30s)         │  ── NAKED + NO_TP alerts
                          │ tp_safety_watchdog (30s)      │  ── retries missing TPs
                          │ lighter_sl_watchdog (30s)     │  ── re-places dropped SLs
                          └───────────────────────────────┘
```

### Concurrent tasks (started by `bot run`)

| Task | File | Purpose |
|---|---|---|
| `ingester` | `bot/ingester.py` | Telethon real-time + polling, persists messages |
| `sse_listener` | `bot/sse_listener.py` | Subscribes to `/exchange/updates` for live prices + order events |
| `reconciler` | `bot/reconciler.py` | Every 60s reconciles DB ↔ exchange (positions, orders, expired limits) |
| `breakeven_supervisor` | `bot/breakeven_watcher.py` | 10 s loop: heartbeat, reconcile in-memory `trigger_index` with DB, REST-rescue positions whose SSE feed went stale. Trigger detection itself is event-driven from SSE — see `evaluate_breakeven_trigger`. |
| `sl_safety_check` | `bot/sl_safety.py` | Detects positions without active SL → `ERROR_NAKED_POSITION`. Also flags missing TP → `ERROR_NO_TP` (only while SL has not been moved to breakeven). |
| `tp_safety_watchdog` | `bot/sl_safety.py` | Re-places missing TP orders for open positions. Rate-limit: 6 attempts/hour per position. |
| `lighter_sl_watchdog` | `bot/sl_safety.py` | Re-places SL on lighter (where the venue occasionally drops triggers). Rate-limit: 3 attempts/hour per position. |

`startup_cleanup` (`bot/startup_cleanup.py`) runs once at boot to cancel dangling orphan orders and mark phantom positions.

---

## Signal funnel

The "yield" from a raw message to an actual fill is heavily filtered. Understanding this is essential for tuning.

```
Telegram messages
       │
       ▼  LLM classifier
   parsed signal? ──── no ──▶ stored as noise (signals.is_signal = false)
       │ yes
       ▼  router checks
   skip_reason set? ──── yes ─▶ stored, no order (symbol_not_found, rate_limit_*, already_in_position, ...)
       │ no
       ▼  POST /exchange/orders
   entry order placed ──── rejected by exchange ─▶ status='rejected'
       │ filled
       ▼  post_fill_placer
   SL + TP placed (status='open') ── partial fail ─▶ ERROR_NAKED_POSITION
       │
       ▼  outcomes
   closed_tp / closed_sl / closed_manual / closed_liquidation
```

Skip reasons populated by the router:

| `signals.skip_reason` | When |
|---|---|
| `symbol_not_found` | Symbol absent from `/exchange/markets` on all enabled venues |
| `rate_limit_global` | `MAX_PLACEMENTS_PER_HOUR_GLOBAL` exceeded |
| `rate_limit_per_channel` | `MAX_PLACEMENTS_PER_HOUR_PER_CHANNEL` exceeded |
| `already_in_position` | A position for the same symbol is already open on the chosen venue |
| `symbol_active_on_all_available_exchanges` | All venues already trading this symbol — no free slot |

---

## Quickstart

Three supported paths: **Docker** (fastest, batteries included), **native local**
(macOS / Linux dev box with system Postgres), and **VPS** (Ubuntu/Debian, systemd).
All three need the same credentials.

### Prerequisites

- A Telegram account; app credentials from <https://my.telegram.org/apps>.
- A VOOI Ultra API key and broker IDs for each exchange. Contact the VOOI team.
- An OpenAI-compatible API key (defaults to `gpt-4o-mini`).
- Python **3.11+** and PostgreSQL **15+** (Docker bundles both).

### Credentials and configuration

```bash
git clone <repo-url> vooi-signal-bot
cd vooi-signal-bot
cp .env.example .env
cp channels.txt.example channels.txt
```

Edit `.env` and fill in:

| Field | Where to get it |
|---|---|
| `TELEGRAM_API_ID`, `TELEGRAM_API_HASH` | <https://my.telegram.org/apps> |
| `VOOI_API_KEY` | VOOI team |
| `LLM_API_KEY` | OpenAI / Anthropic / Azure-OpenAI key |
| `POSTGRES_PASSWORD` and matching `DATABASE_URL` | Choose your own |
| `ALERT_TELEGRAM_BOT_TOKEN` / `_CHAT_ID` (optional) | Create via @BotFather, get chat id from @userinfobot |

Sensible defaults are already set for trading parameters (`DEFAULT_SL_PCT`,
`MIN_PROFIT_PCT_OF_COLLATERAL`, `BREAKEVEN_TRIGGER_PCT`, …). See
[Configuration](#configuration) before changing them.

---

### Path A — Docker (recommended for first run)

```bash
docker compose up -d db                                # start Postgres
docker compose run --rm bot python -m bot tg-login     # one-time interactive Telegram auth
docker compose up -d bot                               # start the bot
docker compose logs -f bot                             # follow output
```

The `tg-login` step prompts for the SMS code that Telegram sends to your phone.
It writes `signal_bot.session` (SQLite) in the project root — keep it safe; it
is the equivalent of a logged-in Telegram session.

Stop everything: `docker compose down`. The DB volume persists across restarts.

---

### Path B — Native local (macOS / Linux dev box)

Useful when you want to iterate on code without rebuilding containers.

```bash
# macOS — Postgres via Homebrew
brew install postgresql@15
brew services start postgresql@15
createdb signalbot
psql signalbot -c "CREATE USER botuser WITH PASSWORD 'change_me_before_production';"
psql signalbot -c "GRANT ALL PRIVILEGES ON DATABASE signalbot TO botuser;"
psql signalbot -c "GRANT ALL ON SCHEMA public TO botuser;"

# Linux — Postgres via apt
sudo apt install postgresql-15
sudo systemctl enable --now postgresql
sudo -u postgres createdb signalbot
sudo -u postgres psql -c "CREATE USER botuser WITH PASSWORD 'change_me_before_production';"
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE signalbot TO botuser;"

# Python project (both platforms)
python3.11 -m venv .venv && source .venv/bin/activate
pip install -e .

# Apply migrations
alembic upgrade head

# Interactive Telegram login (one-time per session file)
python -m bot tg-login

# Add channels and start
python -m bot channels add @some_signals_group
python -m bot channels list
python -m bot run
```

Common gotcha: if you ever see `lock file "postmaster.pid" already exists` from
Postgres after a hard shutdown, delete the stale `postmaster.pid` in the data
directory (`/opt/homebrew/var/postgresql@15/` on macOS,
`/var/lib/postgresql/15/main/` on Linux) and restart the service. Never delete
the file while Postgres is actually running.

---

### Path C — VPS (Ubuntu/Debian + systemd)

Production-ish setup for a small VPS (1 vCPU, 1 GB RAM is enough; reserve 5+ GB
disk for logs and DB).

```bash
# 1) System prep
sudo apt update && sudo apt install -y python3.11 python3.11-venv \
    postgresql-15 git curl
sudo systemctl enable --now postgresql

# 2) Dedicated user
sudo useradd -m -s /bin/bash signalbot
sudo -iu signalbot

# 3) Clone + configure
git clone <repo-url> ~/signal_bot
cd ~/signal_bot
cp .env.example .env
nano .env                              # fill in credentials

# 4) Database
sudo -u postgres createdb signalbot
sudo -u postgres psql -c "CREATE USER botuser WITH PASSWORD 'pick-a-strong-one';"
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE signalbot TO botuser;"
# match DATABASE_URL in .env to the user/password you just set

# 5) Python deps + migrations
python3.11 -m venv .venv
source .venv/bin/activate
pip install -e .
alembic upgrade head

# 6) Telegram login (interactive — needs SMS code)
python -m bot tg-login

# 7) Add channels
python -m bot channels add @some_signals_group

# 8) Install the systemd unit
exit                                   # back to your sudo-able user
sudo cp /home/signalbot/signal_bot/signal-bot.service /etc/systemd/system/
sudo nano /etc/systemd/system/signal-bot.service   # check WorkingDirectory + User
sudo systemctl daemon-reload
sudo systemctl enable --now signal-bot
sudo systemctl status signal-bot
journalctl -u signal-bot -f
```

VPS-specific recommendations:

- **Firewall**: do not expose Postgres to the public internet. Bind it to
  `127.0.0.1` (`listen_addresses` in `postgresql.conf`).
- **Log rotation**: the bot writes to `./logs/bot.log` and `./logs/vooi-raw.log`.
  Add a logrotate config or rely on `journalctl --vacuum-time=14d`.
- **Backups**: `pg_dump signalbot > backup-$(date +%F).sql` daily — the DB is
  the source of truth for PnL accounting.
- **Restart on reboot**: `systemctl enable signal-bot` handles this. The Telethon
  session is persistent in `signal_bot.session`; you do not need to log in again.
- **Network reliability**: if Telegram or the VOOI API gets briefly cut off, the
  bot's `ingester` task crashes and the supervisor stops all six tasks (`exit 0`).
  The unit file has `Restart=on-failure` — adjust to `Restart=always` if you want
  it to recover from any exit without ops intervention.

---

### Verification (any path)

```bash
python -m bot status               # connectivity + counts
python -m bot vooi-check           # VOOI API auth check
python -m bot signals --since 1h   # show recently parsed signals
```

---

## Configuration

All configuration is via environment variables (12-factor). See `.env.example` for the full list with inline comments. The key knobs:

### Trading defaults

| Variable | Default | Effect |
|---|---|---|
| `DEFAULT_LEVERAGE` | `5` | Initial leverage if signal doesn't specify one |
| `MAX_LEVERAGE` | `10` | Hard cap regardless of signal |
| `DEFAULT_MARGIN_MODE` | `cross` | `cross` or `isolated` |
| `DEFAULT_POSITION_SIZE_PCT` | `5` | % of free margin per trade |
| `MAX_POSITION_SIZE_USD` | `1000` | Absolute notional cap |
| `DEFAULT_SL_PCT` | `5` | Max % of MARGIN to lose on a single trade. Price distance = `DEFAULT_SL_PCT / leverage`. |
| `USE_SIGNAL_SL` | `false` | `true`: honor signal's explicit stop_loss when present. `false`: always derive SL from `DEFAULT_SL_PCT` (hard-cap margin loss regardless of what the channel said). |

### Margin-denominated percentages

All "target" percentages in the trading section are denominated as a **fraction
of margin** (collateral), not price. This keeps numbers comparable across
leverages: "I want to risk 5% of margin per trade" is the same intent whether
you're trading 3x or 10x. The bot converts margin-% to price-% internally by
dividing by leverage.

```
price_distance_pct  =  margin_pct  /  leverage
```

| At lev=3 | At lev=5 | At lev=10 |
|---|---|---|
| 5% margin = 1.667% price | 5% margin = 1.0% price | 5% margin = 0.5% price |

### TP calculation

The TP price target is calculated dynamically:

```
TP_distance_price = (MIN_PROFIT_PCT_OF_COLLATERAL / 100 / leverage)
                  + max(actual_overhead_pct, TP_OVERHEAD_FLOOR_PCT / 100 / leverage)

where actual_overhead_pct = (round_trip_taker_fees_bps
                             + exit_slippage_bps
                             + FUNDING_COST_BUFFER_BPS
                             + broker_fee_bps * 2) / 10000
```

Both `MIN_PROFIT_PCT_OF_COLLATERAL` and `TP_OVERHEAD_FLOOR_PCT` are in **% of
margin**. The floor protects against quotes returning unrealistically small
overhead (e.g. some venues report taker=0); in that case the floor widens TP
so real fees and slippage cannot eat the profit.

| Variable | Default | Effect |
|---|---|---|
| `MIN_PROFIT_PCT_OF_COLLATERAL` | `3` | Required net profit on margin (%) after fees. |
| `FUNDING_COST_BUFFER_BPS` | `5` | Estimated funding cost padding. |
| `TP_OVERHEAD_FLOOR_PCT` | `2` | Floor on cost-overhead component, in % of margin. 0 = disabled. |
| `BREAKEVEN_TRIGGER_PCT` | `0.5` | Move SL to BE when unrealised profit reaches this % of margin. |

Worked example — BTC long at entry 77200, lev=5, lighter:
- `MIN_PROFIT=3` of margin = 0.6% of price (`3 / 100 / 5`).
- `TP_OVERHEAD_FLOOR_PCT=2` of margin = 0.4% of price.
- Raw overhead on lighter (taker=0, builder=1.5bps × 2 + slippage 5bps + funding 5bps) = 13 bps = 0.13% of price.
- 0.13% < 0.4% floor → cost overhead = 0.4%.
- Total = 0.6% + 0.4% = **1.0% of price**.
- TP = 77200 × 1.01 = **77972**. Gross +5% margin; net ≈ +4.35% margin after real costs.

Worked example — same trade, signal_sl=75000:
- With `USE_SIGNAL_SL=true`: SL placed at 75000 → −14% of margin if hit.
- With `USE_SIGNAL_SL=false` (default): SL placed at 77200 × (1 − 5%/5/100) = **76428** → −5% of margin if hit.

### SL lifecycle

1. **Entry placed** as a limit order. No SL / TP yet.
2. **Entry fills.** `post_fill_placer` immediately submits two reduce-only trigger orders:
   - SL: either signal-provided (if `USE_SIGNAL_SL=true`) or computed `entry ± DEFAULT_SL_PCT / leverage`.
   - TP: computed per the formula above.
3. **Breakeven move.** Event-driven from SSE: every `marketPrice` frame calls `evaluate_breakeven_trigger`, which checks the in-memory `trigger_index` and, if `BREAKEVEN_TRIGGER_PCT` (% of margin) is crossed in our favor, schedules `move_sl_to_breakeven` as a separate asyncio task. The move cancels the existing SL and places a new one at `entry × (1 ± buffer)` where `buffer ≈ 8 bps` covers exit fees. From this point the position cannot close in the red — worst case is exit at ~0. A 10 s supervisor loop (`tp_breakeven_supervisor_task`) maintains the heartbeat, reconciles the trigger index with the DB, and REST-rescues positions whose SSE feed has gone stale.
4. **SL safety watchdog** (every 30s) verifies every `open` position has an active SL on the exchange. Missing → `ERROR_NAKED_POSITION` alert. A separate `lighter_sl_watchdog` re-places SL on lighter specifically, where the exchange occasionally drops trigger orders for opaque reasons.
5. **TP safety watchdog** (every 30s) verifies every `open` position with `sl_moved_to_be_at IS NULL` has an active TP. Missing → recompute the target via the same formula used at post-fill and re-place via the verified-trigger path. Rate-limited at 6 attempts/hour/position; final failure surfaces `ERROR_NO_TP`. This recovers from the common pattern "VOOI returned 503 during the post-fill TP POST and the bot gave up".

> Caveat at high leverage: at `lev ≥ 7`, the fixed 8-bps BE buffer (in % of
> price) can exceed `BREAKEVEN_TRIGGER_PCT / leverage`. The breakeven move
> would place SL above the current price → instant trigger, near-zero close.
> Increase `BREAKEVEN_TRIGGER_PCT` when raising leverage above 6.

### Risk circuit breakers

| Variable | Default | Effect |
|---|---|---|
| `MAX_PLACEMENTS_PER_HOUR_GLOBAL` | `10` | Total entries across all channels |
| `MAX_PLACEMENTS_PER_HOUR_PER_CHANNEL` | `3` | Per-channel rate limit |
| `DAILY_DD_PCT_PAUSE` | `10` | Pause new entries if realized+unrealized DD ≥ this % |
| `LIMIT_ORDER_TTL_HOURS` | `24` | Auto-cancel unfilled entry limits after N hours |
| `MIN_NOTIONAL_USD_*` | `10` | Hard minimum order notional, per exchange |

### Operational

| Variable | Default | Effect |
|---|---|---|
| `RECONCILER_INTERVAL_SEC` | `60` | DB↔exchange sync period |
| `TELEGRAM_POLL_INTERVAL_SEC` | `60` | Backup polling for missed messages |
| `SSE_PRICE_STALENESS_THRESHOLD_SEC` | `30` | Fall back to REST quote if SSE cache is older |
| `LOG_LEVEL` | `INFO` | `DEBUG` / `INFO` / `WARNING` |

### Secrets — never commit

These must be set per deployment. `.env` is git-ignored.

- `TELEGRAM_API_ID`, `TELEGRAM_API_HASH`
- `VOOI_API_KEY`
- `LLM_API_KEY`
- `POSTGRES_PASSWORD` / full `DATABASE_URL`
- `ALERT_TELEGRAM_BOT_TOKEN`, `ALERT_TELEGRAM_CHAT_ID`

---

## CLI reference

All commands are invoked as `python -m bot <command>` (or `bot <command>` after `pip install -e .`).

### Runtime

| Command | Purpose |
|---|---|
| `run` | Start all 7 concurrent tasks. Foreground process; supervise externally (systemd / docker). |
| `status` | Health probe: DB connectivity, counts, last activity. |
| `vooi-check` | Validate `VOOI_API_KEY` against `/exchange/positions`. |
| `tg-login` | Interactive Telethon login; creates `*.session` file. Run once before `run`. |
| `healthcheck` | Used by Dockerfile HEALTHCHECK. Non-zero exit on failure. |

### Channels

| Command | Purpose |
|---|---|
| `channels add @username` (or numeric id) | Resolve via Telethon, insert into `channels` table. |
| `channels list` | Show all channels with `is_active` flag. |
| `channels remove <id>` | Remove from the DB. To disable without deleting: `UPDATE channels SET is_active = false WHERE id = ...`. |

### Inspection / debugging

| Command | Purpose |
|---|---|
| `signals --since 24h` | List parsed signals in the window. |
| `messages --since 24h` | List raw messages (incl. noise). |
| `signal-dryrun <id>` | Re-parse a stored message without placing orders. |
| `simulate-breakeven <position_id>` | Trigger a breakeven check without waiting for the loop. |
| `vooi-errors list` | Read recent VOOI API errors from `vooi_errors` table. |
| `vooi-errors export <out.json>` | Bundle for sharing with the VOOI team. |
| `vooi-errors mark-reported <ids>` | Mark errors as reported (won't appear in default list). |
| `vooi-errors prune --before <date>` | Delete old rows. |

---

## Database schema

PostgreSQL 15+. All tables in the default `public` schema. Migrations managed by Alembic (`alembic upgrade head` runs automatically on `bot run`).

| Table | Purpose |
|---|---|
| `channels` | Telegram channels being monitored. |
| `messages` | Every raw message ingested (`telegram_message_id` unique per channel). |
| `signals` | LLM-parsed messages. `is_signal=false` rows are noise; `skip_reason` non-null = filtered before routing. |
| `orders` | Every order we sent to VOOI (`entry`, `stopLoss`, `takeProfit`). Tracks status lifecycle. |
| `positions` | Filled positions linking entry/SL/TP orders; carries realized PnL on close. |
| `trades` | Fills derived from `/exchange/trades` (used by `_try_close_from_history` for phantom recovery). |
| `events` | Structured event log (NAKED, breakeven_triggered, etc.) for downstream alerting. |
| `vooi_api_calls` | Raw audit of every VOOI HTTP call — method, path, status, duration, correlation_id. |
| `vooi_errors` | Subset of failed calls, grouped for triage / export. |
| `runtime_state` | KV store for cross-task state (rate-limit counters, daily DD baseline). |

Key invariants:

- `positions.entry_order_id` is NOT NULL — a position exists iff its entry filled.
- `positions.sl_order_id` MAY be NULL momentarily (between fill detection and post-fill placement). `sl_safety_check` fires `ERROR_NAKED_POSITION` if it stays NULL for >30s after `status='open'`.
- `orders.vooi_order_id` is the exchange-side ID; recovered via clientOrderId lookup or attribute match when VOOI's POST returns `{status:"ok"}` without an ID.

---

## Risk management

The bot has multiple safety layers; understand each before running with real funds.

### 1. Order-level

- **Reduce-only triggers**: SL and TP are placed with `reduceOnly=true`, so they can never accidentally open a new position.
- **Min notional check**: orders below `MIN_NOTIONAL_USD_*` are skipped client-side with `order_skip_below_min_notional` to avoid exchange rejection storms.
- **Limit TTL**: unfilled entry limits older than `LIMIT_ORDER_TTL_HOURS` are auto-cancelled by the reconciler.

### 2. Position-level

- **Mandatory SL**: every filled position triggers `post_fill_placer` which places SL + TP. If the SL POST cannot be verified on the exchange (lighter, in particular, returns `clientOrderId=null` which breaks the standard lookup), the order is marked `rejected` and `ERROR_NAKED_POSITION` is fired. **The bot does not silently leave positions without a stop.**
- **Breakeven move**: once price moves `BREAKEVEN_TRIGGER_PCT` in our favor, the existing SL is cancelled and a new SL is placed at entry. 3 retry attempts; if all fail, fires `breakeven_sl_exhausted_ERROR_NAKED_POSITION` and alerts.
- **SL safety watchdog**: every 30s, scans positions with `status='open'` and verifies an active SL exists on the exchange. Missing → NAKED alert.
- **Phantom detection**: positions missing from `/exchange/positions` for ≥3 reconciler cycles (≥180s) are reconciled against trade history. If no closing fill is found, they're marked `closed_manual` with `close_reason='phantom_no_exchange_position'` — a loud signal that something diverged.

### 3. Account-level

- **Rate limits**: `MAX_PLACEMENTS_PER_HOUR_*` cap how aggressively a single channel can drive the bot.
- **Daily drawdown pause**: when realized+unrealized PnL ≥ `DAILY_DD_PCT_PAUSE` of starting equity, new entries are blocked until the next UTC day.
- **Per-trade notional cap**: `MAX_POSITION_SIZE_USD` overrides `DEFAULT_POSITION_SIZE_PCT` if the % calc would produce a larger trade.

### 4. Operational

- **Alerts**: any `ERROR_*` event ([alerts.py](./bot/alerts.py)) is pushed to a configured Telegram chat if `ALERT_TELEGRAM_BOT_TOKEN` is set. The bot will not stop on its own — alerts are advisory.
- **Audit log**: every VOOI API call is written to `logs/vooi-raw.log` (JSONL) with correlation_id. Errors additionally land in `vooi_errors` for export.

---

## Safety

This is **not financial advice and not a turnkey money printer.** It is a faithful executor of whatever Telegram channels you point it at. If those channels publish bad signals, the bot will trade bad signals.

Before running with real funds:

1. **Run on a paper / small-balance account first.** Hardcode `MAX_POSITION_SIZE_USD=10` and `DEFAULT_POSITION_SIZE_PCT=1` for at least a week. Measure the win-rate and slippage on your specific channels.
2. **Read every closed position** in the first week. Verify the bot's PnL math against your exchange statements.
3. **Configure alerts.** Set `ALERT_TELEGRAM_BOT_TOKEN` so you get notified of NAKED positions and daily DD pauses.
4. **Have a kill switch.** `docker compose down` or `systemctl stop signal-bot` will stop new orders within seconds. Open positions will remain — you must close them manually on the exchange.
5. **Channel selection matters more than parameters.** Bad signals tuned aggressively still lose money.

---

## Operations

### Production process management

See [Path C — VPS](#path-c--vps-ubuntudebian--systemd) above for the systemd
setup. Once installed, day-to-day commands:

```bash
sudo systemctl status signal-bot         # current state
sudo systemctl restart signal-bot        # apply config / .env changes
sudo systemctl stop signal-bot           # halt new orders (keeps Postgres up)
journalctl -u signal-bot -f              # tail logs
journalctl -u signal-bot --since "1 hour ago" | grep ERROR
```

### Restart procedure

The Telethon session is a SQLite file; concurrent access by two bot processes leads to `database is locked`. Always verify there is at most one process before starting:

```bash
pgrep -fa "python -m bot"
lsof signal_bot.session 2>/dev/null   # Should show no process
```

Then start cleanly. If the lock file (`*-journal`) exists from a previous crash, SQLite auto-recovers on the next open.

### Health checks

`python -m bot status` returns non-zero on any of:
- DB unreachable
- VOOI API auth failure
- No messages in the last N minutes (configurable)

The Dockerfile `HEALTHCHECK` invokes `python -m bot healthcheck` every 30s.

### Backups

`pg_dump signalbot > backup.sql` regularly. The DB is the source of truth for PnL accounting — losing it loses your audit trail.

---

## Development

### Setup

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```

### Tests

```bash
pytest tests/unit/                 # fast, no external deps
pytest tests/integration/          # requires local Postgres
pytest tests/e2e/                  # requires live VOOI + Telegram
```

### Project layout

```
bot/
├── cli.py                # Typer entry point — every command
├── config.py             # Pydantic Settings — env var schema
├── db.py                 # SQLAlchemy 2.x async engine / session_scope
├── models.py             # ORM models matching the schema above
├── ingester.py           # Telethon listener + polling
├── parser.py             # LLM signal parsing
├── llm_client.py         # OpenAI wrapper
├── router.py             # Venue selection, dedup, rate limits
├── resolver.py           # Symbol normalization + decimal caches
├── orders.py             # place_entry_order, lookups
├── post_fill_placer.py   # On fill: place SL + TP (with verification)
├── breakeven_watcher.py  # SSE-driven BE evaluator + 10 s supervisor
├── sl_safety.py          # NAKED detector
├── sse_listener.py       # /exchange/updates subscriber
├── reconciler.py         # 60s DB↔exchange sync
├── startup_cleanup.py    # Boot-time orphan cleanup
├── vooi_client.py        # httpx wrapper, retries, redaction
├── vooi_audit.py         # vooi_api_calls / vooi_errors writer
├── streamer.py           # Internal event emitter (DB + alerts)
├── alerts.py             # Telegram outbound alerts
├── reports.py            # CLI reports (signals, messages)
├── tp_calculator.py      # TP/SL pricing math
└── __main__.py           # `python -m bot` -> cli.app
```

### Conventions

- All I/O is async. No blocking calls in the main event loop.
- Decimals use `decimal.Decimal`, never floats, on the trade path.
- Every DB transaction goes through `session_scope()` (no manual `session.commit()`).
- Logs are structured (`structlog`). Event names use snake_case verbs (`entry_order_placed`, `breakeven_trigger_detected`).
- Every VOOI API error is logged with `correlation_id`; that ID flows through to `vooi_errors`.

---

## Known limitations

- **Stale SSE rescue is REST-based.** Breakeven detection is normally driven by `marketPrice` SSE frames. If VOOI's SSE goes down for >`SSE_PRICE_STALENESS_THRESHOLD_SEC`, the 10 s supervisor batch-quotes `/exchange/quotes` for stale positions only — far gentler than the old 2 s per-position poll, but still adds REST load when the SSE link is broken.
- **Lighter returns `clientOrderId: null`** in `/exchange/open-orders`, breaking the standard lookup path. `post_fill_placer._verify_trigger_on_exchange` works around this by matching `(asset, side, triggerPrice, size, type)`. If you see `post_fill_trigger_unverified_NAKED` in logs, this is the recovery path firing.
- **Aster does not support bracket orders.** TP/SL must be placed as separate orders after the entry fills — never inline with the entry POST. This is handled automatically.
- **Hyperliquid requires `0x`-prefixed 128-bit hex clientOrderIds.** See `bot/orders.py:make_client_order_id`. The breakeven watcher uses the same helper.
- **VOOI Ultra API may return `503` with the actual error embedded in the body** rather than the appropriate HTTP code. The client unwraps known wrapper strings and re-classifies.
- **Symbol normalization is heuristic.** `BTC/USDT`, `BTCUSDT`, `BTC-PERP` all resolve to `BTC` via `resolver.py`. If your signals use exotic notation, extend `_canonical()`.
- **No spot, no options, no cross-margin transfers.** Out of scope.

---

## Contributing

Pull requests welcome. Please:

1. Open an issue first for non-trivial changes.
2. Keep PRs focused — one logical change per PR.
3. Add tests for new behavior, especially around order placement and reconciler logic.
4. Don't commit `.env`, session files, or live API responses.

---

## License

[MIT](./LICENSE). The Software is provided "as is", without warranty of any kind. Use at your own risk.
