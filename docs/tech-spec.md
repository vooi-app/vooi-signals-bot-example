# Technical Specification — VOOI Signal Bot

**Version:** 1.5 (MVP)  
**Author:** Alexey, PM, VOOI Ultra  
**Audience:** Engineering team (CTO + developers)  
**Status:** Ready for implementation  
**API reference:** `https://vooi-api-app.fly.dev/swagger/json` · base URL `https://perps-api.vooi.io`

---

## Changelog v1.4 → v1.5

| # | Что изменилось | Причина |
|---|---|---|
| A1 | **Market-ордера запрещены в v1.** Если `signal.entry_prices` пустой → `skip_reason='no_entry_price'` | TP нельзя посчитать до знания fill price; limit-ордера дают точную цену входа |
| A2 | **TP и SL размещаются после fill**, не в bracket. Entry-ордер ставится без TP/SL; SSE fill → рассчитываем TP → размещаем TP + SL как отдельные reduce-only ордера | На лимите слипеджа на входе нет, TP считается по точной fill price |
| A3 | **Slippage в TP-формуле исправлен:** для limit-входа `entry_slippage = 0`, учитывается только exit slippage (одностороннее) | Limit fill = точная цена, двойной slippage был избыточен |
| A4 | **Удалены все упоминания JWT-истечения.** API-ключи VOOI бессрочные, R3 закрыт | Per operator: "ключи бессрочные, проблемы нет" |
| A5 | **Funding cost buffer** добавлен в TP формулу: `FUNDING_COST_BUFFER_BPS=5` | Perpetual funding fees съедают прибыль при удержании позиции 8–24h |
| A6 | **Conflict check расширен:** проверяет и `positions` (status='open') И `orders` (status IN ('pending','open'), order_type='entry') | Unfilled limit ордер не создаёт `positions` строку — старый check пропускал дубли |
| A7 | **Telethon real-time event listener** как primary; polling каждые 60s как fallback | 60s polling опаздывает на крипто-сигналах до 59 секунд |
| A8 | **`MAX_POSITION_SIZE_USD`** добавлен в конфиг — абсолютный кап на notional независимо от % | Без капа рост аккаунта автоматически увеличивает размер позиций |
| A9 | **SSE price staleness guard** в `tp_breakeven_watcher`: при cache age >30s fallback на REST | Зависший SSE → цена в cache стухла → breakeven trigger не срабатывает |
| A10 | **Watcher heartbeat check** в `sl_safety_check` | Завис asyncio task → молчание, нет алертов |
| A11 | **`clientOrderId` pre-insert** для Aster batch до отправки запроса | Timeout на batch → нет данных в DB → reconciler не может найти ордер |
| A12 | **`signals.prompt_version`** колонка + `SIGNAL_PARSER_PROMPT_VERSION` константа | Версионирование промпта для отладки регрессий качества парсинга |
| A13 | **`positions.sl_strategy`** колонка `TEXT DEFAULT 'fixed'` | Закладка для trailing SL в post-MVP, не ломает текущую схему |
| A14 | **Checkpoint: Telegram notify** на parsed signal после Stage 2 (см. §16) | Ранний working result без денег |
| A15 | **`bot simulate-breakeven`** CLI команда добавлена в §10 | Acceptance criteria #16 нельзя проверить в prod без риска |
| A16 | **`DEFAULT_SL_PCT` семантика задокументирована** — это % от entry price, с предупреждением о leverage impact | Неоднозначность приводит к SL без реальной защиты при высоком leverage |
| A17 | Новые позиции при ожидании TP/SL placement имеют статус `open_pending_tp_sl` | Явный статус вместо неявного "naked" окна |

---

## 1. Overview

A console-based automated trading bot that:

1. Listens to a configured set of Telegram channels for trading signals (real-time events + 60s polling fallback).
2. Parses each new message with an LLM.
3. Resolves the trading symbol against three perpetual DEXs via VOOI Ultra API.
4. Selects the cheapest available exchange.
5. Applies leverage and margin mode, then places **an entry limit order only** (no bracket).
6. Waits for SSE fill confirmation (`order` frame with `status='filled'`), then:
   - Computes TP price using confirmed `avgEntryPrice` and corrected slippage formula (entry slippage = 0 for limits).
   - Places SL and TP as separate reduce-only trigger orders.
7. **Monitors price** of open positions; when price crosses `entry × (1 ± BREAKEVEN_TRIGGER_PCT/100)` favorably, **moves SL to breakeven+buffer**.
8. Tracks positions through close (TP-hit, SL-hit, manual, liquidation).
9. Logs every VOOI API call.

### Out of scope for v1

- Market orders — if `signal.entry_prices` is empty → `skip_reason='no_entry_price'`. Market order support is v2 after resolving TP placement for unknown fill price.
- Multi-TP ladder execution.
- Custom TP levels from signal author.
- JWT expiry handling — API keys are perpetual, no expiry concern.

---

## 2. High-level architecture

```
                 ┌─────────────────────┐
                 │  Telegram channels  │
                 └──────────┬──────────┘
                            │ Telethon real-time events (primary)
                            │ + 60s polling fallback
                            ▼
                 ┌─────────────────────┐
                 │  Ingester           │──▶ messages (DB)
                 └──────────┬──────────┘
                            ▼
                 ┌─────────────────────┐
                 │  LLM parser         │──▶ signals (DB)
                 └──────────┬──────────┘
                            ▼
                 ┌─────────────────────┐
                 │  Risk gates +       │
                 │  conflict check     │  checks positions AND orders tables
                 └──────────┬──────────┘
                            ▼
                 ┌─────────────────────┐         ┌────────────────────┐
                 │  Quote selector     │◀───────▶│ GET /exchange/quotes│
                 └──────────┬──────────┘         └────────────────────┘
                            ▼
                 ┌─────────────────────┐
                 │  Pre-trade setup    │──▶ POST /exchange/leverage
                 │  leverage+margin    │──▶ POST /exchange/margin-mode
                 └──────────┬──────────┘
                            ▼
                 ┌─────────────────────┐
                 │  Entry order placer │──▶ POST /exchange/orders (entry ONLY)
                 │  (no TP/SL yet)     │   positions.status = open_pending_tp_sl
                 └──────────┬──────────┘
                            │
                  ┌─────────┴──────────────────────┐
                  │  SSE: order fill event          │
                  │  (avgEntryPrice confirmed)      │
                  └──────────┬──────────────────────┘
                             ▼
                 ┌─────────────────────┐
                 │  TP price calculator│  uses avgEntryPrice + exit slippage only
                 │  + Post-fill placer │──▶ POST /exchange/orders (SL, reduce-only)
                 │                     │──▶ POST /exchange/orders (TP, reduce-only)
                 └──────────┬──────────┘   positions.status = open
                            ▼
                 ┌─────────────────────┐         ┌────────────────────┐
                 │  SSE listener       │◀───SSE──│GET /exchange/updates│
                 │  + reconciler       │         └────────────────────┘
                 └──────────┬──────────┘
                            │
                            ├──emit price──▶ ┌──────────────────────┐
                            │                │  TP breakeven watcher│
                            │                │  + staleness guard   │
                            │                └──────────────────────┘
                            ▼
                 ┌─────────────────────┐
                 │  Console + reports  │
                 └─────────────────────┘
```

**Seven concurrent async tasks** in one process:

| Task | Period | Responsibility |
|------|--------|----------------|
| `ingester` | event-driven + 60s fallback | Telegram → parser → risk gates → routing → entry order |
| `post_fill_placer` | event-driven | Handles SSE fill events: compute TP → place TP + SL orders |
| `sse_listener` | event-driven | SSE `/exchange/updates` → DB merge, emit lifecycle events, update price cache |
| `reconciler` | 60s | REST sync, TTL cancellation, catch missed fills |
| `tp_breakeven_watcher` | event-driven (2s poll) | Monitors price cache; triggers `move_sl_to_breakeven()` |
| `console_streamer` | event-driven | Console output |

---

## 3. Configuration

```
# Telegram
TELEGRAM_API_ID=123456
TELEGRAM_API_HASH=abc...
TELEGRAM_SESSION_NAME=signal_bot
SIGNAL_CHECKPOINT_NOTIFY_TELEGRAM=true   # send Telegram alert to operator on each parsed signal

# VOOI
VOOI_API_BASE_URL=https://perps-api.vooi.io
VOOI_API_KEY=...                         # Perpetual API key — no expiry
# Broker identity and fees are now assigned by VOOI server-side based on the
# API key. The bot no longer sets VOOI_BROKER_ID_* / VOOI_BROKER_FEE_BPS_*.

# LLM
LLM_PROVIDER=openai
LLM_MODEL=gpt-4o-mini
LLM_API_KEY=sk-...
SIGNAL_PARSER_PROMPT_VERSION=v1.0        # bump on any prompt change

# Database
DATABASE_URL=postgresql+asyncpg://botuser:botpass@localhost:5432/signalbot

# Trading defaults
DEFAULT_LEVERAGE=5
MAX_LEVERAGE=10
DEFAULT_MARGIN_MODE=cross
DEFAULT_POSITION_SIZE_PCT=5            # % of available margin used as collateral
MAX_POSITION_SIZE_USD=1000             # absolute notional cap per trade
DEFAULT_SL_PCT=5                       # max % of MARGIN to lose. Price-distance = DEFAULT_SL_PCT / leverage.
USE_SIGNAL_SL=false                    # true: use signal SL when present, fallback to DEFAULT_SL_PCT.
                                       # false: always DEFAULT_SL_PCT, ignore signal SL (hard cap).

# Take Profit (all *_PCT values are % of MARGIN; ÷ leverage for price distance)
MIN_PROFIT_PCT_OF_COLLATERAL=3         # required net profit on margin after all fees
FUNDING_COST_BUFFER_BPS=5             # estimated funding cost buffer (entry+exit window)
TP_OVERHEAD_FLOOR_PCT=2                # floor on cost-overhead component (% of margin). 0 = disabled.
BREAKEVEN_TRIGGER_PCT=0.5             # move SL to BE+buffer when unrealised profit reaches this % of margin

# Fee fallbacks (used if GET /exchange/quotes unavailable; in bps)
FEE_FALLBACK_TAKER_BPS_HYPERLIQUID=4.5
FEE_FALLBACK_TAKER_BPS_LIGHTER=0.0
FEE_FALLBACK_TAKER_BPS_ASTER=3.5

LIMIT_ORDER_TTL_HOURS=24

# Min notional per exchange
MIN_NOTIONAL_USD_HYPERLIQUID=10
MIN_NOTIONAL_USD_LIGHTER=10
MIN_NOTIONAL_USD_ASTER=10

# Risk circuit breakers
MAX_PLACEMENTS_PER_HOUR_GLOBAL=10
MAX_PLACEMENTS_PER_HOUR_PER_CHANNEL=3
DAILY_DD_PCT_PAUSE=10

# SSE / polling
TELEGRAM_POLL_INTERVAL_SEC=60
RECONCILER_INTERVAL_SEC=60
SSE_RECONNECT_BACKOFF_SEC=2
SSE_PRICE_STALENESS_THRESHOLD_SEC=30   # fallback to REST if price cache older than this

# Alerting
ALERT_TELEGRAM_BOT_TOKEN=...           # outbound alerts to operator
ALERT_TELEGRAM_CHAT_ID=...

# Logging
LOG_LEVEL=INFO
LOG_FILE_PATH=./logs/bot.log
VOOI_RAW_LOG_PATH=./logs/vooi-raw.log
LOG_RETENTION_DAYS=30
```

---

## 4. Database schema

### 4.1–4.4. `channels`, `messages`, `orders`, `vooi_api_calls`, `vooi_errors`, `runtime_state`

(Identical to v1.2, with additions noted below.)

**`orders` table must have `symbol TEXT NOT NULL` and `side TEXT NOT NULL` columns** (required for conflict check §8.1). If not already present — add in migration.

### 4.3. `signals` *(updated)*

Added column:
```sql
ALTER TABLE signals ADD COLUMN prompt_version TEXT;  -- e.g. 'v1.0'; set from SIGNAL_PARSER_PROMPT_VERSION
```

### 4.5. `positions` *(updated for v1.5)*

```sql
CREATE TABLE positions (
    id                  BIGSERIAL PRIMARY KEY,
    signal_id           BIGINT REFERENCES signals(id),
    entry_order_id      BIGINT NOT NULL REFERENCES orders(id),
    sl_order_id         BIGINT REFERENCES orders(id),
    tp_order_id         BIGINT REFERENCES orders(id),

    exchange            TEXT NOT NULL,
    symbol              TEXT NOT NULL,
    side                TEXT NOT NULL,
    entry_price         NUMERIC NOT NULL,     -- confirmed avgEntryPrice from SSE fill
    size                NUMERIC NOT NULL,
    leverage            INT NOT NULL,
    margin_mode         TEXT,
    liquidation_price   NUMERIC,

    tp_price_initial    NUMERIC,
    sl_price_initial    NUMERIC,
    sl_price_current    NUMERIC,
    sl_moved_to_be_at   TIMESTAMPTZ,

    sl_strategy         TEXT NOT NULL DEFAULT 'fixed',  -- 'fixed' | future: 'trailing'

    status              TEXT NOT NULL,
    -- 'open_pending_tp_sl'  entry filled, TP+SL placement in progress (transient, <5s normally)
    -- 'open'                entry filled, TP and SL are live
    -- 'closed_tp'           TP hit
    -- 'closed_sl'           SL hit (original SL)
    -- 'closed_breakeven'    BE SL hit
    -- 'closed_manual'       operator closed manually
    -- 'liquidated'

    opened_at           TIMESTAMPTZ NOT NULL,
    closed_at           TIMESTAMPTZ,
    close_price         NUMERIC,
    realized_pnl_usd    NUMERIC,
    fees_paid_usd       NUMERIC,
    funding_paid_usd    NUMERIC,
    close_reason        TEXT,

    last_synced_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_positions_status ON positions(status);
CREATE INDEX idx_positions_pending_tp_sl ON positions(exchange, symbol)
    WHERE status = 'open_pending_tp_sl';
CREATE INDEX idx_positions_be_pending ON positions(exchange, symbol)
    WHERE status = 'open' AND sl_moved_to_be_at IS NULL;
```

---

## 5. Module: Ingester *(updated — real-time events)*

Primary mode: Telethon `@client.on(events.NewMessage(chats=channel_ids))` — fires immediately on publish.

Fallback mode: polling loop every `TELEGRAM_POLL_INTERVAL_SEC` — catches messages missed during reconnect or cold start.

Deduplication by `(channel_id, message_id)` covers both modes.

```python
# Primary: real-time
@client.on(events.NewMessage(chats=active_channel_ids()))
async def on_new_message(event):
    await process_message(event.message)

# Fallback: polling (runs in parallel, processes messages newer than last_seen)
async def ingester_polling_loop():
    while True:
        await poll_and_process_new_messages()
        await asyncio.sleep(TELEGRAM_POLL_INTERVAL_SEC)
```

If `SIGNAL_CHECKPOINT_NOTIFY_TELEGRAM=true`: after successful LLM parse that produces `is_signal=true`, send a Telegram alert to operator with parsed fields (signal_id, symbol, side, entry, SL, channel). No money involved — purely informational.

---

## 6. Module: LLM parser

(Identical to v1.4, with additions:)

- Set `signals.prompt_version = SIGNAL_PARSER_PROMPT_VERSION` on every signal row.
- `take_profits` extracted and stored, not used in execution.

---

## 7. Module: Symbol resolver

(Identical to v1.3.)

---

## 8. Module: Conflict check + Quote selector + Order placer

### 8.0. Pre-flight risk gates

(Identical to v1.2.)

### 8.1. Conflict check *(updated — checks orders table)*

```python
# Block on existing open POSITION
existing_position = db.positions.filter(
    status='open', symbol=signal.symbol, side=signal.side
).first()

# Block on existing UNFILLED entry order (not yet a position)
existing_order = db.orders.filter(
    status__in=('pending', 'open'),
    order_type='entry',
    symbol=signal.symbol,
    side=signal.side
).first()

if existing_position or existing_order:
    skip(reason='already_in_position')

# Per-exchange filter (same side on same exchange = skip that exchange)
candidates = [c for c in candidates
              if not db.positions.filter(
                  status='open', exchange=c.exchange, symbol=signal.symbol
              ).exists()]
```

### 8.2. Quote selector

(Identical to v1.3. `feesBps` and `slippageBps` from quotes stored for use in TP formula.)

### 8.3. Position sizing, rounding, entry zone handling

(Identical to v1.2 §8.3–8.3.3, with addition:)

```python
notional_usd = min(notional_usd, Decimal(settings.MAX_POSITION_SIZE_USD))
```

Cap applied after leverage multiplication, before min notional check.

### 8.4. Pre-trade setup

(Identical to v1.2 §8.4 — leverage + margin mode setup before order placement.)

### 8.5. Entry order placement *(updated — no bracket)*

Entry limit order is placed **without** TP/SL. Position row is created with `status='open_pending_tp_sl'`.

#### 8.5.1. Hyperliquid and Lighter

```python
# Pre-insert clientOrderId BEFORE sending request (for Aster parity and reconciler safety)
client_order_id = f"sigbot-{signal.id}-{uuid4().hex[:8]}"
insert_order(
    signal_id=signal.id,
    client_order_id=client_order_id,
    order_type='entry',
    symbol=signal.symbol,
    side=signal.side,
    status='submitting',  # pre-insert, no vooi_order_id yet
    ...
)

body = {
    "exchange": chosen_exchange,
    "asset": signal.symbol_normalized,
    "side": signal.side,
    "size": format_size(size, base_decimals),
    "price": format_price(entry_price, price_decimals, signal.side),  # always limit
    "timeInForce": "gtc",
    "clientOrderId": client_order_id,
    # NO broker field — VOOI assigns it server-side.
    # NO stopLoss / takeProfit — placed after fill
}

response = vooi.post("/exchange/orders", body)
update_order(client_order_id, vooi_order_id=response.orderId, status='pending')

create_position(
    signal_id=signal.id,
    entry_order_id=order.id,
    exchange=chosen_exchange,
    symbol=signal.symbol,
    side=signal.side,
    entry_price=entry_price,  # provisional; updated to avgEntryPrice on fill
    size=size,
    leverage=leverage,
    status='open_pending_tp_sl',
)
```

#### 8.5.2. Aster

Same approach — `POST /exchange/orders` for entry only (no batch with TP/SL). TP and SL placed post-fill as separate reduce-only orders (same as HL/Lighter path below).

**Note:** The prior `POST /exchange/batch-orders` approach for Aster is replaced with two sequential single-order calls after fill. This simplifies partial-fail handling.

### 8.6. TP price calculator *(updated — corrected slippage, funding buffer)*

#### 8.6.1. The formula

```
Let:
  E   = avgEntryPrice (confirmed fill price from SSE, = limit price for limits)
  L   = leverage
  C   = collateral in USD (notional / L)
  fb  = round-trip exit fee overhead in bps
        = quote.feesBps * 2
        VOOI's quote.feesBps already includes any server-side broker/builder
        fee; we double it because the same one-way fee applies on entry and
        exit legs.
  sb  = exit-only slippage in bps (from GET /exchange/quotes, one-way only)
        Limit entry has zero slippage; only exit slippage matters
  fund= FUNDING_COST_BUFFER_BPS (default 5)
  P   = MIN_PROFIT_PCT_OF_COLLATERAL (default 5)        # % of margin
  FL  = TP_OVERHEAD_FLOOR_PCT (default 2)               # % of margin (NOT price)

required_gain = (P  / 100) / L                          # % of price
floor         = (FL / 100) / L                          # % of price
cost_overhead = max((fb + sb + fund) / 10000, floor)

TP price for long:  tp_price = E × (1 + required_gain + cost_overhead)
TP price for short: tp_price = E × (1 - required_gain - cost_overhead)
```

Both `P` and `FL` are denominated in **% of margin** for symmetry: the user
thinks in margin returns, the price-distance is just `÷ L`. The floor protects
against quotes returning unrealistically small overhead (e.g. lighter taker=0)
that would place TP so close to entry that real fees+slippage eat the profit.

#### 8.6.2. Worked example (updated)

Signal: BTC long, limit entry 65000, leverage 10, $500 collateral ($5000 notional).
- Quote feesBps (one-way, server-side broker fee included): 19.5 bps
- Round-trip fee overhead: 19.5 × 2 = 39 bps
- Exit slippage from quote: 5 bps (one-way only)
- Funding buffer: 5 bps
- Raw cost overhead: 49 bps = 0.49% of price
- Floor: TP_OVERHEAD_FLOOR_PCT=2 of margin → 2/10 = 0.20% of price
- 0.49% > 0.20% → use raw overhead 0.49%
- Required net gain: 5% of margin / 10 = 0.5% of price
- TP target: 0.99% above entry
- TP price: 65000 × 1.0099 ≈ 65643.5

*(Composition vs v1.4: no entry slippage, funding buffer added, floor is now %
of margin, broker fee is bundled into quote.feesBps server-side.)*

Counter-example (lighter, where raw overhead is tiny):
- Quote feesBps=0; slippage 5 bps; funding 5 bps → raw 10 bps = 0.10% of price
- Floor: 2% of margin / 5 (lev) = 0.40% of price
- 0.10% < 0.40% → use floor 0.40%
- Required net gain: 3% of margin / 5 = 0.60% of price
- Total 1.00% of price → BTC entry 77200 → TP 77972

#### 8.6.3. Implementation

```python
def compute_tp_price(
    avg_entry_price: Decimal,
    side: str,
    leverage: int,
    exit_fees_bps_round_trip: Decimal,  # quote.feesBps * 2
    exit_slippage_bps: Decimal,          # one-way only (limit entry = no entry slippage)
) -> Decimal:
    P = Decimal(settings.MIN_PROFIT_PCT_OF_COLLATERAL)
    L = Decimal(leverage)
    fund = Decimal(settings.FUNDING_COST_BUFFER_BPS)
    floor_pct = Decimal(settings.TP_OVERHEAD_FLOOR_PCT) / 100 / L   # % margin → % price

    required_gain_pct = P / 100 / L
    cost_overhead_pct = (exit_fees_bps_round_trip + exit_slippage_bps + fund) / 10000
    if cost_overhead_pct < floor_pct:
        cost_overhead_pct = floor_pct
    total_pct = required_gain_pct + cost_overhead_pct

    if side == 'buy':
        return avg_entry_price * (1 + total_pct)
    else:
        return avg_entry_price * (1 - total_pct)


def exit_fees_bps_round_trip_for(quote: GetQuotesItem) -> Decimal:
    # quote.feesBps already includes the server-side broker/builder fee
    return Decimal(quote.quote.feesBps) * 2


def exit_slippage_bps_for(quote: GetQuotesItem) -> Decimal:
    # Only exit slippage — entry slippage = 0 for limit orders
    return Decimal(quote.quote.slippageBps)
```

Fallback when quotes unavailable:
```python
FALLBACK_EXIT_FEES_BPS = {
    'hyperliquid': Decimal(settings.FEE_FALLBACK_TAKER_BPS_HYPERLIQUID),
    'lighter':     Decimal(settings.FEE_FALLBACK_TAKER_BPS_LIGHTER),
    'aster':       Decimal(settings.FEE_FALLBACK_TAKER_BPS_ASTER),
}
exit_fees_bps_round_trip = FALLBACK_EXIT_FEES_BPS[exchange] * 2
exit_slippage_bps = Decimal('5')  # conservative one-way fallback
```

### 8.7. Post-fill TP + SL placement *(new in v1.5)*

Triggered by `post_fill_placer` task when SSE `order` frame arrives with `status='filled'` for an entry order.

```python
async def on_entry_filled(order_fill_event):
    position = db.positions.get(entry_order_id=order_fill_event.order_id)
    if not position or position.status != 'open_pending_tp_sl':
        return  # already handled or wrong state

    avg_entry = Decimal(order_fill_event.avgPrice)

    # Update provisional entry_price to confirmed fill price
    update_position(position, entry_price=avg_entry)

    # Compute prices
    tp_price = compute_tp_price(avg_entry, position.side, position.leverage,
                                 exit_fees_bps_round_trip_for(cached_quote),
                                 exit_slippage_bps_for(cached_quote))
    # If USE_SIGNAL_SL=true and signal.stop_loss is set, use it; otherwise
    # always compute_sl_price_from_pct(DEFAULT_SL_PCT of margin / leverage).
    sl_price = compute_sl_price(position)

    # Place SL (reduce-only, stop trigger)
    sl_client_oid = f"sigbot-{position.signal_id}-sl-{uuid4().hex[:8]}"
    await vooi.post("/exchange/orders", {
        "exchange": position.exchange,
        "asset": position.symbol,
        "side": opposite(position.side),
        "size": format_size(position.size, base_decimals),
        "reduceOnly": True,
        "trigger": {"price": format_price(sl_price, ...), "type": "sl"},
        "clientOrderId": sl_client_oid,
    })

    # Place TP (reduce-only, take profit trigger)
    tp_client_oid = f"sigbot-{position.signal_id}-tp-{uuid4().hex[:8]}"
    await vooi.post("/exchange/orders", {
        "exchange": position.exchange,
        "asset": position.symbol,
        "side": opposite(position.side),
        "size": format_size(position.size, base_decimals),
        "reduceOnly": True,
        "trigger": {"price": format_price(tp_price, ...), "type": "tp"},
        "clientOrderId": tp_client_oid,
    })

    # Persist
    sl_order = insert_order(signal_id=..., order_type='stopLoss', trigger_price=sl_price, ...)
    tp_order = insert_order(signal_id=..., order_type='takeProfit', trigger_price=tp_price, ...)
    update_position(position,
                    sl_order_id=sl_order.id,
                    tp_order_id=tp_order.id,
                    sl_price_initial=sl_price,
                    sl_price_current=sl_price,
                    tp_price_initial=tp_price,
                    status='open')
```

**Failure handling:** if SL or TP placement fails after retry:
- Set `position.status = 'open'` regardless (position exists)
- `sl_safety_check` will detect missing SL within 30s and fire `ERROR_NAKED_POSITION`

**Reconciler** must also handle the `open_pending_tp_sl` state: if an entry fill arrived via REST but `post_fill_placer` was not triggered (SSE miss), the reconciler detects `status='open_pending_tp_sl'` with a filled entry order and re-triggers post-fill placement.

### 8.8. Move SL to breakeven *(updated from v1.4 §8.7)*

(Identical to v1.4 §8.7.1–8.7.4, with updated §8.7.5 below.)

#### 8.8.5. `tp_breakeven_watcher` *(updated — staleness guard)*

```python
async def tp_breakeven_watcher():
    last_tick = time.time()
    while True:
        last_tick = time.time()  # heartbeat for sl_safety_check
        positions = await db.get_open_positions_pending_be()

        for pos in positions:
            cache_age = time.time() - price_cache_updated_at.get((pos.exchange, pos.symbol), 0)

            if cache_age > settings.SSE_PRICE_STALENESS_THRESHOLD_SEC:
                # SSE stale — fallback to REST quote
                try:
                    cur_price = await vooi.get_current_price(pos.symbol, pos.exchange)
                except Exception:
                    continue  # skip this position this tick
            else:
                cur_price = price_cache.get((pos.exchange, pos.symbol))
                if cur_price is None:
                    continue

            E = pos.entry_price
            # BREAKEVEN_TRIGGER_PCT is % of margin; ÷ leverage → % of price.
            trigger_pct = Decimal(settings.BREAKEVEN_TRIGGER_PCT) / 100 / pos.leverage

            if pos.side == 'buy' and cur_price >= E * (1 + trigger_pct):
                await move_sl_to_breakeven(pos)
            elif pos.side == 'sell' and cur_price <= E * (1 - trigger_pct):
                await move_sl_to_breakeven(pos)

        await asyncio.sleep(2)

# Accessible by sl_safety_check:
tp_breakeven_watcher_last_tick: float = 0.0
```

---

## 9. Module: Position tracker

(Identical to v1.4 §9, with addition:)

- New status `open_pending_tp_sl`: position row exists (entry filled), TP/SL not yet placed. Transitions to `open` after post-fill placement.
- Reconciler handles stale `open_pending_tp_sl` rows (entry filled but no TP/SL after >60s) by re-triggering `on_entry_filled`.
- Cancelled SL order where `sl_moved_to_be_at IS NOT NULL` is NOT treated as position close by reconciler. Position remains open under new SL (see §4.4 of review).

---

## 10. CLI

(Identical to v1.4, plus:)

### `bot simulate-breakeven --position-id <N>`

For testing acceptance criterion #16 without waiting for real price movement:

1. Takes an open position.
2. Forcibly sets `price_cache[(exchange, symbol)] = entry_price × 1.03` (simulates 3% move).
3. `tp_breakeven_watcher` detects trigger on next 2s tick and calls `move_sl_to_breakeven()`.
4. Prints `SL_BREAKEVEN` event; resets price_cache to actual market price after.

**Does not place real orders** in simulation mode; uses `--dry-run` flag if you want to test the cancel-replace API calls on a real position in staging.

---

## 11. Console output

(Identical to v1.4, plus:)

```
[...] INFO   SIGNAL_PARSED   BTC long entry=65000 SL=60000 channel=@example_signals signal_id=87
[...] INFO   ENTRY_PLACED    BTC long limit 0.0123 @65000  signal_id=87 (pending fill)
[...] INFO   ENTRY_FILLED    BTC long avgEntry=65000 size=0.0123 signal_id=87
[...] INFO   TP_SL_PLACED    BTC long  TP=65643 SL=60450  pos=42
```

---

## 12. Error handling

(Identical to v1.4 §12, plus:)

| Category | Strategy |
|---|---|
| Entry order fill received but TP placement fails | Inline retry × 3. If still failing, `tp_safety_watchdog` keeps retrying every 30s (rate-limited 6/h per position). `sl_safety_check` emits `ERROR_NO_TP`. |
| Entry order fill received but SL placement fails | Inline retry × 3. `ERROR_NAKED_POSITION` within 30s if no SL appears. `lighter_sl_watchdog` re-places on lighter (3/h). |
| `open_pending_tp_sl` position stuck >60s | Reconciler re-triggers `on_entry_filled` once. If still stuck — emit ERROR. |
| `tp_breakeven_watcher` heartbeat stale >30s | `sl_safety_check` emits `ERROR_WATCHER_HUNG`. Operator must inspect asyncio task. |

Removed from v1.4: JWT expiry handling — API keys are perpetual.

**`sl_safety_check` additions (every 30s):**
```python
async def sl_safety_check():
    while True:
        open_positions = db.positions.filter(status='open')
        for pos in open_positions:
            # 1. Naked SL check
            active_sl = db.orders.filter(
                id=pos.sl_order_id, status__in=('pending', 'open')
            ).exists()
            if not active_sl:
                alert('ERROR_NAKED_POSITION', position=pos)

            # 2. Missing TP check (only while SL has not been moved to BE —
            # once SL ≥ breakeven, the TP is no longer required for safety).
            if pos.sl_moved_to_be_at is None:
                active_tp = db.orders.filter(
                    id=pos.tp_order_id, status__in=('pending', 'open')
                ).exists()
                if not active_tp:
                    alert('ERROR_NO_TP', position=pos)

        # 3. Watcher heartbeat check
        watcher_age = time.time() - tp_breakeven_watcher_last_tick
        if watcher_age > 30:
            alert('ERROR_WATCHER_HUNG', age_sec=watcher_age)

        await asyncio.sleep(30)
```

**`tp_safety_watchdog_task` (every 30s):** scans open positions for a missing
TP order and re-places via the same verified-trigger helper used by post-fill.
Rate-limit: `_TP_WATCHDOG_MAX_ATTEMPTS_PER_HOUR = 6` per position. Beyond that,
emits a final `ERROR_NO_TP` and stops retrying. Skips positions where
`sl_moved_to_be_at IS NOT NULL` (BE-SL already locks in profit).

---

## 13. Logging

(Identical to v1.2.)

---

## 14. Open questions for VOOI API

(v1.4 list, plus:)

5. **Atomic order modification.** Does VOOI plan `PATCH /exchange/orders`?
6. **SSE marketPrice frame frequency** per exchange.
7. **Maker vs taker fee for limit fills.** On HL, limit orders that become makers pay 0.015% (lower than taker 0.045%). Does `GET /exchange/quotes` return the maker fee when we pass a limit order intention? If so, TP formula should use maker fee for entry (currently uses taker conservatively).

---

## 15. Open product questions

(v1.4 list, plus:)

7. **`DEFAULT_SL_PCT=7%` at high leverage** — at 10x leverage this is effectively 70% collateral loss. Is this intentional? Alternatively define SL as % of collateral and derive price delta. Needs operator confirmation.
8. **`MAX_POSITION_SIZE_USD` default value** — spec defaults to $1000. Confirm.

---

## 16. Implementation stages

| Stage | Deliverable | Exit criteria |
|-------|-------------|---------------|
| 0 | Foundation: repo, DB migrations, config, VOOI HTTP client + audit interceptor, startup healthchecks, Docker+systemd | `bot status` connects to VOOI, all checks green |
| 1 | Telegram ingestion: real-time events + polling fallback, channel management | New messages appear in `messages` table in real-time |
| 2 | LLM parser + fixtures | `fixtures/appendix_b_signals.json` created; ≥13/16 fixture tests pass. **Checkpoint: Telegram notify to operator on each parsed signal** |
| 3 | Symbol resolution + risk gates + routing + `signal-dryrun` | `signal-dryrun` works on real signals; **Checkpoint: operator sees full routing trace without money** |
| 4 | Order placement: pre-trade setup + entry-only order + post-fill TP/SL placement | Three real closed trades (one per exchange) with correct TP+SL, PnL in DB matches VOOI UI ±$1 |
| 5 | SSE listener + reconciler + reporting | SSE reconnect test passes; `open_pending_tp_sl` reconciliation tested; PnL report correct |
| 6 | Breakeven watcher + SL safety check + simulate-breakeven CLI | AC #16 and #17 pass; `simulate-breakeven` triggers `SL_BREAKEVEN` event correctly |
| 7 | QA / acceptance: 1-week live run | All 19 AC from §17 satisfied; no `ERROR_NAKED_POSITION` in production |

Stage 6 requires `marketPrice` SSE plumbing from Stage 5.

---

## 17. Acceptance criteria

(v1.4 #1–#15 updated:)

13. For every filled entry, exactly ONE TP placed at `compute_tp_price(avgEntryPrice)` ±1 tick, AND exactly ONE SL placed at signal price or DEFAULT_SL_PCT fallback.
14. `open_pending_tp_sl` never persists longer than 60 seconds without reconciler action.

(New:)

16. When position price crosses entry+2%, within 60 seconds: SL cancelled, new BE SL placed, `sl_moved_to_be_at` set, `SL_BREAKEVEN` event emitted.
17. `sl_safety_check` detects naked position within 30s. `ERROR_WATCHER_HUNG` fires if `tp_breakeven_watcher` heartbeat stale >30s.
18. ≥80% of 16 fixture signals in `fixtures/appendix_b_signals.json` parse correctly.
19. TP price for `close_reason='closed_tp'` positions yields realized PnL within ±0.5% of `MIN_PROFIT_PCT_OF_COLLATERAL` of original collateral.

---

## 18. Repository layout

```
signal-bot/
├── pyproject.toml
├── .env.example
├── .gitignore
├── docker-compose.yml
├── Dockerfile
├── alembic.ini
├── alembic/
├── logs/
│   ├── bot.log
│   ├── vooi-raw.log
│   └── vooi-errors-archive/
├── fixtures/
│   └── appendix_b_signals.json    # ← MUST exist before Stage 2
├── bot/
│   ├── __init__.py
│   ├── __main__.py
│   ├── cli.py
│   ├── config.py
│   ├── db.py
│   ├── models.py
│   ├── ingester.py                # real-time events + polling fallback
│   ├── parser.py                  # LLM parser + prompt versioning
│   ├── resolver.py
│   ├── router.py                  # conflict check (positions+orders) + risk gates + quote selection
│   ├── orders.py                  # pre-trade setup + entry-only order placement
│   ├── post_fill_placer.py        # TP+SL placement after fill
│   ├── tp_calculator.py           # compute_tp_price + compute_breakeven_sl_price
│   ├── breakeven_watcher.py       # tp_breakeven_watcher + staleness guard
│   ├── sl_safety.py               # sl_safety_check + watcher heartbeat
│   ├── sse_listener.py
│   ├── reconciler.py
│   ├── streamer.py
│   ├── reports.py
│   ├── vooi_client.py
│   ├── vooi_audit.py
│   └── llm_client.py
└── tests/
    ├── unit/
    │   ├── test_tp_calculator.py
    │   ├── test_breakeven_sl.py
    │   ├── test_rounding.py
    │   ├── test_position_size.py
    │   └── test_conflict_check.py
    ├── integration/
    │   ├── test_llm_parser.py      # 16 fixtures
    │   ├── test_vooi_client.py
    │   └── test_sse_listener.py
    └── e2e/
        └── test_full_flow.py
```

---

## 19. Out-of-band setup

(Identical to v1.2 §19, except: no JWT generation step — use perpetual API key directly in `.env`.)

---

## Appendix A — VOOI API cheatsheet

(Identical to v1.2.)

## Appendix B — Real-world signal fixtures

**Must be created as `fixtures/appendix_b_signals.json` before Stage 2 starts.**

Format:
```json
[
  {
    "id": 1,
    "raw_text": "🚀 BTC LONG\nEntry: 65000-65500\nTP: 70000\nSL: 63000\nLeverage: 10x",
    "expected": {
      "is_signal": true,
      "symbol": "BTC",
      "side": "buy",
      "entry_prices": [65000, 65500],
      "take_profits": [70000],
      "stop_loss": 63000,
      "leverage": 10
    }
  },
  ...16 total...
]
```

Include: clear signals, ambiguous messages, non-signals, edge cases (no SL, no TP, partial info).

## Appendix C — Fee rates by exchange

(Identical to v1.4 Appendix C.)

---

*End of v1.5.*
