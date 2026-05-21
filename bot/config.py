from decimal import Decimal
from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=False,
        extra="ignore",
    )

    # -------------------------------------------------------------------------
    # Telegram
    # -------------------------------------------------------------------------
    telegram_api_id: int
    telegram_api_hash: str
    telegram_session_name: str = "signal_bot"
    signal_checkpoint_notify_telegram: bool = False

    # -------------------------------------------------------------------------
    # VOOI Ultra API
    # -------------------------------------------------------------------------
    vooi_api_base_url: str = "https://perps-api.vooi.io"
    vooi_api_key: str
    # NOTE: Broker / builder identity and fees are now assigned by VOOI on the
    # server side (keyed off the API key). The bot no longer sends them in the
    # order body; the broker fee is included in VOOI's reported quote.feesBps.

    # -------------------------------------------------------------------------
    # LLM
    # -------------------------------------------------------------------------
    llm_provider: str = "openai"
    llm_model: str = "gpt-4o-mini"
    llm_api_key: str
    signal_parser_prompt_version: str = "v1.0"

    # -------------------------------------------------------------------------
    # Database
    # -------------------------------------------------------------------------
    database_url: str

    # -------------------------------------------------------------------------
    # Trading defaults
    # -------------------------------------------------------------------------
    default_leverage: int = 5
    max_leverage: int = 10
    default_margin_mode: str = "cross"
    default_position_size_pct: Decimal = Decimal("5")
    max_position_size_usd: Decimal = Decimal("1000")

    # DEFAULT_SL_PCT: maximum % of MARGIN allowed to lose on a single trade.
    # The corresponding price distance is DEFAULT_SL_PCT / leverage.
    # E.g. 5%, lev=5 → SL placed 1% away from entry → −5% margin if hit.
    default_sl_pct: Decimal = Decimal("5")

    # USE_SIGNAL_SL: whether to honor an explicit stop_loss from the signal.
    # True  → use signal SL when present, else compute_sl_price_from_pct.
    # False → ignore signal SL completely, always compute_sl_price_from_pct.
    # Set False to enforce a hard cap on per-trade margin loss regardless of
    # what the signal author asked for.
    use_signal_sl: bool = False

    # -------------------------------------------------------------------------
    # Take Profit
    # -------------------------------------------------------------------------
    # Minimum net profit on collateral after all fees (% of collateral)
    min_profit_pct_of_collateral: Decimal = Decimal("5")

    # Estimated perpetual funding cost buffer for holding 8-24h (in bps)
    funding_cost_buffer_bps: Decimal = Decimal("5")

    # Floor on the cost-overhead component of TP, expressed as % of MARGIN.
    # The price-distance equivalent is tp_overhead_floor_pct / leverage. If
    # the dynamic overhead (fees + slippage + funding) is smaller, pad TP
    # target so the overhead allowance is at least this much. 0 = disabled.
    tp_overhead_floor_pct: Decimal = Decimal("0")

    # Move SL to breakeven when unrealized profit reaches this % of MARGIN.
    # The price-distance threshold is breakeven_trigger_pct / leverage.
    breakeven_trigger_pct: Decimal = Decimal("2")

    # -------------------------------------------------------------------------
    # Fee fallbacks (used when GET /exchange/quotes unavailable; in bps)
    # -------------------------------------------------------------------------
    fee_fallback_taker_bps_hyperliquid: Decimal = Decimal("4.5")
    fee_fallback_taker_bps_lighter: Decimal = Decimal("0.0")
    fee_fallback_taker_bps_aster: Decimal = Decimal("3.5")

    # How long to keep unfilled limit orders before auto-cancelling (hours)
    limit_order_ttl_hours: int = 24

    # -------------------------------------------------------------------------
    # Min notional per exchange (USD)
    # -------------------------------------------------------------------------
    min_notional_usd_hyperliquid: Decimal = Decimal("10")
    min_notional_usd_lighter: Decimal = Decimal("10")
    min_notional_usd_aster: Decimal = Decimal("10")

    # -------------------------------------------------------------------------
    # Risk circuit breakers
    # -------------------------------------------------------------------------
    max_placements_per_hour_global: int = 10
    max_placements_per_hour_per_channel: int = 3
    daily_dd_pct_pause: Decimal = Decimal("10")

    # -------------------------------------------------------------------------
    # SSE / Polling
    # -------------------------------------------------------------------------
    telegram_poll_interval_sec: int = 60
    reconciler_interval_sec: int = 60
    sse_reconnect_backoff_sec: int = 2
    sse_price_staleness_threshold_sec: int = 30

    # -------------------------------------------------------------------------
    # Alerting
    # -------------------------------------------------------------------------
    alert_telegram_bot_token: str = ""
    alert_telegram_chat_id: str = ""

    # -------------------------------------------------------------------------
    # Logging
    # -------------------------------------------------------------------------
    log_level: str = "INFO"
    log_file_path: str = "./logs/bot.log"
    vooi_raw_log_path: str = "./logs/vooi-raw.log"
    log_retention_days: int = 30

    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------
    def get_fee_fallback_bps(self, exchange: str) -> Decimal:
        mapping = {
            "hyperliquid": self.fee_fallback_taker_bps_hyperliquid,
            "lighter": self.fee_fallback_taker_bps_lighter,
            "aster": self.fee_fallback_taker_bps_aster,
        }
        result = mapping.get(exchange.lower())
        if result is None:
            raise ValueError(f"Unknown exchange: {exchange}")
        return result

    def get_min_notional_usd(self, exchange: str) -> Decimal:
        mapping = {
            "hyperliquid": self.min_notional_usd_hyperliquid,
            "lighter": self.min_notional_usd_lighter,
            "aster": self.min_notional_usd_aster,
        }
        result = mapping.get(exchange.lower())
        if result is None:
            raise ValueError(f"Unknown exchange: {exchange}")
        return result


# Singleton instance — imported everywhere
settings = Settings()
