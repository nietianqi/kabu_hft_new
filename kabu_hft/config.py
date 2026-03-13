from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path


DEFAULT_CONFIG: dict = {
    "api_password": "YOUR_KABU_API_PASSWORD",
    "base_url": "http://localhost:18080",
    "ws_url": "ws://localhost:18080/kabusapi/websocket",
    "dry_run": True,
    "status_interval_s": 30,
    "journal_path": "trades.csv",
    "markout_seconds": 30,
    "rate_limit_per_second": 4.0,
    "fail_on_startup_positions": True,
    "startup_position_products": [0, 2],
    "shutdown_emergency_timeout_s": 5.0,
    "order_profile": {
        "mode": "cash",
        "allow_short": False,
        "account_type": 4,
        "cash_buy_fund_type": "02",
        "cash_buy_deliv_type": 2,
        "cash_sell_fund_type": "",
        "cash_sell_deliv_type": 0,
        "margin_trade_type": 3,
        "margin_open_fund_type": "11",
        "margin_open_deliv_type": 0,
        "margin_close_deliv_type": 2,
        "front_order_type_limit": 20,
        "front_order_type_market": 10,
    },
    "global": {
        "entry_threshold": 0.40,
        "exit_threshold": 0.15,
        "strong_threshold": 0.75,
        "fair_value_beta": 0.75,
        "max_fair_shift_ticks": 3.0,
        "inventory_skew_ticks": 1.0,
        "max_fair_drift_ticks": 1.5,
        "obi_depth": 5,
        "obi_decay": 0.70,
        "lob_ofi_depth": 5,
        "lob_ofi_decay": 0.80,
        "tape_window_sec": 15,
        "zscore_window": 300,
        "mp_ema_alpha": 0.10,
        "max_hold_seconds": 45,
        "max_pending_ms": 2500,
        "min_order_lifetime_ms": 250,
        "min_edge_ticks": 0.25,
        "cooling_seconds": 300,
        "consecutive_loss_limit": 3,
        "max_spread_ticks": 3.0,
        "stale_quote_ms": 2000,
        "poll_interval_ms": 350,
        "min_board_interval_ms": 8.0,
        "queue_spread_max_ticks": 1.0,
        "queue_min_top_qty": 300,
        "abnormal_max_spread_ticks": 6.0,
        "max_event_rate_hz": 160.0,
        "event_burst_min_events": 6,
        "state_window_ms": 3000,
        "jump_threshold_ticks": 4.0,
        "max_requotes_per_minute": 30,
        "allow_aggressive_entry": False,
        "allow_aggressive_exit": True,
        "commission_per_share": 0.0,
        "signal_weights": {
            "lob_ofi": 0.30,
            "obi": 0.25,
            "tape_ofi": 0.20,
            "micro_momentum": 0.15,
            "microprice_tilt": 0.10,
        },
    },
    "symbols": [
        {
            "symbol": "9984",
            "exchange": 1,
            "tick_size": 50.0,
            "base_qty": 100,
            "fixed_qty": None,
            "max_qty": 300,
            "max_inventory_qty": 300,
            "max_notional": 3_000_000,
            "daily_loss_limit": -50_000,
        },
        {
            "symbol": "4568",
            "exchange": 1,
            "tick_size": 5.0,
            "base_qty": 100,
            "fixed_qty": None,
            "max_qty": 300,
            "max_inventory_qty": 300,
            "max_notional": 3_000_000,
            "daily_loss_limit": -50_000,
        },
    ],
}


def _deep_merge(base: dict, override: dict) -> dict:
    merged = deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


@dataclass(slots=True)
class SignalWeights:
    lob_ofi: float = 0.30
    obi: float = 0.25
    tape_ofi: float = 0.20
    micro_momentum: float = 0.15
    microprice_tilt: float = 0.10

    @classmethod
    def from_dict(cls, payload: dict | None) -> "SignalWeights":
        payload = payload or {}
        return cls(
            lob_ofi=float(payload.get("lob_ofi", 0.30)),
            obi=float(payload.get("obi", 0.25)),
            tape_ofi=float(payload.get("tape_ofi", 0.20)),
            micro_momentum=float(payload.get("micro_momentum", 0.15)),
            microprice_tilt=float(payload.get("microprice_tilt", 0.10)),
        )


@dataclass(slots=True)
class OrderProfile:
    mode: str = "cash"
    allow_short: bool = False
    account_type: int = 4
    cash_buy_fund_type: str = "02"
    cash_buy_deliv_type: int = 2
    cash_sell_fund_type: str = ""
    cash_sell_deliv_type: int = 0
    margin_trade_type: int = 3
    margin_open_fund_type: str = "11"
    margin_open_deliv_type: int = 0
    margin_close_deliv_type: int = 2
    front_order_type_limit: int = 20
    front_order_type_market: int = 10

    @classmethod
    def from_dict(cls, payload: dict | None) -> "OrderProfile":
        payload = payload or {}
        return cls(
            mode=str(payload.get("mode", "cash")),
            allow_short=bool(payload.get("allow_short", False)),
            account_type=int(payload.get("account_type", 4)),
            cash_buy_fund_type=str(payload.get("cash_buy_fund_type", "02")),
            cash_buy_deliv_type=int(payload.get("cash_buy_deliv_type", 2)),
            cash_sell_fund_type=str(payload.get("cash_sell_fund_type", "")),
            cash_sell_deliv_type=int(payload.get("cash_sell_deliv_type", 0)),
            margin_trade_type=int(payload.get("margin_trade_type", 3)),
            margin_open_fund_type=str(payload.get("margin_open_fund_type", "11")),
            margin_open_deliv_type=int(payload.get("margin_open_deliv_type", 0)),
            margin_close_deliv_type=int(payload.get("margin_close_deliv_type", 2)),
            front_order_type_limit=int(payload.get("front_order_type_limit", 20)),
            front_order_type_market=int(payload.get("front_order_type_market", 10)),
        )


@dataclass(slots=True)
class StrategyConfig:
    symbol: str
    exchange: int
    tick_size: float
    base_qty: int
    fixed_qty: int | None
    max_qty: int
    max_inventory_qty: int
    max_notional: float
    daily_loss_limit: float
    entry_threshold: float
    exit_threshold: float
    strong_threshold: float
    fair_value_beta: float
    max_fair_shift_ticks: float
    inventory_skew_ticks: float
    max_fair_drift_ticks: float
    obi_depth: int
    obi_decay: float
    lob_ofi_depth: int
    lob_ofi_decay: float
    tape_window_sec: int
    zscore_window: int
    mp_ema_alpha: float
    max_hold_seconds: int
    max_pending_ms: int
    min_order_lifetime_ms: int
    min_edge_ticks: float
    cooling_seconds: int
    consecutive_loss_limit: int
    max_spread_ticks: float
    stale_quote_ms: int
    poll_interval_ms: int
    min_board_interval_ms: float
    queue_spread_max_ticks: float
    queue_min_top_qty: int
    abnormal_max_spread_ticks: float
    max_event_rate_hz: float
    event_burst_min_events: int
    state_window_ms: int
    jump_threshold_ticks: float
    max_requotes_per_minute: int
    allow_aggressive_entry: bool
    allow_aggressive_exit: bool
    commission_per_share: float
    signal_weights: SignalWeights = field(default_factory=SignalWeights)


@dataclass(slots=True)
class AppConfig:
    api_password: str
    base_url: str
    ws_url: str
    dry_run: bool
    status_interval_s: int
    journal_path: str
    markout_seconds: int
    rate_limit_per_second: float
    fail_on_startup_positions: bool
    startup_position_products: list[int]
    shutdown_emergency_timeout_s: float
    order_profile: OrderProfile
    strategies: list[StrategyConfig]


def load_config(path: str | Path | None) -> AppConfig:
    path_obj = Path(path) if path else None
    payload = deepcopy(DEFAULT_CONFIG)
    if path_obj and path_obj.exists():
        with path_obj.open("r", encoding="utf-8") as handle:
            payload = _deep_merge(payload, json.load(handle))

    order_profile = OrderProfile.from_dict(payload.get("order_profile"))
    global_cfg = payload.get("global", {})

    strategies: list[StrategyConfig] = []
    for raw_symbol in payload.get("symbols", []):
        merged = _deep_merge(global_cfg, raw_symbol)
        raw_fixed_qty = merged.get("fixed_qty")
        fixed_qty = int(raw_fixed_qty) if raw_fixed_qty is not None else None
        strategies.append(
            StrategyConfig(
                symbol=str(raw_symbol["symbol"]),
                exchange=int(raw_symbol.get("exchange", 1)),
                tick_size=float(raw_symbol["tick_size"]),
                base_qty=int(raw_symbol.get("base_qty", 100)),
                fixed_qty=fixed_qty if fixed_qty and fixed_qty > 0 else None,
                max_qty=int(raw_symbol.get("max_qty", raw_symbol.get("base_qty", 100))),
                max_inventory_qty=int(raw_symbol.get("max_inventory_qty", raw_symbol.get("max_qty", raw_symbol.get("base_qty", 100)))),
                max_notional=float(raw_symbol.get("max_notional", 0.0)),
                daily_loss_limit=float(raw_symbol.get("daily_loss_limit", -50_000)),
                entry_threshold=float(merged.get("entry_threshold", 0.40)),
                exit_threshold=float(merged.get("exit_threshold", 0.15)),
                strong_threshold=float(merged.get("strong_threshold", 0.75)),
                fair_value_beta=float(merged.get("fair_value_beta", 0.75)),
                max_fair_shift_ticks=float(merged.get("max_fair_shift_ticks", 3.0)),
                inventory_skew_ticks=float(merged.get("inventory_skew_ticks", 1.0)),
                max_fair_drift_ticks=float(merged.get("max_fair_drift_ticks", 1.5)),
                obi_depth=int(merged.get("obi_depth", 5)),
                obi_decay=float(merged.get("obi_decay", 0.70)),
                lob_ofi_depth=int(merged.get("lob_ofi_depth", 5)),
                lob_ofi_decay=float(merged.get("lob_ofi_decay", 0.80)),
                tape_window_sec=int(merged.get("tape_window_sec", 15)),
                zscore_window=int(merged.get("zscore_window", 300)),
                mp_ema_alpha=float(merged.get("mp_ema_alpha", 0.10)),
                max_hold_seconds=int(merged.get("max_hold_seconds", 45)),
                max_pending_ms=int(merged.get("max_pending_ms", 2500)),
                min_order_lifetime_ms=int(merged.get("min_order_lifetime_ms", 250)),
                min_edge_ticks=float(merged.get("min_edge_ticks", 0.25)),
                cooling_seconds=int(merged.get("cooling_seconds", 300)),
                consecutive_loss_limit=int(merged.get("consecutive_loss_limit", 3)),
                max_spread_ticks=float(merged.get("max_spread_ticks", 3.0)),
                stale_quote_ms=int(merged.get("stale_quote_ms", 1200)),
                poll_interval_ms=int(merged.get("poll_interval_ms", 350)),
                min_board_interval_ms=float(merged.get("min_board_interval_ms", 8.0)),
                queue_spread_max_ticks=float(merged.get("queue_spread_max_ticks", 1.0)),
                queue_min_top_qty=int(merged.get("queue_min_top_qty", 300)),
                abnormal_max_spread_ticks=float(merged.get("abnormal_max_spread_ticks", 6.0)),
                max_event_rate_hz=float(merged.get("max_event_rate_hz", 160.0)),
                event_burst_min_events=int(merged.get("event_burst_min_events", 6)),
                state_window_ms=int(merged.get("state_window_ms", 3000)),
                jump_threshold_ticks=float(merged.get("jump_threshold_ticks", 4.0)),
                max_requotes_per_minute=int(merged.get("max_requotes_per_minute", 30)),
                allow_aggressive_entry=bool(merged.get("allow_aggressive_entry", False)),
                allow_aggressive_exit=bool(merged.get("allow_aggressive_exit", True)),
                commission_per_share=float(merged.get("commission_per_share", 0.0)),
                signal_weights=SignalWeights.from_dict(merged.get("signal_weights")),
            )
        )

    if len(strategies) > 50:
        raise ValueError("kabu PUSH supports at most 50 registered symbols.")

    return AppConfig(
        api_password=str(payload.get("api_password", "")),
        base_url=str(payload.get("base_url", "http://localhost:18080")),
        ws_url=str(payload.get("ws_url", "ws://localhost:18080/kabusapi/websocket")),
        dry_run=bool(payload.get("dry_run", True)),
        status_interval_s=int(payload.get("status_interval_s", 30)),
        journal_path=str(payload.get("journal_path", "trades.csv")),
        markout_seconds=int(payload.get("markout_seconds", 30)),
        rate_limit_per_second=float(payload.get("rate_limit_per_second", 4.0)),
        fail_on_startup_positions=bool(payload.get("fail_on_startup_positions", True)),
        startup_position_products=[int(product) for product in payload.get("startup_position_products", [0, 2])],
        shutdown_emergency_timeout_s=float(payload.get("shutdown_emergency_timeout_s", 5.0)),
        order_profile=order_profile,
        strategies=strategies,
    )
