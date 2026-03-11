# kabu_hft — kabu Station Microstructure HFT Scaffold

A self-contained, asyncio-based high-frequency trading scaffold for Japanese equities (TSE) using the **kabu Station API** (Mitsubishi UFJ eSmart Securities). No dependency on vn.py/VeighNa — every layer is purpose-built for kabu's constraints.

---

## Architecture

```
kabu Station (localhost:18080)
    │
    ├─ WebSocket PUSH ──► KabuWebSocket ──► KabuAdapter (normalize bid/ask)
    │                                             │
    │                                             ▼
    │                                       BoardSnapshot / TradePrint
    │                                             │
    │                              ┌──────────────┼──────────────┐
    │                              ▼              ▼              ▼
    │                         SignalStack    RiskGuard     ExecutionController
    │                         (5 signals)  (session/      (passive limit,
    │                                       sizing/        requote budget,
    │                                       PnL/ATR)       order reconcile)
    │                              └──────────────┼──────────────┘
    │                                             ▼
    │                                       HFTStrategy
    │                                             │
    └─ REST (sendorder/cancel/orders) ◄───────────┘
                                                  │
                                             TradeJournal
                                          (trades.csv + markout.csv)
```

---

## Key Design Decisions

### 1. kabu bid/ask semantic reversal

The kabu PUSH API uses **reversed** bid/ask naming compared to international convention:

| kabu field | International meaning |
|------------|----------------------|
| `AskPrice` / `AskQty` | **Best bid** (最良買気配) |
| `BidPrice` / `BidQty` | **Best ask** (最良売気配) |
| `Buy1..10` | Bid-side depth |
| `Sell1..10` | Ask-side depth |

`KabuAdapter.board()` normalizes these to standard `bid`/`ask` internally. All downstream code uses the standard convention.

### 2. Tape-OFI is approximate

The kabu PUSH has no independent tick-by-tick trade stream. `TradePrint` is derived from:
- Volume delta (`TradingVolume[t] - TradingVolume[t-1]`)
- Trade side inferred from price vs. best bid/ask/mid (quote rule)

This is a structural limitation of the broker API, not the strategy code.

### 3. Single-sided working orders only

Simultaneous buy and sell limit orders on the same symbol risk triggering broker-side cross-trade rejection. This scaffold enforces **one working order at a time per symbol** (OPENING → OPEN → CLOSING → FLAT state machine).

### 4. Localhost gateway latency

The system connects to `kabu Station` desktop software at `localhost:18080`. This is a broker-client hop, not exchange co-location. Achievable latency is tens of milliseconds, suitable for passive microstructure scalping — not for FPGA-level HFT.

---

## Microstructure Signals

All five signals are z-scored online (rolling window) then linearly combined into `composite_alpha`:

| Signal | Description | Weight (default) |
|--------|-------------|-----------------|
| **LOB-OFI** | Order Flow Imbalance across 5 depth levels (bid add/cancel vs ask add/cancel) | 0.30 |
| **OBI** | Weighted Order Book Imbalance (exponential depth decay) | 0.25 |
| **Tape-OFI** | 15-second rolling buy/sell volume imbalance (approximated) | 0.20 |
| **Micro-momentum** | `(microprice − EMA(microprice)) / tick_size` | 0.15 |
| **Microprice tilt** | `(microprice − mid) / (spread/2)` | 0.10 |

Microprice: `(ask_size × bid + bid_size × ask) / (bid_size + ask_size)`

---

## Prerequisites

- Python 3.11+
- **kabu Station** desktop app running (obtainable from Mitsubishi UFJ eSmart Securities)
- API password set in kabu Station settings
- `pip install aiohttp>=3.9 websockets>=12` (or `pip install -r requirements.txt`)
- Optional: `pip install orjson` for faster JSON parsing (~3x faster JSON decode)

---

## Quick Start

```bash
# 1. Clone
git clone https://github.com/nietianqi/kabu_hft_new.git
cd kabu_hft_new

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure
cp config.json my_config.json
# Edit my_config.json:
#   - Set "api_password" to your kabu API password
#   - Adjust "symbols" (up to 50 per kabu PUSH limit)
#   - Keep "dry_run": true for paper trading first

# 4. Run (paper trading)
python main.py --config my_config.json

# 5. Run tests
python -m pytest tests/ -v
```

---

## Configuration Reference

| Key | Default | Description |
|-----|---------|-------------|
| `api_password` | `""` | kabu Station API password |
| `base_url` | `http://localhost:18080` | REST endpoint |
| `ws_url` | `ws://localhost:18080/kabusapi/websocket` | WebSocket endpoint |
| `dry_run` | `true` | Paper trading (no real orders sent) |
| `journal_path` | `trades.csv` | CSV file for completed trades |
| `markout_seconds` | `30` | Seconds after exit to record markout P&L |
| `rate_limit_per_second` | `4.0` | Max REST requests per second (kabu limit ~5) |
| `status_interval_s` | `30` | Seconds between status log lines |

### Order Profile

| Key | Description |
|-----|-------------|
| `mode` | `"cash"` or `"margin_daytrade"` |
| `allow_short` | Allow short selling (cash mode only) |
| `account_type` | kabu account type code (default: 4 = 特定口座) |

### Per-Symbol Strategy

| Key | Default | Description |
|-----|---------|-------------|
| `tick_size` | required | Minimum price increment (e.g. 50 for 9984) |
| `base_qty` | `100` | Default order size (shares) |
| `max_qty` | `300` | Maximum single order size |
| `max_inventory_qty` | `300` | Maximum position size |
| `max_notional` | `3_000_000` | Maximum position value (JPY) |
| `daily_loss_limit` | `-50_000` | Kill-switch daily P&L floor (JPY) |
| `entry_threshold` | `0.40` | Minimum composite z-score to open |
| `exit_threshold` | `0.15` | Composite z-score reversal to close |
| `strong_threshold` | `0.75` | Threshold for price improvement / aggressive exit |
| `max_hold_seconds` | `45` | Force close after N seconds regardless of signal |
| `max_pending_ms` | `2500` | Cancel unmatched entry after N ms |
| `max_spread_ticks` | `3.0` | Skip entry if spread > N ticks |
| `stale_quote_ms` | `1200` | Skip entry if last board update > N ms ago |
| `cooling_seconds` | `300` | Pause duration after consecutive losses |
| `consecutive_loss_limit` | `3` | Trigger cooling after N losses in a row |
| `max_requotes_per_minute` | `30` | Rate limit on order requotes |
| `allow_aggressive_entry` | `false` | Allow market orders on very strong signals |
| `allow_aggressive_exit` | `true` | Allow market orders on forced close |

---

## Output Files

### `trades.csv`
One row per completed round-trip:

| Column | Description |
|--------|-------------|
| `ts_jst` | Exit timestamp (JST ISO-8601) |
| `symbol` | Ticker code |
| `side` | `+1` = long, `-1` = short |
| `qty` | Shares traded |
| `entry_price` | Average fill price on entry |
| `exit_price` | Average fill price on exit |
| `realized_pnl` | Gross P&L (JPY, no commission) |
| `hold_ms` | Hold time in milliseconds |
| `exit_reason` | `signal_reverse`, `max_hold_time`, `session_end`, `emergency_shutdown`, etc. |
| `obi_z` … `composite` | Signal z-scores at time of entry |

### `trades.markout.csv`
Markout P&L: mid-price captured `markout_seconds` after exit, measuring residual alpha decay post-trade.

---

## Risk Controls

1. **Session guard** — no new entries outside 09:00–11:25 / 12:30–15:25 JST; force close after 11:30 / 15:30
2. **Daily loss kill-switch** — halts all new entries when `daily_pnl ≤ daily_loss_limit`
3. **Consecutive loss cooling** — N consecutive losses trigger a cooling-off period
4. **ATR-based sizing** — reduces position size when realized volatility is elevated
5. **Max hold time** — force-closes any position held longer than `max_hold_seconds`
6. **Stale quote filter** — no entries if market data is older than `stale_quote_ms`
7. **Spread filter** — no entries if bid/ask spread exceeds `max_spread_ticks`
8. **REST rate limiter** — proactive token-bucket limits REST calls to `rate_limit_per_second`
9. **Emergency close** — on SIGINT/SIGTERM, attempts forced market close of all open positions before exit
10. **WebSocket re-registration** — symbols are re-registered with kabu on every WebSocket reconnect

---

## Open Source References

This scaffold's design is informed by:
- [NautilusTrader](https://github.com/nautechsystems/nautilus_trader) — unified research/live architecture, Rust-native event loop
- [hftbacktest](https://github.com/nkaz001/hftbacktest) — queue position, feed latency, and order latency modeling
- [ABIDES](https://github.com/abides-sim/abides) — explicit latency modeling, agent-based market simulation
- [Hummingbot](https://github.com/hummingbot/hummingbot) — connector abstraction, maker/taker strategy patterns

---

## License

MIT
