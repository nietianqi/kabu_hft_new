# Kabu HFT Design

## 1. Objective

Build a self-contained microstructure trading system on top of kabu Station API:

- no vn.py dependency
- standard internal market model
- signal-driven passive execution
- robust OMS reconciliation and risk controls
- replay-ready evaluation outputs

## 2. External constraints

### 2.1 Broker topology

This is not exchange-native colocation. Expected path:

`strategy -> localhost API -> kabu station -> broker -> exchange`

Practical implication: optimize for correctness and quote-quality first, not nanosecond latency.

### 2.2 PUSH limits

- only REST-registered symbols are streamed
- up to 50 symbols
- session gaps around lunch/close require stale-data guards

### 2.3 Bid/ask semantics

In kabu PUSH, names are reversed:

- `AskPrice/AskQty` => internal best bid
- `BidPrice/BidQty` => internal best ask

Strategy must never consume raw kabu fields directly.

### 2.4 Tape observability

No native exchange-like time-and-sales stream. Tape OFI is approximated from:

- `TradingVolume` delta
- quote-rule side inference

## 3. Layered architecture

1. Adapter layer
2. Normalized market-data layer
3. Signal engine
4. Strategy decision layer
5. Execution layer
6. OMS + reconciliation
7. Risk and capital controls
8. Replay/evaluation
9. Monitoring and logs

Current code maps to this structure through:

- `kabu_hft/gateway/`
- `kabu_hft/signals/`
- `kabu_hft/core/strategy.py`
- `kabu_hft/execution/`
- `kabu_hft/risk/`
- `kabu_hft/journal.py`

## 4. Market model contract

Core internal shape:

- `BoardSnapshot` with normalized bid/ask and L2 levels
- `TradePrint` as proxied tape event
- monotonic handling rules:
  - reject out-of-order snapshots
  - ignore exact duplicate snapshots
  - reject invalid spread quotes

## 5. Signal stack

Five signals are implemented and z-scored online:

1. weighted order-book imbalance
2. LOB-OFI
3. tape-OFI (proxy)
4. micro-momentum
5. microprice tilt

Composite alpha is a weighted linear combination with configurable weights.

## 6. Strategy and execution

### 6.1 Strategy role

Strategy determines target direction/urgency only.
Order-placement mechanics are delegated to execution.

### 6.2 Execution role

Single-sided working-order model per symbol:

`FLAT -> OPENING -> OPEN -> CLOSING -> FLAT`

Execution responsibilities:

- passive price selection
- requote budgeting
- timeout cancellation
- dry-run paper fills
- reconciliation-driven state correction

## 7. OMS and reconciliation

Primary order truth is broker query (`GET /orders`), not local assumption.

Reconciliation loop:

- poll while there is a working order
- apply cumulative fill delta
- finalize order state on broker-final transitions

## 8. Risk and capital controls

Implemented controls:

- session windows (open/close permissions)
- stale quote filter
- max spread gate
- max hold timer
- daily drawdown stop
- consecutive-loss cooling
- ATR-aware sizing
- max inventory / max notional

Operational controls:

- startup broker-position check
- optional startup block if positions exist
- emergency close on shutdown
- REST pacing via token bucket

## 9. Evaluation outputs

`TradeJournal` emits:

- `trades.csv` for round-trips
- `*.markout.csv` for delayed post-exit markout

Markout scheduling now flushes pending entries on close for deterministic logs.

## 10. Future roadmap

1. replay runner with latency and fill models
2. symbol-pool rotation for >50 watchlist
3. maker queue-value estimation
4. hot-path migration (Rust/Cython/Numba)
