# kabu_hft

Asyncio-based microstructure HFT scaffold for Japanese equities via kabu Station API.

## What this project is

- Self-built stack (no vn.py dependency).
- Designed for kabu broker topology: `strategy -> localhost -> kabu station -> broker -> exchange`.
- Focused on passive short-horizon execution with microstructure alpha.

## Core constraints

- kabu quote semantics are reversed vs global convention:
  - `AskPrice/AskQty` means internal best bid.
  - `BidPrice/BidQty` means internal best ask.
- PUSH only streams registered symbols (max 50).
- Tape stream is approximate (derived from `TradingVolume` delta + quote-rule inference).

## Current architecture

```text
kabu_hft/
  app.py                  # process lifecycle, startup checks, graceful shutdown
  config.py               # typed config + defaults
  journal.py              # trades.csv + markout.csv
  core/strategy.py        # signal->decision->execution orchestration
  gateway/kabu_gateway.py # REST/WS adapter + normalizer
  execution/engine.py     # single-sided execution state machine
  risk/guard.py           # limits, stale quote, session guard, sizing
  signals/microstructure.py
```

## Adaptive execution modes

- `NORMAL`: passive fair-value quoting (reservation price around microstructure alpha).
- `QUEUE`: queue-defense mode for one-tick spread regimes (retreat when top queue is thin).
- `ABNORMAL`: close-only behavior (no new entries, prioritize risk reduction).

## Quick start

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python main.py --config config.json
```

Default is `dry_run=true`.

## Key config keys

- `dry_run`: paper mode.
- `journal_path`: trade log path.
- `markout_seconds`: delayed markout horizon.
- `rate_limit_per_second`: proactive REST pacing.
- `fail_on_startup_positions`: block startup if broker already has open positions.
- `startup_position_products`: position products to inspect at startup.
- `shutdown_emergency_timeout_s`: graceful emergency-close timeout.

## Testing

Use module-style execution from repository root:

```bash
python -m unittest tests.test_gateway -v
python -m unittest tests.test_signals -v
python -m unittest tests.test_execution -v
python -m unittest tests.test_market_regime -v
python -m unittest tests.test_risk -v
python -m unittest tests.test_replay -v
python -m unittest tests.test_journal -v
```

## Latency Report

Compare environments (for example home PC vs AWS) from runtime logs:

```bash
python -m kabu_hft.telemetry.latency_report ^
  --log home.log --label home ^
  --log aws.log --label aws ^
  --json-out latency_report.json
```

Optional: add `--trades-csv trades.csv` to include stale-exit rate from trade logs.

## Docs

- [Design](docs/DESIGN.md)
- [Test Plan](docs/TEST_PLAN.md)
- [Runbook](docs/RUNBOOK.md)
