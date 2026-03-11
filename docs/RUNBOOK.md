# Kabu HFT Runbook

## 1. Startup checklist

1. kabu station is running and authenticated.
2. API password in `config.json` is correct.
3. `dry_run` is set as intended.
4. Symbol list size is <= 50.
5. Tick sizes are correct per symbol.
6. Log/journal paths are writable.

## 2. Recommended startup sequence

```bash
python main.py --config config.json
```

Expected logs:

- token acquired
- existing-position check (live mode)
- symbols registered
- websocket connected
- periodic strategy status lines

## 3. Position safety behavior

- In live mode, startup checks broker positions for configured products.
- If positions exist and `fail_on_startup_positions=true`, startup aborts.
- If `false`, process starts but logs a warning with details.

## 4. Shutdown behavior

On SIGINT/SIGTERM:

1. websocket is stopped
2. status task is cancelled
3. live mode attempts emergency close for open inventory
4. strategies are stopped
5. REST session is closed
6. journal is flushed and closed

Emergency close timeout is controlled by `shutdown_emergency_timeout_s`.

## 5. Incident playbooks

### REST 429 spikes

- reduce `rate_limit_per_second`
- lower requote pressure
- review cancel reason distribution

### Frequent stale quotes

- increase `stale_quote_ms` cautiously
- reduce trading aggressiveness
- verify local machine and kabu station health

### Position mismatch suspicion

- pause strategy
- query broker orders/positions manually
- restart with `fail_on_startup_positions=true`
- only resume after manual reconciliation

### Reconnect loops

- inspect network and kabu station status
- verify symbol registration response after reconnect
- reduce symbol set temporarily

## 6. Operational guardrails

- never disable all risk limits simultaneously
- keep kill-switch thresholds finite
- do not auto-recover from severe incidents without operator acknowledgement
- avoid changing both execution and risk parameters mid-session without logging
