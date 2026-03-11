# Kabu HFT Test Plan

## 1. Test layers

### L0 Environment checks

- kabu station logged in
- token endpoint reachable
- register endpoint reachable
- websocket reachable
- log directory writable
- system clock sane

### L1 Unit tests

Scope:

- normalizer and market model invariants
- signal math
- risk gates
- execution state transitions
- journal persistence

### L2 Replay tests

Scope:

- signal stability under realistic message cadence
- cancel/replace behavior
- markout consistency
- stale and spread gating

### L3 Integration tests

Scope:

- adapter -> strategy -> execution -> risk chain
- partial fill handling
- reconciliation with broker truth
- reconnect and symbol re-registration

### L4 Shadow-live tests

Scope:

- no real orders
- hypothetical decisions and fill proxies
- intraday drift and alpha behavior

### L5 Stress and fault drills

Scope:

- websocket disconnect/reconnect storms
- REST 429 and 5xx bursts
- delayed broker responses
- duplicated and out-of-order quote bursts
- log backpressure

## 2. Mandatory cases

1. semantic reversal mapping (`AskPrice` -> best bid, `BidPrice` -> best ask)
2. spread/mid correctness
3. microprice direction under asymmetric queue sizes
4. OFI direction for representative queue updates
5. tape proxy attribution on volume jumps
6. alpha reversal cancellation behavior
7. partial fill lifecycle (`WORKING -> PARTIAL -> FILLED`)
8. stale quote entry block
9. REST 429 retry pacing and bounded retries
10. REST 5xx bounded retries and state safety
11. lunch/close open-block and close-only behavior
12. drawdown and inventory hard limits
13. duplicate quote suppression
14. out-of-order quote rejection
15. jump quote safety (no blind chase)
16. startup existing-position guard behavior

## 3. Existing automated coverage

- `tests/test_gateway.py`
  - semantic reversal
  - tape volume delta sizing
  - duplicate/out-of-order quote suppression
  - password propagation in sendorder payload
- `tests/test_signals.py`
  - positive flow produces positive composite
- `tests/test_execution.py`
  - dry-run entry/exit round-trip
- `tests/test_journal.py`
  - csv persistence
  - append semantics
  - hold time calculation
  - pending markout flush on close
  - nested path creation

## 4. Running tests

Run from repository root:

```bash
python -m unittest tests.test_gateway -v
python -m unittest tests.test_signals -v
python -m unittest tests.test_execution -v
python -m unittest tests.test_journal -v
```

If your environment has import-path quirks, ensure current working directory is repository root.
