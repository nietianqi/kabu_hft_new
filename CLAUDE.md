# CLAUDE.md

This file is the single source of truth for AI collaboration in this repository.
Both Claude Code and Codex must follow these rules.

## Core Trading Invariants

1. Kabu quote semantics are reversed in raw payloads:
   - `AskPrice/AskQty` => internal best bid / bid size
   - `BidPrice/BidQty` => internal best ask / ask size
2. Internal side convention:
   - `+1` = long / buy side
   - `-1` = short / sell side
3. Strategy code must not consume raw kabu fields directly.
   Always use normalized internal structures.

## Exchange Rules

1. For **new orders** after the policy change:
   - use `Exchange=9` (SOR) or `Exchange=27` (TSE+), not `1`.
2. For **register API** subscriptions:
   - use `Exchange=1` for TSE symbols (the app normalizes this).
3. For **margin close (repayment)**:
   - close by matching `HoldID` and `Exchange` of the existing lot.
   - do not mix hold lots across exchanges.

## Collaboration Workflow

1. One session should fix only 1-3 bugs (P0/P1 first).
2. Prefer one primary file per task; edit other files only for required wiring/tests.
3. Before editing a dataclass field, grep all references and update all call sites.
4. Every logic change must include tests in the same session.
5. Always show real `git diff` and test command outputs.

## Required Session Output

Each AI session must output:
1. Assumptions
2. Changed files
3. Tests run and result
4. Residual risks

## Safety Guards

1. Do not auto-retry order mutation APIs unless there is an explicit idempotency strategy.
2. Keep startup risk checks enabled in live mode unless explicitly overridden.
3. Keep log volume bounded (throttle repeated warnings).

## Merge Gate

All changes must pass:

```bash
python -m unittest discover -s tests -p "test_*.py" -v
```

