from __future__ import annotations

import time
from dataclasses import dataclass

from kabu_hft.gateway import BoardSnapshot


@dataclass(slots=True)
class MarketDataHealth:
    has_quote: bool
    is_stale: bool
    is_spread_valid: bool
    duplicate_count: int
    out_of_order_count: int
    last_quote_age_ms: float


class BookState:
    def __init__(self) -> None:
        self.latest: BoardSnapshot | None = None
        self.previous: BoardSnapshot | None = None
        self.duplicate_count = 0
        self.out_of_order_count = 0
        self.last_update_ns = 0

    def update(self, snapshot: BoardSnapshot) -> bool:
        if snapshot.out_of_order:
            self.out_of_order_count += 1
            return False
        if snapshot.duplicate:
            self.duplicate_count += 1
            return False
        self.previous = self.latest
        self.latest = snapshot
        self.last_update_ns = time.time_ns()
        return True

    def health(self, stale_ms: int, now_ns: int | None = None) -> MarketDataHealth:
        now = now_ns if now_ns is not None else time.time_ns()
        if self.latest is None or self.last_update_ns <= 0:
            return MarketDataHealth(
                has_quote=False,
                is_stale=True,
                is_spread_valid=False,
                duplicate_count=self.duplicate_count,
                out_of_order_count=self.out_of_order_count,
                last_quote_age_ms=float("inf"),
            )
        age_ms = max(0.0, (now - self.last_update_ns) / 1_000_000)
        return MarketDataHealth(
            has_quote=True,
            is_stale=age_ms > stale_ms,
            is_spread_valid=self.latest.spread > 0,
            duplicate_count=self.duplicate_count,
            out_of_order_count=self.out_of_order_count,
            last_quote_age_ms=age_ms,
        )
