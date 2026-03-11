from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass
from enum import Enum

from kabu_hft.gateway import BoardSnapshot


class MarketState(str, Enum):
    NORMAL = "NORMAL"
    QUEUE = "QUEUE"
    ABNORMAL = "ABNORMAL"


@dataclass(slots=True)
class MarketStateView:
    state: MarketState
    reason: str
    spread_ticks: float
    event_rate_hz: float
    stale_ms: float
    jump_ticks: float


class MarketStateDetector:
    def __init__(
        self,
        *,
        tick_size: float,
        stale_quote_ms: int,
        queue_spread_max_ticks: float,
        abnormal_max_spread_ticks: float,
        max_event_rate_hz: float,
        state_window_ms: int,
        jump_threshold_ticks: float,
    ) -> None:
        self.tick_size = max(tick_size, 1e-9)
        self.stale_quote_ms = max(stale_quote_ms, 1)
        self.queue_spread_max_ticks = max(queue_spread_max_ticks, 0.0)
        self.abnormal_max_spread_ticks = max(abnormal_max_spread_ticks, 0.5)
        self.max_event_rate_hz = max(max_event_rate_hz, 1.0)
        self.state_window_ns = max(state_window_ms, 250) * 1_000_000
        self.jump_threshold_ticks = max(jump_threshold_ticks, 0.5)
        self._event_times: deque[int] = deque()
        self._prev_mid = 0.0

    def evaluate(self, snapshot: BoardSnapshot, now_ns: int | None = None) -> MarketStateView:
        now = now_ns if now_ns is not None else time.time_ns()
        self._event_times.append(now)
        while self._event_times and now - self._event_times[0] > self.state_window_ns:
            self._event_times.popleft()

        spread_ticks = snapshot.spread / self.tick_size if snapshot.spread > 0 else 0.0
        stale_ms = max(0.0, (now - snapshot.ts_ns) / 1_000_000)
        stale = stale_ms > self.stale_quote_ms

        jump_ticks = 0.0
        if self._prev_mid > 0.0:
            jump_ticks = abs(snapshot.mid - self._prev_mid) / self.tick_size
        self._prev_mid = snapshot.mid

        event_rate_hz = self._event_rate_hz(now)

        if not snapshot.valid:
            return MarketStateView(
                state=MarketState.ABNORMAL,
                reason="invalid_quote",
                spread_ticks=spread_ticks,
                event_rate_hz=event_rate_hz,
                stale_ms=stale_ms,
                jump_ticks=jump_ticks,
            )
        if stale:
            return MarketStateView(
                state=MarketState.ABNORMAL,
                reason="stale_quote",
                spread_ticks=spread_ticks,
                event_rate_hz=event_rate_hz,
                stale_ms=stale_ms,
                jump_ticks=jump_ticks,
            )
        if spread_ticks >= self.abnormal_max_spread_ticks:
            return MarketStateView(
                state=MarketState.ABNORMAL,
                reason="spread_blowout",
                spread_ticks=spread_ticks,
                event_rate_hz=event_rate_hz,
                stale_ms=stale_ms,
                jump_ticks=jump_ticks,
            )
        if event_rate_hz >= self.max_event_rate_hz:
            return MarketStateView(
                state=MarketState.ABNORMAL,
                reason="event_burst",
                spread_ticks=spread_ticks,
                event_rate_hz=event_rate_hz,
                stale_ms=stale_ms,
                jump_ticks=jump_ticks,
            )
        if jump_ticks >= self.jump_threshold_ticks:
            return MarketStateView(
                state=MarketState.ABNORMAL,
                reason="price_jump",
                spread_ticks=spread_ticks,
                event_rate_hz=event_rate_hz,
                stale_ms=stale_ms,
                jump_ticks=jump_ticks,
            )
        if spread_ticks <= self.queue_spread_max_ticks + 1e-9:
            return MarketStateView(
                state=MarketState.QUEUE,
                reason="one_tick_queue",
                spread_ticks=spread_ticks,
                event_rate_hz=event_rate_hz,
                stale_ms=stale_ms,
                jump_ticks=jump_ticks,
            )
        return MarketStateView(
            state=MarketState.NORMAL,
            reason="normal_flow",
            spread_ticks=spread_ticks,
            event_rate_hz=event_rate_hz,
            stale_ms=stale_ms,
            jump_ticks=jump_ticks,
        )

    def _event_rate_hz(self, now_ns: int) -> float:
        if len(self._event_times) < 2:
            return 0.0
        duration_ns = max(now_ns - self._event_times[0], 1)
        return len(self._event_times) * 1_000_000_000 / duration_ns
