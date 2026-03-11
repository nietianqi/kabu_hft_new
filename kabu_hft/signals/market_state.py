"""Market microstructure state classifier.

Three states:
  NORMAL   – spread ≥ threshold; classic price-discovery mode
  QUEUE    – spread compressed to ~1 tick; queue position dominates
  ABNORMAL – stale quote / frozen / extreme event rate / outside session
"""
from __future__ import annotations

import time
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from kabu_hft.gateway import BoardSnapshot
    from kabu_hft.risk.guard import SessionGuard


class MarketState(Enum):
    NORMAL = "normal"
    QUEUE = "queue"
    ABNORMAL = "abnormal"


_STATE_GATE: dict[MarketState, float] = {
    MarketState.NORMAL: 1.0,
    MarketState.QUEUE: 0.6,
    MarketState.ABNORMAL: 0.0,
}


class MarketStateDetector:
    """
    Lightweight online classifier — call ``update()`` once per board event.

    Event-rate EMA:
        rate_ema = alpha * (1 / dt_sec) + (1 - alpha) * prev_rate_ema
    where ``dt_sec`` is the wall-clock gap between successive board events.

    State priority (checked in order):
        1. ABNORMAL if snapshot.valid is False or quote is stale
        2. ABNORMAL if outside session CLOSE_WINDOWS
        3. ABNORMAL if event_rate out of [event_rate_freeze, event_rate_high]
        4. QUEUE if spread < queue_spread_ticks * tick_size
        5. NORMAL otherwise
    """

    def __init__(
        self,
        queue_spread_ticks: float = 1.5,
        event_rate_high: float = 100.0,
        event_rate_freeze: float = 0.5,
        event_rate_ema_alpha: float = 0.10,
        stale_quote_ms: int = 1200,
        tick_size: float = 50.0,
    ) -> None:
        self._queue_spread = queue_spread_ticks * tick_size
        self._rate_high = event_rate_high
        self._rate_freeze = event_rate_freeze
        self._alpha = event_rate_ema_alpha
        self._stale_ns = stale_quote_ms * 1_000_000
        self._tick_size = tick_size

        self._event_rate: float = 0.0      # EMA events/sec
        self._last_recv_ns: int = 0
        self._state: MarketState = MarketState.NORMAL

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def update(
        self,
        snapshot: "BoardSnapshot",
        recv_ns: int,
        now_dt: datetime,
        session_guard: "SessionGuard",
    ) -> MarketState:
        """
        Update internal state and return current ``MarketState``.

        Parameters
        ----------
        snapshot     : Normalized board snapshot (already passed through KabuAdapter).
        recv_ns      : Wall-clock nanosecond timestamp of message receipt.
        now_dt       : Current datetime (timezone-aware, used for session check).
        session_guard: ``SessionGuard`` instance from ``RiskGuard``.
        """
        self._update_event_rate(recv_ns)

        # Priority 1: invalid or stale quote
        if not snapshot.valid or (recv_ns - snapshot.ts_ns) > self._stale_ns:
            self._state = MarketState.ABNORMAL
            return self._state

        # Priority 2: outside trading windows
        if not session_guard.is_close_allowed(now_dt):
            self._state = MarketState.ABNORMAL
            return self._state

        # Priority 3: pathological event rate (frozen or ultra-noisy)
        rate = self._event_rate
        if rate > 0 and (rate > self._rate_high or rate < self._rate_freeze):
            self._state = MarketState.ABNORMAL
            return self._state

        # Priority 4: compressed spread → QUEUE mode
        if snapshot.spread < self._queue_spread:
            self._state = MarketState.QUEUE
            return self._state

        # Default
        self._state = MarketState.NORMAL
        return self._state

    @property
    def state(self) -> MarketState:
        return self._state

    @property
    def event_rate(self) -> float:
        """Exponential moving average of board events per second."""
        return self._event_rate

    def state_gate(self) -> float:
        """
        Multiplicative gate for composite alpha.

        Returns 1.0 (NORMAL), 0.6 (QUEUE), or 0.0 (ABNORMAL).
        """
        return _STATE_GATE[self._state]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _update_event_rate(self, recv_ns: int) -> None:
        if self._last_recv_ns > 0:
            dt_ns = recv_ns - self._last_recv_ns
            if dt_ns > 0:
                instant_rate = 1_000_000_000.0 / dt_ns
                if self._event_rate == 0.0:
                    self._event_rate = instant_rate
                else:
                    self._event_rate = (
                        self._alpha * instant_rate
                        + (1.0 - self._alpha) * self._event_rate
                    )
        self._last_recv_ns = recv_ns
