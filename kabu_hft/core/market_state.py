from __future__ import annotations

import logging
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum

from kabu_hft.gateway import BoardSnapshot

logger = logging.getLogger("kabu.market_state")


class MarketState(str, Enum):
    NORMAL = "NORMAL"
    QUEUE = "QUEUE"
    ABNORMAL = "ABNORMAL"


# 気配フラグ values that indicate non-normal quote states.
# 0102 = 特別気配 (special/circuit-breaker), 0103 = 注意気配 (warning),
# 0107 = 寄前気配 (pre-open).  0101 = 一般気配 (normal, not in this set).
_SPECIAL_QUOTE_SIGNS: frozenset[str] = frozenset({"0102", "0103", "0107"})


@dataclass(slots=True)
class MarketStateView:
    state: MarketState
    reason: str
    spread_ticks: float
    event_rate_hz: float
    stale_ms: float
    jump_ticks: float
    trade_lag_ms: float = field(default=0.0)
    """Milliseconds since last trade (CurrentPriceTime) vs last quote (BidTime).
    A large value (>5000ms) means no recent trades; Tape-OFI is unreliable."""


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
        event_burst_min_events: int = 6,
    ) -> None:
        self.tick_size = max(tick_size, 1e-9)
        self.stale_quote_ms = max(stale_quote_ms, 1)
        self.queue_spread_max_ticks = max(queue_spread_max_ticks, 0.0)
        self.abnormal_max_spread_ticks = max(abnormal_max_spread_ticks, 0.5)
        self.max_event_rate_hz = max(max_event_rate_hz, 1.0)
        self.state_window_ns = max(state_window_ms, 250) * 1_000_000
        self.jump_threshold_ticks = max(jump_threshold_ticks, 0.5)
        self.event_burst_min_events = max(event_burst_min_events, 2)
        self._event_times: deque[int] = deque()
        self._prev_mid = 0.0
        self._trade_drought_warn_interval_ns = 10_000_000_000  # 10 seconds
        self._last_trade_drought_warn_ns: dict[str, int] = {}

    def evaluate(self, snapshot: BoardSnapshot, now_ns: int | None = None) -> MarketStateView:
        now = now_ns if now_ns is not None else time.time_ns()
        # Use exchange timestamp for event rate to measure actual market frequency,
        # not the rate at which the strategy loop processes events (which is throttled).
        event_ts = snapshot.ts_ns if snapshot.ts_ns > 0 else now
        self._event_times.append(event_ts)
        while self._event_times and event_ts - self._event_times[0] > self.state_window_ns:
            self._event_times.popleft()

        spread_ticks = snapshot.spread / self.tick_size if snapshot.spread > 0 else 0.0
        stale_ms = max(0.0, (now - snapshot.ts_ns) / 1_000_000)
        stale = stale_ms > self.stale_quote_ms

        # How long since the last executed trade vs the most recent bid/ask update.
        # Large trade_lag means no recent transactions: Tape-OFI signal is unreliable.
        quote_ts = max(snapshot.bid_ts_ns, snapshot.ask_ts_ns)
        trade_lag_ms = (
            max(0.0, (quote_ts - snapshot.current_ts_ns) / 1_000_000)
            if quote_ts > 0 and snapshot.current_ts_ns > 0
            else 0.0
        )
        if trade_lag_ms > 5_000:
            symbol = snapshot.symbol
            last_warn_ns = self._last_trade_drought_warn_ns.get(symbol, 0)
            if now - last_warn_ns >= self._trade_drought_warn_interval_ns:
                logger.warning(
                    "trade drought symbol=%s ts_lag=%.0fms: no recent trades, Tape-OFI unreliable",
                    symbol,
                    trade_lag_ms,
                )
                self._last_trade_drought_warn_ns[symbol] = now

        jump_ticks = 0.0
        if self._prev_mid > 0.0:
            jump_ticks = abs(snapshot.mid - self._prev_mid) / self.tick_size
        self._prev_mid = snapshot.mid

        event_rate_hz = self._event_rate_hz(event_ts)

        if not snapshot.valid:
            return MarketStateView(
                state=MarketState.ABNORMAL,
                reason="invalid_quote",
                spread_ticks=spread_ticks,
                event_rate_hz=event_rate_hz,
                stale_ms=stale_ms,
                jump_ticks=jump_ticks,
                trade_lag_ms=trade_lag_ms,
            )
        if stale:
            return MarketStateView(
                state=MarketState.ABNORMAL,
                reason="stale_quote",
                spread_ticks=spread_ticks,
                event_rate_hz=event_rate_hz,
                stale_ms=stale_ms,
                jump_ticks=jump_ticks,
                trade_lag_ms=trade_lag_ms,
            )
        if snapshot.bid_sign in _SPECIAL_QUOTE_SIGNS or snapshot.ask_sign in _SPECIAL_QUOTE_SIGNS:
            return MarketStateView(
                state=MarketState.ABNORMAL,
                reason="special_quote_sign",
                spread_ticks=spread_ticks,
                event_rate_hz=event_rate_hz,
                stale_ms=stale_ms,
                jump_ticks=jump_ticks,
                trade_lag_ms=trade_lag_ms,
            )
        if spread_ticks >= self.abnormal_max_spread_ticks:
            return MarketStateView(
                state=MarketState.ABNORMAL,
                reason="spread_blowout",
                spread_ticks=spread_ticks,
                event_rate_hz=event_rate_hz,
                stale_ms=stale_ms,
                jump_ticks=jump_ticks,
                trade_lag_ms=trade_lag_ms,
            )
        if (
            len(self._event_times) >= self.event_burst_min_events
            and event_rate_hz >= self.max_event_rate_hz
        ):
            return MarketStateView(
                state=MarketState.ABNORMAL,
                reason="event_burst",
                spread_ticks=spread_ticks,
                event_rate_hz=event_rate_hz,
                stale_ms=stale_ms,
                jump_ticks=jump_ticks,
                trade_lag_ms=trade_lag_ms,
            )
        if jump_ticks >= self.jump_threshold_ticks:
            return MarketStateView(
                state=MarketState.ABNORMAL,
                reason="price_jump",
                spread_ticks=spread_ticks,
                event_rate_hz=event_rate_hz,
                stale_ms=stale_ms,
                jump_ticks=jump_ticks,
                trade_lag_ms=trade_lag_ms,
            )
        if spread_ticks <= self.queue_spread_max_ticks + 1e-9:
            return MarketStateView(
                state=MarketState.QUEUE,
                reason="one_tick_queue",
                spread_ticks=spread_ticks,
                event_rate_hz=event_rate_hz,
                stale_ms=stale_ms,
                jump_ticks=jump_ticks,
                trade_lag_ms=trade_lag_ms,
            )
        return MarketStateView(
            state=MarketState.NORMAL,
            reason="normal_flow",
            spread_ticks=spread_ticks,
            event_rate_hz=event_rate_hz,
            stale_ms=stale_ms,
            jump_ticks=jump_ticks,
            trade_lag_ms=trade_lag_ms,
        )

    def _event_rate_hz(self, now_ns: int) -> float:
        if len(self._event_times) < 2:
            return 0.0
        duration_ns = max(now_ns - self._event_times[0], 1)
        # N events contain (N-1) intervals.
        return (len(self._event_times) - 1) * 1_000_000_000 / duration_ns
