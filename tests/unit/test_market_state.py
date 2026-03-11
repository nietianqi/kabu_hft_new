"""Unit tests for MarketStateDetector (5 tests).

Tests:
1. NORMAL state: spread=3 ticks, moderate event rate, fresh quote, in session → NORMAL
2. QUEUE state: spread=1 tick → QUEUE
3. ABNORMAL: stale quote (now_ns - snap.ts_ns > stale_ms*1e6)
4. ABNORMAL: outside session (outside JST trading windows)
5. state_gate values: NORMAL→1.0, QUEUE→0.6, ABNORMAL→0.0
"""
from __future__ import annotations

import time
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from kabu_hft.gateway import BoardSnapshot, Level
from kabu_hft.signals.market_state import MarketState, MarketStateDetector

JST = timezone(timedelta(hours=9))


def _make_snapshot(
    bid: float = 1000.0,
    ask: float = 1150.0,   # spread = 150 = 3 ticks (tick_size=50)
    bid_size: int = 200,
    ask_size: int = 200,
    symbol: str = "9984",
    ts_ns: int | None = None,
    valid_override: bool | None = None,
) -> BoardSnapshot:
    if ts_ns is None:
        ts_ns = time.time_ns()
    snap = BoardSnapshot(
        symbol=symbol,
        exchange=1,
        ts_ns=ts_ns,
        bid=bid,
        ask=ask,
        bid_size=bid_size,
        ask_size=ask_size,
        last=0.0,
        last_size=0,
        volume=0,
        vwap=0.0,
        bids=(Level(price=bid, size=bid_size),),
        asks=(Level(price=ask, size=ask_size),),
        prev_board=None,
    )
    if valid_override is not None:
        # Patch the `valid` property via a mock wrapper
        m = MagicMock(wraps=snap)
        type(m).valid = MagicMock(return_value=valid_override)
        # Use a simple object to avoid wraps issues
        class _Wrapper:
            def __init__(self, inner: BoardSnapshot, v: bool) -> None:
                self.__dict__.update(inner.__dict__)
                self._valid = v
            @property
            def valid(self) -> bool:
                return self._valid
            def __getattr__(self, item):
                return getattr(snap, item)
        return _Wrapper(snap, valid_override)  # type: ignore[return-value]
    return snap


def _make_session_guard(in_session: bool = True) -> MagicMock:
    sg = MagicMock()
    sg.is_close_allowed.return_value = in_session
    return sg


def _make_detector(
    queue_spread_ticks: float = 1.5,
    tick_size: float = 50.0,
    event_rate_high: float = 100.0,
    event_rate_freeze: float = 0.5,
    stale_quote_ms: int = 1200,
) -> MarketStateDetector:
    return MarketStateDetector(
        queue_spread_ticks=queue_spread_ticks,
        event_rate_high=event_rate_high,
        event_rate_freeze=event_rate_freeze,
        stale_quote_ms=stale_quote_ms,
        tick_size=tick_size,
    )


class TestMarketStateNormal(unittest.TestCase):
    """Test 1: spread=3 ticks, moderate event rate, fresh quote, in session → NORMAL."""

    def test_normal_state_fresh_quote_in_session(self) -> None:
        detector = _make_detector()  # queue_spread_ticks=1.5, tick_size=50 → threshold=75
        # spread = ask - bid = 1150 - 1000 = 150 (3 ticks) → above threshold → not QUEUE
        snap = _make_snapshot(bid=1000.0, ask=1150.0)  # spread=150

        now_ns = time.time_ns()
        # Two consecutive calls so event_rate is seeded to a moderate value (~10 events/s)
        dt_100ms = 100_000_000  # 100 ms → 10 events/sec
        snap1 = _make_snapshot(bid=1000.0, ask=1150.0, ts_ns=now_ns - dt_100ms)
        snap2 = _make_snapshot(bid=1000.0, ask=1150.0, ts_ns=now_ns)

        sg = _make_session_guard(in_session=True)

        detector.update(snap1, now_ns - dt_100ms, datetime.now(JST), sg)
        state = detector.update(snap2, now_ns, datetime.now(JST), sg)

        self.assertEqual(state, MarketState.NORMAL)
        self.assertEqual(detector.state, MarketState.NORMAL)


class TestMarketStateQueue(unittest.TestCase):
    """Test 2: spread=1 tick → QUEUE."""

    def test_queue_state_tight_spread(self) -> None:
        detector = _make_detector()  # threshold = 1.5 * 50 = 75
        # spread = ask - bid = 1050 - 1000 = 50 = 1 tick (< 75 threshold)
        snap = _make_snapshot(bid=1000.0, ask=1050.0)

        now_ns = time.time_ns()
        dt = 100_000_000
        snap1 = _make_snapshot(bid=1000.0, ask=1050.0, ts_ns=now_ns - dt)
        snap2 = _make_snapshot(bid=1000.0, ask=1050.0, ts_ns=now_ns)

        sg = _make_session_guard(in_session=True)
        detector.update(snap1, now_ns - dt, datetime.now(JST), sg)
        state = detector.update(snap2, now_ns, datetime.now(JST), sg)

        self.assertEqual(state, MarketState.QUEUE)
        self.assertEqual(detector.state, MarketState.QUEUE)

    def test_normal_spread_2_ticks_not_queue(self) -> None:
        """spread = 2 ticks (100) > threshold (75) → NORMAL, not QUEUE."""
        detector = _make_detector()
        now_ns = time.time_ns()
        dt = 100_000_000
        snap1 = _make_snapshot(bid=1000.0, ask=1100.0, ts_ns=now_ns - dt)
        snap2 = _make_snapshot(bid=1000.0, ask=1100.0, ts_ns=now_ns)
        sg = _make_session_guard(in_session=True)
        detector.update(snap1, now_ns - dt, datetime.now(JST), sg)
        state = detector.update(snap2, now_ns, datetime.now(JST), sg)
        self.assertEqual(state, MarketState.NORMAL)


class TestMarketStateAbnormalStale(unittest.TestCase):
    """Test 3: ABNORMAL when quote is stale."""

    def test_stale_quote_triggers_abnormal(self) -> None:
        detector = _make_detector(stale_quote_ms=1200)
        # ts_ns is 2 seconds old → stale (> 1200 ms)
        stale_ts_ns = time.time_ns() - 2_000_000_000
        snap = _make_snapshot(bid=1000.0, ask=1150.0, ts_ns=stale_ts_ns)

        now_ns = time.time_ns()
        sg = _make_session_guard(in_session=True)
        state = detector.update(snap, now_ns, datetime.now(JST), sg)

        self.assertEqual(state, MarketState.ABNORMAL)

    def test_fresh_quote_not_stale(self) -> None:
        """Quote 200 ms old is not stale (< 1200 ms threshold)."""
        detector = _make_detector(stale_quote_ms=1200)
        now_ns = time.time_ns()
        ts_200ms = now_ns - 200_000_000
        snap1 = _make_snapshot(bid=1000.0, ask=1150.0, ts_ns=ts_200ms - 100_000_000)
        snap2 = _make_snapshot(bid=1000.0, ask=1150.0, ts_ns=ts_200ms)
        sg = _make_session_guard(in_session=True)
        detector.update(snap1, now_ns - 100_000_000, datetime.now(JST), sg)
        state = detector.update(snap2, now_ns, datetime.now(JST), sg)
        self.assertNotEqual(state, MarketState.ABNORMAL)


class TestMarketStateAbnormalOutsideSession(unittest.TestCase):
    """Test 4: ABNORMAL when outside JST trading windows."""

    def test_outside_session_triggers_abnormal(self) -> None:
        detector = _make_detector()
        snap = _make_snapshot(bid=1000.0, ask=1150.0)  # fresh, normal spread

        now_ns = time.time_ns()
        sg = _make_session_guard(in_session=False)  # session_guard says we're outside
        state = detector.update(snap, now_ns, datetime.now(JST), sg)

        self.assertEqual(state, MarketState.ABNORMAL)

    def test_inside_session_not_abnormal_due_to_session(self) -> None:
        """In-session guard does not force ABNORMAL."""
        detector = _make_detector()
        now_ns = time.time_ns()
        dt = 100_000_000
        snap1 = _make_snapshot(bid=1000.0, ask=1150.0, ts_ns=now_ns - dt)
        snap2 = _make_snapshot(bid=1000.0, ask=1150.0, ts_ns=now_ns)
        sg = _make_session_guard(in_session=True)
        detector.update(snap1, now_ns - dt, datetime.now(JST), sg)
        state = detector.update(snap2, now_ns, datetime.now(JST), sg)
        # Should be NORMAL (spread 3 ticks, fresh, in session)
        self.assertEqual(state, MarketState.NORMAL)


class TestMarketStateGate(unittest.TestCase):
    """Test 5: state_gate() returns correct multipliers for each state."""

    def _force_state(self, detector: MarketStateDetector, target: MarketState) -> None:
        """Drive detector into the requested state by manipulating inputs."""
        now_ns = time.time_ns()
        sg_in = _make_session_guard(in_session=True)
        sg_out = _make_session_guard(in_session=False)

        if target is MarketState.NORMAL:
            dt = 100_000_000  # 10 events/sec (moderate)
            snap1 = _make_snapshot(bid=1000.0, ask=1150.0, ts_ns=now_ns - dt)
            snap2 = _make_snapshot(bid=1000.0, ask=1150.0, ts_ns=now_ns)
            detector.update(snap1, now_ns - dt, datetime.now(JST), sg_in)
            detector.update(snap2, now_ns, datetime.now(JST), sg_in)
        elif target is MarketState.QUEUE:
            dt = 100_000_000
            snap1 = _make_snapshot(bid=1000.0, ask=1050.0, ts_ns=now_ns - dt)
            snap2 = _make_snapshot(bid=1000.0, ask=1050.0, ts_ns=now_ns)
            detector.update(snap1, now_ns - dt, datetime.now(JST), sg_in)
            detector.update(snap2, now_ns, datetime.now(JST), sg_in)
        elif target is MarketState.ABNORMAL:
            snap = _make_snapshot(bid=1000.0, ask=1150.0)
            detector.update(snap, now_ns, datetime.now(JST), sg_out)

    def test_normal_gate_is_1_0(self) -> None:
        detector = _make_detector()
        self._force_state(detector, MarketState.NORMAL)
        self.assertEqual(detector.state, MarketState.NORMAL)
        self.assertAlmostEqual(detector.state_gate(), 1.0)

    def test_queue_gate_is_0_6(self) -> None:
        detector = _make_detector()
        self._force_state(detector, MarketState.QUEUE)
        self.assertEqual(detector.state, MarketState.QUEUE)
        self.assertAlmostEqual(detector.state_gate(), 0.6)

    def test_abnormal_gate_is_0_0(self) -> None:
        detector = _make_detector()
        self._force_state(detector, MarketState.ABNORMAL)
        self.assertEqual(detector.state, MarketState.ABNORMAL)
        self.assertAlmostEqual(detector.state_gate(), 0.0)

    def test_gate_suppresses_composite(self) -> None:
        """Applying state_gate() to a composite score produces expected scaled value."""
        detector = _make_detector()
        composite = 0.8

        self._force_state(detector, MarketState.NORMAL)
        self.assertAlmostEqual(detector.state_gate() * composite, 0.8)

        detector2 = _make_detector()
        self._force_state(detector2, MarketState.QUEUE)
        self.assertAlmostEqual(detector2.state_gate() * composite, 0.48, places=5)

        detector3 = _make_detector()
        self._force_state(detector3, MarketState.ABNORMAL)
        self.assertAlmostEqual(detector3.state_gate() * composite, 0.0)


class TestMarketStateEventRate(unittest.TestCase):
    """Extra: event rate EMA drives ABNORMAL at extreme rates."""

    def test_high_event_rate_triggers_abnormal(self) -> None:
        """Ultra-fast events (>100/sec) → ABNORMAL."""
        detector = _make_detector(event_rate_high=100.0, event_rate_freeze=0.5)
        now_ns = time.time_ns()
        sg = _make_session_guard(in_session=True)

        # Seed the EMA with a series of extremely fast events (1ms apart → 1000/sec)
        dt_1ms = 1_000_000
        snap = _make_snapshot(bid=1000.0, ask=1150.0)
        base = now_ns - 20 * dt_1ms

        last_state = None
        for i in range(20):
            recv = base + i * dt_1ms
            s = _make_snapshot(bid=1000.0, ask=1150.0, ts_ns=recv)
            last_state = detector.update(s, recv, datetime.now(JST), sg)

        # After many 1ms events, rate EMA should converge well above 100
        self.assertGreater(detector.event_rate, 100.0)
        self.assertEqual(last_state, MarketState.ABNORMAL)


if __name__ == "__main__":
    unittest.main()
