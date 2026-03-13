import time
import unittest
from unittest.mock import patch

from kabu_hft.core.market_state import MarketState, MarketStateDetector
from kabu_hft.gateway import BoardSnapshot, Level


def _snapshot(*, bid: float, ask: float, ts_ns: int) -> BoardSnapshot:
    return BoardSnapshot(
        symbol="9984",
        exchange=1,
        ts_ns=ts_ns,
        bid=bid,
        ask=ask,
        bid_size=500,
        ask_size=600,
        last=(bid + ask) / 2.0,
        last_size=100,
        volume=1000,
        vwap=(bid + ask) / 2.0,
        bids=(Level(price=bid, size=500),),
        asks=(Level(price=ask, size=600),),
    )


class MarketRegimeTests(unittest.TestCase):
    def test_queue_state_on_one_tick_spread(self) -> None:
        detector = MarketStateDetector(
            tick_size=1.0,
            stale_quote_ms=1_000,
            queue_spread_max_ticks=1.0,
            abnormal_max_spread_ticks=6.0,
            max_event_rate_hz=200.0,
            state_window_ms=3_000,
            jump_threshold_ticks=5.0,
        )
        now_ns = time.time_ns()
        state = detector.evaluate(_snapshot(bid=100.0, ask=101.0, ts_ns=now_ns), now_ns)
        self.assertEqual(state.state, MarketState.QUEUE)

    def test_normal_state_on_wider_spread(self) -> None:
        detector = MarketStateDetector(
            tick_size=1.0,
            stale_quote_ms=1_000,
            queue_spread_max_ticks=1.0,
            abnormal_max_spread_ticks=6.0,
            max_event_rate_hz=200.0,
            state_window_ms=3_000,
            jump_threshold_ticks=5.0,
        )
        now_ns = time.time_ns()
        state = detector.evaluate(_snapshot(bid=100.0, ask=102.0, ts_ns=now_ns), now_ns)
        self.assertEqual(state.state, MarketState.NORMAL)

    def test_abnormal_state_on_stale_quote(self) -> None:
        detector = MarketStateDetector(
            tick_size=1.0,
            stale_quote_ms=100,
            queue_spread_max_ticks=1.0,
            abnormal_max_spread_ticks=6.0,
            max_event_rate_hz=200.0,
            state_window_ms=3_000,
            jump_threshold_ticks=5.0,
        )
        now_ns = time.time_ns()
        state = detector.evaluate(_snapshot(bid=100.0, ask=101.0, ts_ns=now_ns - 200_000_000), now_ns)
        self.assertEqual(state.state, MarketState.ABNORMAL)
        self.assertEqual(state.reason, "stale_quote")

    def test_event_rate_uses_intervals_not_samples(self) -> None:
        detector = MarketStateDetector(
            tick_size=1.0,
            stale_quote_ms=1_000,
            queue_spread_max_ticks=1.0,
            abnormal_max_spread_ticks=6.0,
            max_event_rate_hz=160.0,
            state_window_ms=3_000,
            jump_threshold_ticks=5.0,
            event_burst_min_events=2,
        )
        base = time.time_ns()
        detector.evaluate(_snapshot(bid=100.0, ask=101.0, ts_ns=base), now_ns=base)
        second = detector.evaluate(
            _snapshot(bid=100.0, ask=101.0, ts_ns=base + 8_000_000),
            now_ns=base + 8_000_000,
        )
        self.assertEqual(second.state, MarketState.QUEUE)

    def test_event_burst_requires_min_events(self) -> None:
        detector = MarketStateDetector(
            tick_size=1.0,
            stale_quote_ms=1_000,
            queue_spread_max_ticks=1.0,
            abnormal_max_spread_ticks=6.0,
            max_event_rate_hz=100.0,
            state_window_ms=3_000,
            jump_threshold_ticks=5.0,
            event_burst_min_events=6,
        )
        base = time.time_ns()
        for index in range(5):
            state = detector.evaluate(
                _snapshot(bid=100.0, ask=101.0, ts_ns=base + index * 1_000_000),
                now_ns=base + index * 1_000_000,
            )
            self.assertEqual(state.state, MarketState.QUEUE)

        burst = detector.evaluate(
            _snapshot(bid=100.0, ask=101.0, ts_ns=base + 5_000_000),
            now_ns=base + 5_000_000,
        )
        self.assertEqual(burst.state, MarketState.ABNORMAL)
        self.assertEqual(burst.reason, "event_burst")

    def test_trade_drought_warning_is_throttled(self) -> None:
        detector = MarketStateDetector(
            tick_size=1.0,
            stale_quote_ms=5_000,
            queue_spread_max_ticks=1.0,
            abnormal_max_spread_ticks=6.0,
            max_event_rate_hz=200.0,
            state_window_ms=3_000,
            jump_threshold_ticks=5.0,
        )
        base = time.time_ns()

        def _drought_snapshot(ts_ns: int, quote_ts_ns: int, trade_ts_ns: int) -> BoardSnapshot:
            snapshot = _snapshot(bid=100.0, ask=101.0, ts_ns=ts_ns)
            snapshot.bid_ts_ns = quote_ts_ns
            snapshot.ask_ts_ns = quote_ts_ns
            snapshot.current_ts_ns = trade_ts_ns
            return snapshot

        with patch("kabu_hft.core.market_state.logger.warning") as mock_warn:
            detector.evaluate(
                _drought_snapshot(base, base, base - 10_000_000_000),
                now_ns=base,
            )
            detector.evaluate(
                _drought_snapshot(base + 1_000_000_000, base + 1_000_000_000, base - 9_000_000_000),
                now_ns=base + 1_000_000_000,
            )
            detector.evaluate(
                _drought_snapshot(base + 11_000_000_000, base + 11_000_000_000, base + 1_000_000_000),
                now_ns=base + 11_000_000_000,
            )

        self.assertEqual(mock_warn.call_count, 2)


if __name__ == "__main__":
    unittest.main()
