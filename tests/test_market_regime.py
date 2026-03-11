import time
import unittest

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


if __name__ == "__main__":
    unittest.main()
