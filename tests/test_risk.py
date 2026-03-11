import time
import unittest
from datetime import datetime, timedelta, timezone

from kabu_hft.gateway import BoardSnapshot, Level
from kabu_hft.risk import RiskGuard

JST = timezone(timedelta(hours=9))


def _make_guard(*, stale_quote_ms: int = 1500, daily_loss_limit: float = -10_000) -> RiskGuard:
    return RiskGuard(
        base_qty=100,
        max_qty=300,
        max_inventory_qty=300,
        max_notional=3_000_000,
        daily_loss_limit=daily_loss_limit,
        consecutive_loss_limit=3,
        cooling_seconds=60,
        max_hold_seconds=120,
        max_spread_ticks=3.0,
        stale_quote_ms=stale_quote_ms,
        tick_size=1.0,
        allow_short=False,
        entry_threshold=0.4,
    )


def _make_snapshot(ts_ns: int) -> BoardSnapshot:
    return BoardSnapshot(
        symbol="9984",
        exchange=1,
        ts_ns=ts_ns,
        bid=100.0,
        ask=101.0,
        bid_size=500,
        ask_size=500,
        last=100.0,
        last_size=100,
        volume=1000,
        vwap=100.5,
        bids=(Level(price=100.0, size=500),),
        asks=(Level(price=101.0, size=500),),
    )


class RiskGuardTests(unittest.TestCase):
    def test_can_open_blocks_stale_quote(self) -> None:
        guard = _make_guard(stale_quote_ms=100)
        now_ns = time.time_ns()
        snapshot = _make_snapshot(now_ns - 200_000_000)
        allowed, reason = guard.can_open(
            snapshot=snapshot,
            direction=1,
            signal_strength=1.0,
            inventory_qty=0,
            now_ns=now_ns,
            now_dt=datetime(2026, 3, 11, 9, 30, tzinfo=JST),
        )
        self.assertFalse(allowed)
        self.assertEqual(reason, "stale_quote")

    def test_can_open_blocks_midday(self) -> None:
        guard = _make_guard()
        now_ns = time.time_ns()
        snapshot = _make_snapshot(now_ns)
        allowed, reason = guard.can_open(
            snapshot=snapshot,
            direction=1,
            signal_strength=1.0,
            inventory_qty=0,
            now_ns=now_ns,
            now_dt=datetime(2026, 3, 11, 11, 40, tzinfo=JST),
        )
        self.assertFalse(allowed)
        self.assertEqual(reason, "outside_open_session")

    def test_close_only_window_before_break(self) -> None:
        guard = _make_guard()
        now_ns = time.time_ns()
        snapshot = _make_snapshot(now_ns)

        can_open, reason = guard.can_open(
            snapshot=snapshot,
            direction=1,
            signal_strength=1.0,
            inventory_qty=0,
            now_ns=now_ns,
            now_dt=datetime(2026, 3, 11, 11, 27, tzinfo=JST),
        )
        self.assertFalse(can_open)
        self.assertEqual(reason, "outside_open_session")

        must_close, close_reason = guard.must_close(
            open_ts_ns=now_ns - 1_000_000,
            snapshot=snapshot,
            now_ns=now_ns,
            now_dt=datetime(2026, 3, 11, 11, 27, tzinfo=JST),
        )
        self.assertFalse(must_close)
        self.assertEqual(close_reason, "")

        must_close_after, close_reason_after = guard.must_close(
            open_ts_ns=now_ns - 1_000_000,
            snapshot=snapshot,
            now_ns=now_ns,
            now_dt=datetime(2026, 3, 11, 11, 31, tzinfo=JST),
        )
        self.assertTrue(must_close_after)
        self.assertEqual(close_reason_after, "session_end")

    def test_daily_loss_limit_blocks_new_entry(self) -> None:
        guard = _make_guard(daily_loss_limit=-50)
        now_ns = time.time_ns()
        snapshot = _make_snapshot(now_ns)
        guard.record_trade(
            symbol="9984",
            side=1,
            qty=100,
            entry_price=100.0,
            exit_price=99.0,
            entry_ts_ns=now_ns - 2_000_000_000,
            exit_ts_ns=now_ns - 1_000_000_000,
            commission=0.0,
        )
        allowed, reason = guard.can_open(
            snapshot=snapshot,
            direction=1,
            signal_strength=1.0,
            inventory_qty=0,
            now_ns=now_ns,
            now_dt=datetime(2026, 3, 11, 9, 30, tzinfo=JST),
        )
        self.assertFalse(allowed)
        self.assertEqual(reason, "daily_loss_limit")


if __name__ == "__main__":
    unittest.main()
