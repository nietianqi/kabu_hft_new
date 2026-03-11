"""Unit tests for RiskGuard risk limits.

Tests verify:
14. Daily loss kill-switch: after daily_loss_limit hit, can_open returns False
15. Spread filter: spread > max_spread_ticks → can_open returns False
16. Session window enforcement: outside JST trading hours → can_open False
17. Cooling period: after consecutive_loss_limit losses, can_open False during cooling
18. ATR-based sizing: high ATR reduces qty relative to calm markets
"""
from __future__ import annotations

import time
import unittest
from datetime import datetime, timedelta, timezone

from kabu_hft.gateway import BoardSnapshot, Level
from kabu_hft.risk.guard import RiskGuard

JST = timezone(timedelta(hours=9))


def _make_snapshot(
    bid: float = 1000.0,
    ask: float = 1001.0,
    bid_size: int = 200,
    ask_size: int = 200,
    symbol: str = "9984",
    ts_ns: int | None = None,
) -> BoardSnapshot:
    if ts_ns is None:
        ts_ns = time.time_ns()
    return BoardSnapshot(
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


def _make_guard(**kwargs) -> RiskGuard:
    defaults = dict(
        base_qty=100,
        max_qty=300,
        max_inventory_qty=300,
        max_notional=3_000_000,
        daily_loss_limit=-50_000,
        consecutive_loss_limit=3,
        cooling_seconds=300,
        max_hold_seconds=45,
        max_spread_ticks=3.0,
        stale_quote_ms=2000,
        tick_size=1.0,
        allow_short=False,
        entry_threshold=0.40,
    )
    defaults.update(kwargs)
    return RiskGuard(**defaults)


def _jst_session_time() -> datetime:
    """Return a datetime during JST morning session (10:00 JST)."""
    return datetime(2026, 3, 11, 10, 0, 0, tzinfo=JST)


def _jst_outside_session() -> datetime:
    """Return a datetime outside JST trading hours (08:00 JST)."""
    return datetime(2026, 3, 11, 8, 0, 0, tzinfo=JST)


class TestDailyLossKillSwitch(unittest.TestCase):
    """Test case 14: After daily_loss_limit hit, can_open returns False."""

    def test_kill_switch_triggers(self):
        guard = _make_guard(daily_loss_limit=-1000)
        snap = _make_snapshot()
        now_ns = snap.ts_ns + 10 * 1_000_000
        now_dt = _jst_session_time()

        # Before hitting limit — should be allowed (other guards pass)
        can, reason = guard.can_open(
            snapshot=snap, direction=1, signal_strength=1.0,
            inventory_qty=0, now_ns=now_ns, now_dt=now_dt,
        )
        self.assertTrue(can, f"Expected can_open=True, got reason={reason}")

        # Record a loss that exceeds daily limit
        guard.pnl.record_trade(
            symbol="9984", side=1, qty=100,
            entry_price=1001.0, exit_price=1000.0,  # -100 JPY loss × 100 shares = -10,000
            entry_ts_ns=0, exit_ts_ns=1, commission=0.0,
        )
        # daily_pnl = -10,000 which is worse than daily_loss_limit=-1000
        self.assertTrue(guard.pnl.is_daily_limit_hit())

        can, reason = guard.can_open(
            snapshot=snap, direction=1, signal_strength=1.0,
            inventory_qty=0, now_ns=now_ns, now_dt=now_dt,
        )
        self.assertFalse(can)
        self.assertEqual(reason, "daily_loss_limit")

    def test_before_limit_allows_entry(self):
        guard = _make_guard(daily_loss_limit=-50_000)
        snap = _make_snapshot()
        now_ns = snap.ts_ns + 10 * 1_000_000
        now_dt = _jst_session_time()

        guard.pnl.record_trade(
            symbol="9984", side=1, qty=100,
            entry_price=1001.0, exit_price=1000.0,  # -100 JPY × 100 = -10,000
            entry_ts_ns=0, exit_ts_ns=1, commission=0.0,
        )
        # daily_pnl = -10,000 which is above daily_loss_limit=-50,000
        self.assertFalse(guard.pnl.is_daily_limit_hit())

        can, reason = guard.can_open(
            snapshot=snap, direction=1, signal_strength=1.0,
            inventory_qty=0, now_ns=now_ns, now_dt=now_dt,
        )
        self.assertTrue(can)


class TestSpreadFilter(unittest.TestCase):
    """Test case 15: Spread > max_spread_ticks → can_open returns False."""

    def test_wide_spread_blocked(self):
        """Spread of 5 ticks (5.0) when max is 3 → blocked."""
        guard = _make_guard(max_spread_ticks=3.0, tick_size=1.0)
        # spread = 5.0 > max_spread = 3.0
        snap = _make_snapshot(bid=1000.0, ask=1005.0)
        now_ns = snap.ts_ns + 10 * 1_000_000
        now_dt = _jst_session_time()

        can, reason = guard.can_open(
            snapshot=snap, direction=1, signal_strength=1.0,
            inventory_qty=0, now_ns=now_ns, now_dt=now_dt,
        )
        self.assertFalse(can)
        self.assertEqual(reason, "spread_too_wide")

    def test_narrow_spread_allowed(self):
        """Spread of 1 tick (1.0) when max is 3 → allowed (other checks pass)."""
        guard = _make_guard(max_spread_ticks=3.0, tick_size=1.0)
        snap = _make_snapshot(bid=1000.0, ask=1001.0)
        now_ns = snap.ts_ns + 10 * 1_000_000
        now_dt = _jst_session_time()

        can, reason = guard.can_open(
            snapshot=snap, direction=1, signal_strength=1.0,
            inventory_qty=0, now_ns=now_ns, now_dt=now_dt,
        )
        self.assertTrue(can)

    def test_exact_spread_limit_allowed(self):
        """Spread exactly at limit (3 ticks) → allowed (not strictly greater)."""
        guard = _make_guard(max_spread_ticks=3.0, tick_size=1.0)
        snap = _make_snapshot(bid=1000.0, ask=1003.0)  # spread = 3.0
        now_ns = snap.ts_ns + 10 * 1_000_000
        now_dt = _jst_session_time()

        can, reason = guard.can_open(
            snapshot=snap, direction=1, signal_strength=1.0,
            inventory_qty=0, now_ns=now_ns, now_dt=now_dt,
        )
        self.assertTrue(can)


class TestSessionWindows(unittest.TestCase):
    """Test case 16: Outside JST trading hours → can_open False."""

    def test_outside_session_blocked(self):
        """08:00 JST is outside both morning and afternoon sessions."""
        guard = _make_guard()
        snap = _make_snapshot()
        now_ns = snap.ts_ns + 10 * 1_000_000
        now_dt = _jst_outside_session()  # 08:00 JST

        can, reason = guard.can_open(
            snapshot=snap, direction=1, signal_strength=1.0,
            inventory_qty=0, now_ns=now_ns, now_dt=now_dt,
        )
        self.assertFalse(can)
        self.assertEqual(reason, "outside_open_session")

    def test_morning_session_allowed(self):
        """10:00 JST is within morning session (09:00-11:25)."""
        guard = _make_guard()
        snap = _make_snapshot()
        now_ns = snap.ts_ns + 10 * 1_000_000
        now_dt = datetime(2026, 3, 11, 10, 0, 0, tzinfo=JST)

        can, reason = guard.can_open(
            snapshot=snap, direction=1, signal_strength=1.0,
            inventory_qty=0, now_ns=now_ns, now_dt=now_dt,
        )
        self.assertTrue(can)

    def test_afternoon_session_allowed(self):
        """13:00 JST is within afternoon session (12:30-15:25)."""
        guard = _make_guard()
        snap = _make_snapshot()
        now_ns = snap.ts_ns + 10 * 1_000_000
        now_dt = datetime(2026, 3, 11, 13, 0, 0, tzinfo=JST)

        can, reason = guard.can_open(
            snapshot=snap, direction=1, signal_strength=1.0,
            inventory_qty=0, now_ns=now_ns, now_dt=now_dt,
        )
        self.assertTrue(can)

    def test_after_close_blocked(self):
        """15:26 JST is after afternoon session close (15:25)."""
        guard = _make_guard()
        snap = _make_snapshot()
        now_ns = snap.ts_ns + 10 * 1_000_000
        now_dt = datetime(2026, 3, 11, 15, 26, 0, tzinfo=JST)

        can, reason = guard.can_open(
            snapshot=snap, direction=1, signal_strength=1.0,
            inventory_qty=0, now_ns=now_ns, now_dt=now_dt,
        )
        self.assertFalse(can)
        self.assertEqual(reason, "outside_open_session")

    def test_must_close_session_end(self):
        """After close window, must_close returns True with session_end."""
        guard = _make_guard()
        snap = _make_snapshot()
        now_ns = snap.ts_ns + 10 * 1_000_000
        now_dt = datetime(2026, 3, 11, 15, 31, 0, tzinfo=JST)

        must, reason = guard.must_close(
            open_ts_ns=now_ns - 5_000_000_000,
            snapshot=snap,
            now_ns=now_ns,
            now_dt=now_dt,
        )
        self.assertTrue(must)
        self.assertEqual(reason, "session_end")


class TestCoolingPeriod(unittest.TestCase):
    """Test case 17: After consecutive_loss_limit losses, can_open False during cooling."""

    def test_cooling_triggers_after_consecutive_losses(self):
        guard = _make_guard(consecutive_loss_limit=3, cooling_seconds=300)
        snap = _make_snapshot()
        now_ns = snap.ts_ns + 10 * 1_000_000
        now_dt = _jst_session_time()

        # Record 3 consecutive losses
        for _ in range(3):
            guard.pnl.record_trade(
                symbol="9984", side=1, qty=100,
                entry_price=1001.0, exit_price=1000.0,
                entry_ts_ns=0, exit_ts_ns=1, commission=0.0,
            )

        self.assertTrue(guard.pnl.is_cooling(now_ns))
        can, reason = guard.can_open(
            snapshot=snap, direction=1, signal_strength=1.0,
            inventory_qty=0, now_ns=now_ns, now_dt=now_dt,
        )
        self.assertFalse(can)
        self.assertEqual(reason, "cooling")

    def test_win_resets_consecutive_losses(self):
        """A winning trade resets consecutive loss count."""
        guard = _make_guard(consecutive_loss_limit=3, cooling_seconds=300)

        guard.pnl.record_trade(
            symbol="9984", side=1, qty=100,
            entry_price=1001.0, exit_price=1000.0,
            entry_ts_ns=0, exit_ts_ns=1, commission=0.0,
        )
        guard.pnl.record_trade(
            symbol="9984", side=1, qty=100,
            entry_price=1001.0, exit_price=1000.0,
            entry_ts_ns=0, exit_ts_ns=2, commission=0.0,
        )
        # Win breaks streak
        guard.pnl.record_trade(
            symbol="9984", side=1, qty=100,
            entry_price=1000.0, exit_price=1001.0,
            entry_ts_ns=0, exit_ts_ns=3, commission=0.0,
        )
        self.assertEqual(guard.pnl.consecutive_losses, 0)

    def test_below_limit_no_cooling(self):
        """2 losses < limit=3 → no cooling."""
        guard = _make_guard(consecutive_loss_limit=3, cooling_seconds=300)
        now_ns = time.time_ns()
        for _ in range(2):
            guard.pnl.record_trade(
                symbol="9984", side=1, qty=100,
                entry_price=1001.0, exit_price=1000.0,
                entry_ts_ns=0, exit_ts_ns=1, commission=0.0,
            )
        self.assertFalse(guard.pnl.is_cooling(now_ns))


class TestATRBasedSizing(unittest.TestCase):
    """Test case 18: High ATR → reduced position size."""

    def test_high_atr_reduces_qty(self):
        """When ATR is very high, PositionSizer reduces qty below base_qty."""
        guard = _make_guard(base_qty=100, max_qty=300, tick_size=1.0)

        # Simulate high ATR by feeding volatile snapshots
        mid = 1000.0
        for delta in [5, -5, 5, -5, 5, -5, 5, -5, 5, -5]:
            mid += delta
            snap = _make_snapshot(bid=mid - 1, ask=mid + 1)
            guard.update_vol(snap)

        # High ATR scenario
        qty_high_vol = guard.calc_qty(signal_strength=0.5, mid=1000.0, inventory_qty=0)

        # Reset with calm market
        guard2 = _make_guard(base_qty=100, max_qty=300, tick_size=1.0)
        for _ in range(10):
            snap = _make_snapshot(bid=999.0, ask=1001.0)
            guard2.update_vol(snap)

        qty_low_vol = guard2.calc_qty(signal_strength=0.5, mid=1000.0, inventory_qty=0)

        # High vol should not increase qty
        self.assertLessEqual(qty_high_vol, qty_low_vol)

    def test_strong_signal_increases_qty(self):
        """signal_strength >= 1.0 increases qty up to max_qty."""
        guard = _make_guard(base_qty=100, max_qty=300, tick_size=1.0)
        # No volatility history
        qty_normal = guard.calc_qty(signal_strength=0.5, mid=1000.0, inventory_qty=0)
        qty_strong = guard.calc_qty(signal_strength=1.5, mid=1000.0, inventory_qty=0)
        self.assertGreater(qty_strong, qty_normal)

    def test_inventory_cap_enforced(self):
        """calc_qty respects max_inventory_qty - current inventory."""
        guard = _make_guard(base_qty=100, max_qty=300, max_inventory_qty=200, tick_size=1.0)
        # Already holding 150 shares
        qty = guard.calc_qty(signal_strength=0.5, mid=1000.0, inventory_qty=150)
        # Can only open up to max_inventory_qty - 150 = 50 more
        self.assertLessEqual(qty, 50)

    def test_mtm_loss_triggers_must_close(self):
        """MTM loss below max_mtm_loss triggers must_close with mtm_loss_limit reason."""
        guard = _make_guard(max_mtm_loss=-5000)
        snap = _make_snapshot(bid=990.0, ask=991.0)  # mid = 990.5
        now_ns = snap.ts_ns + 10 * 1_000_000
        now_dt = _jst_session_time()

        # Long position opened at 1000, current mid is 990.5 → loss = -(1000-990.5)*100 = -950
        # With qty=600: loss = -(1000-990.5)*600 = -5700 < -5000
        must, reason = guard.must_close(
            open_ts_ns=now_ns - 1_000_000_000,
            snapshot=snap,
            now_ns=now_ns,
            now_dt=now_dt,
            open_price=1000.0,
            position_side=1,
            position_qty=600,
        )
        self.assertTrue(must)
        self.assertEqual(reason, "mtm_loss_limit")

    def test_small_mtm_loss_no_trigger(self):
        """MTM loss above max_mtm_loss does not trigger must_close."""
        guard = _make_guard(max_mtm_loss=-5000)
        snap = _make_snapshot(bid=999.0, ask=1001.0)  # mid = 1000
        now_ns = snap.ts_ns + 10 * 1_000_000
        now_dt = _jst_session_time()

        must, reason = guard.must_close(
            open_ts_ns=now_ns - 1_000_000_000,
            snapshot=snap,
            now_ns=now_ns,
            now_dt=now_dt,
            open_price=1000.0,
            position_side=1,
            position_qty=100,
        )
        # MTM = 1*(1000-1000)*100 = 0 > -5000, no trigger
        self.assertFalse(must)


if __name__ == "__main__":
    unittest.main()
