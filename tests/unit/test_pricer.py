"""Unit tests for ReservationPricer (6 tests + extras).

Tests:
1. fair_value formula: mid=1000, composite=1.0, beta=0.5, tick=50 → fair=1025
2. reservation with neutral inventory (side=0, qty=0) → reservation = fair
3. reservation with light inventory skew (|frac| < 0.3)
4. reservation with heavy inventory skew (|frac| > 0.6)
5. half_spread base case: zero ATR, low event rate, NORMAL → base_half_spread * tick
6. half_spread scales with ATR: high ATR → wider half_spread than base
"""
from __future__ import annotations

import unittest

from kabu_hft.core.pricer import ReservationPricer
from kabu_hft.signals.market_state import MarketState


def _make_pricer(
    fair_value_beta: float = 0.5,
    base_half_spread_ticks: float = 0.5,
    inv_skew_medium_threshold: float = 0.3,
    inv_skew_heavy_threshold: float = 0.6,
    inv_skew_light: float = 0.5,
    inv_skew_medium: float = 1.0,
    inv_skew_heavy: float = 2.0,
    tick_size: float = 50.0,
    max_inventory_qty: int = 300,
) -> ReservationPricer:
    return ReservationPricer(
        fair_value_beta=fair_value_beta,
        base_half_spread_ticks=base_half_spread_ticks,
        inv_skew_medium_threshold=inv_skew_medium_threshold,
        inv_skew_heavy_threshold=inv_skew_heavy_threshold,
        inv_skew_light=inv_skew_light,
        inv_skew_medium=inv_skew_medium,
        inv_skew_heavy=inv_skew_heavy,
        tick_size=tick_size,
        max_inventory_qty=max_inventory_qty,
    )


class TestFairValue(unittest.TestCase):
    """Test 1: fair_value formula."""

    def test_fair_value_formula(self) -> None:
        """mid=1000, composite=1.0, beta=0.5, tick=50 → fair = 1000 + 0.5*1.0*50 = 1025."""
        pricer = _make_pricer(fair_value_beta=0.5, tick_size=50.0)
        fair = pricer.fair_value(mid=1000.0, composite_z=1.0)
        self.assertAlmostEqual(fair, 1025.0)

    def test_fair_value_negative_composite(self) -> None:
        """Negative z-score displaces fair value below mid."""
        pricer = _make_pricer(fair_value_beta=0.5, tick_size=50.0)
        fair = pricer.fair_value(mid=1000.0, composite_z=-2.0)
        self.assertAlmostEqual(fair, 1000.0 + 0.5 * (-2.0) * 50.0)  # 1000 - 50 = 950
        self.assertAlmostEqual(fair, 950.0)

    def test_fair_value_zero_composite(self) -> None:
        """Zero z-score → fair_value = mid."""
        pricer = _make_pricer(fair_value_beta=0.5, tick_size=50.0)
        fair = pricer.fair_value(mid=2500.0, composite_z=0.0)
        self.assertAlmostEqual(fair, 2500.0)

    def test_fair_value_scales_with_beta(self) -> None:
        """Larger beta produces proportionally larger displacement."""
        pricer_lo = _make_pricer(fair_value_beta=0.5, tick_size=50.0)
        pricer_hi = _make_pricer(fair_value_beta=1.0, tick_size=50.0)
        fair_lo = pricer_lo.fair_value(mid=1000.0, composite_z=1.0)
        fair_hi = pricer_hi.fair_value(mid=1000.0, composite_z=1.0)
        self.assertAlmostEqual(fair_hi - 1000.0, 2 * (fair_lo - 1000.0))


class TestReservationNeutral(unittest.TestCase):
    """Test 2: reservation with neutral inventory (side=0, qty=0) → reservation = fair."""

    def test_neutral_inventory_no_skew(self) -> None:
        """When position is flat, inv_fraction=0 → reservation = fair."""
        pricer = _make_pricer()
        fair = pricer.fair_value(mid=1000.0, composite_z=0.5)
        res = pricer.reservation(fair=fair, position_side=0, position_qty=0)
        self.assertAlmostEqual(res, fair)

    def test_neutral_side_nonzero_qty(self) -> None:
        """side=0 means flat regardless of qty → no skew."""
        pricer = _make_pricer()
        fair = 1050.0
        res = pricer.reservation(fair=fair, position_side=0, position_qty=100)
        # inv_fraction = 0 * 100 / 300 = 0 → lambda * 0 = 0 → reservation = fair
        self.assertAlmostEqual(res, fair)


class TestReservationLightSkew(unittest.TestCase):
    """Test 3: reservation with light inventory skew (|frac| < 0.3)."""

    def test_light_long_skew(self) -> None:
        """Small long position → small downward reservation skew (lean toward sell)."""
        pricer = _make_pricer(
            inv_skew_light=0.5,
            inv_skew_medium_threshold=0.3,
            inv_skew_medium=1.0,
            tick_size=50.0,
            max_inventory_qty=300,
        )
        # |frac| = 60/300 = 0.2 < 0.3 → lambda = inv_skew_light = 0.5
        fair = 1000.0
        res = pricer.reservation(fair=fair, position_side=1, position_qty=60)
        inv_frac = 1 * 60 / 300  # = 0.2
        expected = fair - 0.5 * inv_frac * 50.0  # = 1000 - 5 = 995
        self.assertAlmostEqual(res, expected)
        self.assertLess(res, fair)  # skew is downward for long position

    def test_light_short_skew(self) -> None:
        """Small short position → small upward reservation skew (lean toward buy)."""
        pricer = _make_pricer(inv_skew_light=0.5, tick_size=50.0, max_inventory_qty=300)
        fair = 1000.0
        res = pricer.reservation(fair=fair, position_side=-1, position_qty=60)
        inv_frac = -1 * 60 / 300  # = -0.2
        expected = fair - 0.5 * inv_frac * 50.0  # = 1000 + 5 = 1005
        self.assertAlmostEqual(res, expected)
        self.assertGreater(res, fair)  # skew is upward for short position


class TestReservationHeavySkew(unittest.TestCase):
    """Test 4: reservation with heavy inventory skew (|frac| > 0.6)."""

    def test_heavy_long_skew_large_discount(self) -> None:
        """Large long position → heavy lambda → large downward skew."""
        pricer = _make_pricer(
            inv_skew_heavy_threshold=0.6,
            inv_skew_heavy=2.0,
            inv_skew_medium=1.0,
            inv_skew_light=0.5,
            tick_size=50.0,
            max_inventory_qty=300,
        )
        # |frac| = 240/300 = 0.8 > 0.6 → lambda = inv_skew_heavy = 2.0
        fair = 1000.0
        res = pricer.reservation(fair=fair, position_side=1, position_qty=240)
        inv_frac = 1 * 240 / 300  # = 0.8
        expected = fair - 2.0 * inv_frac * 50.0  # = 1000 - 80 = 920
        self.assertAlmostEqual(res, expected)
        self.assertAlmostEqual(res, 920.0)

    def test_heavy_skew_greater_than_light(self) -> None:
        """Heavy lambda (|frac|>0.6) produces a larger skew than light lambda (|frac|<0.3)."""
        pricer = _make_pricer(
            inv_skew_light=0.5, inv_skew_medium=1.0, inv_skew_heavy=2.0,
            tick_size=50.0, max_inventory_qty=300,
        )
        fair = 1000.0
        res_light = pricer.reservation(fair=fair, position_side=1, position_qty=60)    # |frac|=0.2
        res_heavy = pricer.reservation(fair=fair, position_side=1, position_qty=240)   # |frac|=0.8
        # Both are below fair for long positions, but heavy should be further below
        self.assertLess(res_heavy, res_light)

    def test_medium_skew_boundary(self) -> None:
        """Exactly at medium threshold uses medium lambda."""
        pricer = _make_pricer(
            inv_skew_medium_threshold=0.3, inv_skew_medium=1.0,
            tick_size=50.0, max_inventory_qty=300,
        )
        # |frac| = 90/300 = 0.3 → medium lambda = 1.0
        fair = 1000.0
        res = pricer.reservation(fair=fair, position_side=1, position_qty=90)
        inv_frac = 90 / 300
        expected = fair - 1.0 * inv_frac * 50.0
        self.assertAlmostEqual(res, expected)


class TestHalfSpreadBase(unittest.TestCase):
    """Test 5: half_spread base case: NORMAL state, low event rate → base_half_spread * tick."""

    def test_base_case_normal_state_low_activity(self) -> None:
        """No ATR premium, event_rate=5 (<20), NORMAL → half_spread = base_half_spread * tick."""
        pricer = _make_pricer(base_half_spread_ticks=0.5, tick_size=50.0)
        # atr_pct = atr / max(mid * 0.001, 1.0)
        # With atr=0: vol_factor = 1.0 + max(0, 0 - 1) * 0.5 = 1.0
        # With event_rate=5: rate_factor = 1.0 + max(0, (5-20)/100) = 1.0
        # NORMAL: state_factor = 1.0
        hs = pricer.half_spread(atr=0.0, mid=1000.0, event_rate=5.0, state=MarketState.NORMAL)
        expected = 0.5 * 50.0  # = 25.0
        self.assertAlmostEqual(hs, expected)

    def test_base_case_no_spread_inflation_below_atr_threshold(self) -> None:
        """When atr_pct < 1.0 (ATR < 0.1% of mid), vol_factor stays at 1.0."""
        pricer = _make_pricer(base_half_spread_ticks=1.0, tick_size=50.0)
        mid = 2000.0
        # 0.1% of 2000 = 2.0; atr = 1.5 < 2.0 → atr_pct = 1.5/2.0 = 0.75 < 1 → vol_factor=1.0
        hs = pricer.half_spread(atr=1.5, mid=mid, event_rate=10.0, state=MarketState.NORMAL)
        expected = 1.0 * 50.0  # vol_factor=1, rate_factor=1, state_factor=1
        self.assertAlmostEqual(hs, expected)


class TestHalfSpreadScalesWithATR(unittest.TestCase):
    """Test 6: half_spread scales with ATR."""

    def test_high_atr_widens_spread(self) -> None:
        """High ATR (atr_pct > 1) increases vol_factor and produces wider half_spread."""
        pricer = _make_pricer(base_half_spread_ticks=0.5, tick_size=50.0)
        mid = 1000.0
        base_hs = pricer.half_spread(atr=0.0, mid=mid, event_rate=5.0, state=MarketState.NORMAL)

        # atr = 5.0, 0.1% of 1000 = 1.0 → atr_pct = 5.0/1.0 = 5.0 > 1
        # vol_factor = 1.0 + (5.0 - 1.0) * 0.5 = 1 + 2 = 3.0
        high_hs = pricer.half_spread(atr=5.0, mid=mid, event_rate=5.0, state=MarketState.NORMAL)
        self.assertGreater(high_hs, base_hs)

    def test_high_atr_exact_value(self) -> None:
        """Verify exact half_spread value with known ATR."""
        pricer = _make_pricer(base_half_spread_ticks=0.5, tick_size=50.0)
        mid = 1000.0
        atr = 5.0
        # 0.1% of 1000 = 1.0 → atr_pct = 5.0/1.0 = 5.0
        # vol_factor = 1.0 + max(0, 5.0 - 1.0) * 0.5 = 1.0 + 2.0 = 3.0
        # rate_factor = 1.0 (event_rate=5 < 20)
        # state_factor = 1.0 (NORMAL)
        # base = 0.5 * 50.0 = 25.0
        # hs = 25.0 * 3.0 * 1.0 * 1.0 = 75.0
        hs = pricer.half_spread(atr=atr, mid=mid, event_rate=5.0, state=MarketState.NORMAL)
        self.assertAlmostEqual(hs, 75.0)

    def test_abnormal_state_widens_spread_1_5x(self) -> None:
        """ABNORMAL state applies 1.5x state_factor to defend against adverse selection."""
        pricer = _make_pricer(base_half_spread_ticks=0.5, tick_size=50.0)
        mid = 1000.0
        normal_hs = pricer.half_spread(atr=0.0, mid=mid, event_rate=5.0, state=MarketState.NORMAL)
        abnormal_hs = pricer.half_spread(atr=0.0, mid=mid, event_rate=5.0, state=MarketState.ABNORMAL)
        self.assertAlmostEqual(abnormal_hs, 1.5 * normal_hs)

    def test_high_event_rate_widens_spread(self) -> None:
        """Event rate > 20 applies a rate_factor > 1, widening spread."""
        pricer = _make_pricer(base_half_spread_ticks=0.5, tick_size=50.0)
        mid = 1000.0
        base_hs = pricer.half_spread(atr=0.0, mid=mid, event_rate=5.0, state=MarketState.NORMAL)
        # event_rate=120: rate_factor = 1 + (120-20)/100 = 2.0
        fast_hs = pricer.half_spread(atr=0.0, mid=mid, event_rate=120.0, state=MarketState.NORMAL)
        self.assertGreater(fast_hs, base_hs)
        expected = 0.5 * 50.0 * 1.0 * 2.0 * 1.0  # = 50.0
        self.assertAlmostEqual(fast_hs, expected)


if __name__ == "__main__":
    unittest.main()
