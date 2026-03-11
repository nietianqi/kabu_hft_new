"""Reservation price computation for adaptive fair-value market making.

Three layers:
1. fair_value     = mid + beta * composite_z * tick_size
2. reservation    = fair_value − λ(inv_fraction) × inv_fraction × tick_size
3. half_spread    = base × vol_factor × rate_factor × state_factor × tick_size

Reference: Avellaneda-Stoikov (2008) adapted for discrete tick markets.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from kabu_hft.signals.market_state import MarketState


class ReservationPricer:
    """
    Stateless pricer — no internal state, safe to call from any async context.

    Parameters
    ----------
    fair_value_beta          : Sensitivity of fair_value to composite z-score (in ticks).
                               fair_value = mid ± beta * composite_z * tick_size
    base_half_spread_ticks   : Base half-spread in units of tick_size.
    inv_skew_medium_threshold: |inv_fraction| above this → medium λ.
    inv_skew_heavy_threshold : |inv_fraction| above this → heavy λ.
    inv_skew_light/medium/heavy : λ multipliers for the three inventory segments.
    tick_size                : Minimum price increment of the instrument (JPY).
    max_inventory_qty        : Normalising denominator for inv_fraction.
    """

    def __init__(
        self,
        fair_value_beta: float = 0.5,
        base_half_spread_ticks: float = 0.5,
        inv_skew_medium_threshold: float = 0.3,
        inv_skew_heavy_threshold: float = 0.6,
        inv_skew_light: float = 0.5,
        inv_skew_medium: float = 1.0,
        inv_skew_heavy: float = 2.0,
        tick_size: float = 50.0,
        max_inventory_qty: int = 300,
    ) -> None:
        self.fair_value_beta = fair_value_beta
        self.base_half_spread_ticks = base_half_spread_ticks
        self.inv_skew_medium_threshold = inv_skew_medium_threshold
        self.inv_skew_heavy_threshold = inv_skew_heavy_threshold
        self.inv_skew_light = inv_skew_light
        self.inv_skew_medium = inv_skew_medium
        self.inv_skew_heavy = inv_skew_heavy
        self.tick_size = tick_size
        self.max_inventory_qty = max(max_inventory_qty, 1)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def fair_value(self, mid: float, composite_z: float) -> float:
        """
        Estimate short-term fair value by displacing mid by composite alpha.

        fair_value = mid + beta * composite_z * tick_size

        Composite z is already dimensionless (z-scored); multiplying by tick_size
        converts it to price units, and beta calibrates the sensitivity.
        """
        return mid + self.fair_value_beta * composite_z * self.tick_size

    def reservation(
        self,
        fair: float,
        position_side: int,
        position_qty: int,
    ) -> float:
        """
        Skew fair value toward position-reducing quotes.

        reservation = fair − λ(inv_fraction) × inv_fraction × tick_size

        inv_fraction ∈ [−1, +1]:
          +1 → fully long (max inventory) → skew downward (lean toward sell)
          −1 → fully short → skew upward (lean toward buy)

        λ is piecewise: light / medium / heavy based on |inv_fraction|.
        """
        inv_fraction = position_side * position_qty / self.max_inventory_qty
        λ = self._lambda(inv_fraction)
        return fair - λ * inv_fraction * self.tick_size

    def half_spread(
        self,
        atr: float,
        mid: float,
        event_rate: float,
        state: "MarketState",
    ) -> float:
        """
        Volatility- and activity-adjusted half-spread.

        half_spread = base × vol_factor × rate_factor × state_factor

        vol_factor  : widens with ATR relative to 0.1% of mid
        rate_factor : widens mildly when event rate > 20 events/sec
        state_factor: 1.5× in ABNORMAL (defensive widening)
        """
        from kabu_hft.signals.market_state import MarketState

        base = self.base_half_spread_ticks * self.tick_size
        atr_pct = atr / max(mid * 0.001, 1.0)     # ATR as fraction of 0.1% of mid
        vol_factor = 1.0 + max(0.0, atr_pct - 1.0) * 0.5
        rate_factor = 1.0 + max(0.0, (event_rate - 20.0) / 100.0)
        state_factor = 1.5 if state == MarketState.ABNORMAL else 1.0
        return base * vol_factor * rate_factor * state_factor

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _lambda(self, inv_fraction: float) -> float:
        """Three-segment skew multiplier keyed by |inventory fraction|."""
        abs_f = abs(inv_fraction)
        if abs_f >= self.inv_skew_heavy_threshold:
            return self.inv_skew_heavy
        if abs_f >= self.inv_skew_medium_threshold:
            return self.inv_skew_medium
        return self.inv_skew_light
