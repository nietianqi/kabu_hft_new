from __future__ import annotations

from dataclasses import dataclass, field


# TSE tick size schedule: list of (price_threshold, tick_size) pairs,
# sorted ascending by price. The tick size applies for prices >= threshold.
# Reference: https://www.jpx.co.jp/english/equities/trading/domestic/04.html
TSE_TICK_SCHEDULE: list[tuple[float, float]] = [
    (0.0, 0.1),
    (1_000.0, 0.5),
    (3_000.0, 1.0),
    (5_000.0, 5.0),
    (10_000.0, 10.0),
    (30_000.0, 50.0),
    (50_000.0, 100.0),
    (100_000.0, 500.0),
    (300_000.0, 1_000.0),
    (500_000.0, 5_000.0),
    (1_000_000.0, 10_000.0),
    (3_000_000.0, 50_000.0),
    (5_000_000.0, 100_000.0),
    (10_000_000.0, 500_000.0),
    (30_000_000.0, 1_000_000.0),
    (50_000_000.0, 5_000_000.0),
]


@dataclass(slots=True)
class Instrument:
    """Describes the trading characteristics of a listed security.

    Separates market-microstructure constants (tick size schedule, lot size)
    from strategy-level risk parameters (max notional, daily loss, etc.).
    """

    symbol: str
    exchange: int
    tick_size: float  # Static minimum tick at a typical price level (used as fallback)
    lot_size: int = 100  # Standard lot size (minimum order qty multiple)
    price_precision: int = 1  # Decimal places for display / rounding
    tick_schedule: list[tuple[float, float]] = field(default_factory=list)
    """Price-band tick schedule: [(min_price, tick_size), ...] sorted ascending.
    If empty, *tick_size* is used for all price levels.
    """

    def tick_for_price(self, price: float) -> float:
        """Return the correct minimum price increment for *price*.

        Uses ``tick_schedule`` if populated, otherwise falls back to ``tick_size``.
        """
        if not self.tick_schedule:
            return self.tick_size
        tick = self.tick_size
        for threshold, ts in self.tick_schedule:
            if price >= threshold:
                tick = ts
            else:
                break
        return tick

    def round_to_tick(self, price: float) -> float:
        """Round *price* to the nearest valid tick for that price level."""
        tick = self.tick_for_price(price)
        if tick <= 0:
            return price
        return round(round(price / tick) * tick, self.price_precision)


def make_tse_instrument(
    symbol: str,
    exchange: int = 1,
    tick_size: float = 1.0,
    lot_size: int = 100,
    price_precision: int = 0,
) -> Instrument:
    """Convenience constructor for a TSE-listed instrument with standard tick schedule."""
    return Instrument(
        symbol=symbol,
        exchange=exchange,
        tick_size=tick_size,
        lot_size=lot_size,
        price_precision=price_precision,
        tick_schedule=list(TSE_TICK_SCHEDULE),
    )
