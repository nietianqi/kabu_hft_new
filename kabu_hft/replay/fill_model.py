from __future__ import annotations

import logging
from dataclasses import dataclass, field

from kabu_hft.gateway import BoardSnapshot

logger = logging.getLogger("kabu.replay.fill")


@dataclass(slots=True)
class SimOrder:
    """A simulated order submitted to the :class:`PriceCrossFillModel`."""

    order_id: str
    side: int        # +1 = buy, -1 = sell
    qty: int
    price: float     # limit price; 0 means market
    is_market: bool
    sent_ns: int     # nanosecond timestamp when order was submitted to the model


@dataclass(slots=True)
class FillResult:
    """Result of a simulated fill."""

    order: SimOrder
    fill_price: float
    fill_qty: int
    fill_ns: int     # nanosecond timestamp when the fill was detected


class PriceCrossFillModel:
    """Price-cross limit order fill simulator.

    Models order lifecycle during a board-event replay:

    * A simulated **round-trip latency** (``latency_us`` microseconds) is
      applied before an order becomes "active".  This means a limit order
      submitted at board event *t* cannot fill until at least *t + latency*.

    * Fill condition (when the order is active):
      - **Market buy** → fills at ``snapshot.ask``
      - **Market sell** → fills at ``snapshot.bid``
      - **Limit buy** → fills when ``snapshot.ask ≤ price``; fill at
        ``min(price, snapshot.ask)`` (price improvement possible)
      - **Limit sell** → fills when ``snapshot.bid ≥ price``; fill at
        ``max(price, snapshot.bid)``

    * No queue-position modeling — assumes immediate fill once the condition
      is met.  This is optimistic; use the `latency_us` parameter to partially
      compensate.

    Usage::

        model = PriceCrossFillModel(latency_us=5_000)
        model.submit(SimOrder("id1", side=1, qty=100, price=9980.0,
                              is_market=False, sent_ns=now_ns))
        fills = model.on_board(snapshot, now_ns)
    """

    def __init__(self, latency_us: int = 5_000) -> None:
        self._latency_ns: int = latency_us * 1_000
        self._pending: list[SimOrder] = []

    @property
    def pending_count(self) -> int:
        return len(self._pending)

    def submit(self, order: SimOrder) -> None:
        """Add an order to the pending queue."""
        self._pending.append(order)
        logger.debug(
            "sim submit order_id=%s side=%+d qty=%d price=%.2f market=%s",
            order.order_id,
            order.side,
            order.qty,
            order.price,
            order.is_market,
        )

    def cancel(self, order_id: str) -> bool:
        """Remove an order by ID (returns True if found and removed)."""
        before = len(self._pending)
        self._pending = [o for o in self._pending if o.order_id != order_id]
        return len(self._pending) < before

    def on_board(self, snapshot: BoardSnapshot, now_ns: int) -> list[FillResult]:
        """Check for fills given the new board snapshot.

        Returns a list of :class:`FillResult` for any newly filled orders.
        """
        if not snapshot.valid:
            return []

        filled: list[FillResult] = []
        remaining: list[SimOrder] = []
        active_threshold_ns = now_ns - self._latency_ns

        for order in self._pending:
            if order.sent_ns > active_threshold_ns:
                # Latency not yet elapsed — order not yet active
                remaining.append(order)
                continue

            fill_price = self._check_fill(order, snapshot)
            if fill_price is not None:
                result = FillResult(
                    order=order,
                    fill_price=fill_price,
                    fill_qty=order.qty,
                    fill_ns=now_ns,
                )
                filled.append(result)
                logger.debug(
                    "sim fill order_id=%s side=%+d qty=%d fill_price=%.2f",
                    order.order_id,
                    order.side,
                    order.qty,
                    fill_price,
                )
            else:
                remaining.append(order)

        self._pending = remaining
        return filled

    @staticmethod
    def _check_fill(order: SimOrder, snapshot: BoardSnapshot) -> float | None:
        """Return fill price if the order should fill, else None."""
        if order.is_market:
            return snapshot.ask if order.side > 0 else snapshot.bid

        if order.side > 0:  # limit buy
            if snapshot.ask <= order.price:
                return min(order.price, snapshot.ask)  # price improvement possible
        else:  # limit sell
            if snapshot.bid >= order.price:
                return max(order.price, snapshot.bid)

        return None

    def clear(self) -> None:
        """Cancel all pending orders."""
        self._pending.clear()
