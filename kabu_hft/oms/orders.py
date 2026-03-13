from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger("kabu.oms")


class OrderStatus(str, Enum):
    NEW_PENDING = "NEW_PENDING"
    WORKING = "WORKING"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCEL_PENDING = "CANCEL_PENDING"
    CANCELED = "CANCELED"
    REJECTED = "REJECTED"
    UNKNOWN = "UNKNOWN"


@dataclass(slots=True)
class WorkingOrderRecord:
    order_id: str
    symbol: str
    side: int
    qty: int
    price: float
    status: OrderStatus = OrderStatus.NEW_PENDING
    cum_qty: int = 0
    avg_fill_price: float = 0.0
    cancel_reason: str = ""
    tags: dict[str, str] = field(default_factory=dict)

    @property
    def leaves_qty(self) -> int:
        return max(self.qty - self.cum_qty, 0)

    @property
    def is_final(self) -> bool:
        return self.status in {
            OrderStatus.FILLED,
            OrderStatus.CANCELED,
            OrderStatus.REJECTED,
        }


class OrderLedger:
    def __init__(self) -> None:
        self._records: dict[str, WorkingOrderRecord] = {}

    def add(self, record: WorkingOrderRecord) -> None:
        if record.order_id in self._records:
            logger.warning("duplicate order_id=%s — overwriting existing record", record.order_id)
        self._records[record.order_id] = record

    def get(self, order_id: str) -> WorkingOrderRecord | None:
        return self._records.get(order_id)

    def mark_working(self, order_id: str) -> None:
        record = self._records.get(order_id)
        if record and not record.is_final:
            record.status = OrderStatus.WORKING

    def mark_cancel_pending(self, order_id: str, reason: str = "") -> None:
        record = self._records.get(order_id)
        if record and not record.is_final:
            record.status = OrderStatus.CANCEL_PENDING
            record.cancel_reason = reason

    def apply_fill(self, order_id: str, fill_qty: int, fill_price: float) -> None:
        record = self._records.get(order_id)
        if record is None or record.is_final or fill_qty <= 0:
            return
        new_cum_qty = min(record.qty, record.cum_qty + fill_qty)
        if new_cum_qty <= 0:
            return
        if record.cum_qty == 0:
            record.avg_fill_price = fill_price
        else:
            prev_value = record.cum_qty * record.avg_fill_price
            fill_value = fill_qty * fill_price
            record.avg_fill_price = (prev_value + fill_value) / max(new_cum_qty, 1)
        record.cum_qty = new_cum_qty
        record.status = (
            OrderStatus.FILLED
            if record.cum_qty >= record.qty
            else OrderStatus.PARTIALLY_FILLED
        )

    def mark_canceled(self, order_id: str) -> None:
        record = self._records.get(order_id)
        if record and not record.is_final:
            record.status = OrderStatus.CANCELED

    def mark_rejected(self, order_id: str) -> None:
        record = self._records.get(order_id)
        if record and not record.is_final:
            record.status = OrderStatus.REJECTED

    def mark_filled(self, order_id: str) -> None:
        record = self._records.get(order_id)
        if record is None or record.is_final:
            return
        if record.cum_qty < record.qty:
            record.cum_qty = record.qty
        if record.avg_fill_price <= 0:
            record.avg_fill_price = record.price
        record.status = OrderStatus.FILLED

    def snapshot(self) -> dict[str, dict]:
        return {
            order_id: {
                "symbol": record.symbol,
                "side": record.side,
                "qty": record.qty,
                "price": record.price,
                "status": record.status.value,
                "cum_qty": record.cum_qty,
                "avg_fill_price": record.avg_fill_price,
                "cancel_reason": record.cancel_reason,
            }
            for order_id, record in self._records.items()
        }
