from __future__ import annotations

from dataclasses import dataclass

from kabu_hft.gateway import OrderSnapshot
from kabu_hft.oms.orders import OrderStatus, WorkingOrderRecord


@dataclass(slots=True)
class ReconciliationIssue:
    order_id: str
    local_status: str
    broker_status: str
    severity: str
    message: str


def reconcile_order_state(
    local: WorkingOrderRecord,
    broker: OrderSnapshot,
) -> tuple[WorkingOrderRecord, ReconciliationIssue | None]:
    issue: ReconciliationIssue | None = None
    broker_status = broker.status

    if not local.is_final:
        if broker_status == "filled":
            local.status = OrderStatus.FILLED
        elif broker_status == "cancelled":
            local.status = OrderStatus.CANCELED
        elif broker_status == "partial":
            local.status = OrderStatus.PARTIALLY_FILLED
        else:
            if local.status == OrderStatus.NEW_PENDING:
                local.status = OrderStatus.WORKING

    if broker.cum_qty < local.cum_qty:
        issue = ReconciliationIssue(
            order_id=local.order_id,
            local_status=local.status.value,
            broker_status=broker_status,
            severity="high",
            message="broker cum_qty is behind local cum_qty",
        )

    if broker.cum_qty > local.cum_qty:
        local.cum_qty = broker.cum_qty
    if broker.avg_fill_price > 0:
        local.avg_fill_price = broker.avg_fill_price

    return local, issue
