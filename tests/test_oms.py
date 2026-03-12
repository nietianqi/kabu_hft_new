import unittest

from kabu_hft.gateway import OrderSnapshot
from kabu_hft.oms import (
    OrderLedger,
    OrderStatus,
    PositionLedger,
    WorkingOrderRecord,
    reconcile_order_state,
)


class OmsTests(unittest.TestCase):
    def test_order_ledger_partial_to_filled(self) -> None:
        ledger = OrderLedger()
        record = WorkingOrderRecord(
            order_id="A1",
            symbol="9984",
            side=1,
            qty=100,
            price=100.0,
        )
        ledger.add(record)
        ledger.mark_working("A1")
        ledger.apply_fill("A1", fill_qty=40, fill_price=100.0)
        self.assertEqual(ledger.get("A1").status, OrderStatus.PARTIALLY_FILLED)  # type: ignore[union-attr]
        ledger.apply_fill("A1", fill_qty=60, fill_price=101.0)
        self.assertEqual(ledger.get("A1").status, OrderStatus.FILLED)  # type: ignore[union-attr]

    def test_position_ledger_flip_and_realized(self) -> None:
        ledger = PositionLedger()
        state = ledger.apply_fill("9984", side=1, qty=100, price=100.0)
        self.assertEqual(state.qty, 100)
        state = ledger.apply_fill("9984", side=-1, qty=150, price=101.0)
        self.assertEqual(state.side, -1)
        self.assertEqual(state.qty, 50)
        self.assertGreater(state.realized_pnl, 0.0)

    def test_reconcile_order_state_detects_inconsistency(self) -> None:
        local = WorkingOrderRecord(
            order_id="A1",
            symbol="9984",
            side=1,
            qty=100,
            price=100.0,
            status=OrderStatus.PARTIALLY_FILLED,
            cum_qty=60,
        )
        broker = OrderSnapshot(
            order_id="A1",
            side=1,
            order_qty=100,
            cum_qty=20,
            leaves_qty=80,
            price=100.0,
            avg_fill_price=100.0,
            state_code=2,
            order_state_code=2,
            is_final=False,
            raw={},
        )
        reconciled, issue = reconcile_order_state(local, broker)
        self.assertIsNotNone(issue)
        self.assertEqual(reconciled.order_id, "A1")

    def test_mark_filled_keeps_terminal_status(self) -> None:
        ledger = OrderLedger()
        record = WorkingOrderRecord(
            order_id="A2",
            symbol="9984",
            side=1,
            qty=100,
            price=100.0,
        )
        ledger.add(record)
        ledger.mark_working("A2")
        ledger.apply_fill("A2", fill_qty=100, fill_price=100.0)
        ledger.mark_filled("A2")
        self.assertEqual(ledger.get("A2").status, OrderStatus.FILLED)  # type: ignore[union-attr]


if __name__ == "__main__":
    unittest.main()
