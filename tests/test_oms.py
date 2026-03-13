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

    def test_add_duplicate_order_id_warns_and_overwrites(self) -> None:
        """add() must warn on duplicate order_id but still accept the new record."""
        import logging
        ledger = OrderLedger()
        r1 = WorkingOrderRecord(order_id="DUP", symbol="9984", side=1, qty=100, price=100.0)
        r2 = WorkingOrderRecord(order_id="DUP", symbol="9984", side=1, qty=200, price=101.0)
        ledger.add(r1)
        with self.assertLogs("kabu.oms", level=logging.WARNING):
            ledger.add(r2)
        self.assertEqual(ledger.get("DUP").qty, 200)  # type: ignore[union-attr]

    def test_mark_rejected_does_not_overwrite_filled(self) -> None:
        """mark_rejected() must not downgrade a terminal FILLED status."""
        ledger = OrderLedger()
        r = WorkingOrderRecord(order_id="B1", symbol="9984", side=1, qty=100, price=100.0)
        ledger.add(r)
        ledger.mark_working("B1")
        ledger.apply_fill("B1", fill_qty=100, fill_price=100.0)
        self.assertEqual(ledger.get("B1").status, OrderStatus.FILLED)  # type: ignore[union-attr]
        ledger.mark_rejected("B1")  # must be a no-op now
        self.assertEqual(ledger.get("B1").status, OrderStatus.FILLED)  # type: ignore[union-attr]

    def test_mark_filled_does_not_overwrite_canceled(self) -> None:
        """mark_filled() must not overwrite a terminal CANCELED status."""
        ledger = OrderLedger()
        r = WorkingOrderRecord(order_id="C1", symbol="9984", side=1, qty=100, price=100.0)
        ledger.add(r)
        ledger.mark_working("C1")
        ledger.mark_canceled("C1")
        self.assertEqual(ledger.get("C1").status, OrderStatus.CANCELED)  # type: ignore[union-attr]
        ledger.mark_filled("C1")  # must be a no-op now
        self.assertEqual(ledger.get("C1").status, OrderStatus.CANCELED)  # type: ignore[union-attr]

    def test_position_apply_fill_rejects_invalid_side(self) -> None:
        """apply_fill() with side=0 must raise ValueError."""
        ledger = PositionLedger()
        with self.assertRaises(ValueError):
            ledger.apply_fill("9984", side=0, qty=100, price=100.0)

    def test_reconcile_does_not_downgrade_terminal_status(self) -> None:
        """reconcile_order_state() must not overwrite a terminal local status."""
        from kabu_hft.gateway import OrderSnapshot

        local = WorkingOrderRecord(
            order_id="T1",
            symbol="9984",
            side=1,
            qty=100,
            price=100.0,
            status=OrderStatus.FILLED,
            cum_qty=100,
        )
        # Broker sends a stale "partial" snapshot after local is already FILLED
        broker = OrderSnapshot(
            order_id="T1",
            side=1,
            order_qty=100,
            cum_qty=50,
            leaves_qty=50,
            price=100.0,
            avg_fill_price=100.0,
            state_code=2,
            order_state_code=2,
            is_final=False,
            raw={},
        )
        reconciled, _ = reconcile_order_state(local, broker)
        self.assertEqual(reconciled.status, OrderStatus.FILLED)


if __name__ == "__main__":
    unittest.main()
