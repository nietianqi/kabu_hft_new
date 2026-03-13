import asyncio
import unittest

from kabu_hft.config import OrderProfile
from kabu_hft.execution import ExecutionController, ExecutionState, QuoteMode
from kabu_hft.execution.engine import WorkingOrder
from kabu_hft.gateway import BoardSnapshot, KabuApiError, KabuRestClient, Level, TradePrint


class DummyRestClient(KabuRestClient):
    def __init__(self) -> None:
        super().__init__("http://localhost:18080")


class ExecutionTests(unittest.IsolatedAsyncioTestCase):
    async def test_dry_run_entry_and_exit_complete_round_trip(self) -> None:
        controller = ExecutionController(
            symbol="9984",
            exchange=1,
            rest_client=DummyRestClient(),
            order_profile=OrderProfile(),
            dry_run=True,
            tick_size=1.0,
            strong_threshold=0.8,
            min_edge_ticks=0.1,
            max_pending_ms=2_000,
            min_order_lifetime_ms=100,
            max_requotes_per_minute=20,
            allow_aggressive_entry=False,
            allow_aggressive_exit=True,
            queue_model=False,
        )
        snapshot = BoardSnapshot(
            symbol="9984",
            exchange=1,
            ts_ns=1,
            bid=100.0,
            ask=101.0,
            bid_size=600,
            ask_size=300,
            last=100.5,
            last_size=0,
            volume=1_000,
            vwap=100.5,
            bids=(Level(100.0, 600),),
            asks=(Level(101.0, 300),),
            prev_board=None,
        )

        opened = await controller.open(
            direction=1,
            qty=100,
            snapshot=snapshot,
            score=1.0,
            microprice=100.8,
            reason="test_open",
        )
        self.assertTrue(opened)
        self.assertEqual(controller.state, ExecutionState.OPENING)

        controller.sync_paper_trade(
            TradePrint(
                symbol="9984",
                exchange=1,
                ts_ns=2,
                price=100.0,
                size=100,
                side=-1,
                cumulative_volume=1_100,
            )
        )
        self.assertEqual(controller.state, ExecutionState.OPEN)

        closed = await controller.close(
            snapshot=snapshot,
            score=-1.0,
            reason="test_close",
            force=False,
        )
        self.assertTrue(closed)
        controller.sync_paper_trade(
            TradePrint(
                symbol="9984",
                exchange=1,
                ts_ns=3,
                price=101.0,
                size=100,
                side=1,
                cumulative_volume=1_200,
            )
        )
        self.assertEqual(controller.state, ExecutionState.FLAT)
        self.assertEqual(len(controller.drain_round_trips()), 1)

    async def test_queue_defense_retreats_when_top_queue_is_thin(self) -> None:
        controller = ExecutionController(
            symbol="9984",
            exchange=1,
            rest_client=DummyRestClient(),
            order_profile=OrderProfile(),
            dry_run=True,
            tick_size=1.0,
            strong_threshold=0.8,
            min_edge_ticks=0.0,
            max_pending_ms=2_000,
            min_order_lifetime_ms=100,
            max_requotes_per_minute=20,
            allow_aggressive_entry=False,
            allow_aggressive_exit=True,
        )
        snapshot = BoardSnapshot(
            symbol="9984",
            exchange=1,
            ts_ns=1,
            bid=100.0,
            ask=101.0,
            bid_size=50,
            ask_size=700,
            last=100.5,
            last_size=0,
            volume=1_000,
            vwap=100.5,
            bids=(Level(100.0, 50),),
            asks=(Level(101.0, 700),),
            prev_board=None,
        )

        decision = controller.preview_entry(
            direction=1,
            snapshot=snapshot,
            score=0.7,
            microprice=100.6,
            mode=QuoteMode.QUEUE_DEFENSE,
            reservation_price=100.5,
            queue_qty_threshold=100,
        )
        self.assertEqual(decision.price, 99.0)

    async def test_cancel_working_treats_code_43_as_already_filled(self) -> None:
        class CancelRaceRestClient(DummyRestClient):
            async def cancel_order(self, _order_id: str) -> dict:
                raise KabuApiError(
                    "PUT /kabusapi/cancelorder failed with status 500",
                    status=500,
                    payload={"Code": 43, "Message": "該当注文は既に約定済です"},
                )

        controller = ExecutionController(
            symbol="9984",
            exchange=1,
            rest_client=CancelRaceRestClient(),
            order_profile=OrderProfile(),
            dry_run=False,
            tick_size=1.0,
            strong_threshold=0.8,
            min_edge_ticks=0.1,
            max_pending_ms=2_000,
            min_order_lifetime_ms=100,
            max_requotes_per_minute=20,
            allow_aggressive_entry=False,
            allow_aggressive_exit=True,
            queue_model=False,
        )
        controller.working_order = WorkingOrder(
            order_id="ORDER-43",
            purpose="entry",
            side=1,
            qty=100,
            price=100.0,
            is_market=False,
            sent_ts_ns=1,
            reason="test",
        )

        cancelled = await controller.cancel_working(reason="requote")

        self.assertTrue(cancelled)
        self.assertIsNotNone(controller.working_order)
        assert controller.working_order is not None
        self.assertTrue(controller.working_order.cancel_requested)

    async def test_cancel_working_resets_cancel_requested_on_error(self) -> None:
        class FailingCancelRestClient(DummyRestClient):
            async def cancel_order(self, _order_id: str) -> dict:
                raise KabuApiError(
                    "PUT /kabusapi/cancelorder failed with status 500",
                    status=500,
                    payload={"Code": 500001, "Message": "temporary error"},
                )

        controller = ExecutionController(
            symbol="9984",
            exchange=1,
            rest_client=FailingCancelRestClient(),
            order_profile=OrderProfile(),
            dry_run=False,
            tick_size=1.0,
            strong_threshold=0.8,
            min_edge_ticks=0.1,
            max_pending_ms=2_000,
            min_order_lifetime_ms=100,
            max_requotes_per_minute=20,
            allow_aggressive_entry=False,
            allow_aggressive_exit=True,
            queue_model=False,
        )
        controller.working_order = WorkingOrder(
            order_id="ORDER-FAIL",
            purpose="entry",
            side=1,
            qty=100,
            price=100.0,
            is_market=False,
            sent_ts_ns=1,
            reason="test",
        )

        with self.assertRaises(KabuApiError):
            await controller.cancel_working(reason="requote")
        assert controller.working_order is not None
        self.assertFalse(controller.working_order.cancel_requested)


if __name__ == "__main__":
    unittest.main()
