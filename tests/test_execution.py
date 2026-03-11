import asyncio
import unittest

from kabu_hft.config import OrderProfile
from kabu_hft.execution import ExecutionController, ExecutionState
from kabu_hft.gateway import BoardSnapshot, KabuRestClient, Level, TradePrint


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


if __name__ == "__main__":
    unittest.main()
