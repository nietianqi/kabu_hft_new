import unittest
from unittest.mock import AsyncMock

from kabu_hft.config import load_config
from kabu_hft.core import HFTStrategy
from kabu_hft.gateway import BoardSnapshot, KabuRestClient, Level
from kabu_hft.signals import SignalPacket


class DummyRestClient(KabuRestClient):
    def __init__(self) -> None:
        super().__init__("http://localhost:18080")


def _snapshot(*, bid: float = 100.0, ask: float = 101.0, ts_ns: int = 1_000_000_000) -> BoardSnapshot:
    return BoardSnapshot(
        symbol="9984",
        exchange=1,
        ts_ns=ts_ns,
        bid=bid,
        ask=ask,
        bid_size=500,
        ask_size=500,
        last=(bid + ask) / 2.0,
        last_size=0,
        volume=1000,
        vwap=(bid + ask) / 2.0,
        bids=(Level(bid, 500),),
        asks=(Level(ask, 500),),
    )


def _signal(score: float) -> SignalPacket:
    return SignalPacket(
        ts_ns=0,
        obi_raw=0.0,
        lob_ofi_raw=0.0,
        tape_ofi_raw=0.0,
        micro_momentum_raw=0.0,
        microprice_tilt_raw=0.0,
        microprice=0.0,
        mid=0.0,
        obi_z=0.0,
        lob_ofi_z=0.0,
        tape_ofi_z=0.0,
        micro_momentum_z=0.0,
        microprice_tilt_z=0.0,
        composite=score,
    )


class StrategyAdaptiveTests(unittest.TestCase):
    def test_queue_threshold_drops_on_stronger_alpha(self) -> None:
        app_cfg = load_config(None)
        cfg = app_cfg.strategies[0]
        strategy = HFTStrategy(
            config=cfg,
            order_profile=app_cfg.order_profile,
            rest_client=DummyRestClient(),
            dry_run=True,
        )
        snapshot = _snapshot()
        weak = strategy._queue_threshold(snapshot, signal_strength=0.6)
        strong = strategy._queue_threshold(snapshot, signal_strength=2.0)
        self.assertLessEqual(strong, weak)


class StrategyExitPolicyTests(unittest.IsolatedAsyncioTestCase):
    def _make_strategy(self, *, disable_stop_loss: bool) -> HFTStrategy:
        app_cfg = load_config(None)
        cfg = app_cfg.strategies[0]
        cfg.tick_size = 1.0
        cfg.take_profit_ticks = 1.0
        cfg.take_profit_min_hold_ms = 0
        cfg.stop_loss_ticks = 1.0
        cfg.disable_stop_loss = disable_stop_loss
        strategy = HFTStrategy(
            config=cfg,
            order_profile=app_cfg.order_profile,
            rest_client=DummyRestClient(),
            dry_run=True,
        )
        strategy.risk.must_close = lambda **_: (False, "")
        return strategy

    @staticmethod
    def _set_long_inventory(strategy: HFTStrategy, *, avg_price: float, qty: int, opened_ts_ns: int) -> None:
        strategy.execution.inventory.side = 1
        strategy.execution.inventory.qty = qty
        strategy.execution.inventory.avg_price = avg_price
        strategy.execution.inventory.opened_ts_ns = opened_ts_ns
        strategy.execution.inventory.entry_qty = qty

    async def test_take_profit_closes_open_position(self) -> None:
        strategy = self._make_strategy(disable_stop_loss=False)
        self._set_long_inventory(strategy, avg_price=100.0, qty=100, opened_ts_ns=1_000_000_000)
        strategy.execution.close = AsyncMock(return_value=True)
        snapshot = _snapshot(bid=101.0, ask=102.0, ts_ns=2_000_000_000)

        await strategy._process_signal(snapshot, _signal(0.20), snapshot.ts_ns)

        strategy.execution.close.assert_awaited_once()
        kwargs = strategy.execution.close.await_args.kwargs
        self.assertEqual(kwargs["reason"], "take_profit")
        self.assertTrue(kwargs["force"])

    async def test_disable_stop_loss_keeps_position_open(self) -> None:
        strategy = self._make_strategy(disable_stop_loss=True)
        self._set_long_inventory(strategy, avg_price=100.0, qty=100, opened_ts_ns=1_000_000_000)
        strategy.execution.close = AsyncMock(return_value=True)
        snapshot = _snapshot(bid=99.0, ask=100.0, ts_ns=2_000_000_000)

        await strategy._process_signal(snapshot, _signal(0.10), snapshot.ts_ns)

        strategy.execution.close.assert_not_awaited()

    async def test_enabled_stop_loss_closes_loser(self) -> None:
        strategy = self._make_strategy(disable_stop_loss=False)
        self._set_long_inventory(strategy, avg_price=100.0, qty=100, opened_ts_ns=1_000_000_000)
        strategy.execution.close = AsyncMock(return_value=True)
        snapshot = _snapshot(bid=99.0, ask=100.0, ts_ns=2_000_000_000)

        await strategy._process_signal(snapshot, _signal(0.10), snapshot.ts_ns)

        strategy.execution.close.assert_awaited_once()
        kwargs = strategy.execution.close.await_args.kwargs
        self.assertEqual(kwargs["reason"], "stop_loss")


if __name__ == "__main__":
    unittest.main()
