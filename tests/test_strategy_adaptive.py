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

    def test_entry_filter_blocks_when_spread_below_take_profit_target(self) -> None:
        app_cfg = load_config(None)
        cfg = app_cfg.strategies[0]
        cfg.tick_size = 1.0
        cfg.take_profit_ticks = 1.0
        strategy = HFTStrategy(
            config=cfg,
            order_profile=app_cfg.order_profile,
            rest_client=DummyRestClient(),
            dry_run=True,
        )
        snapshot = _snapshot(bid=100.0, ask=100.5)
        allowed, reason = strategy._entry_filter(
            snapshot=snapshot,
            direction=1,
            entry_price=100.0,
            is_market=False,
            fair_price=101.0,
            score=1.0,
            trade_lag_ms=0.0,
        )
        self.assertFalse(allowed)
        self.assertEqual(reason, "spread_below_take_profit_target")

    def test_entry_filter_blocks_when_fair_cannot_cover_one_tick_target(self) -> None:
        app_cfg = load_config(None)
        cfg = app_cfg.strategies[0]
        cfg.tick_size = 1.0
        cfg.take_profit_ticks = 1.0
        strategy = HFTStrategy(
            config=cfg,
            order_profile=app_cfg.order_profile,
            rest_client=DummyRestClient(),
            dry_run=True,
        )
        snapshot = _snapshot(bid=100.0, ask=101.0)
        allowed, reason = strategy._entry_filter(
            snapshot=snapshot,
            direction=1,
            entry_price=100.0,
            is_market=False,
            fair_price=100.4,
            score=1.0,
            trade_lag_ms=0.0,
        )
        self.assertFalse(allowed)
        self.assertEqual(reason, "fair_below_long_tp_target")

    def test_entry_filter_blocks_when_alpha_below_fast_scalp_threshold(self) -> None:
        app_cfg = load_config(None)
        cfg = app_cfg.strategies[0]
        cfg.tick_size = 1.0
        cfg.take_profit_ticks = 1.0
        strategy = HFTStrategy(
            config=cfg,
            order_profile=app_cfg.order_profile,
            rest_client=DummyRestClient(),
            dry_run=True,
        )
        snapshot = _snapshot(bid=100.0, ask=101.0)
        allowed, reason = strategy._entry_filter(
            snapshot=snapshot,
            direction=1,
            entry_price=100.0,
            is_market=False,
            fair_price=101.5,
            score=0.50,
            trade_lag_ms=0.0,
        )
        self.assertFalse(allowed)
        self.assertEqual(reason, "alpha_below_fast_scalp_threshold")

    def test_entry_filter_blocks_when_trade_drought_makes_exit_timing_unreliable(self) -> None:
        app_cfg = load_config(None)
        cfg = app_cfg.strategies[0]
        cfg.tick_size = 1.0
        cfg.take_profit_ticks = 1.0
        cfg.max_trade_lag_ms_for_entry = 2500
        strategy = HFTStrategy(
            config=cfg,
            order_profile=app_cfg.order_profile,
            rest_client=DummyRestClient(),
            dry_run=True,
        )
        snapshot = _snapshot(bid=100.0, ask=101.0)
        allowed, reason = strategy._entry_filter(
            snapshot=snapshot,
            direction=1,
            entry_price=100.0,
            is_market=False,
            fair_price=101.5,
            score=1.0,
            trade_lag_ms=3000.0,
        )
        self.assertFalse(allowed)
        self.assertEqual(reason, "trade_drought")

    def test_entry_filter_allows_passive_long_when_one_tick_target_supported(self) -> None:
        app_cfg = load_config(None)
        cfg = app_cfg.strategies[0]
        cfg.tick_size = 1.0
        cfg.take_profit_ticks = 1.0
        cfg.entry_buffer_ticks = 0.25
        strategy = HFTStrategy(
            config=cfg,
            order_profile=app_cfg.order_profile,
            rest_client=DummyRestClient(),
            dry_run=True,
        )
        snapshot = _snapshot(bid=100.0, ask=101.0)
        allowed, reason = strategy._entry_filter(
            snapshot=snapshot,
            direction=1,
            entry_price=100.0,
            is_market=False,
            fair_price=101.5,
            score=1.0,
            trade_lag_ms=0.0,
        )
        self.assertTrue(allowed)
        self.assertEqual(reason, "ok")


class StrategyExitPolicyTests(unittest.IsolatedAsyncioTestCase):
    def _make_strategy(self) -> HFTStrategy:
        app_cfg = load_config(None)
        cfg = app_cfg.strategies[0]
        cfg.tick_size = 1.0
        cfg.take_profit_ticks = 1.0
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

    async def test_open_position_places_plus_one_tick_take_profit_quote(self) -> None:
        strategy = self._make_strategy()
        self._set_long_inventory(strategy, avg_price=100.0, qty=100, opened_ts_ns=1_000_000_000)
        strategy.execution.close = AsyncMock(return_value=True)
        strategy.risk.must_close = lambda **_: (False, "")
        snapshot = _snapshot(bid=99.0, ask=100.0, ts_ns=2_000_000_000)

        await strategy._process_signal(snapshot, _signal(0.20), snapshot.ts_ns)

        strategy.execution.close.assert_awaited_once()
        kwargs = strategy.execution.close.await_args.kwargs
        self.assertEqual(kwargs["reason"], "take_profit_quote")
        self.assertFalse(kwargs["force"])
        self.assertEqual(kwargs["target_price"], 101.0)

    async def test_take_profit_quote_waits_for_min_hold_time(self) -> None:
        strategy = self._make_strategy()
        strategy.config.take_profit_min_hold_ms = 500
        strategy.take_profit_min_hold_ns = 500_000_000
        self._set_long_inventory(strategy, avg_price=100.0, qty=100, opened_ts_ns=1_900_000_000)
        strategy.execution.close = AsyncMock(return_value=True)
        strategy.risk.must_close = lambda **_: (False, "")
        snapshot = _snapshot(bid=100.0, ask=101.0, ts_ns=2_000_000_000)

        await strategy._process_signal(snapshot, _signal(0.80), snapshot.ts_ns)

        strategy.execution.close.assert_not_awaited()

    async def test_must_close_signal_is_ignored_while_losing(self) -> None:
        strategy = self._make_strategy()
        self._set_long_inventory(strategy, avg_price=100.0, qty=100, opened_ts_ns=1_000_000_000)
        strategy.execution.close = AsyncMock(return_value=True)
        strategy.risk.must_close = lambda **_: (True, "max_hold_time")
        snapshot = _snapshot(bid=99.0, ask=100.0, ts_ns=2_000_000_000)

        await strategy._process_signal(snapshot, _signal(0.10), snapshot.ts_ns)

        strategy.execution.close.assert_awaited_once()
        kwargs = strategy.execution.close.await_args.kwargs
        self.assertEqual(kwargs["reason"], "take_profit_quote")
        self.assertFalse(kwargs["force"])
        self.assertEqual(kwargs["target_price"], 101.0)

    async def test_must_close_force_exits_when_position_is_profitable(self) -> None:
        strategy = self._make_strategy()
        self._set_long_inventory(strategy, avg_price=100.0, qty=100, opened_ts_ns=1_000_000_000)
        strategy.execution.close = AsyncMock(return_value=True)
        strategy.risk.must_close = lambda **_: (True, "session_end")
        snapshot = _snapshot(bid=101.0, ask=102.0, ts_ns=2_000_000_000)

        await strategy._process_signal(snapshot, _signal(0.10), snapshot.ts_ns)

        strategy.execution.close.assert_awaited_once()
        kwargs = strategy.execution.close.await_args.kwargs
        self.assertEqual(kwargs["reason"], "session_end")
        self.assertTrue(kwargs["force"])

    async def test_external_inventory_conflict_blocks_new_entry(self) -> None:
        strategy = self._make_strategy()
        strategy.execution.open = AsyncMock(return_value=True)
        strategy.execution.has_external_inventory = True
        snapshot = _snapshot(bid=100.0, ask=101.0, ts_ns=2_000_000_000)

        await strategy._process_signal(snapshot, _signal(0.80), snapshot.ts_ns)

        strategy.execution.open.assert_not_awaited()

    async def test_spread_blowout_within_confirm_window_does_not_close(self) -> None:
        """P0: single tick of wide spread must NOT force-close within the confirm window."""
        strategy = self._make_strategy()
        strategy.config.spread_blowout_confirm_ms = 500
        self._set_long_inventory(strategy, avg_price=100.0, qty=100, opened_ts_ns=1_000_000_000)
        strategy.execution.close = AsyncMock(return_value=True)

        # spread = ask - bid = 107 - 100 = 7 ticks (> abnormal_max_spread_ticks=6)
        now_ns = 10_000_000_000
        snap = _snapshot(bid=100.0, ask=107.0, ts_ns=now_ns)

        await strategy._process_signal(snap, _signal(0.10), now_ns)

        strategy.execution.close.assert_not_awaited()
        # confirm timer should have been started
        self.assertGreater(strategy._spread_blowout_since_ns, 0)

    async def test_spread_blowout_after_confirm_window_triggers_close(self) -> None:
        """P0: after confirm window expires the position must be force-closed."""
        strategy = self._make_strategy()
        strategy.config.spread_blowout_confirm_ms = 500
        self._set_long_inventory(strategy, avg_price=100.0, qty=100, opened_ts_ns=1_000_000_000)
        strategy.execution.close = AsyncMock(return_value=True)

        T = 10_000_000_000
        snap1 = _snapshot(bid=100.0, ask=107.0, ts_ns=T)
        snap2 = _snapshot(bid=100.0, ask=107.0, ts_ns=T + 600_000_000)  # 600ms later > 500ms

        # First tick: sets the timer, does NOT close
        await strategy._process_signal(snap1, _signal(0.10), T)
        strategy.execution.close.assert_not_awaited()

        # Second tick: confirm window expired → must close
        await strategy._process_signal(snap2, _signal(0.10), T + 600_000_000)
        strategy.execution.close.assert_awaited_once()
        kwargs = strategy.execution.close.await_args.kwargs
        self.assertEqual(kwargs["reason"], "abnormal_spread_blowout")
        self.assertTrue(kwargs["force"])

    async def test_max_hold_hard_cap_closes_underwater_position(self) -> None:
        """P1: hard-cap forces close even when pnl_ticks < 0 after mult × max_hold."""
        strategy = self._make_strategy()
        strategy.config.max_hold_seconds = 45
        strategy.config.max_hold_hard_mult = 3      # hard cap = 135s
        # Position opened 136 seconds ago — past the 135s hard cap
        now_ns = 200_000_000_000
        opened_ts_ns = now_ns - 136_000_000_000
        self._set_long_inventory(strategy, avg_price=100.0, qty=100, opened_ts_ns=opened_ts_ns)
        strategy.execution.close = AsyncMock(return_value=True)
        # Simulate: must_close returns True but position is underwater (pnl_ticks < 0)
        strategy.risk.must_close = lambda **_: (True, "max_hold_time")
        # bid=98 < entry=100 → pnl_ticks = -2 → normal must_close gate blocks
        snap = _snapshot(bid=98.0, ask=99.0, ts_ns=now_ns - 500_000_000)

        await strategy._process_signal(snap, _signal(0.10), now_ns)

        strategy.execution.close.assert_awaited_once()
        kwargs = strategy.execution.close.await_args.kwargs
        self.assertEqual(kwargs["reason"], "max_hold_hard_cap")
        self.assertTrue(kwargs["force"])


if __name__ == "__main__":
    unittest.main()
