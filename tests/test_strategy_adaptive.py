import unittest

from kabu_hft.config import load_config
from kabu_hft.core import HFTStrategy
from kabu_hft.gateway import BoardSnapshot, KabuRestClient, Level


class DummyRestClient(KabuRestClient):
    def __init__(self) -> None:
        super().__init__("http://localhost:18080")


def _snapshot() -> BoardSnapshot:
    return BoardSnapshot(
        symbol="9984",
        exchange=1,
        ts_ns=1_000_000_000,
        bid=100.0,
        ask=101.0,
        bid_size=500,
        ask_size=500,
        last=100.5,
        last_size=0,
        volume=1000,
        vwap=100.5,
        bids=(Level(100.0, 500),),
        asks=(Level(101.0, 500),),
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


if __name__ == "__main__":
    unittest.main()
