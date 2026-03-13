import unittest

from kabu_hft.config import SignalWeights
from kabu_hft.gateway import BoardSnapshot, Level, TradePrint
from kabu_hft.signals.microstructure import OnlineZScore
from kabu_hft.signals import SignalStack


class SignalStackTests(unittest.TestCase):
    def test_online_zscore_warmup_adapts_to_window(self) -> None:
        zscore = OnlineZScore(window=20)
        for value in range(19):
            self.assertEqual(zscore.update(float(value)), 0.0)
        self.assertGreater(zscore.update(19.0), 0.0)

    def test_positive_order_flow_produces_positive_composite(self) -> None:
        stack = SignalStack(
            obi_depth=3,
            obi_decay=0.8,
            lob_ofi_depth=3,
            lob_ofi_decay=0.8,
            tape_window_sec=10,
            mp_ema_alpha=0.1,
            tick_size=1.0,
            zscore_window=20,
            weights=SignalWeights(),
        )

        prev = None
        last_signal = None
        for index in range(30):
            snapshot = BoardSnapshot(
                symbol="9984",
                exchange=1,
                ts_ns=1_000_000_000 + index,
                bid=100.0 + 0.1 * index,
                ask=101.0 + 0.1 * index,
                bid_size=900 + index * 10,
                ask_size=400,
                last=100.5,
                last_size=0,
                volume=1_000 + index,
                vwap=100.5,
                bids=(Level(100.0 + 0.1 * index, 900 + index * 10), Level(99.0 + 0.1 * index, 700)),
                asks=(Level(101.0 + 0.1 * index, 400), Level(102.0 + 0.1 * index, 350)),
                prev_board=prev,
            )
            stack.on_trade(
                TradePrint(
                    symbol="9984",
                    exchange=1,
                    ts_ns=snapshot.ts_ns,
                    price=snapshot.ask,
                    size=100,
                    side=1,
                    cumulative_volume=snapshot.volume,
                )
            )
            last_signal = stack.on_board(snapshot)
            prev = snapshot

        self.assertIsNotNone(last_signal)
        assert last_signal is not None
        self.assertGreater(last_signal.obi_raw, 0)
        self.assertGreater(last_signal.composite, 0)


if __name__ == "__main__":
    unittest.main()
