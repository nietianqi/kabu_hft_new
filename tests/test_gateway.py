import unittest

from kabu_hft.gateway import KabuAdapter


class GatewayAdapterTests(unittest.TestCase):
    def test_normalizes_reversed_bid_ask_semantics(self) -> None:
        raw = {
            "Symbol": "9984",
            "Exchange": 1,
            "AskPrice": 9980,
            "AskQty": 400,
            "BidPrice": 9990,
            "BidQty": 500,
            "Buy1": {"Price": 9980, "Qty": 400},
            "Sell1": {"Price": 9990, "Qty": 500},
            "CurrentPrice": 9985,
            "CurrentPriceTime": "2026-03-11T09:00:00+09:00",
            "TradingVolume": 1000,
        }

        snapshot = KabuAdapter.board(raw, None)
        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual(snapshot.bid, 9980.0)
        self.assertEqual(snapshot.ask, 9990.0)
        self.assertEqual(snapshot.bid_size, 400)
        self.assertEqual(snapshot.ask_size, 500)
        self.assertEqual(snapshot.bids[0].price, 9980.0)
        self.assertEqual(snapshot.asks[0].price, 9990.0)

    def test_trade_size_uses_volume_delta(self) -> None:
        raw = {
            "Symbol": "9984",
            "Exchange": 1,
            "CurrentPrice": 9990,
            "CurrentPriceTime": "2026-03-11T09:00:01+09:00",
            "TradingVolume": 1200,
        }
        trade = KabuAdapter.trade(raw, None, prev_volume=1000, last_trade_price=9985)
        self.assertIsNotNone(trade)
        assert trade is not None
        self.assertEqual(trade.size, 200)
        self.assertEqual(trade.price, 9990.0)


if __name__ == "__main__":
    unittest.main()
