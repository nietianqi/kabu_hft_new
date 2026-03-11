import unittest

from kabu_hft.adapter import KabuNormalizer


class NormalizerTests(unittest.TestCase):
    def test_reversed_bid_ask_is_normalized(self) -> None:
        raw = {
            "Symbol": "9984",
            "Exchange": 1,
            "AskPrice": 100.0,
            "AskQty": 500,
            "BidPrice": 101.0,
            "BidQty": 300,
            "Buy1": {"Price": 100.0, "Qty": 500},
            "Sell1": {"Price": 101.0, "Qty": 300},
            "CurrentPriceTime": "2026-03-11T09:00:01+09:00",
            "TradingVolume": 1000,
        }
        snapshot = KabuNormalizer.normalize_board(raw, None)
        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual(snapshot.best_bid, 100.0)
        self.assertEqual(snapshot.best_ask, 101.0)
        self.assertTrue(snapshot.valid)

    def test_duplicate_and_out_of_order_flags(self) -> None:
        first_raw = {
            "Symbol": "9984",
            "Exchange": 1,
            "AskPrice": 100.0,
            "AskQty": 500,
            "BidPrice": 101.0,
            "BidQty": 300,
            "CurrentPriceTime": "2026-03-11T09:00:01+09:00",
            "TradingVolume": 1000,
        }
        duplicate_raw = dict(first_raw)
        older_raw = dict(first_raw)
        older_raw["CurrentPriceTime"] = "2026-03-11T09:00:00+09:00"

        first = KabuNormalizer.normalize_board(first_raw, None)
        assert first is not None
        duplicate = KabuNormalizer.normalize_board(duplicate_raw, first)
        older = KabuNormalizer.normalize_board(older_raw, first)
        self.assertTrue(duplicate.duplicate)  # type: ignore[union-attr]
        self.assertTrue(older.out_of_order)  # type: ignore[union-attr]

    def test_trade_is_volume_delta_based(self) -> None:
        prev_book = KabuNormalizer.normalize_board(
            {
                "Symbol": "9984",
                "Exchange": 1,
                "AskPrice": 100.0,
                "AskQty": 500,
                "BidPrice": 101.0,
                "BidQty": 300,
                "CurrentPriceTime": "2026-03-11T09:00:01+09:00",
                "TradingVolume": 1000,
            },
            None,
        )
        assert prev_book is not None
        trade = KabuNormalizer.normalize_trade(
            {
                "Symbol": "9984",
                "Exchange": 1,
                "CurrentPrice": 101.0,
                "CurrentPriceTime": "2026-03-11T09:00:02+09:00",
                "TradingVolume": 1150,
            },
            prev_book=prev_book,
            prev_cum_volume=1000,
            last_trade_price=100.5,
        )
        self.assertIsNotNone(trade)
        assert trade is not None
        self.assertEqual(trade.size, 150)
        self.assertEqual(trade.side, 1)


if __name__ == "__main__":
    unittest.main()
