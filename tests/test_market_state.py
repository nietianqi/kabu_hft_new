import time
import unittest

from kabu_hft.adapter import KabuNormalizer
from kabu_hft.market import BookState


def _raw(ts: str, *, ask_price: float = 100.0, bid_price: float = 101.0, volume: int = 1000) -> dict:
    return {
        "Symbol": "9984",
        "Exchange": 1,
        "AskPrice": ask_price,
        "AskQty": 500,
        "BidPrice": bid_price,
        "BidQty": 300,
        "CurrentPrice": (ask_price + bid_price) / 2,
        "CurrentPriceTime": ts,
        "TradingVolume": volume,
    }


class MarketStateTests(unittest.TestCase):
    def test_update_rejects_duplicate_and_out_of_order(self) -> None:
        state = BookState()
        first = KabuNormalizer.normalize_board(_raw("2026-03-11T09:00:01+09:00"), None)
        assert first is not None
        self.assertTrue(state.update(first))

        duplicate = KabuNormalizer.normalize_board(_raw("2026-03-11T09:00:01+09:00"), first)
        assert duplicate is not None
        self.assertFalse(state.update(duplicate))

        older = KabuNormalizer.normalize_board(_raw("2026-03-11T09:00:00+09:00"), first)
        assert older is not None
        self.assertFalse(state.update(older))

        self.assertEqual(state.duplicate_count, 1)
        self.assertEqual(state.out_of_order_count, 1)

    def test_health_reports_stale(self) -> None:
        state = BookState()
        first = KabuNormalizer.normalize_board(_raw("2026-03-11T09:00:01+09:00"), None)
        assert first is not None
        self.assertTrue(state.update(first))

        now_ns = state.last_update_ns + 2_000_000_000
        health = state.health(stale_ms=500, now_ns=now_ns)
        self.assertTrue(health.is_stale)
        self.assertGreaterEqual(health.last_quote_age_ms, 2000.0)


if __name__ == "__main__":
    unittest.main()
