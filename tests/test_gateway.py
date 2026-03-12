import json
import unittest
from datetime import datetime, timedelta, timezone

from kabu_hft.config import OrderProfile
from kabu_hft.gateway import KabuAdapter, KabuRestClient, KabuWebSocket


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

    def test_trade_prefers_trading_volume_time_when_present(self) -> None:
        raw = {
            "Symbol": "9984",
            "Exchange": 1,
            "CurrentPrice": 9990,
            "CurrentPriceTime": "2026-03-11T09:00:01+09:00",
            "TradingVolumeTime": "2026-03-11T09:00:02+09:00",
            "TradingVolume": 1200,
        }
        trade = KabuAdapter.trade(raw, None, prev_volume=1000, last_trade_price=9985)
        self.assertIsNotNone(trade)
        assert trade is not None
        self.assertEqual(
            trade.ts_ns,
            int(datetime.fromisoformat("2026-03-11T09:00:02+09:00").timestamp() * 1_000_000_000),
        )


class GatewayTransportTests(unittest.IsolatedAsyncioTestCase):
    async def test_sendorder_uses_stored_password(self) -> None:
        captured: dict = {}

        async def fake_request(method, path, **kwargs):
            captured["method"] = method
            captured["path"] = path
            captured["json_body"] = kwargs.get("json_body")
            return {"OrderId": "ORDER-1"}

        client = KabuRestClient("http://localhost:18080")
        client._password = "abc123"  # type: ignore[attr-defined]
        client._request_json = fake_request  # type: ignore[method-assign]

        await client.send_entry_order(
            symbol="9984",
            exchange=1,
            side=1,
            qty=100,
            price=9980.0,
            is_market=False,
            profile=OrderProfile(),
        )
        self.assertEqual(captured["method"], "POST")
        self.assertEqual(captured["path"], "/kabusapi/sendorder")
        self.assertEqual(captured["json_body"]["Password"], "abc123")

    async def test_ws_drops_duplicate_and_out_of_order_quote(self) -> None:
        board_events = []
        jst = timezone(timedelta(hours=9))
        current_ts = datetime.now(jst)

        ws = KabuWebSocket(
            url="ws://localhost:18080/kabusapi/websocket",
            on_board=lambda snapshot: board_events.append(snapshot),
            on_trade=None,
        )

        base = {
            "Symbol": "9984",
            "Exchange": 1,
            "AskPrice": 9980,
            "AskQty": 400,
            "BidPrice": 9990,
            "BidQty": 500,
            "Buy1": {"Price": 9980, "Qty": 400},
            "Sell1": {"Price": 9990, "Qty": 500},
            "CurrentPrice": 9985,
            "TradingVolume": 1000,
        }
        first = {**base, "CurrentPriceTime": current_ts.isoformat()}
        duplicate = {**base, "CurrentPriceTime": current_ts.isoformat()}
        out_of_order = {**base, "CurrentPriceTime": (current_ts - timedelta(seconds=1)).isoformat()}

        ws._dispatch(json.dumps(first))
        ws._dispatch(json.dumps(duplicate))
        ws._dispatch(json.dumps(out_of_order))

        self.assertEqual(len(board_events), 1)


if __name__ == "__main__":
    unittest.main()
