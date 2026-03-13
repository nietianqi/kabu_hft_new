import json
import unittest
from datetime import datetime, timedelta, timezone

from kabu_hft.config import OrderProfile
from kabu_hft.gateway import KabuAdapter, KabuApiError, KabuRestClient, KabuWebSocket


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

    def test_board_timestamp_prefers_bid_or_ask_time_over_current_price_time(self) -> None:
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
            "BidTime": "2026-03-11T09:00:01+09:00",
            "AskTime": "2026-03-11T09:00:00.500000+09:00",
            "TradingVolume": 1000,
        }
        snapshot = KabuAdapter.board(raw, None)
        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual(snapshot.ts_source, "bid_time")
        self.assertEqual(
            snapshot.ts_ns,
            int(datetime.fromisoformat("2026-03-11T09:00:01+09:00").timestamp() * 1_000_000_000),
        )

    def test_board_timestamp_without_exchange_time_uses_zero(self) -> None:
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
            "TradingVolume": 1000,
        }
        snapshot = KabuAdapter.board(raw, None)
        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual(snapshot.ts_ns, 0)
        self.assertEqual(snapshot.ts_source, "no_exchange_time")

    def test_order_snapshot_does_not_infer_cum_qty_from_non_fill_details(self) -> None:
        raw = {
            "ID": "ORDER-1",
            "State": 2,
            "OrderState": 2,
            "Side": "2",
            "OrderQty": 100,
            "CumQty": 0,
            "Price": 1000.0,
            "Details": [
                {
                    "RecType": 1,  # new/accepted record, not an execution fill
                    "Qty": 100,
                    "Price": 1000.0,
                    "ExecutionID": "",
                    "ExecutionDay": "2026-03-13T10:00:00+09:00",
                }
            ],
        }
        snapshot = KabuAdapter.order_snapshot(raw)
        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual(snapshot.cum_qty, 0)
        self.assertEqual(snapshot.avg_fill_price, 0.0)
        self.assertEqual(snapshot.status, "working")

    def test_order_snapshot_uses_fill_details_for_average_price(self) -> None:
        raw = {
            "ID": "ORDER-2",
            "State": 3,
            "OrderState": 3,
            "Side": "2",
            "OrderQty": 100,
            "CumQty": 80,
            "Price": 1000.0,
            "Details": [
                {
                    "RecType": 8,
                    "Qty": 50,
                    "Price": 1001.0,
                    "ExecutionID": "E1",
                    "ExecutionDay": "2026-03-13T10:00:00+09:00",
                },
                {
                    "RecType": 8,
                    "Qty": 30,
                    "Price": 1002.0,
                    "ExecutionID": "E2",
                    "ExecutionDay": "2026-03-13T10:00:01+09:00",
                },
            ],
        }
        snapshot = KabuAdapter.order_snapshot(raw)
        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual(snapshot.cum_qty, 80)
        self.assertAlmostEqual(snapshot.avg_fill_price, (50 * 1001.0 + 30 * 1002.0) / 80)
        self.assertGreater(snapshot.fill_ts_ns, 0)


class GatewayTransportTests(unittest.IsolatedAsyncioTestCase):
    async def test_request_json_does_not_retry_order_mutation_500(self) -> None:
        class _Response:
            def __init__(self, status: int, payload: dict):
                self.status = status
                self._payload = payload

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

            async def text(self) -> str:
                return json.dumps(self._payload)

        class _Session:
            def __init__(self):
                self.calls = 0

            def request(self, *_args, **_kwargs):
                self.calls += 1
                return _Response(500, {"Code": 500001, "Message": "temporary server error"})

        client = KabuRestClient("http://localhost:18080")
        client._token = "token"  # type: ignore[attr-defined]
        client._session = _Session()  # type: ignore[attr-defined]

        with self.assertRaises(KabuApiError):
            await client._request_json("POST", "/kabusapi/sendorder", json_body={"Price": 100.0})

        self.assertEqual(client._session.calls, 1)  # type: ignore[attr-defined]

    async def test_request_json_retries_non_order_api(self) -> None:
        class _Response:
            def __init__(self, status: int, payload: dict):
                self.status = status
                self._payload = payload

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

            async def text(self) -> str:
                return json.dumps(self._payload)

        class _Session:
            def __init__(self):
                self.calls = 0
                self.responses = [
                    (500, {"Code": 500001}),
                    (503, {"Code": 500002}),
                    (200, {"ok": True}),
                ]

            def request(self, *_args, **_kwargs):
                self.calls += 1
                status, payload = self.responses[min(self.calls - 1, len(self.responses) - 1)]
                return _Response(status, payload)

        client = KabuRestClient("http://localhost:18080")
        client._token = "token"  # type: ignore[attr-defined]
        client._session = _Session()  # type: ignore[attr-defined]

        result = await client._request_json("GET", "/kabusapi/orders", params={"product": 0})
        self.assertEqual(result, {"ok": True})
        self.assertEqual(client._session.calls, 3)  # type: ignore[attr-defined]

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

    async def test_margin_mode_uses_credit_branch(self) -> None:
        captured: dict = {}

        async def fake_request(method, path, **kwargs):
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
            profile=OrderProfile(mode="margin"),
        )
        self.assertEqual(captured["json_body"]["CashMargin"], 2)
        self.assertEqual(captured["json_body"]["MarginTradeType"], OrderProfile().margin_trade_type)

    async def test_unknown_mode_is_rejected(self) -> None:
        client = KabuRestClient("http://localhost:18080")
        client._password = "abc123"  # type: ignore[attr-defined]

        async def fake_request(*_args, **_kwargs):
            return {"OrderId": "ORDER-1"}

        client._request_json = fake_request  # type: ignore[method-assign]
        with self.assertRaises(ValueError):
            await client.send_entry_order(
                symbol="9984",
                exchange=1,
                side=1,
                qty=100,
                price=9980.0,
                is_market=False,
                profile=OrderProfile(mode="marginn"),
            )

    async def test_sendorder_retries_exchange_27_on_tse_plus_suppression(self) -> None:
        sent_bodies: list[dict] = []

        async def fake_request(_method, _path, **kwargs):
            body = kwargs.get("json_body") or {}
            sent_bodies.append(dict(body))
            if body.get("Exchange") == 1:
                raise KabuApiError(
                    "POST /kabusapi/sendorder failed with status 500",
                    status=500,
                    payload={"Code": 100378, "Message": "現物買・売注文抑止エラー"},  # 现货买卖订单抑制错误
                )
            return {"OrderId": "ORDER-1"}

        client = KabuRestClient("http://localhost:18080")
        client._password = "abc123"  # type: ignore[attr-defined]
        client._request_json = fake_request  # type: ignore[method-assign]

        result = await client.send_entry_order(
            symbol="9984",
            exchange=1,
            side=1,
            qty=100,
            price=9980.0,
            is_market=False,
            profile=OrderProfile(mode="cash"),
        )

        self.assertEqual(result.get("OrderId"), "ORDER-1")
        self.assertEqual(len(sent_bodies), 2)
        self.assertEqual(sent_bodies[0].get("Exchange"), 1)
        self.assertEqual(sent_bodies[1].get("Exchange"), 27)

    async def test_sendorder_does_not_retry_on_non_tse_plus_error(self) -> None:
        sent_bodies: list[dict] = []

        async def fake_request(_method, _path, **kwargs):
            body = kwargs.get("json_body") or {}
            sent_bodies.append(dict(body))
            raise KabuApiError(
                "POST /kabusapi/sendorder failed with status 400",
                status=400,
                payload={"Code": 4001002, "Message": "invalid param"},
            )

        client = KabuRestClient("http://localhost:18080")
        client._password = "abc123"  # type: ignore[attr-defined]
        client._request_json = fake_request  # type: ignore[method-assign]

        with self.assertRaises(KabuApiError):
            await client.send_entry_order(
                symbol="9984",
                exchange=1,
                side=1,
                qty=100,
                price=9980.0,
                is_market=False,
                profile=OrderProfile(mode="cash"),
            )
        self.assertEqual(len(sent_bodies), 1)

    async def test_margin_exit_uses_close_positions_for_matching_exchange(self) -> None:
        captured: dict = {}

        async def fake_request(_method, _path, **kwargs):
            captured["json_body"] = kwargs.get("json_body")
            return {"OrderId": "ORDER-EXIT-1"}

        async def fake_positions(symbol=None, product=2):
            _ = (symbol, product)
            return [
                {"ExecutionID": "HOLD-1", "Symbol": "9984", "Exchange": 1, "Side": "2", "LeavesQty": 100, "Price": 1000},
                {"ExecutionID": "HOLD-27", "Symbol": "9984", "Exchange": 27, "Side": "2", "LeavesQty": 100, "Price": 1000},
            ]

        client = KabuRestClient("http://localhost:18080")
        client._password = "abc123"  # type: ignore[attr-defined]
        client._request_json = fake_request  # type: ignore[method-assign]
        client.get_positions = fake_positions  # type: ignore[method-assign]

        await client.send_exit_order(
            symbol="9984",
            exchange=27,
            position_side=1,
            qty=100,
            price=9980.0,
            is_market=False,
            profile=OrderProfile(mode="margin"),
        )
        body = captured["json_body"]
        self.assertEqual(body["Exchange"], 27)
        self.assertEqual(body["ClosePositions"], [{"HoldID": "HOLD-27", "Qty": 100}])

    async def test_margin_exit_retries_once_on_code8_and_prefers_hold_id(self) -> None:
        sent_bodies: list[dict] = []

        async def fake_request(_method, _path, **kwargs):
            body = kwargs.get("json_body") or {}
            sent_bodies.append(dict(body))
            if len(sent_bodies) == 1:
                raise KabuApiError(
                    "POST /kabusapi/sendorder failed with status 500",
                    status=500,
                    payload={"Code": 8, "Message": "決済指定内容に誤りがあります"},
                )
            return {"OrderId": "ORDER-EXIT-RETRY"}

        async def fake_positions(symbol=None, product=2):
            _ = (symbol, product)
            return [
                {
                    "HoldID": "HOLD-REAL",
                    "ExecutionID": "EXEC-LEGACY",
                    "Symbol": "9984",
                    "Exchange": 27,
                    "Side": "2",
                    "LeavesQty": 100,
                    "HoldQty": 0,
                    "Price": 1000,
                    "MarginTradeType": 1,
                }
            ]

        client = KabuRestClient("http://localhost:18080")
        client._password = "abc123"  # type: ignore[attr-defined]
        client._request_json = fake_request  # type: ignore[method-assign]
        client.get_positions = fake_positions  # type: ignore[method-assign]

        result = await client.send_exit_order(
            symbol="9984",
            exchange=27,
            position_side=1,
            qty=100,
            price=9980.0,
            is_market=False,
            profile=OrderProfile(mode="margin", margin_trade_type=3),
        )

        self.assertEqual(result.get("OrderId"), "ORDER-EXIT-RETRY")
        self.assertEqual(len(sent_bodies), 2)
        self.assertEqual(sent_bodies[0]["ClosePositions"], [{"HoldID": "HOLD-REAL", "Qty": 100}])
        self.assertEqual(sent_bodies[1]["ClosePositions"], [{"HoldID": "HOLD-REAL", "Qty": 100}])
        self.assertEqual(sent_bodies[0]["MarginTradeType"], 1)
        self.assertEqual(sent_bodies[1]["MarginTradeType"], 1)

    def test_position_lot_uses_leaves_qty_for_inventory(self) -> None:
        lot = KabuAdapter.position_lot(
            {
                "HoldID": "HOLD-REAL",
                "Symbol": "6532",
                "Exchange": 27,
                "Side": "2",
                "LeavesQty": 100,
                "HoldQty": 0,
                "Price": 4331.0,
            }
        )

        self.assertIsNotNone(lot)
        assert lot is not None
        self.assertEqual(lot.qty, 100)
        self.assertEqual(lot.closable_qty, 100)

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

    async def test_ws_reset_stream_state_clears_volume_baseline(self) -> None:
        ws = KabuWebSocket(
            url="ws://localhost:18080/kabusapi/websocket",
            on_board=lambda _snapshot: None,
            on_trade=None,
        )
        ws._volumes["9984"] = 123  # type: ignore[attr-defined]
        ws._snapshots["9984"] = object()  # type: ignore[attr-defined]
        ws._last_trade_price["9984"] = 9999.0  # type: ignore[attr-defined]
        ws._reset_stream_state()
        self.assertEqual(ws._volumes, {})  # type: ignore[attr-defined]
        self.assertEqual(ws._snapshots, {})  # type: ignore[attr-defined]
        self.assertEqual(ws._last_trade_price, {})  # type: ignore[attr-defined]


if __name__ == "__main__":
    unittest.main()
