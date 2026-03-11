from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Awaitable, Callable, Optional

import aiohttp
import websockets

from kabu_hft.config import OrderProfile

logger = logging.getLogger("kabu.gateway")

try:
    import orjson
except ImportError:  # pragma: no cover - optional speedup
    orjson = None


def _loads(payload: str | bytes) -> Any:
    if orjson is not None:
        return orjson.loads(payload)
    return json.loads(payload)


def _parse_float(value: Any, default: float = 0.0) -> float:
    if value in (None, ""):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_int(value: Any, default: int = 0) -> int:
    if value in (None, ""):
        return default
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _to_ns(ts_str: str | None) -> int:
    if not ts_str:
        return time.time_ns()
    try:
        return int(datetime.fromisoformat(ts_str).timestamp() * 1_000_000_000)
    except ValueError:
        return time.time_ns()


def _kabu_side(internal_side: int) -> str:
    if internal_side > 0:
        return "2"
    if internal_side < 0:
        return "1"
    raise ValueError("internal side must be +1 or -1")


def _internal_side(raw_side: Any) -> int:
    side = str(raw_side)
    if side in {"2", "BUY", "Buy"}:
        return 1
    if side in {"1", "SELL", "Sell"}:
        return -1
    return 0


@dataclass(slots=True)
class Level:
    price: float
    size: int


@dataclass(slots=True)
class BoardSnapshot:
    symbol: str
    exchange: int
    ts_ns: int
    bid: float
    ask: float
    bid_size: int
    ask_size: int
    last: float
    last_size: int
    volume: int
    vwap: float
    bids: tuple[Level, ...] = field(default_factory=tuple)
    asks: tuple[Level, ...] = field(default_factory=tuple)
    prev_board: Optional["BoardSnapshot"] = field(default=None, repr=False)

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2.0

    @property
    def spread(self) -> float:
        return self.ask - self.bid

    @property
    def valid(self) -> bool:
        return self.bid > 0 and self.ask > 0 and self.bid < self.ask


@dataclass(slots=True)
class TradePrint:
    symbol: str
    exchange: int
    ts_ns: int
    price: float
    size: int
    side: int
    cumulative_volume: int


@dataclass(slots=True)
class OrderSnapshot:
    order_id: str
    side: int
    order_qty: int
    cum_qty: int
    leaves_qty: int
    price: float
    avg_fill_price: float
    state_code: int
    order_state_code: int
    is_final: bool
    raw: dict[str, Any] = field(repr=False, default_factory=dict)

    @property
    def status(self) -> str:
        if self.is_final and self.cum_qty >= self.order_qty > 0:
            return "filled"
        if self.is_final and self.cum_qty == 0:
            return "cancelled"
        if self.cum_qty > 0:
            return "partial"
        return "working"


@dataclass(slots=True)
class PositionLot:
    hold_id: str
    symbol: str
    side: int
    qty: int
    price: float


class KabuApiError(RuntimeError):
    def __init__(self, message: str, *, status: int = 0, payload: Any = None):
        super().__init__(message)
        self.status = status
        self.payload = payload


class KabuAdapter:
    @staticmethod
    def _parse_levels(raw: dict[str, Any], prefix: str, descending: bool) -> tuple[Level, ...]:
        levels: list[Level] = []
        for index in range(1, 11):
            entry = raw.get(f"{prefix}{index}")
            if not isinstance(entry, dict):
                continue
            price = _parse_float(entry.get("Price"))
            qty = _parse_int(entry.get("Qty"))
            if price > 0 and qty > 0:
                levels.append(Level(price=price, size=qty))
        levels.sort(key=lambda level: level.price, reverse=descending)
        return tuple(levels)

    @classmethod
    def board(cls, raw: dict[str, Any], prev: BoardSnapshot | None) -> BoardSnapshot | None:
        bids = cls._parse_levels(raw, "Buy", descending=True)
        asks = cls._parse_levels(raw, "Sell", descending=False)

        bid = _parse_float(raw.get("AskPrice")) or (bids[0].price if bids else 0.0)
        ask = _parse_float(raw.get("BidPrice")) or (asks[0].price if asks else 0.0)
        bid_size = _parse_int(raw.get("AskQty")) or (bids[0].size if bids else 0)
        ask_size = _parse_int(raw.get("BidQty")) or (asks[0].size if asks else 0)

        snapshot = BoardSnapshot(
            symbol=str(raw.get("Symbol", "")),
            exchange=_parse_int(raw.get("Exchange"), 1),
            ts_ns=_to_ns(
                raw.get("CurrentPriceTime")
                or raw.get("BidTime")
                or raw.get("AskTime")
            ),
            bid=bid,
            ask=ask,
            bid_size=bid_size,
            ask_size=ask_size,
            last=_parse_float(raw.get("CurrentPrice")),
            last_size=0,
            volume=_parse_int(raw.get("TradingVolume")),
            vwap=_parse_float(raw.get("VWAP")),
            bids=bids,
            asks=asks,
            prev_board=prev,
        )
        if not snapshot.valid:
            return None
        return snapshot

    @staticmethod
    def trade(
        raw: dict[str, Any],
        prev_board: BoardSnapshot | None,
        prev_volume: int,
        last_trade_price: float | None,
    ) -> TradePrint | None:
        cumulative_volume = _parse_int(raw.get("TradingVolume"))
        size = max(0, cumulative_volume - max(prev_volume, 0))
        if size <= 0:
            return None

        price = _parse_float(raw.get("CurrentPrice"))
        if price <= 0:
            return None

        side = 0
        if prev_board and prev_board.valid:
            if price >= prev_board.ask:
                side = 1
            elif price <= prev_board.bid:
                side = -1
            elif price > prev_board.mid:
                side = 1
            elif price < prev_board.mid:
                side = -1
            elif last_trade_price is not None:
                if price > last_trade_price:
                    side = 1
                elif price < last_trade_price:
                    side = -1

        return TradePrint(
            symbol=str(raw.get("Symbol", "")),
            exchange=_parse_int(raw.get("Exchange"), 1),
            ts_ns=_to_ns(raw.get("CurrentPriceTime")),
            price=price,
            size=size,
            side=side,
            cumulative_volume=cumulative_volume,
        )

    @staticmethod
    def order_snapshot(raw: dict[str, Any]) -> OrderSnapshot | None:
        order_id = str(raw.get("ID") or raw.get("OrderId") or "")
        if not order_id:
            return None

        order_qty = _parse_int(raw.get("OrderQty") or raw.get("Qty"))
        cum_qty = _parse_int(raw.get("CumQty"))
        price = _parse_float(raw.get("Price"))
        state_code = _parse_int(raw.get("State"))
        order_state_code = _parse_int(raw.get("OrderState"))
        is_final = state_code == 5 or order_state_code == 5
        details = raw.get("Details") or []

        weighted_value = 0.0
        weighted_qty = 0
        for detail in details:
            if not isinstance(detail, dict):
                continue
            detail_qty = _parse_int(detail.get("Qty"))
            detail_price = _parse_float(detail.get("Price"))
            if detail_qty > 0 and detail_price > 0:
                weighted_value += detail_qty * detail_price
                weighted_qty += detail_qty

        avg_fill_price = 0.0
        if weighted_qty > 0:
            avg_fill_price = weighted_value / weighted_qty
            if cum_qty == 0:
                cum_qty = weighted_qty
        elif cum_qty > 0 and price > 0:
            avg_fill_price = price

        leaves_qty = max(order_qty - cum_qty, 0)
        return OrderSnapshot(
            order_id=order_id,
            side=_internal_side(raw.get("Side")),
            order_qty=order_qty,
            cum_qty=cum_qty,
            leaves_qty=leaves_qty,
            price=price,
            avg_fill_price=avg_fill_price,
            state_code=state_code,
            order_state_code=order_state_code,
            is_final=is_final,
            raw=raw,
        )

    @staticmethod
    def position_lot(raw: dict[str, Any]) -> PositionLot | None:
        hold_id = str(raw.get("ExecutionID") or raw.get("HoldID") or "")
        symbol = str(raw.get("Symbol") or "")
        qty = _parse_int(raw.get("LeavesQty") or raw.get("HoldQty") or raw.get("Qty"))
        if not hold_id or not symbol or qty <= 0:
            return None
        return PositionLot(
            hold_id=hold_id,
            symbol=symbol,
            side=_internal_side(raw.get("Side")),
            qty=qty,
            price=_parse_float(raw.get("Price") or raw.get("ExecutionPrice")),
        )


class _TokenBucket:
    """Simple token-bucket rate limiter for outgoing REST requests."""

    def __init__(self, rate_per_sec: float) -> None:
        self._rate = max(rate_per_sec, 0.1)
        self._interval = 1.0 / self._rate
        self._next_allowed: float = 0.0

    async def acquire(self) -> None:
        now = time.monotonic()
        wait = self._next_allowed - now
        if wait > 0:
            await asyncio.sleep(wait)
        self._next_allowed = max(self._next_allowed, time.monotonic()) + self._interval


class KabuRestClient:
    def __init__(self, base_url: str, rate_per_sec: float = 4.0):
        self.base_url = base_url.rstrip("/")
        self._token: str | None = None
        self._password: str | None = None
        self._session: aiohttp.ClientSession | None = None
        self._bucket = _TokenBucket(rate_per_sec)

    async def start(self) -> None:
        if self._session is None:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=5),
                connector=aiohttp.TCPConnector(limit=16, keepalive_timeout=20),
            )

    async def stop(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def get_token(self, password: str) -> str:
        data = await self._request_json(
            "POST",
            "/kabusapi/token",
            json_body={"APIPassword": password},
            include_token=False,
        )
        token = str(data.get("Token") or "")
        if not token:
            raise KabuApiError("token response missing Token", payload=data)
        self._token = token
        self._password = password
        return token

    async def register_symbols(self, symbols: list[dict[str, Any]]) -> dict[str, Any]:
        return await self._request_json("PUT", "/kabusapi/register", json_body={"Symbols": symbols})

    async def get_orders(self, order_id: str | None = None, product: int = 0) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"product": product}
        if order_id:
            params["id"] = order_id
        data = await self._request_json("GET", "/kabusapi/orders", params=params)
        return data if isinstance(data, list) else [data]

    async def get_positions(self, symbol: str | None = None, product: int = 2) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"product": product}
        if symbol:
            params["symbol"] = symbol
        data = await self._request_json("GET", "/kabusapi/positions", params=params)
        return data if isinstance(data, list) else [data]

    async def cancel_order(self, order_id: str) -> dict[str, Any]:
        return await self._request_json(
            "PUT",
            "/kabusapi/cancelorder",
            json_body={"OrderId": order_id, "Password": self._password or ""},
        )

    async def send_entry_order(
        self,
        *,
        symbol: str,
        exchange: int,
        side: int,
        qty: int,
        price: float,
        is_market: bool,
        profile: OrderProfile,
    ) -> dict[str, Any]:
        front_order_type = (
            profile.front_order_type_market if is_market else profile.front_order_type_limit
        )
        body: dict[str, Any] = {
            "Password": "",
            "Symbol": symbol,
            "Exchange": exchange,
            "SecurityType": 1,
            "Side": _kabu_side(side),
            "Qty": qty,
            "FrontOrderType": front_order_type,
            "Price": 0 if is_market else price,
            "ExpireDay": 0,
            "AccountType": profile.account_type,
        }

        if profile.mode == "margin_daytrade":
            body.update(
                {
                    "CashMargin": 2,
                    "MarginTradeType": profile.margin_trade_type,
                    "DelivType": profile.margin_open_deliv_type,
                    "FundType": profile.margin_open_fund_type,
                }
            )
        else:
            if side < 0 and not profile.allow_short:
                raise ValueError("cash mode does not support opening short inventory")
            body.update(
                {
                    "CashMargin": 1,
                    "DelivType": profile.cash_buy_deliv_type if side > 0 else profile.cash_sell_deliv_type,
                    "FundType": profile.cash_buy_fund_type if side > 0 else profile.cash_sell_fund_type,
                }
            )

        return await self._request_json("POST", "/kabusapi/sendorder", json_body=body)

    async def send_exit_order(
        self,
        *,
        symbol: str,
        exchange: int,
        position_side: int,
        qty: int,
        price: float,
        is_market: bool,
        profile: OrderProfile,
    ) -> dict[str, Any]:
        front_order_type = (
            profile.front_order_type_market if is_market else profile.front_order_type_limit
        )
        broker_side = -position_side
        body: dict[str, Any] = {
            "Password": "",
            "Symbol": symbol,
            "Exchange": exchange,
            "SecurityType": 1,
            "Side": _kabu_side(broker_side),
            "Qty": qty,
            "FrontOrderType": front_order_type,
            "Price": 0 if is_market else price,
            "ExpireDay": 0,
            "AccountType": profile.account_type,
        }

        if profile.mode == "margin_daytrade":
            body.update(
                {
                    "CashMargin": 3,
                    "MarginTradeType": profile.margin_trade_type,
                    "DelivType": profile.margin_close_deliv_type,
                    "ClosePositions": await self._build_close_positions(symbol, position_side, qty),
                }
            )
        else:
            body.update(
                {
                    "CashMargin": 1,
                    "DelivType": profile.cash_buy_deliv_type if broker_side > 0 else profile.cash_sell_deliv_type,
                    "FundType": profile.cash_buy_fund_type if broker_side > 0 else profile.cash_sell_fund_type,
                }
            )

        return await self._request_json("POST", "/kabusapi/sendorder", json_body=body)

    async def _build_close_positions(
        self,
        symbol: str,
        position_side: int,
        qty: int,
    ) -> list[dict[str, Any]]:
        positions = [KabuAdapter.position_lot(raw) for raw in await self.get_positions(symbol)]
        usable_positions = [pos for pos in positions if pos and pos.side == position_side]

        remaining = qty
        close_positions: list[dict[str, Any]] = []
        for position in usable_positions:
            take_qty = min(position.qty, remaining)
            close_positions.append({"HoldID": position.hold_id, "Qty": take_qty})
            remaining -= take_qty
            if remaining == 0:
                break

        if remaining > 0:
            raise KabuApiError(
                f"not enough inventory to close {symbol} qty={qty}",
                payload=[
                    {
                        "hold_id": position.hold_id,
                        "symbol": position.symbol,
                        "side": position.side,
                        "qty": position.qty,
                        "price": position.price,
                    }
                    for position in usable_positions
                ],
            )
        return close_positions

    async def _request_json(
        self,
        method: str,
        path: str,
        *,
        json_body: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        include_token: bool = True,
    ) -> Any:
        if self._session is None:
            raise RuntimeError("REST session has not been started")

        await self._bucket.acquire()

        url = f"{self.base_url}{path}"
        headers = {"Content-Type": "application/json"}
        if include_token:
            headers["X-API-KEY"] = self._token or ""

        for attempt in range(3):
            async with self._session.request(
                method,
                url,
                json=json_body,
                params=params,
                headers=headers,
            ) as response:
                text = await response.text()
                payload = _loads(text) if text else {}
                if response.status < 400:
                    return payload

                should_retry = response.status in {429, 500, 502, 503, 504}
                if should_retry and attempt < 2:
                    await asyncio.sleep(0.1 * (2**attempt))
                    continue

                raise KabuApiError(
                    f"{method} {path} failed with status {response.status}",
                    status=response.status,
                    payload=payload,
                )


class KabuWebSocket:
    def __init__(
        self,
        *,
        url: str,
        on_board: Callable[[BoardSnapshot], None],
        on_trade: Callable[[TradePrint], None] | None = None,
        on_reconnect: Callable[[], Awaitable[None]] | None = None,
        on_raw: Callable[[str, dict[str, Any], int], None] | None = None,
    ):
        self._url = url
        self._on_board = on_board
        self._on_trade = on_trade
        self._on_reconnect = on_reconnect
        self._on_raw = on_raw
        self._running = False
        self._ws: websockets.ClientConnection | None = None
        self._snapshots: dict[str, BoardSnapshot] = {}
        self._volumes: dict[str, int] = {}
        self._last_trade_price: dict[str, float] = {}

    async def run(self) -> None:
        self._running = True
        retry_sleep = 1.0
        while self._running:
            try:
                async with websockets.connect(
                    self._url,
                    ping_interval=20,
                    ping_timeout=10,
                    max_size=2**20,
                ) as connection:
                    self._ws = connection
                    retry_sleep = 1.0
                    logger.info("websocket connected: %s", self._url)
                    if self._on_reconnect is not None:
                        try:
                            await self._on_reconnect()
                        except Exception as exc:  # pragma: no cover
                            logger.warning("on_reconnect callback failed: %s", exc)
                    async for raw_message in connection:
                        if not self._running:
                            break
                        self._dispatch(raw_message)
            except Exception as exc:  # pragma: no cover - network dependent
                if not self._running:
                    break
                logger.warning("websocket disconnected: %s", exc)
                await asyncio.sleep(retry_sleep)
                retry_sleep = min(retry_sleep * 2, 5.0)
            finally:
                self._ws = None

    def stop(self) -> None:
        self._running = False
        if self._ws is not None:
            asyncio.create_task(self._ws.close())

    def _dispatch(self, raw_message: str | bytes) -> None:
        recv_ns = time.time_ns()
        data = _loads(raw_message)
        if not isinstance(data, dict):
            return

        symbol = str(data.get("Symbol", ""))

        # Notify raw callback BEFORE normalization so recorder captures kabu field names.
        if self._on_raw is not None:
            try:
                self._on_raw(symbol, data, recv_ns)
            except Exception as exc:  # pragma: no cover
                logger.warning("on_raw callback error for %s: %s", symbol, exc)

        prev_snapshot = self._snapshots.get(symbol)
        snapshot = KabuAdapter.board(data, prev_snapshot)
        if snapshot is None:
            return

        latency_ms = max(0.0, (recv_ns - snapshot.ts_ns) / 1_000_000)
        if latency_ms > 500:
            logger.warning("market data latency %.1fms for %s", latency_ms, symbol)

        prev_volume = self._volumes.get(symbol, snapshot.volume)
        self._snapshots[symbol] = snapshot
        self._volumes[symbol] = snapshot.volume
        self._on_board(snapshot)

        if self._on_trade:
            trade = KabuAdapter.trade(
                data,
                prev_snapshot,
                prev_volume=prev_volume,
                last_trade_price=self._last_trade_price.get(symbol),
            )
            if trade is not None:
                self._last_trade_price[symbol] = trade.price
                self._on_trade(trade)
