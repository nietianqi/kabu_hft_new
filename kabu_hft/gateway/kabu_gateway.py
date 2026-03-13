from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Optional

from kabu_hft.config import OrderProfile

logger = logging.getLogger("kabu.gateway")

if TYPE_CHECKING:
    import aiohttp
    import websockets

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
        return 0
    try:
        return int(datetime.fromisoformat(ts_str).timestamp() * 1_000_000_000)
    except ValueError:
        return 0


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


_MARGIN_MODES: frozenset[str] = frozenset(
    {
        "margin",
        "margin_daytrade",
        "margin_general",
        "credit",
        "shinyo",
    }
)

_TSE_PLUS_RETRY_CODES: frozenset[int] = frozenset({100368, 100378})
_ORDER_MUTATION_PATHS: frozenset[str] = frozenset(
    {
        "/kabusapi/sendorder",
        "/kabusapi/sendorder/future",
        "/kabusapi/sendorder/option",
        "/kabusapi/cancelorder",
    }
)


def _is_margin_mode(mode: str) -> bool:
    normalized = str(mode or "").strip().lower()
    if normalized in {"", "cash", "spot"}:
        return False
    if normalized in _MARGIN_MODES:
        return True
    raise ValueError(
        f"unsupported order_profile.mode={mode!r}; expected one of cash/spot/margin variants"
    )


def _extract_error_code(payload: Any) -> int | None:
    candidates: list[Any] = []
    if isinstance(payload, dict):
        candidates.extend(
            [
                payload.get("Code"),
                payload.get("ResultCode"),
                payload.get("code"),
                payload.get("result_code"),
            ]
        )
    elif isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict):
                candidates.extend(
                    [
                        item.get("Code"),
                        item.get("ResultCode"),
                        item.get("code"),
                        item.get("result_code"),
                    ]
                )
    for raw in candidates:
        if raw in (None, ""):
            continue
        try:
            return int(raw)
        except (TypeError, ValueError):
            continue
    return None


def _extract_error_message(payload: Any) -> str | None:
    if isinstance(payload, dict):
        for key in ("Message", "Result", "message", "result"):
            value = payload.get(key)
            if value not in (None, ""):
                return str(value)
    elif isinstance(payload, list):
        for item in payload:
            if not isinstance(item, dict):
                continue
            for key in ("Message", "Result", "message", "result"):
                value = item.get(key)
                if value not in (None, ""):
                    return str(value)
    return None


def _is_fill_detail(detail: dict[str, Any]) -> bool:
    """Return True only for detail rows that represent execution fills.

    kabu orders Details can include multiple record types (new/order accepted/cancel/expire/etc).
    RecType=1 is often a new-order record and its Qty is the order quantity, not executed quantity.
    """
    rec_type = _parse_int(detail.get("RecType"))
    if rec_type in {3, 8}:
        return True
    execution_id = str(detail.get("ExecutionID") or "").strip()
    return execution_id.startswith("E")


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
    duplicate: bool = False
    out_of_order: bool = False
    # 气配标志（quote sign），来自 kabu Station API。
    # Note: field names are reversed in the API — AskSign carries the bid-side sign,
    # BidSign carries the ask-side sign.  We store them corrected here.
    bid_sign: str = ""   # from kabu AskSign (bid side, naming is reversed in API)
    ask_sign: str = ""   # from kabu BidSign (ask side, naming is reversed in API)
    bid_ts_ns: int = 0
    ask_ts_ns: int = 0
    current_ts_ns: int = 0
    ts_source: str = ""

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
    fill_ts_ns: int = 0  # Exchange fill timestamp; 0 if not available
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
    exchange: int
    side: int
    qty: int
    price: float


class KabuApiError(RuntimeError):
    def __init__(self, message: str, *, status: int = 0, payload: Any = None):
        super().__init__(message)
        self.status = status
        self.payload = payload

    def __str__(self) -> str:
        base = super().__str__()
        code = _extract_error_code(self.payload)
        message = _extract_error_message(self.payload)
        suffix_parts: list[str] = []
        if code is not None:
            suffix_parts.append(f"code={code}")
        if message:
            suffix_parts.append(f"message={message}")
        if suffix_parts:
            return f"{base} ({', '.join(suffix_parts)})"
        return base


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

        _bid_raw = _parse_float(raw.get("AskPrice"))
        bid = _bid_raw if _bid_raw > 0 else (bids[0].price if bids else 0.0)
        _ask_raw = _parse_float(raw.get("BidPrice"))
        ask = _ask_raw if _ask_raw > 0 else (asks[0].price if asks else 0.0)
        _bid_sz_raw = _parse_int(raw.get("AskQty"))
        bid_size = _bid_sz_raw if _bid_sz_raw > 0 else (bids[0].size if bids else 0)
        _ask_sz_raw = _parse_int(raw.get("BidQty"))
        ask_size = _ask_sz_raw if _ask_sz_raw > 0 else (asks[0].size if asks else 0)

        # 气配标志 — kabu API 字段名与实际含义相反：AskSign 存储买方气配，BidSign 存储卖方气配。
        bid_sign = str(raw.get("AskSign") or "")
        ask_sign = str(raw.get("BidSign") or "")

        current_ts_ns = _to_ns(raw.get("CurrentPriceTime"))
        bid_ts_ns = _to_ns(raw.get("BidTime"))
        ask_ts_ns = _to_ns(raw.get("AskTime"))
        if bid_ts_ns >= ask_ts_ns and bid_ts_ns >= current_ts_ns and bid_ts_ns > 0:
            ts_ns = bid_ts_ns
            ts_source = "bid_time"
        elif ask_ts_ns >= bid_ts_ns and ask_ts_ns >= current_ts_ns and ask_ts_ns > 0:
            ts_ns = ask_ts_ns
            ts_source = "ask_time"
        elif current_ts_ns > 0:
            ts_ns = current_ts_ns
            ts_source = "current_price_time"
        else:
            ts_ns = 0
            ts_source = "no_exchange_time"
        volume = _parse_int(raw.get("TradingVolume"))

        out_of_order = bool(
            prev is not None and ts_ns > 0 and prev.ts_ns > 0 and ts_ns < prev.ts_ns
        )
        duplicate = bool(
            prev is not None
            and not out_of_order
            and ts_ns == prev.ts_ns
            and bid == prev.bid
            and ask == prev.ask
            and bid_size == prev.bid_size
            and ask_size == prev.ask_size
            and volume == prev.volume
        )

        snapshot = BoardSnapshot(
            symbol=str(raw.get("Symbol", "")),
            exchange=_parse_int(raw.get("Exchange"), 1),
            ts_ns=ts_ns,
            bid=bid,
            ask=ask,
            bid_size=bid_size,
            ask_size=ask_size,
            last=_parse_float(raw.get("CurrentPrice")),
            last_size=0,
            volume=volume,
            vwap=_parse_float(raw.get("VWAP")),
            bids=bids,
            asks=asks,
            prev_board=prev,
            duplicate=duplicate,
            out_of_order=out_of_order,
            bid_sign=bid_sign,
            ask_sign=ask_sign,
            bid_ts_ns=bid_ts_ns,
            ask_ts_ns=ask_ts_ns,
            current_ts_ns=current_ts_ns,
            ts_source=ts_source,
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
            ts_ns=_to_ns(raw.get("TradingVolumeTime") or raw.get("CurrentPriceTime")),
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

        fill_weighted_value = 0.0
        fill_weighted_qty = 0
        latest_fill_ts_ns = 0
        for detail in details:
            if not isinstance(detail, dict):
                continue
            detail_ts = _to_ns(
                detail.get("ExecutionDay")
                or detail.get("TransactTime")
                or detail.get("RecvTime")
                or detail.get("Time")
            )
            if _is_fill_detail(detail):
                detail_qty = _parse_int(detail.get("Qty"))
                detail_price = _parse_float(detail.get("Price"))
                if detail_qty > 0 and detail_price > 0:
                    fill_weighted_value += detail_qty * detail_price
                    fill_weighted_qty += detail_qty
                    if detail_ts > latest_fill_ts_ns:
                        latest_fill_ts_ns = detail_ts

        avg_fill_price = 0.0
        if cum_qty > 0 and fill_weighted_qty > 0:
            avg_fill_price = fill_weighted_value / fill_weighted_qty
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
            fill_ts_ns=latest_fill_ts_ns,
            raw=raw,
        )

    @staticmethod
    def position_lot(raw: dict[str, Any]) -> PositionLot | None:
        hold_id = str(raw.get("ExecutionID") or raw.get("HoldID") or "")
        symbol = str(raw.get("Symbol") or "")
        qty = _parse_int(raw.get("HoldQty") or raw.get("LeavesQty") or raw.get("Qty"))
        if not hold_id or not symbol or qty <= 0:
            return None
        return PositionLot(
            hold_id=hold_id,
            symbol=symbol,
            exchange=_parse_int(raw.get("Exchange"), 1),
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
        self._session: Any | None = None
        self._bucket = _TokenBucket(rate_per_sec)

    async def start(self) -> None:
        if self._session is None:
            import aiohttp

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

    @property
    def token(self) -> str | None:
        return self._token

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

    @staticmethod
    def _is_tse_plus_retry_error(exc: KabuApiError) -> bool:
        if exc.status not in {400, 500}:
            return False
        code = _extract_error_code(exc.payload)
        return code in _TSE_PLUS_RETRY_CODES

    async def _sendorder_with_exchange_retry(
        self,
        *,
        symbol: str,
        exchange: int,
        body: dict[str, Any],
    ) -> dict[str, Any]:
        try:
            return await self._request_json("POST", "/kabusapi/sendorder", json_body=body)
        except KabuApiError as exc:
            if exchange != 1 or not self._is_tse_plus_retry_error(exc):
                raise
            retry_body = dict(body)
            retry_body["Exchange"] = 27
            code = _extract_error_code(exc.payload)
            logger.warning(
                "sendorder rejected on exchange=1 symbol=%s code=%s; retrying exchange=27 (东证+)",
                symbol,
                code,
            )
            return await self._request_json("POST", "/kabusapi/sendorder", json_body=retry_body)

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
            "Password": self._password or "",
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

        if _is_margin_mode(profile.mode):
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

        return await self._sendorder_with_exchange_retry(
            symbol=symbol,
            exchange=exchange,
            body=body,
        )

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
            "Password": self._password or "",
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

        if _is_margin_mode(profile.mode):
            body.update(
                {
                    "CashMargin": 3,
                    "MarginTradeType": profile.margin_trade_type,
                    "DelivType": profile.margin_close_deliv_type,
                    "ClosePositions": await self._build_close_positions(
                        symbol=symbol,
                        exchange=exchange,
                        position_side=position_side,
                        qty=qty,
                    ),
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

        return await self._sendorder_with_exchange_retry(
            symbol=symbol,
            exchange=exchange,
            body=body,
        )

    async def _build_close_positions(
        self,
        *,
        symbol: str,
        exchange: int,
        position_side: int,
        qty: int,
    ) -> list[dict[str, Any]]:
        positions = [KabuAdapter.position_lot(raw) for raw in await self.get_positions(symbol)]
        usable_positions = [
            pos
            for pos in positions
            if pos
            and pos.side == position_side
            and pos.exchange == exchange
        ]

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
                f"not enough inventory to close {symbol} exchange={exchange} qty={qty}",
                payload=[
                    {
                        "hold_id": position.hold_id,
                        "symbol": position.symbol,
                        "exchange": position.exchange,
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

                # Avoid automatic retries for order-mutation APIs to reduce duplicate-order risk.
                should_retry = (
                    response.status in {429, 500, 502, 503, 504}
                    and path not in _ORDER_MUTATION_PATHS
                )
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
        api_token: str | None = None,
    ):
        self._url = url
        self._on_board = on_board
        self._on_trade = on_trade
        self._on_reconnect = on_reconnect
        self._api_token = api_token or ""
        self._running = False
        self._ws: Any | None = None
        self._snapshots: dict[str, BoardSnapshot] = {}
        self._volumes: dict[str, int] = {}
        self._last_trade_price: dict[str, float] = {}
        self._last_latency_warn_ns: dict[str, int] = {}
        self._latency_warn_interval_ns = 2_000_000_000  # 2s per symbol
        self._latency_samples: dict[str, deque[float]] = {}
        self._latency_sample_limit = 512
        self._last_latency_stats_ns: dict[str, int] = {}
        self._latency_stats_interval_ns = 30_000_000_000  # 30s

    async def run(self) -> None:
        import websockets

        self._running = True
        retry_sleep = 1.0
        while self._running:
            try:
                connection = await self._connect(websockets)
                self._ws = connection
                self._reset_stream_state()
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
                if self._ws is not None:
                    try:
                        await self._ws.close()
                    except Exception:
                        pass
                self._ws = None

    def stop(self) -> None:
        self._running = False
        if self._ws is not None:
            asyncio.create_task(self._ws.close())

    def set_api_token(self, token: str | None) -> None:
        self._api_token = token or ""

    def _reset_stream_state(self) -> None:
        self._snapshots.clear()
        self._volumes.clear()
        self._last_trade_price.clear()

    async def _connect(self, websockets_module: Any) -> Any:
        kwargs: dict[str, Any] = {
            # kabu Station does not respond to WebSocket-level ping frames,
            # causing the websockets library to time out every ~65 s and close
            # with code 1011.  Disable automatic pings here; the gateway's own
            # reconnect loop handles connection recovery if the socket goes dead.
            "ping_interval": None,
            "ping_timeout": None,
            "max_size": 2**20,
        }
        if self._api_token:
            headers = {"X-API-KEY": self._api_token}
            kwargs["additional_headers"] = headers
            try:
                return await websockets_module.connect(self._url, **kwargs)
            except TypeError:
                kwargs.pop("additional_headers", None)
                kwargs["extra_headers"] = headers
                return await websockets_module.connect(self._url, **kwargs)
        return await websockets_module.connect(self._url, **kwargs)

    def _dispatch(self, raw_message: str | bytes) -> None:
        recv_ns = time.time_ns()
        data = _loads(raw_message)
        if not isinstance(data, dict):
            return

        symbol = str(data.get("Symbol", ""))
        prev_snapshot = self._snapshots.get(symbol)
        snapshot = KabuAdapter.board(data, prev_snapshot)
        if snapshot is None:
            return

        # Drop stale out-of-order quotes so they don't corrupt OFI deltas.
        if snapshot.out_of_order:
            logger.debug(
                "drop out-of-order quote symbol=%s prev_ts=%s new_ts=%s",
                symbol,
                prev_snapshot.ts_ns if prev_snapshot else 0,
                snapshot.ts_ns,
            )
            return

        # Drop exact duplicate events to avoid duplicate signal/order evaluation.
        if snapshot.duplicate:
            return

        latency_ms = (
            max(0.0, (recv_ns - snapshot.ts_ns) / 1_000_000)
            if snapshot.ts_ns > 0
            else -1.0
        )
        bid_latency_ms = (
            max(0.0, (recv_ns - snapshot.bid_ts_ns) / 1_000_000)
            if snapshot.bid_ts_ns > 0
            else -1.0
        )
        ask_latency_ms = (
            max(0.0, (recv_ns - snapshot.ask_ts_ns) / 1_000_000)
            if snapshot.ask_ts_ns > 0
            else -1.0
        )
        current_latency_ms = (
            max(0.0, (recv_ns - snapshot.current_ts_ns) / 1_000_000)
            if snapshot.current_ts_ns > 0
            else -1.0
        )
        if latency_ms >= 0:
            self._record_latency(symbol, latency_ms)
        if latency_ms > 500 and self._should_log_latency_warn(symbol, recv_ns):
            logger.warning(
                "market data latency %.1fms for %s (source=%s bid=%.1fms ask=%.1fms current=%.1fms)",
                latency_ms,
                symbol,
                snapshot.ts_source,
                bid_latency_ms,
                ask_latency_ms,
                current_latency_ms,
            )
        if latency_ms >= 0:
            self._maybe_log_latency_stats(symbol, recv_ns)

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

    def _should_log_latency_warn(self, symbol: str, now_ns: int) -> bool:
        last = self._last_latency_warn_ns.get(symbol, 0)
        if now_ns - last < self._latency_warn_interval_ns:
            return False
        self._last_latency_warn_ns[symbol] = now_ns
        return True

    def _record_latency(self, symbol: str, latency_ms: float) -> None:
        buf = self._latency_samples.get(symbol)
        if buf is None:
            buf = deque()
            self._latency_samples[symbol] = buf
        buf.append(latency_ms)
        while len(buf) > self._latency_sample_limit:
            buf.popleft()

    def _maybe_log_latency_stats(self, symbol: str, now_ns: int) -> None:
        last = self._last_latency_stats_ns.get(symbol, 0)
        if now_ns - last < self._latency_stats_interval_ns:
            return
        stats = self.get_latency_stats(symbol)
        if stats is None:
            return
        self._last_latency_stats_ns[symbol] = now_ns
        logger.info(
            "latency stats symbol=%s samples=%d p50=%.1fms p90=%.1fms p99=%.1fms max=%.1fms",
            symbol,
            stats["samples"],
            stats["p50_ms"],
            stats["p90_ms"],
            stats["p99_ms"],
            stats["max_ms"],
        )

    def get_latency_stats(self, symbol: str) -> dict[str, float | int] | None:
        buf = self._latency_samples.get(symbol)
        if not buf:
            return None
        values = sorted(buf)
        size = len(values)
        return {
            "samples": size,
            "p50_ms": self._percentile(values, 0.50),
            "p90_ms": self._percentile(values, 0.90),
            "p99_ms": self._percentile(values, 0.99),
            "max_ms": values[-1],
        }

    @staticmethod
    def _percentile(values: list[float], q: float) -> float:
        if not values:
            return 0.0
        if len(values) == 1:
            return values[0]
        idx = int(round((len(values) - 1) * q))
        idx = max(0, min(idx, len(values) - 1))
        return values[idx]
