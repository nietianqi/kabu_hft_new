"""
DEPRECATED: KabuNormalizer, NormalizedBook, NormalizedTrade, and NormalizedLevel
are superseded by KabuAdapter / BoardSnapshot / TradePrint in kabu_hft.gateway.
Use those types directly. This module will be removed in a future version.
"""
from __future__ import annotations

import warnings
warnings.warn(
    "kabu_hft.adapter.normalizer is deprecated. Use kabu_hft.gateway.KabuAdapter instead.",
    DeprecationWarning,
    stacklevel=2,
)

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


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


@dataclass(slots=True)
class NormalizedLevel:
    price: float
    qty: int


@dataclass(slots=True)
class NormalizedBook:
    symbol: str
    exchange: int
    ts_exchange_ns: int
    best_bid: float
    best_bid_qty: int
    best_ask: float
    best_ask_qty: int
    bids: tuple[NormalizedLevel, ...] = field(default_factory=tuple)
    asks: tuple[NormalizedLevel, ...] = field(default_factory=tuple)
    last_price: float = 0.0
    cum_volume: int = 0
    vwap: float = 0.0
    duplicate: bool = False
    out_of_order: bool = False

    @property
    def mid(self) -> float:
        return (self.best_bid + self.best_ask) / 2.0

    @property
    def spread(self) -> float:
        return self.best_ask - self.best_bid

    @property
    def valid(self) -> bool:
        return self.best_bid > 0 and self.best_ask > 0 and self.best_bid < self.best_ask


@dataclass(slots=True)
class NormalizedTrade:
    symbol: str
    exchange: int
    ts_exchange_ns: int
    price: float
    size: int
    side: int
    cumulative_volume: int


class KabuNormalizer:
    @staticmethod
    def _parse_levels(raw: dict[str, Any], prefix: str, descending: bool) -> tuple[NormalizedLevel, ...]:
        levels: list[NormalizedLevel] = []
        for index in range(1, 11):
            entry = raw.get(f"{prefix}{index}")
            if not isinstance(entry, dict):
                continue
            price = _parse_float(entry.get("Price"))
            qty = _parse_int(entry.get("Qty"))
            if price > 0 and qty > 0:
                levels.append(NormalizedLevel(price=price, qty=qty))
        levels.sort(key=lambda level: level.price, reverse=descending)
        return tuple(levels)

    @classmethod
    def normalize_board(
        cls,
        raw: dict[str, Any],
        prev: NormalizedBook | None,
    ) -> NormalizedBook | None:
        bids = cls._parse_levels(raw, "Buy", descending=True)
        asks = cls._parse_levels(raw, "Sell", descending=False)

        # kabu reversed mapping:
        # AskPrice/AskQty -> best bid
        # BidPrice/BidQty -> best ask
        best_bid = _parse_float(raw.get("AskPrice")) or (bids[0].price if bids else 0.0)
        best_ask = _parse_float(raw.get("BidPrice")) or (asks[0].price if asks else 0.0)
        best_bid_qty = _parse_int(raw.get("AskQty")) or (bids[0].qty if bids else 0)
        best_ask_qty = _parse_int(raw.get("BidQty")) or (asks[0].qty if asks else 0)

        ts_exchange_ns = _to_ns(
            raw.get("CurrentPriceTime")
            or raw.get("BidTime")
            or raw.get("AskTime")
        )

        out_of_order = bool(prev and ts_exchange_ns and prev.ts_exchange_ns and ts_exchange_ns < prev.ts_exchange_ns)
        duplicate = bool(
            prev
            and ts_exchange_ns == prev.ts_exchange_ns
            and best_bid == prev.best_bid
            and best_ask == prev.best_ask
            and best_bid_qty == prev.best_bid_qty
            and best_ask_qty == prev.best_ask_qty
            and _parse_int(raw.get("TradingVolume")) == prev.cum_volume
        )

        normalized = NormalizedBook(
            symbol=str(raw.get("Symbol", "")),
            exchange=_parse_int(raw.get("Exchange"), 1),
            ts_exchange_ns=ts_exchange_ns,
            best_bid=best_bid,
            best_bid_qty=best_bid_qty,
            best_ask=best_ask,
            best_ask_qty=best_ask_qty,
            bids=bids,
            asks=asks,
            last_price=_parse_float(raw.get("CurrentPrice")),
            cum_volume=_parse_int(raw.get("TradingVolume")),
            vwap=_parse_float(raw.get("VWAP")),
            duplicate=duplicate,
            out_of_order=out_of_order,
        )
        if not normalized.valid:
            return None
        return normalized

    @staticmethod
    def normalize_trade(
        raw: dict[str, Any],
        prev_book: NormalizedBook | None,
        prev_cum_volume: int,
        last_trade_price: float | None,
    ) -> NormalizedTrade | None:
        cum_volume = _parse_int(raw.get("TradingVolume"))
        size = max(0, cum_volume - max(prev_cum_volume, 0))
        if size <= 0:
            return None

        price = _parse_float(raw.get("CurrentPrice"))
        if price <= 0:
            return None

        side = 0
        if prev_book and prev_book.valid:
            if price >= prev_book.best_ask:
                side = 1
            elif price <= prev_book.best_bid:
                side = -1
            elif price > prev_book.mid:
                side = 1
            elif price < prev_book.mid:
                side = -1
            elif last_trade_price is not None:
                if price > last_trade_price:
                    side = 1
                elif price < last_trade_price:
                    side = -1

        return NormalizedTrade(
            symbol=str(raw.get("Symbol", "")),
            exchange=_parse_int(raw.get("Exchange"), 1),
            ts_exchange_ns=_to_ns(raw.get("CurrentPriceTime")),
            price=price,
            size=size,
            side=side,
            cumulative_volume=cum_volume,
        )
