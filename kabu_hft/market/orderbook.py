from __future__ import annotations

from dataclasses import dataclass, field

from kabu_hft.adapter import NormalizedBook, NormalizedLevel


@dataclass(slots=True)
class InMemoryOrderBook:
    symbol: str
    exchange: int
    bids: list[NormalizedLevel] = field(default_factory=list)
    asks: list[NormalizedLevel] = field(default_factory=list)
    ts_exchange_ns: int = 0
    cum_volume: int = 0

    def apply(self, snapshot: NormalizedBook) -> None:
        self.symbol = snapshot.symbol
        self.exchange = snapshot.exchange
        self.bids = list(snapshot.bids)
        self.asks = list(snapshot.asks)
        self.ts_exchange_ns = snapshot.ts_exchange_ns
        self.cum_volume = snapshot.cum_volume

    @property
    def best_bid(self) -> float:
        return self.bids[0].price if self.bids else 0.0

    @property
    def best_ask(self) -> float:
        return self.asks[0].price if self.asks else 0.0

    @property
    def spread(self) -> float:
        if not self.bids or not self.asks:
            return 0.0
        return self.best_ask - self.best_bid
