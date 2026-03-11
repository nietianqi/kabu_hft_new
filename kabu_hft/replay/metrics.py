from __future__ import annotations

from dataclasses import dataclass
from statistics import mean

from kabu_hft.execution import RoundTrip


@dataclass(slots=True)
class ReplaySummary:
    symbol: str
    num_events: int
    num_signals: int
    num_entries: int
    num_exits: int
    num_round_trips: int
    gross_pnl: float
    avg_pnl: float
    win_rate: float
    avg_abs_alpha: float


class ReplayMetrics:
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.num_events = 0
        self.num_signals = 0
        self.num_entries = 0
        self.num_exits = 0
        self.round_trips: list[RoundTrip] = []
        self.alpha_abs_samples: list[float] = []

    def on_event(self) -> None:
        self.num_events += 1

    def on_signal(self, alpha: float) -> None:
        self.num_signals += 1
        self.alpha_abs_samples.append(abs(alpha))

    def on_entry(self) -> None:
        self.num_entries += 1

    def on_exit(self) -> None:
        self.num_exits += 1

    def on_round_trip(self, trade: RoundTrip) -> None:
        self.round_trips.append(trade)

    def summary(self) -> ReplaySummary:
        pnls = [trade.realized_pnl for trade in self.round_trips]
        wins = [pnl for pnl in pnls if pnl > 0]
        gross_pnl = sum(pnls)
        return ReplaySummary(
            symbol=self.symbol,
            num_events=self.num_events,
            num_signals=self.num_signals,
            num_entries=self.num_entries,
            num_exits=self.num_exits,
            num_round_trips=len(self.round_trips),
            gross_pnl=gross_pnl,
            avg_pnl=(gross_pnl / len(pnls)) if pnls else 0.0,
            win_rate=(len(wins) / len(pnls)) if pnls else 0.0,
            avg_abs_alpha=mean(self.alpha_abs_samples) if self.alpha_abs_samples else 0.0,
        )
