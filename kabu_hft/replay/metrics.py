from __future__ import annotations

import math
import statistics
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from kabu_hft.execution import RoundTrip
    from kabu_hft.signals import SignalPacket


@dataclass
class TradeAttribution:
    """Per-trade P&L attribution record."""

    trade_id: int
    symbol: str
    side: int
    qty: int
    entry_price: float
    exit_price: float
    realized_pnl: float
    hold_ms: float
    exit_reason: str
    composite_at_entry: float
    obi_z_at_entry: float
    lob_ofi_z_at_entry: float
    tape_ofi_z_at_entry: float


@dataclass
class HorizonStats:
    """Forward return statistics at a given board-event horizon."""

    horizon_events: int
    sample_count: int
    mean_forward_return: float      # mean of (mid[t+N] - mid[t])
    signal_return_corr: float       # Pearson corr(composite_z, forward_return)
    mean_abs_forward_return: float  # mean |mid[t+N] - mid[t]|


@dataclass
class ReplayMetrics:
    """Aggregated metrics computed from a completed replay run.

    Typical usage::

        metrics = ReplayMetrics()
        metrics.add_trade(trade, signal_at_entry)
        metrics.add_signal_sequence(signal_packets)
        summary = metrics.summary()
    """

    # Accumulated raw data
    _trades: list[TradeAttribution] = field(default_factory=list, repr=False)
    _mids: list[float] = field(default_factory=list, repr=False)       # mid per board event
    _composites: list[float] = field(default_factory=list, repr=False)  # composite_z per board event
    _board_count: int = 0
    _fill_count: int = 0

    def add_board(self, mid: float, composite_z: float) -> None:
        self._mids.append(mid)
        self._composites.append(composite_z)
        self._board_count += 1

    def add_trade(
        self,
        trade: "RoundTrip",
        signal: "SignalPacket | None",
    ) -> None:
        hold_ms = max(0.0, (trade.exit_ts_ns - trade.entry_ts_ns) / 1_000_000)
        self._trades.append(
            TradeAttribution(
                trade_id=len(self._trades),
                symbol=trade.symbol,
                side=trade.side,
                qty=trade.qty,
                entry_price=trade.entry_price,
                exit_price=trade.exit_price,
                realized_pnl=trade.realized_pnl,
                hold_ms=hold_ms,
                exit_reason=trade.exit_reason,
                composite_at_entry=signal.composite if signal else 0.0,
                obi_z_at_entry=signal.obi_z if signal else 0.0,
                lob_ofi_z_at_entry=signal.lob_ofi_z if signal else 0.0,
                tape_ofi_z_at_entry=signal.tape_ofi_z if signal else 0.0,
            )
        )
        self._fill_count += 1

    def alpha_at_horizons(
        self, horizons: list[int] = [1, 5, 10, 30]
    ) -> dict[int, HorizonStats]:
        """Compute forward return statistics at multiple board-event horizons.

        Returns a dict mapping horizon (in board events) to :class:`HorizonStats`.
        Uses Pearson correlation between composite z-score at *t* and the
        mid-price change at *t + horizon*.
        """
        mids = self._mids
        composites = self._composites
        n = len(mids)
        result: dict[int, HorizonStats] = {}

        for h in horizons:
            if n <= h:
                result[h] = HorizonStats(
                    horizon_events=h,
                    sample_count=0,
                    mean_forward_return=0.0,
                    signal_return_corr=0.0,
                    mean_abs_forward_return=0.0,
                )
                continue

            fwd_returns = [mids[i + h] - mids[i] for i in range(n - h)]
            sigs = composites[: n - h]
            mean_fwd = sum(fwd_returns) / len(fwd_returns)
            mean_abs = sum(abs(r) for r in fwd_returns) / len(fwd_returns)
            corr = _pearson(sigs, fwd_returns)
            result[h] = HorizonStats(
                horizon_events=h,
                sample_count=len(fwd_returns),
                mean_forward_return=mean_fwd,
                signal_return_corr=corr,
                mean_abs_forward_return=mean_abs,
            )

        return result

    def pnl_by_signal_bucket(
        self,
        buckets: list[float] = [0.0, 0.3, 0.6, 1.0, 2.0],
    ) -> dict[str, dict]:
        """Aggregate trades by |composite_at_entry| bucket.

        Returns a dict keyed by bucket label, with count/mean_pnl/win_rate.
        """
        result: dict[str, dict] = {}
        for i in range(len(buckets)):
            lo = buckets[i]
            hi = buckets[i + 1] if i + 1 < len(buckets) else float("inf")
            label = f"[{lo:.1f},{hi:.1f})" if hi != float("inf") else f"[{lo:.1f},∞)"
            bucket_trades = [
                t for t in self._trades if lo <= abs(t.composite_at_entry) < hi
            ]
            if not bucket_trades:
                result[label] = {"count": 0, "mean_pnl": 0.0, "win_rate": 0.0}
            else:
                pnls = [t.realized_pnl for t in bucket_trades]
                result[label] = {
                    "count": len(pnls),
                    "mean_pnl": sum(pnls) / len(pnls),
                    "win_rate": sum(1 for p in pnls if p > 0) / len(pnls),
                }
        return result

    def pnl_by_exit_reason(self) -> dict[str, dict]:
        """Aggregate trades by exit_reason."""
        reasons: dict[str, list[float]] = {}
        for trade in self._trades:
            reasons.setdefault(trade.exit_reason, []).append(trade.realized_pnl)
        return {
            reason: {
                "count": len(pnls),
                "total_pnl": sum(pnls),
                "mean_pnl": sum(pnls) / len(pnls),
                "win_rate": sum(1 for p in pnls if p > 0) / len(pnls),
            }
            for reason, pnls in reasons.items()
        }

    def pnl_by_hold_bucket(
        self,
        buckets_ms: list[float] = [0, 500, 2000, 5000, 15000, 45000],
    ) -> dict[str, dict]:
        """Aggregate trades by holding duration bucket (milliseconds)."""
        result: dict[str, dict] = {}
        for i in range(len(buckets_ms)):
            lo = buckets_ms[i]
            hi = buckets_ms[i + 1] if i + 1 < len(buckets_ms) else float("inf")
            label = f"[{lo:.0f}ms,{hi:.0f}ms)" if hi != float("inf") else f"[{lo:.0f}ms,∞)"
            bucket_trades = [t for t in self._trades if lo <= t.hold_ms < hi]
            if not bucket_trades:
                result[label] = {"count": 0, "mean_pnl": 0.0}
            else:
                pnls = [t.realized_pnl for t in bucket_trades]
                result[label] = {"count": len(pnls), "mean_pnl": sum(pnls) / len(pnls)}
        return result

    def summary(self) -> dict:
        """Return a structured summary dict with all key metrics."""
        trades = self._trades
        n = len(trades)
        pnls = [t.realized_pnl for t in trades]
        total_pnl = sum(pnls)
        win_rate = sum(1 for p in pnls if p > 0) / n if n else 0.0
        avg_hold = sum(t.hold_ms for t in trades) / n if n else 0.0
        std_pnl = statistics.stdev(pnls) if n > 1 else 0.0
        sharpe = (total_pnl / n) / std_pnl if std_pnl > 0 else 0.0

        alpha_horizons = self.alpha_at_horizons([1, 5, 10, 30])

        return {
            "board_events": self._board_count,
            "total_trades": n,
            "fills": self._fill_count,
            "total_pnl": round(total_pnl, 2),
            "win_rate": round(win_rate, 4),
            "avg_pnl_per_trade": round(total_pnl / n, 2) if n else 0.0,
            "avg_hold_ms": round(avg_hold, 1),
            "std_pnl": round(std_pnl, 2),
            "trade_sharpe": round(sharpe, 4),
            "max_single_loss": round(min(pnls, default=0.0), 2),
            "max_single_win": round(max(pnls, default=0.0), 2),
            "alpha_horizons": {
                h: {
                    "samples": s.sample_count,
                    "mean_fwd_return": round(s.mean_forward_return, 6),
                    "signal_corr": round(s.signal_return_corr, 4),
                }
                for h, s in alpha_horizons.items()
            },
            "by_exit_reason": self.pnl_by_exit_reason(),
            "by_signal_bucket": self.pnl_by_signal_bucket(),
            "by_hold_bucket": self.pnl_by_hold_bucket(),
        }


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _pearson(xs: list[float], ys: list[float]) -> float:
    """Pearson correlation coefficient, returns 0.0 on degenerate input."""
    n = len(xs)
    if n < 2 or len(ys) != n:
        return 0.0
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    cov = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    var_x = sum((x - mean_x) ** 2 for x in xs)
    var_y = sum((y - mean_y) ** 2 for y in ys)
    denom = math.sqrt(var_x * var_y)
    return cov / denom if denom > 1e-12 else 0.0
