from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, time as dt_time, timedelta, timezone

from kabu_hft.clock import Clock, LiveClock
from kabu_hft.gateway import BoardSnapshot

logger = logging.getLogger("kabu.risk")

JST = timezone(timedelta(hours=9))


class SessionGuard:
    OPEN_WINDOWS = (
        (dt_time(9, 0), dt_time(11, 25)),
        (dt_time(12, 30), dt_time(15, 25)),
    )
    CLOSE_WINDOWS = (
        (dt_time(9, 0), dt_time(11, 30)),
        (dt_time(12, 30), dt_time(15, 30)),
    )

    @staticmethod
    def _in_windows(now: datetime, windows: tuple[tuple[dt_time, dt_time], ...]) -> bool:
        local_now = now.astimezone(JST).time().replace(tzinfo=None)
        return any(start <= local_now <= end for start, end in windows)

    def is_open_allowed(self, now: datetime) -> bool:
        return self._in_windows(now, self.OPEN_WINDOWS)

    def is_close_allowed(self, now: datetime) -> bool:
        return self._in_windows(now, self.CLOSE_WINDOWS)


class VolatilityEstimator:
    def __init__(self, alpha: float = 0.05):
        self.alpha = alpha
        self.prev_mid = 0.0
        self.atr = 0.0

    def update(self, mid: float, spread: float) -> float:
        if self.prev_mid > 0:
            move = abs(mid - self.prev_mid)
            estimate = max(move, spread)
            self.atr = estimate if self.atr == 0 else self.alpha * estimate + (1.0 - self.alpha) * self.atr
        self.prev_mid = mid
        return self.atr


@dataclass(slots=True)
class TradeRecord:
    ts_ns: int
    symbol: str
    side: int
    qty: int
    entry_price: float
    exit_price: float
    pnl: float
    hold_seconds: float


class PnLTracker:
    def __init__(
        self,
        daily_loss_limit: float,
        consecutive_loss_limit: int,
        cooling_seconds: int,
        max_hold_seconds: int,
        clock: Clock = LiveClock(),
    ):
        self.daily_loss_limit = daily_loss_limit
        self.consecutive_loss_limit = consecutive_loss_limit
        self.cooling_ns = cooling_seconds * 1_000_000_000
        self.max_hold_ns = max_hold_seconds * 1_000_000_000
        self._clock = clock
        self.daily_pnl = 0.0
        self.consecutive_losses = 0
        self.cooling_until_ns = 0
        self.trades: list[TradeRecord] = []

    def record_trade(
        self,
        *,
        symbol: str,
        side: int,
        qty: int,
        entry_price: float,
        exit_price: float,
        entry_ts_ns: int,
        exit_ts_ns: int,
        commission: float = 0.0,
    ) -> float:
        hold_seconds = max(0.0, (exit_ts_ns - entry_ts_ns) / 1_000_000_000)
        pnl = side * (exit_price - entry_price) * qty - commission
        self.daily_pnl += pnl
        self.trades.append(
            TradeRecord(
                ts_ns=exit_ts_ns,
                symbol=symbol,
                side=side,
                qty=qty,
                entry_price=entry_price,
                exit_price=exit_price,
                pnl=pnl,
                hold_seconds=hold_seconds,
            )
        )

        if pnl < 0:
            self.consecutive_losses += 1
            if self.consecutive_losses >= self.consecutive_loss_limit:
                self.cooling_until_ns = self._clock.time_ns() + self.cooling_ns
        else:
            self.consecutive_losses = 0
        return pnl

    def is_daily_limit_hit(self) -> bool:
        return self.daily_pnl <= self.daily_loss_limit

    def is_cooling(self, now_ns: int) -> bool:
        return now_ns < self.cooling_until_ns

    def should_force_close(self, open_ts_ns: int, now_ns: int) -> bool:
        return open_ts_ns > 0 and now_ns - open_ts_ns >= self.max_hold_ns

    def summary(self) -> dict[str, float | int | bool]:
        total = len(self.trades)
        wins = sum(1 for trade in self.trades if trade.pnl > 0)
        return {
            "daily_pnl": self.daily_pnl,
            "total_trades": total,
            "win_rate": wins / total if total else 0.0,
            "consecutive_losses": self.consecutive_losses,
            "cooling": self.cooling_until_ns > self._clock.time_ns(),
        }


class PositionSizer:
    def __init__(self, base_qty: int, max_qty: int, max_inventory_qty: int, max_notional: float):
        self.base_qty = max(base_qty, 1)
        self.max_qty = max(max_qty, self.base_qty)
        self.max_inventory_qty = max(max_inventory_qty, self.base_qty)
        self.max_notional = max_notional

    def calc_qty(
        self,
        *,
        signal_strength: float,
        atr: float,
        mid: float,
        inventory_qty: int,
        daily_loss_limit: float,
        daily_pnl: float,
    ) -> int:
        qty = self.base_qty
        if atr > 2.5 * max(mid * 0.001, 1.0):
            qty = max(self.base_qty // 2, 1)

        limit = abs(daily_loss_limit)
        loss_fraction = abs(min(daily_pnl, 0.0)) / max(limit, 1.0)
        if loss_fraction > 0.5:
            qty = max(qty // 2, 1)

        if signal_strength >= 1.0:
            qty = min(int(qty * 1.5), self.max_qty)

        qty = min(qty, max(self.max_inventory_qty - inventory_qty, 0))
        if self.max_notional > 0 and mid > 0:
            qty = min(qty, int(self.max_notional // mid))
        return max(qty, 0)


class RiskGuard:
    def __init__(
        self,
        *,
        base_qty: int,
        max_qty: int,
        max_inventory_qty: int,
        max_notional: float,
        daily_loss_limit: float,
        consecutive_loss_limit: int,
        cooling_seconds: int,
        max_hold_seconds: int,
        max_spread_ticks: float,
        stale_quote_ms: int,
        tick_size: float,
        allow_short: bool,
        entry_threshold: float,
        clock: Clock = LiveClock(),
    ):
        self.session = SessionGuard()
        self.pnl = PnLTracker(daily_loss_limit, consecutive_loss_limit, cooling_seconds, max_hold_seconds, clock=clock)
        self.vol = VolatilityEstimator()
        self.sizer = PositionSizer(base_qty, max_qty, max_inventory_qty, max_notional)
        self.max_spread = max_spread_ticks * tick_size
        self.stale_quote_ns = stale_quote_ms * 1_000_000
        self.tick_size = tick_size
        self.allow_short = allow_short
        self.entry_threshold = entry_threshold

    def update_vol(self, snapshot: BoardSnapshot) -> float:
        return self.vol.update(snapshot.mid, snapshot.spread)

    def calc_qty(self, *, signal_strength: float, mid: float, inventory_qty: int) -> int:
        return self.sizer.calc_qty(
            signal_strength=signal_strength,
            atr=self.vol.atr,
            mid=mid,
            inventory_qty=inventory_qty,
            daily_loss_limit=self.pnl.daily_loss_limit,
            daily_pnl=self.pnl.daily_pnl,
        )

    def can_open(
        self,
        *,
        snapshot: BoardSnapshot,
        direction: int,
        signal_strength: float,
        inventory_qty: int,
        now_ns: int,
        now_dt: datetime,
    ) -> tuple[bool, str]:
        if inventory_qty > 0:
            return False, "inventory_not_flat"
        if direction < 0 and not self.allow_short:
            return False, "short_disabled"
        if not self.session.is_open_allowed(now_dt):
            return False, "outside_open_session"
        if self.pnl.is_daily_limit_hit():
            return False, "daily_loss_limit"
        if self.pnl.is_cooling(now_ns):
            return False, "cooling"
        if not snapshot.valid:
            return False, "invalid_quote"
        if now_ns - snapshot.ts_ns > self.stale_quote_ns:
            return False, "stale_quote"
        if snapshot.spread > self.max_spread:
            return False, "spread_too_wide"
        if signal_strength < self.entry_threshold:
            return False, "alpha_too_small"
        return True, "ok"

    def must_close(
        self,
        *,
        open_ts_ns: int,
        snapshot: BoardSnapshot,
        now_ns: int,
        now_dt: datetime,
    ) -> tuple[bool, str]:
        if self.pnl.should_force_close(open_ts_ns, now_ns):
            return True, "max_hold_time"
        if not self.session.is_close_allowed(now_dt):
            return True, "session_end"
        if not snapshot.valid:
            return True, "invalid_quote"
        if now_ns - snapshot.ts_ns > self.stale_quote_ns:
            return True, "stale_quote"
        return False, ""

    def should_cancel_entry(
        self,
        *,
        working_price: float,
        desired_price: float,
        signal_strength: float,
        working_age_ns: int,
        min_lifetime_ns: int,
        snapshot: BoardSnapshot,
        now_ns: int,
    ) -> tuple[bool, str]:
        if working_age_ns < min_lifetime_ns:
            return False, ""
        if now_ns - snapshot.ts_ns > self.stale_quote_ns:
            return True, "stale_quote"
        if snapshot.spread > self.max_spread:
            return True, "spread_expanded"
        if signal_strength < max(self.entry_threshold * 0.6, 0.15):
            return True, "alpha_decay"
        if abs(working_price - desired_price) >= self.tick_size * 0.5:
            return True, "requote"
        return False, ""

    def record_trade(self, **kwargs) -> float:
        pnl = self.pnl.record_trade(**kwargs)
        logger.info(
            "trade symbol=%s side=%+d qty=%d pnl=%.2f daily_pnl=%.2f",
            kwargs["symbol"],
            kwargs["side"],
            kwargs["qty"],
            pnl,
            self.pnl.daily_pnl,
        )
        return pnl

    def summary(self) -> dict[str, float | int | bool]:
        summary = self.pnl.summary()
        summary["atr"] = self.vol.atr
        return summary
