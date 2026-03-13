from __future__ import annotations

import logging
import math
from collections import deque
from dataclasses import dataclass

logger = logging.getLogger("kabu.signals")

from kabu_hft.config import SignalWeights
from kabu_hft.gateway import BoardSnapshot, TradePrint


class OnlineZScore:
    __slots__ = ("window", "buf", "sum_x", "sum_x2")

    def __init__(self, window: int):
        self.window = window
        self.buf: deque[float] = deque()
        self.sum_x = 0.0
        self.sum_x2 = 0.0

    def update(self, value: float) -> float:
        self.buf.append(value)
        self.sum_x += value
        self.sum_x2 += value * value

        if len(self.buf) > self.window:
            removed = self.buf.popleft()
            self.sum_x -= removed
            self.sum_x2 -= removed * removed

        count = len(self.buf)
        if count < 50:
            return 0.0

        mean = self.sum_x / count
        variance = max(0.0, self.sum_x2 / count - mean * mean)
        if variance <= 1e-12:
            return 0.0

        std = math.sqrt(variance)
        return max(-4.0, min(4.0, (value - mean) / std))


@dataclass(slots=True)
class SignalPacket:
    ts_ns: int
    obi_raw: float
    lob_ofi_raw: float
    tape_ofi_raw: float
    micro_momentum_raw: float
    microprice_tilt_raw: float
    microprice: float
    mid: float
    obi_z: float
    lob_ofi_z: float
    tape_ofi_z: float
    micro_momentum_z: float
    microprice_tilt_z: float
    composite: float


class OBISignal:
    def __init__(self, depth: int, decay: float):
        self.weights = [decay**index for index in range(depth)]

    def compute(self, snapshot: BoardSnapshot) -> float:
        bid_weight = 0.0
        ask_weight = 0.0
        for index, weight in enumerate(self.weights):
            if index < len(snapshot.bids):
                bid_weight += weight * snapshot.bids[index].size
            if index < len(snapshot.asks):
                ask_weight += weight * snapshot.asks[index].size
        total = bid_weight + ask_weight
        return 0.0 if total <= 0 else (bid_weight - ask_weight) / total


class LOBOFISignal:
    def __init__(self, depth: int, decay: float):
        self.weights = [decay**index for index in range(depth)]

    def compute(self, snapshot: BoardSnapshot) -> float:
        prev = snapshot.prev_board
        if prev is None:
            return 0.0

        score = 0.0
        for index, weight in enumerate(self.weights):
            score += weight * (
                self._delta(snapshot.bids, prev.bids, index, is_bid=True)
                - self._delta(snapshot.asks, prev.asks, index, is_bid=False)
            )
        return score

    @staticmethod
    def _delta(curr_levels, prev_levels, index: int, *, is_bid: bool) -> float:
        has_curr = index < len(curr_levels)
        has_prev = index < len(prev_levels)
        if not has_curr and not has_prev:
            return 0.0
        if has_curr and not has_prev:
            return float(curr_levels[index].size)
        if has_prev and not has_curr:
            return -float(prev_levels[index].size)

        curr = curr_levels[index]
        prev = prev_levels[index]
        if is_bid:
            if curr.price > prev.price:
                return float(curr.size)
            if curr.price < prev.price:
                return -float(prev.size)
            return float(curr.size - prev.size)

        if curr.price < prev.price:
            return float(curr.size)
        if curr.price > prev.price:
            return -float(prev.size)
        return float(curr.size - prev.size)


class TapeOFISignal:
    def __init__(self, window_sec: int):
        self.window_ns = window_sec * 1_000_000_000
        self.events: deque[tuple[int, int, int]] = deque()
        self.buy_volume = 0
        self.sell_volume = 0
        self._last_ts_ns: int = 0

    def on_trade(self, trade: TradePrint) -> float:
        if self._last_ts_ns > 0 and trade.ts_ns < self._last_ts_ns:
            logger.debug(
                "tape_ofi out-of-order skipped ts_ns=%d last=%d",
                trade.ts_ns,
                self._last_ts_ns,
            )
            return self.current
        self._last_ts_ns = trade.ts_ns
        self.events.append((trade.ts_ns, trade.size if trade.side > 0 else 0, trade.size if trade.side < 0 else 0))
        self.buy_volume += trade.size if trade.side > 0 else 0
        self.sell_volume += trade.size if trade.side < 0 else 0
        self._trim(trade.ts_ns)
        return self.current

    def _trim(self, now_ns: int) -> None:
        while self.events and now_ns - self.events[0][0] > self.window_ns:
            _, buy_volume, sell_volume = self.events.popleft()
            self.buy_volume -= buy_volume
            self.sell_volume -= sell_volume

    @property
    def current(self) -> float:
        total = self.buy_volume + self.sell_volume
        if total <= 0:
            return 0.0
        return (self.buy_volume - self.sell_volume) / total


class MicropriceSignals:
    def __init__(self, ema_alpha: float, tick_size: float):
        self.ema_alpha = ema_alpha
        self.tick_size = tick_size
        self.ema: float | None = None

    def compute(self, snapshot: BoardSnapshot) -> tuple[float, float, float]:
        total_size = snapshot.bid_size + snapshot.ask_size
        if total_size <= 0:
            return snapshot.mid, 0.0, 0.0

        microprice = (
            snapshot.ask_size * snapshot.bid + snapshot.bid_size * snapshot.ask
        ) / total_size

        if self.ema is None:
            self.ema = microprice
        else:
            self.ema = self.ema_alpha * microprice + (1.0 - self.ema_alpha) * self.ema

        tick_size = self.tick_size if self.tick_size > 0 else 1.0
        micro_momentum = (microprice - self.ema) / tick_size

        half_spread = snapshot.spread / 2.0
        microprice_tilt = 0.0 if half_spread <= 0 else (microprice - snapshot.mid) / half_spread
        return microprice, micro_momentum, microprice_tilt


class SignalStack:
    def __init__(
        self,
        *,
        obi_depth: int,
        obi_decay: float,
        lob_ofi_depth: int,
        lob_ofi_decay: float,
        tape_window_sec: int,
        mp_ema_alpha: float,
        tick_size: float,
        zscore_window: int,
        weights: SignalWeights,
    ):
        self.obi = OBISignal(obi_depth, obi_decay)
        self.lob_ofi = LOBOFISignal(lob_ofi_depth, lob_ofi_decay)
        self.tape = TapeOFISignal(tape_window_sec)
        self.micro = MicropriceSignals(mp_ema_alpha, tick_size)
        self.weights = weights
        self.zscores = {
            "obi": OnlineZScore(zscore_window),
            "lob_ofi": OnlineZScore(zscore_window),
            "tape_ofi": OnlineZScore(zscore_window),
            "micro_momentum": OnlineZScore(zscore_window),
            "microprice_tilt": OnlineZScore(zscore_window),
        }
        self.last: SignalPacket | None = None

    def on_board(self, snapshot: BoardSnapshot) -> SignalPacket:
        obi_raw = self.obi.compute(snapshot)
        lob_ofi_raw = self.lob_ofi.compute(snapshot)
        tape_ofi_raw = self.tape.current
        microprice, micro_momentum_raw, microprice_tilt_raw = self.micro.compute(snapshot)

        obi_z = self.zscores["obi"].update(obi_raw)
        lob_ofi_z = self.zscores["lob_ofi"].update(lob_ofi_raw)
        tape_ofi_z = self.zscores["tape_ofi"].update(tape_ofi_raw)
        micro_momentum_z = self.zscores["micro_momentum"].update(micro_momentum_raw)
        microprice_tilt_z = self.zscores["microprice_tilt"].update(microprice_tilt_raw)

        composite = (
            self.weights.lob_ofi * lob_ofi_z
            + self.weights.obi * obi_z
            + self.weights.tape_ofi * tape_ofi_z
            + self.weights.micro_momentum * micro_momentum_z
            + self.weights.microprice_tilt * microprice_tilt_z
        )

        self.last = SignalPacket(
            ts_ns=snapshot.ts_ns,
            obi_raw=obi_raw,
            lob_ofi_raw=lob_ofi_raw,
            tape_ofi_raw=tape_ofi_raw,
            micro_momentum_raw=micro_momentum_raw,
            microprice_tilt_raw=microprice_tilt_raw,
            microprice=microprice,
            mid=snapshot.mid,
            obi_z=obi_z,
            lob_ofi_z=lob_ofi_z,
            tape_ofi_z=tape_ofi_z,
            micro_momentum_z=micro_momentum_z,
            microprice_tilt_z=microprice_tilt_z,
            composite=composite,
        )
        return self.last

    def on_trade(self, trade: TradePrint) -> float:
        return self.tape.on_trade(trade)
