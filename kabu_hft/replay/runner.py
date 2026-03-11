from __future__ import annotations

import asyncio
from dataclasses import dataclass

from kabu_hft.clock import SimulatedClock
from kabu_hft.config import OrderProfile, SignalWeights
from kabu_hft.execution import ExecutionController, ExecutionState
from kabu_hft.gateway import BoardSnapshot, KabuAdapter
from kabu_hft.replay.loader import JsonlReplayLoader
from kabu_hft.replay.metrics import ReplayMetrics, ReplaySummary
from kabu_hft.signals import SignalStack


@dataclass(slots=True)
class ReplayConfig:
    symbol: str
    exchange: int = 1
    tick_size: float = 1.0
    qty: int = 100
    entry_threshold: float = 0.8
    exit_threshold: float = 0.2
    max_hold_events: int = 50
    strong_threshold: float = 1.2
    queue_model: bool = True  # If True, simulate queue position before paper fills
    order_latency_events: int = 0  # Number of board events to delay before paper order is acknowledged


class ReplayRunner:
    def __init__(self, config: ReplayConfig):
        self.config = config
        self._clock = SimulatedClock()
        self.signal_stack = SignalStack(
            obi_depth=5,
            obi_decay=0.7,
            lob_ofi_depth=5,
            lob_ofi_decay=0.8,
            tape_window_sec=15,
            mp_ema_alpha=0.1,
            tick_size=config.tick_size,
            zscore_window=200,
            weights=SignalWeights(),
        )
        self.execution = ExecutionController(
            symbol=config.symbol,
            exchange=config.exchange,
            rest_client=None,
            order_profile=OrderProfile(),
            dry_run=True,
            tick_size=config.tick_size,
            strong_threshold=config.strong_threshold,
            min_edge_ticks=0.0,
            max_pending_ms=2_000,
            min_order_lifetime_ms=0,
            max_requotes_per_minute=120,
            allow_aggressive_entry=True,
            allow_aggressive_exit=True,
            clock=self._clock,
            queue_model=config.queue_model,
        )
        self.metrics = ReplayMetrics(config.symbol)
        self._entry_event_index = -1
        self._pending_ack_index = -1  # event_index at which paper order becomes active

    async def run(self, loader: JsonlReplayLoader) -> ReplaySummary:
        prev_snapshot: BoardSnapshot | None = None
        prev_volume = 0
        last_trade_price: float | None = None
        event_index = 0

        for raw in loader.iter_events():
            if str(raw.get("Symbol", "")) != self.config.symbol:
                continue
            event_index += 1
            self.metrics.on_event()
            snapshot = KabuAdapter.board(raw, prev_snapshot)
            if snapshot is None:
                continue

            # Advance simulated clock to the exchange timestamp of this event.
            if snapshot.ts_ns > 0:
                self._clock.set(snapshot.ts_ns)

            # Only attempt paper fills if the simulated order latency has elapsed.
            order_active = (
                self.config.order_latency_events <= 0
                or self._pending_ack_index < 0
                or event_index >= self._pending_ack_index
            )
            if order_active:
                self.execution.sync_paper_board(snapshot)
            trade = KabuAdapter.trade(
                raw,
                prev_snapshot,
                prev_volume=prev_volume,
                last_trade_price=last_trade_price,
            )
            if trade is not None:
                self.signal_stack.on_trade(trade)
                if order_active:
                    self.execution.sync_paper_trade(trade)
                last_trade_price = trade.price
            prev_volume = snapshot.volume

            signal = self.signal_stack.on_board(snapshot)
            alpha = signal.composite
            self.metrics.on_signal(alpha)

            if self.execution.state == ExecutionState.FLAT and abs(alpha) >= self.config.entry_threshold:
                direction = 1 if alpha > 0 else -1
                opened = await self.execution.open(
                    direction=direction,
                    qty=self.config.qty,
                    snapshot=snapshot,
                    score=abs(alpha),
                    microprice=signal.microprice,
                    reason="replay_entry",
                )
                if opened:
                    self._entry_event_index = event_index
                    self._pending_ack_index = event_index + self.config.order_latency_events
                    self.metrics.on_entry()

            elif self.execution.state == ExecutionState.OPEN:
                holding_too_long = (
                    self._entry_event_index > 0
                    and event_index - self._entry_event_index >= self.config.max_hold_events
                )
                reversed_alpha = (
                    self.execution.inventory.side > 0 and alpha <= -self.config.exit_threshold
                ) or (
                    self.execution.inventory.side < 0 and alpha >= self.config.exit_threshold
                )
                if holding_too_long or reversed_alpha:
                    closed = await self.execution.close(
                        snapshot=snapshot,
                        score=alpha,
                        reason="replay_exit",
                        force=True,
                    )
                    if closed:
                        self.metrics.on_exit()

            for trade_done in self.execution.drain_round_trips():
                self.metrics.on_round_trip(trade_done)
                self._entry_event_index = -1
                self._pending_ack_index = -1

            prev_snapshot = snapshot

        # Force final close using last snapshot for deterministic end-state metrics.
        if self.execution.state == ExecutionState.OPEN and prev_snapshot is not None:
            closed = await self.execution.close(
                snapshot=prev_snapshot,
                score=0.0,
                reason="replay_final_close",
                force=True,
            )
            if closed:
                self.metrics.on_exit()
            self.execution.sync_paper_board(prev_snapshot)
            for trade_done in self.execution.drain_round_trips():
                self.metrics.on_round_trip(trade_done)

        return self.metrics.summary()


def run_replay(path: str, config: ReplayConfig) -> ReplaySummary:
    runner = ReplayRunner(config)
    loader = JsonlReplayLoader(path)
    return asyncio.run(runner.run(loader))
