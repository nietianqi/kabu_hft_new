from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime

from kabu_hft.config import OrderProfile, StrategyConfig
from kabu_hft.execution import ExecutionController, ExecutionState, RoundTrip
from kabu_hft.gateway import BoardSnapshot, TradePrint
from kabu_hft.replay.fill_model import FillResult, PriceCrossFillModel, SimOrder
from kabu_hft.replay.loader import ReplayLoader
from kabu_hft.replay.metrics import ReplayMetrics
from kabu_hft.risk import RiskGuard
from kabu_hft.signals import SignalPacket, SignalStack

logger = logging.getLogger("kabu.replay.runner")

_DUMMY_PROFILE = OrderProfile()  # default cash profile for replay


@dataclass
class ReplayResult:
    """Full results from a completed replay run."""

    trades: list[RoundTrip] = field(default_factory=list)
    signal_at_entry: dict[int, SignalPacket] = field(default_factory=dict)  # trade_id → signal
    board_count: int = 0
    trade_print_count: int = 0
    fill_count: int = 0
    metrics: ReplayMetrics = field(default_factory=ReplayMetrics)


class ReplayRunner:
    """Feeds a :class:`~kabu_hft.replay.loader.ReplayLoader` stream through the
    :class:`~kabu_hft.signals.SignalStack` and a simplified execution model,
    collecting :class:`ReplayResult` with full metrics.

    The runner is **synchronous** and uses a
    :class:`~kabu_hft.replay.fill_model.PriceCrossFillModel` instead of the
    live REST order API.  All strategy decisions (entry/exit thresholds, risk
    limits) use the same :class:`~kabu_hft.config.StrategyConfig` as live
    trading so results are directly comparable.

    Usage::

        runner = ReplayRunner(config)
        loader = ReplayLoader(["data/board_20260311.jsonl"])
        result = runner.run(loader)
        print(result.metrics.summary())
    """

    def __init__(
        self,
        config: StrategyConfig,
        latency_us: int = 5_000,
        order_profile: OrderProfile | None = None,
    ) -> None:
        self.config = config
        self._fill_model = PriceCrossFillModel(latency_us=latency_us)
        self._profile = order_profile or _DUMMY_PROFILE

        self._signals = SignalStack(
            obi_depth=config.obi_depth,
            obi_decay=config.obi_decay,
            lob_ofi_depth=config.lob_ofi_depth,
            lob_ofi_decay=config.lob_ofi_decay,
            tape_window_sec=config.tape_window_sec,
            mp_ema_alpha=config.mp_ema_alpha,
            tick_size=config.tick_size,
            zscore_window=config.zscore_window,
            weights=config.signal_weights,
        )
        self._exec = ExecutionController(
            symbol=config.symbol,
            exchange=config.exchange,
            rest_client=None,  # type: ignore[arg-type]  # not used in replay
            order_profile=self._profile,
            dry_run=True,
            tick_size=config.tick_size,
            strong_threshold=config.strong_threshold,
            min_edge_ticks=config.min_edge_ticks,
            max_pending_ms=config.max_pending_ms,
            min_order_lifetime_ms=config.min_order_lifetime_ms,
            max_requotes_per_minute=config.max_requotes_per_minute,
            allow_aggressive_entry=config.allow_aggressive_entry,
            allow_aggressive_exit=config.allow_aggressive_exit,
        )
        self._risk = RiskGuard(
            base_qty=config.base_qty,
            max_qty=config.max_qty,
            max_inventory_qty=config.max_inventory_qty,
            max_notional=config.max_notional,
            daily_loss_limit=config.daily_loss_limit,
            consecutive_loss_limit=config.consecutive_loss_limit,
            cooling_seconds=config.cooling_seconds,
            max_hold_seconds=config.max_hold_seconds,
            max_spread_ticks=config.max_spread_ticks,
            stale_quote_ms=config.stale_quote_ms,
            tick_size=config.tick_size,
            allow_short=self._profile.allow_short,
            entry_threshold=config.entry_threshold,
            max_mtm_loss=config.max_mtm_loss,
        )
        self._result = ReplayResult()
        self._signal_at_entry: SignalPacket | None = None
        self._order_counter = 0

    def run(self, loader: ReplayLoader) -> ReplayResult:
        """Process the entire loader stream synchronously.

        Returns the accumulated :class:`ReplayResult`.
        """
        self._result = ReplayResult()
        self._signal_at_entry = None
        self._fill_model.clear()

        start_wall = time.monotonic()

        for recv_ns, snapshot, trade in loader.stream(
            symbol_filter=self.config.symbol if loader._paths else None
        ):
            self._step(recv_ns, snapshot, trade)

        # Close any open position at end of replay
        if self._exec.has_inventory and self._result.board_count > 0:
            # Use the last valid snapshot if available (replay ended with open position)
            logger.warning(
                "replay ended with open inventory for %s — no exit recorded",
                self.config.symbol,
            )

        elapsed = time.monotonic() - start_wall
        logger.info(
            "replay done: %d board events, %d trades, %.2fs wall time",
            self._result.board_count,
            len(self._result.trades),
            elapsed,
        )
        return self._result

    # ------------------------------------------------------------------
    # Internal step logic
    # ------------------------------------------------------------------

    def _step(
        self,
        recv_ns: int,
        snapshot: BoardSnapshot,
        trade: TradePrint | None,
    ) -> None:
        self._result.board_count += 1
        now_ns = recv_ns if recv_ns > 0 else snapshot.ts_ns

        # 1. Update signals
        if trade is not None:
            self._signals.on_trade(trade)
            self._result.trade_print_count += 1

        signal = self._signals.on_board(snapshot)
        self._result.metrics.add_board(snapshot.mid, signal.composite)

        # 2. Check fill model fills
        fills = self._fill_model.on_board(snapshot, now_ns)
        for fill in fills:
            self._apply_fill(fill, snapshot, now_ns)

        # 3. Update risk volatility
        self._risk.update_vol(snapshot)

        # 4. Check order timeout
        if self._exec.working_order is not None:
            if self._exec.working_age_ns(now_ns) > self._exec.max_pending_ns:
                self._cancel_working("timeout", snapshot, now_ns)

        # 5. Strategy decision
        self._strategy_tick(snapshot, signal, now_ns)

        # 6. Collect completed trades
        self._drain_trades(signal)

    def _strategy_tick(
        self,
        snapshot: BoardSnapshot,
        signal: SignalPacket,
        now_ns: int,
    ) -> None:
        state = self._exec.state
        score = signal.composite
        now_dt = datetime.fromtimestamp(now_ns / 1e9)

        if state is ExecutionState.OPENING:
            working = self._exec.working_order
            if working is None:
                return
            desired = self._exec.preview_entry(
                direction=working.side,
                snapshot=snapshot,
                score=abs(score),
                microprice=signal.microprice,
            )
            should_cancel, reason = self._risk.should_cancel_entry(
                working_price=working.price,
                desired_price=desired.price,
                signal_strength=abs(score),
                working_age_ns=self._exec.working_age_ns(now_ns),
                min_lifetime_ns=self._exec.min_order_lifetime_ns,
                snapshot=snapshot,
                now_ns=now_ns,
            )
            if should_cancel and self._exec.can_requote(now_ns):
                self._cancel_working(reason, snapshot, now_ns)
            return

        if state is ExecutionState.CLOSING:
            inv = self._exec.inventory
            must_close, reason = self._risk.must_close(
                open_ts_ns=inv.opened_ts_ns,
                snapshot=snapshot,
                now_ns=now_ns,
                now_dt=now_dt,
                open_price=inv.avg_price,
                position_side=inv.side,
                position_qty=inv.qty,
            )
            if must_close and self._exec.working_age_ns(now_ns) > self._exec.min_order_lifetime_ns:
                self._cancel_working(reason, snapshot, now_ns)
            return

        if state is ExecutionState.OPEN:
            inv = self._exec.inventory
            must_close, reason = self._risk.must_close(
                open_ts_ns=inv.opened_ts_ns,
                snapshot=snapshot,
                now_ns=now_ns,
                now_dt=now_dt,
                open_price=inv.avg_price,
                position_side=inv.side,
                position_qty=inv.qty,
            )
            signal_reversed = (
                self._exec.inventory.side > 0 and score <= -self.config.exit_threshold
            ) or (
                self._exec.inventory.side < 0 and score >= self.config.exit_threshold
            )
            if must_close or signal_reversed:
                self._send_exit(
                    snapshot=snapshot,
                    score=score,
                    reason=reason or "signal_reverse",
                    force=must_close,
                    now_ns=now_ns,
                )
            return

        # FLAT — consider opening
        if score == 0.0:
            return
        direction = 1 if score > 0 else -1
        allowed, _ = self._risk.can_open(
            snapshot=snapshot,
            direction=direction,
            signal_strength=abs(score),
            inventory_qty=self._exec.inventory.qty,
            now_ns=now_ns,
            now_dt=now_dt,
        )
        if not allowed:
            return

        qty = self._risk.calc_qty(
            signal_strength=abs(score),
            mid=snapshot.mid,
            inventory_qty=self._exec.inventory.qty,
        )
        if qty <= 0:
            return

        self._send_entry(
            direction=direction,
            qty=qty,
            snapshot=snapshot,
            score=abs(score),
            microprice=signal.microprice,
            now_ns=now_ns,
            signal=signal,
        )

    def _send_entry(
        self,
        *,
        direction: int,
        qty: int,
        snapshot: BoardSnapshot,
        score: float,
        microprice: float,
        now_ns: int,
        signal: SignalPacket,
    ) -> None:
        if self._exec.state is not ExecutionState.FLAT:
            return
        decision = self._exec.preview_entry(
            direction=direction, snapshot=snapshot, score=score, microprice=microprice
        )
        if not decision.is_market and decision.edge_ticks < self._exec.selector.min_edge_ticks:
            return

        self._order_counter += 1
        order_id = f"RPL-{self.config.symbol}-{self._order_counter}"
        sim_order = SimOrder(
            order_id=order_id,
            side=direction,
            qty=qty,
            price=decision.price,
            is_market=decision.is_market,
            sent_ns=now_ns,
        )

        # Register in execution controller (dry_run path, no actual REST call)
        from kabu_hft.execution.engine import WorkingOrder
        self._exec.working_order = WorkingOrder(
            order_id=order_id,
            purpose="entry",
            side=direction,
            qty=qty,
            price=decision.price,
            is_market=decision.is_market,
            sent_ns=now_ns,
            reason="alpha_entry",
        )
        self._exec.stats["sent_orders"] += 1
        self._exec.stats["open_attempts"] += 1
        self._signal_at_entry = signal

        # Submit to fill model
        if decision.is_market:
            fill_price = snapshot.ask if direction > 0 else snapshot.bid
            self._exec._apply_fill(qty=qty, fill_price=fill_price, fill_ts_ns=now_ns)
            self._exec._finalize_working_order(final_status="filled")
        else:
            self._fill_model.submit(sim_order)

    def _send_exit(
        self,
        *,
        snapshot: BoardSnapshot,
        score: float,
        reason: str,
        force: bool,
        now_ns: int,
    ) -> None:
        if self._exec.inventory.qty <= 0 or self._exec.working_order is not None:
            return
        decision = self._exec.selector.exit(
            position_side=self._exec.inventory.side,
            snapshot=snapshot,
            score=score,
            force=force,
        )
        qty = self._exec.inventory.qty
        self._order_counter += 1
        order_id = f"RPL-{self.config.symbol}-{self._order_counter}"
        sim_order = SimOrder(
            order_id=order_id,
            side=-self._exec.inventory.side,
            qty=qty,
            price=decision.price,
            is_market=decision.is_market,
            sent_ns=now_ns,
        )

        from kabu_hft.execution.engine import WorkingOrder
        self._exec.working_order = WorkingOrder(
            order_id=order_id,
            purpose="exit",
            side=-self._exec.inventory.side,
            qty=qty,
            price=decision.price,
            is_market=decision.is_market,
            sent_ns=now_ns,
            reason=reason,
        )
        self._exec.stats["sent_orders"] += 1
        self._exec.stats["close_attempts"] += 1

        if decision.is_market:
            fill_price = snapshot.bid if self._exec.inventory.side > 0 else snapshot.ask
            self._exec._apply_fill(qty=qty, fill_price=fill_price, fill_ts_ns=now_ns)
            self._exec._finalize_working_order(final_status="filled")
        else:
            self._fill_model.submit(sim_order)

    def _cancel_working(
        self, reason: str, snapshot: BoardSnapshot, now_ns: int
    ) -> None:
        if self._exec.working_order is None:
            return
        order_id = self._exec.working_order.order_id
        self._fill_model.cancel(order_id)
        self._exec._finalize_working_order(final_status="cancelled")
        self._exec.stats["cancel_orders"] += 1

    def _apply_fill(
        self, fill: FillResult, snapshot: BoardSnapshot, now_ns: int
    ) -> None:
        if self._exec.working_order is None:
            return
        if self._exec.working_order.order_id != fill.order.order_id:
            return
        self._exec._apply_fill(
            qty=fill.fill_qty,
            fill_price=fill.fill_price,
            fill_ts_ns=fill.fill_ns,
        )
        self._exec.working_order.cum_qty += fill.fill_qty
        if self._exec.working_order.cum_qty >= self._exec.working_order.qty:
            self._exec._finalize_working_order(final_status="filled")
        self._result.fill_count += 1

    def _drain_trades(self, current_signal: SignalPacket) -> None:
        for trade in self._exec.drain_round_trips():
            self._risk.record_trade(
                symbol=trade.symbol,
                side=trade.side,
                qty=trade.qty,
                entry_price=trade.entry_price,
                exit_price=trade.exit_price,
                entry_ts_ns=trade.entry_ts_ns,
                exit_ts_ns=trade.exit_ts_ns,
                commission=0.0,
            )
            self._result.trades.append(trade)
            trade_id = len(self._result.trades) - 1
            self._result.signal_at_entry[trade_id] = self._signal_at_entry or current_signal
            self._result.metrics.add_trade(trade, self._signal_at_entry)
            self._signal_at_entry = None
