from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone

from kabu_hft.config import OrderProfile, StrategyConfig
from kabu_hft.core.market_state import MarketState, MarketStateDetector
from kabu_hft.execution import ExecutionController, ExecutionState, QuoteMode
from kabu_hft.gateway import BoardSnapshot, KabuAdapter, KabuRestClient, TradePrint
from kabu_hft.journal import TradeJournal
from kabu_hft.risk import RiskGuard
from kabu_hft.signals import SignalPacket, SignalStack

logger = logging.getLogger("kabu.strategy")
JST = timezone(timedelta(hours=9))


class HFTStrategy:
    def __init__(
        self,
        *,
        config: StrategyConfig,
        order_profile: OrderProfile,
        rest_client: KabuRestClient,
        dry_run: bool,
        journal: TradeJournal | None = None,
        markout_seconds: int = 30,
    ):
        self.config = config
        self.rest_client = rest_client
        self.dry_run = dry_run
        self.signals = SignalStack(
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
        self.risk = RiskGuard(
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
            allow_short=order_profile.allow_short,
            entry_threshold=config.entry_threshold,
        )
        self.execution = ExecutionController(
            symbol=config.symbol,
            exchange=config.exchange,
            rest_client=rest_client,
            order_profile=order_profile,
            dry_run=dry_run,
            tick_size=config.tick_size,
            strong_threshold=config.strong_threshold,
            min_edge_ticks=config.min_edge_ticks,
            max_pending_ms=config.max_pending_ms,
            min_order_lifetime_ms=config.min_order_lifetime_ms,
            max_requotes_per_minute=config.max_requotes_per_minute,
            allow_aggressive_entry=config.allow_aggressive_entry,
            allow_aggressive_exit=config.allow_aggressive_exit,
        )
        self.market_state = MarketStateDetector(
            tick_size=config.tick_size,
            stale_quote_ms=config.stale_quote_ms,
            queue_spread_max_ticks=config.queue_spread_max_ticks,
            abnormal_max_spread_ticks=config.abnormal_max_spread_ticks,
            max_event_rate_hz=config.max_event_rate_hz,
            event_burst_min_events=config.event_burst_min_events,
            state_window_ms=config.state_window_ms,
            jump_threshold_ticks=config.jump_threshold_ticks,
        )

        self.journal = journal
        self.markout_seconds = markout_seconds
        self._last_snapshot: BoardSnapshot | None = None
        self._signal_at_entry: SignalPacket | None = None
        self._mid_ref: list[float] = [0.0]
        self._last_market_state: str = MarketState.NORMAL.value
        self._last_market_reason: str = "init"
        self._last_fair_price: float = 0.0
        self._last_reservation_price: float = 0.0
        self._pending_entry_alpha: float = 0.0

        self.started = False
        self.latest_item: tuple[BoardSnapshot, SignalPacket, int] | None = None
        self.signal_event = asyncio.Event()
        self.exec_task: asyncio.Task | None = None
        self.timeout_task: asyncio.Task | None = None
        self.reconcile_task: asyncio.Task | None = None
        self.board_count = 0
        self.trade_count = 0
        self.signal_count = 0
        self.last_board_ns = 0
        self.min_board_interval_ns = int(config.min_board_interval_ms * 1_000_000)

    async def start(self) -> None:
        if self.started:
            return
        self.started = True
        self.exec_task = asyncio.create_task(self._signal_loop(), name=f"{self.config.symbol}-signal-loop")
        self.timeout_task = asyncio.create_task(self._timeout_loop(), name=f"{self.config.symbol}-timeout-loop")
        if not self.dry_run:
            self.reconcile_task = asyncio.create_task(self._reconcile_loop(), name=f"{self.config.symbol}-reconcile-loop")
        logger.info("strategy started symbol=%s dry_run=%s", self.config.symbol, self.dry_run)

    async def stop(self) -> None:
        self.started = False
        for task in (self.exec_task, self.timeout_task, self.reconcile_task):
            if task is not None:
                task.cancel()
        for task in (self.exec_task, self.timeout_task, self.reconcile_task):
            if task is None:
                continue
            try:
                await task
            except asyncio.CancelledError:
                pass

    def on_board(self, snapshot: BoardSnapshot) -> None:
        if not self.started or snapshot.symbol != self.config.symbol:
            return

        # Paper fill must be evaluated on every board event, even if throttled,
        # to match live behavior where fills arrive independently of the rate limiter.
        self.execution.sync_paper_board(snapshot)
        self._mid_ref[0] = snapshot.mid

        now_ns = time.time_ns()
        if now_ns - self.last_board_ns < self.min_board_interval_ns:
            return
        self.last_board_ns = now_ns
        self.board_count += 1

        self._last_snapshot = snapshot
        self._drain_completed_trades()
        self.risk.update_vol(snapshot)

        signal = self.signals.on_board(snapshot)
        self.signal_count += 1
        self.latest_item = (snapshot, signal, now_ns)
        self.signal_event.set()

    def on_trade(self, trade: TradePrint) -> None:
        if not self.started or trade.symbol != self.config.symbol:
            return
        self.trade_count += 1
        self.signals.on_trade(trade)
        self.execution.sync_paper_trade(trade)
        self._drain_completed_trades()

    async def _signal_loop(self) -> None:
        while self.started:
            await self.signal_event.wait()
            self.signal_event.clear()
            item = self.latest_item
            if item is None:
                continue
            snapshot, signal, now_ns = item
            try:
                await self._process_signal(snapshot, signal, now_ns)
            except Exception as exc:
                logger.exception("signal loop error symbol=%s error=%s", self.config.symbol, exc)

    async def _process_signal(self, snapshot: BoardSnapshot, signal: SignalPacket, now_ns: int) -> None:
        self._drain_completed_trades()

        # Handle stranded partial fill: entry was partially filled then cancelled.
        # Force-close the residual inventory immediately before any other logic.
        if self.execution.has_stranded_partial and self.execution.state is ExecutionState.OPEN:
            self.execution.has_stranded_partial = False
            await self.execution.close(
                snapshot=snapshot,
                score=0.0,
                reason="stranded_partial_close",
                force=True,
            )
            return

        now_dt = datetime.now(JST)
        score = signal.composite
        state = self.execution.state
        market_view = self.market_state.evaluate(snapshot, now_ns)
        self._last_market_state = market_view.state.value
        self._last_market_reason = market_view.reason
        fair_price, reservation_price = self._fair_and_reservation(snapshot, score)
        self._last_fair_price = fair_price
        self._last_reservation_price = reservation_price

        if state is not ExecutionState.OPENING:
            self._pending_entry_alpha = 0.0

        if market_view.state is MarketState.ABNORMAL:
            if state is ExecutionState.OPENING and self.execution.working_age_ns(now_ns) > self.execution.min_order_lifetime_ns:
                await self.execution.cancel_working(reason=f"abnormal_{market_view.reason}")
            elif state is ExecutionState.OPEN:
                await self.execution.close(
                    snapshot=snapshot,
                    score=score,
                    reason=f"abnormal_{market_view.reason}",
                    force=True,
                )
            return

        if state is ExecutionState.OPENING:
            working = self.execution.working_order
            if working is None:
                return
            queue_threshold = self._queue_threshold(snapshot, abs(score))
            mode = self._parse_mode(working.mode)
            desired = self.execution.preview_entry(
                direction=working.side,
                snapshot=snapshot,
                score=abs(score),
                microprice=signal.microprice,
                mode=mode,
                reservation_price=reservation_price,
                queue_qty_threshold=queue_threshold,
            )
            should_cancel, reason = self.risk.should_cancel_entry(
                working_price=working.price,
                desired_price=desired.price,
                signal_strength=abs(score),
                working_age_ns=self.execution.working_age_ns(now_ns),
                min_lifetime_ns=self.execution.min_order_lifetime_ns,
                snapshot=snapshot,
                now_ns=now_ns,
            )
            if self._pending_entry_alpha != 0.0 and score * self._pending_entry_alpha < 0 and abs(score) >= self.config.exit_threshold:
                should_cancel = True
                reason = "alpha_flip"
            fair_drift_ticks = abs(fair_price - working.price) / max(self.config.tick_size, 1e-9)
            if fair_drift_ticks >= self.config.max_fair_drift_ticks:
                should_cancel = True
                reason = "fair_drift"
            if should_cancel and self.execution.can_requote(now_ns):
                await self.execution.cancel_working(reason=reason)
            return

        if state is ExecutionState.CLOSING:
            must_close, reason = self.risk.must_close(
                open_ts_ns=self.execution.inventory.opened_ts_ns,
                snapshot=snapshot,
                now_ns=now_ns,
                now_dt=now_dt,
            )
            if must_close and self.execution.working_age_ns(now_ns) > self.execution.min_order_lifetime_ns:
                await self.execution.cancel_working(reason=reason)
            return

        if state is ExecutionState.OPEN:
            must_close, reason = self.risk.must_close(
                open_ts_ns=self.execution.inventory.opened_ts_ns,
                snapshot=snapshot,
                now_ns=now_ns,
                now_dt=now_dt,
            )
            signal_reversed = (
                self.execution.inventory.side > 0 and score <= -self.config.exit_threshold
            ) or (
                self.execution.inventory.side < 0 and score >= self.config.exit_threshold
            )
            if must_close or signal_reversed:
                await self.execution.close(
                    snapshot=snapshot,
                    score=score,
                    reason=reason or "signal_reverse",
                    force=must_close or abs(score) >= self.config.strong_threshold,
                )
            return

        if score == 0.0:
            return
        direction = 1 if score > 0 else -1
        allowed, reason = self.risk.can_open(
            snapshot=snapshot,
            direction=direction,
            signal_strength=abs(score),
            inventory_qty=self.execution.inventory.qty,
            now_ns=now_ns,
            now_dt=now_dt,
        )
        if not allowed:
            return

        qty = self.risk.calc_qty(
            signal_strength=abs(score),
            mid=snapshot.mid,
            inventory_qty=self.execution.inventory.qty,
        )
        if qty <= 0:
            return

        quote_mode = self._mode_for_market(market_view.state)
        queue_threshold = self._queue_threshold(snapshot, abs(score))
        opened = await self.execution.open(
            direction=direction,
            qty=qty,
            snapshot=snapshot,
            score=abs(score),
            microprice=signal.microprice,
            reason="alpha_entry",
            mode=quote_mode,
            reservation_price=reservation_price,
            queue_qty_threshold=queue_threshold,
        )
        if opened:
            self._signal_at_entry = signal
            self._pending_entry_alpha = score

    def _fair_and_reservation(self, snapshot: BoardSnapshot, score: float) -> tuple[float, float]:
        tick = max(self.config.tick_size, 1e-9)
        fair_shift_ticks = max(
            -self.config.max_fair_shift_ticks,
            min(self.config.max_fair_shift_ticks, self.config.fair_value_beta * score),
        )
        fair_price = snapshot.mid + fair_shift_ticks * tick

        inventory_ratio = 0.0
        if self.config.max_inventory_qty > 0:
            signed_inventory = self.execution.inventory.side * self.execution.inventory.qty
            inventory_ratio = signed_inventory / self.config.max_inventory_qty
        skew_multiplier = 1.0
        if abs(inventory_ratio) >= 0.66:
            skew_multiplier = 1.5
        skew_ticks = self.config.inventory_skew_ticks * skew_multiplier * inventory_ratio
        reservation_price = fair_price - skew_ticks * tick
        return fair_price, reservation_price

    def _mode_for_market(self, market_state: MarketState) -> QuoteMode:
        if market_state is MarketState.QUEUE:
            return QuoteMode.QUEUE_DEFENSE
        if market_state is MarketState.ABNORMAL:
            return QuoteMode.CLOSE_ONLY
        return QuoteMode.PASSIVE_FAIR_VALUE

    def _queue_threshold(self, snapshot: BoardSnapshot, signal_strength: float) -> int:
        base = max(self.config.queue_min_top_qty, 1)
        inventory_ratio = 0.0
        if self.config.max_inventory_qty > 0:
            inventory_ratio = min(
                1.0,
                self.execution.inventory.qty / self.config.max_inventory_qty,
            )
        spread_factor = 1.0 if snapshot.spread <= self.config.tick_size + 1e-9 else 0.8
        # Stronger alpha lowers retreat threshold so we are more willing to defend best level.
        alpha_discount = min(max(signal_strength - 1.0, 0.0) * 0.20, 0.40)
        alpha_factor = 1.0 - alpha_discount
        threshold = int(base * spread_factor * alpha_factor * (1.0 + inventory_ratio))
        return max(threshold, 1)

    @staticmethod
    def _parse_mode(raw: str) -> QuoteMode:
        try:
            return QuoteMode(raw)
        except ValueError:
            return QuoteMode.PASSIVE_FAIR_VALUE

    async def emergency_close(self) -> None:
        """Attempt a forced market close of any open inventory. Called during shutdown."""
        if not self.execution.has_inventory:
            return
        snapshot = self._last_snapshot
        if snapshot is None or not snapshot.valid:
            logger.warning(
                "emergency_close symbol=%s: no valid snapshot, position may remain open",
                self.config.symbol,
            )
            return
        logger.warning(
            "emergency_close symbol=%s side=%+d qty=%d",
            self.config.symbol,
            self.execution.inventory.side,
            self.execution.inventory.qty,
        )
        if self.execution.working_order is not None:
            await self.execution.cancel_working(reason="emergency_shutdown")
            await asyncio.sleep(0.2)
        await self.execution.close(
            snapshot=snapshot,
            score=0.0,
            reason="emergency_shutdown",
            force=True,
        )
        await asyncio.sleep(0.5)
        self._drain_completed_trades()

    async def _timeout_loop(self) -> None:
        while self.started:
            await asyncio.sleep(0.25)
            try:
                await self.execution.check_timeout(time.time_ns())
                self._drain_completed_trades()
            except Exception as exc:
                logger.exception("timeout loop error symbol=%s error=%s", self.config.symbol, exc)

    async def _reconcile_loop(self) -> None:
        interval = max(self.config.poll_interval_ms / 1000.0, 0.1)
        while self.started:
            await asyncio.sleep(interval)
            order_id = self.execution.current_order_id
            if not order_id:
                continue
            try:
                records = await self.rest_client.get_orders(order_id=order_id)
                for raw in records:
                    snapshot = KabuAdapter.order_snapshot(raw)
                    if snapshot is not None:
                        self.execution.reconcile_with_broker(snapshot)
                        self.execution.sync_order_snapshot(snapshot)
                self._drain_completed_trades()
            except Exception as exc:
                logger.warning("reconcile error symbol=%s order_id=%s error=%s", self.config.symbol, order_id, exc)

    def _drain_completed_trades(self) -> None:
        for trade in self.execution.drain_round_trips():
            self.risk.record_trade(
                symbol=trade.symbol,
                side=trade.side,
                qty=trade.qty,
                entry_price=trade.entry_price,
                exit_price=trade.exit_price,
                entry_ts_ns=trade.entry_ts_ns,
                exit_ts_ns=trade.exit_ts_ns,
                commission=0.0,
            )
            if self.journal is not None:
                self.journal.log_trade(trade, self._signal_at_entry)
                self.journal.schedule_markout(trade=trade, mid_ref=self._mid_ref)
            self._signal_at_entry = None

    def status(self) -> dict[str, object]:
        signal = self.signals.last
        return {
            "symbol": self.config.symbol,
            "state": self.execution.state.value,
            "market_state": self._last_market_state,
            "market_reason": self._last_market_reason,
            "board_count": self.board_count,
            "trade_count": self.trade_count,
            "signal_count": self.signal_count,
            "execution": self.execution.snapshot(),
            "risk": self.risk.summary(),
            "signal": {
                "composite": signal.composite if signal else 0.0,
                "obi_z": signal.obi_z if signal else 0.0,
                "lob_ofi_z": signal.lob_ofi_z if signal else 0.0,
                "tape_ofi_z": signal.tape_ofi_z if signal else 0.0,
                "fair_price": self._last_fair_price,
                "reservation_price": self._last_reservation_price,
            },
        }
