from __future__ import annotations

import logging
import math
import time
from collections import deque
from dataclasses import dataclass
from enum import Enum

from typing import Optional

from kabu_hft.clock import Clock, LiveClock
from kabu_hft.config import OrderProfile
from kabu_hft.gateway import BoardSnapshot, KabuApiError, KabuRestClient, OrderSnapshot, TradePrint
from kabu_hft.oms.orders import OrderLedger, OrderStatus, WorkingOrderRecord
from kabu_hft.oms.reconciliation import reconcile_order_state

logger = logging.getLogger("kabu.execution")


def _extract_error_code(payload: object) -> int | None:
    candidates: list[object] = []
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


class ExecutionState(str, Enum):
    FLAT = "FLAT"
    OPENING = "OPENING"
    OPEN = "OPEN"
    CLOSING = "CLOSING"


class QuoteMode(str, Enum):
    PASSIVE_FAIR_VALUE = "PASSIVE_FAIR_VALUE"
    QUEUE_DEFENSE = "QUEUE_DEFENSE"
    CLOSE_ONLY = "CLOSE_ONLY"


@dataclass(slots=True)
class PriceDecision:
    price: float
    is_market: bool
    edge_ticks: float


@dataclass(slots=True)
class WorkingOrder:
    order_id: str
    purpose: str
    side: int
    qty: int
    price: float
    is_market: bool
    sent_ts_ns: int
    reason: str
    mode: str = QuoteMode.PASSIVE_FAIR_VALUE.value
    cum_qty: int = 0
    avg_fill_price: float = 0.0
    cancel_requested: bool = False
    queue_ahead_qty: int = 0  # Estimated lots ahead of us in the queue (paper trading)


@dataclass(slots=True)
class Inventory:
    side: int = 0
    qty: int = 0
    avg_price: float = 0.0
    opened_ts_ns: int = 0
    entry_qty: int = 0
    exit_qty: int = 0
    exit_value: float = 0.0


@dataclass(slots=True)
class RoundTrip:
    symbol: str
    side: int
    qty: int
    entry_price: float
    exit_price: float
    entry_ts_ns: int
    exit_ts_ns: int
    realized_pnl: float
    exit_reason: str


class RequoteBudget:
    def __init__(self, max_requotes_per_minute: int):
        self.max_requotes_per_minute = max_requotes_per_minute
        self.timestamps: deque[int] = deque()

    def _trim(self, now_ns: int) -> None:
        window_ns = 60 * 1_000_000_000
        while self.timestamps and now_ns - self.timestamps[0] > window_ns:
            self.timestamps.popleft()

    def allow(self, now_ns: int) -> bool:
        """Check only — does NOT consume budget. Call consume() after successful send."""
        self._trim(now_ns)
        return len(self.timestamps) < self.max_requotes_per_minute

    def consume(self, now_ns: int) -> None:
        """Record a requote usage. Call only after a cancel+re-send succeeds."""
        self.timestamps.append(now_ns)


class PriceSelector:
    def __init__(
        self,
        *,
        tick_size: float,
        strong_threshold: float,
        min_edge_ticks: float,
        allow_aggressive_entry: bool,
        allow_aggressive_exit: bool,
    ):
        self.tick_size = tick_size
        self.strong_threshold = strong_threshold
        self.min_edge_ticks = min_edge_ticks
        self.allow_aggressive_entry = allow_aggressive_entry
        self.allow_aggressive_exit = allow_aggressive_exit

    def _retreat_price(self, direction: int, snapshot: BoardSnapshot) -> float:
        if direction > 0:
            return max(snapshot.bid - self.tick_size, self.tick_size)
        return max(snapshot.ask + self.tick_size, self.tick_size)

    def _improve_price(self, direction: int, snapshot: BoardSnapshot) -> float:
        if direction > 0:
            return min(snapshot.bid + self.tick_size, snapshot.ask - self.tick_size)
        return max(snapshot.ask - self.tick_size, snapshot.bid + self.tick_size)

    def entry(
        self,
        *,
        direction: int,
        snapshot: BoardSnapshot,
        score: float,
        microprice: float,
        mode: QuoteMode,
        reservation_price: float | None,
        queue_qty_threshold: int,
    ) -> PriceDecision:
        reference_price = reservation_price if reservation_price and reservation_price > 0 else microprice
        if direction > 0:
            price = snapshot.bid
            if mode is QuoteMode.QUEUE_DEFENSE and snapshot.bid_size < max(queue_qty_threshold, 1):
                price = self._retreat_price(direction, snapshot)
            elif (
                mode is QuoteMode.PASSIVE_FAIR_VALUE
                and reservation_price is not None
                and reservation_price <= snapshot.bid - self.tick_size
            ):
                price = self._retreat_price(direction, snapshot)
            elif score >= self.strong_threshold and snapshot.spread >= 2 * self.tick_size:
                price = self._improve_price(direction, snapshot)
            is_market = (
                self.allow_aggressive_entry
                and mode is not QuoteMode.CLOSE_ONLY
                and score >= self.strong_threshold * 1.5
            )
            if is_market:
                price = snapshot.ask
            edge_ticks = (reference_price - price) / max(self.tick_size, 1e-9)
        else:
            price = snapshot.ask
            if mode is QuoteMode.QUEUE_DEFENSE and snapshot.ask_size < max(queue_qty_threshold, 1):
                price = self._retreat_price(direction, snapshot)
            elif (
                mode is QuoteMode.PASSIVE_FAIR_VALUE
                and reservation_price is not None
                and reservation_price >= snapshot.ask + self.tick_size
            ):
                price = self._retreat_price(direction, snapshot)
            elif score >= self.strong_threshold and snapshot.spread >= 2 * self.tick_size:
                price = self._improve_price(direction, snapshot)
            is_market = (
                self.allow_aggressive_entry
                and mode is not QuoteMode.CLOSE_ONLY
                and score >= self.strong_threshold * 1.5
            )
            if is_market:
                price = snapshot.bid
            edge_ticks = (price - reference_price) / max(self.tick_size, 1e-9)
        return PriceDecision(price=price, is_market=is_market, edge_ticks=edge_ticks)

    def exit(
        self,
        *,
        position_side: int,
        snapshot: BoardSnapshot,
        score: float,
        force: bool,
    ) -> PriceDecision:
        if position_side > 0:
            price = snapshot.ask
            if score <= -self.strong_threshold and snapshot.spread >= 2 * self.tick_size:
                price = max(snapshot.ask - self.tick_size, snapshot.bid + self.tick_size)
            is_market = force and self.allow_aggressive_exit
            if is_market:
                price = snapshot.bid
        else:
            price = snapshot.bid
            if score >= self.strong_threshold and snapshot.spread >= 2 * self.tick_size:
                price = min(snapshot.bid + self.tick_size, snapshot.ask - self.tick_size)
            is_market = force and self.allow_aggressive_exit
            if is_market:
                price = snapshot.ask
        return PriceDecision(price=price, is_market=is_market, edge_ticks=0.0)


class ExecutionController:
    def __init__(
        self,
        *,
        symbol: str,
        exchange: int,
        rest_client: Optional[KabuRestClient],
        order_profile: OrderProfile,
        dry_run: bool,
        tick_size: float,
        strong_threshold: float,
        min_edge_ticks: float,
        max_pending_ms: int,
        min_order_lifetime_ms: int,
        max_requotes_per_minute: int,
        allow_aggressive_entry: bool,
        allow_aggressive_exit: bool,
        clock: Clock = LiveClock(),
        queue_model: bool = True,
    ):
        self.symbol = symbol
        self.exchange = exchange
        self.rest_client = rest_client
        self.order_profile = order_profile
        self.dry_run = dry_run
        self.selector = PriceSelector(
            tick_size=tick_size,
            strong_threshold=strong_threshold,
            min_edge_ticks=min_edge_ticks,
            allow_aggressive_entry=allow_aggressive_entry,
            allow_aggressive_exit=allow_aggressive_exit,
        )
        self.max_pending_ns = max_pending_ms * 1_000_000
        self.min_order_lifetime_ns = min_order_lifetime_ms * 1_000_000
        self.requotes = RequoteBudget(max_requotes_per_minute)

        self._clock = clock
        self.queue_model = queue_model
        self.working_order: WorkingOrder | None = None
        self.inventory = Inventory()
        self.closed_trades: deque[RoundTrip] = deque()
        self.paper_order_counter = 0
        self.paper_last_fill_reason = ""
        self.has_stranded_partial: bool = False
        self._entry_blocked_until_ns: int = 0
        self._exit_blocked_until_ns: int = 0
        self._last_position_sync_ns: int = 0
        self._position_sync_interval_ns: int = 1_000_000_000
        self._manual_close_lock: bool = False
        self.has_external_inventory: bool = False
        self.broker_hold_qty: int = 0
        self.broker_closable_qty: int = 0
        self._order_ledger = OrderLedger()
        self.stats = {
            "sent_orders": 0,
            "cancel_orders": 0,
            "fills": 0,
            "open_attempts": 0,
            "close_attempts": 0,
        }

    @property
    def state(self) -> ExecutionState:
        if self.working_order is not None:
            return ExecutionState.OPENING if self.working_order.purpose == "entry" else ExecutionState.CLOSING
        if self.inventory.qty > 0:
            return ExecutionState.OPEN
        return ExecutionState.FLAT

    @property
    def current_order_id(self) -> str:
        return self.working_order.order_id if self.working_order else ""

    @property
    def has_inventory(self) -> bool:
        return self.inventory.qty > 0

    def has_external_inventory_conflict(self) -> bool:
        return self.has_external_inventory or self._manual_close_lock

    def working_age_ns(self, now_ns: int) -> int:
        if self.working_order is None:
            return 0
        return max(0, now_ns - self.working_order.sent_ts_ns)

    def preview_entry(
        self,
        *,
        direction: int,
        snapshot: BoardSnapshot,
        score: float,
        microprice: float,
        mode: QuoteMode = QuoteMode.PASSIVE_FAIR_VALUE,
        reservation_price: float | None = None,
        queue_qty_threshold: int = 0,
    ) -> PriceDecision:
        return self.selector.entry(
            direction=direction,
            snapshot=snapshot,
            score=score,
            microprice=microprice,
            mode=mode,
            reservation_price=reservation_price,
            queue_qty_threshold=queue_qty_threshold,
        )

    async def open(
        self,
        *,
        direction: int,
        qty: int,
        snapshot: BoardSnapshot,
        score: float,
        microprice: float,
        reason: str,
        mode: QuoteMode = QuoteMode.PASSIVE_FAIR_VALUE,
        reservation_price: float | None = None,
        queue_qty_threshold: int = 0,
    ) -> bool:
        now_ns = self._clock.time_ns()
        if self.state is not ExecutionState.FLAT or qty <= 0:
            return False
        if now_ns < self._entry_blocked_until_ns:
            return False

        decision = self.preview_entry(
            direction=direction,
            snapshot=snapshot,
            score=score,
            microprice=microprice,
            mode=mode,
            reservation_price=reservation_price,
            queue_qty_threshold=queue_qty_threshold,
        )
        if not decision.is_market and decision.edge_ticks < self.selector.min_edge_ticks:
            return False

        self.stats["open_attempts"] += 1
        if self.dry_run:
            order_id = self._next_paper_order_id()
        else:
            if self.rest_client is None:
                raise RuntimeError("rest_client is required for live trading (dry_run=False)")
            try:
                response = await self.rest_client.send_entry_order(
                    symbol=self.symbol,
                    exchange=self.exchange,
                    side=direction,
                    qty=qty,
                    price=decision.price,
                    is_market=decision.is_market,
                    profile=self.order_profile,
                )
            except KabuApiError as exc:
                error_code = _extract_error_code(exc.payload)
                if error_code == 4002004:
                    self._entry_blocked_until_ns = max(self._entry_blocked_until_ns, now_ns + 15_000_000_000)
                    # Trigger-check error: FrontOrderType / price mismatch for this session.
                    # Treat as a non-fatal rejection, but back off to avoid hammering sendorder.
                    logger.warning(
                        "entry rejected symbol=%s code=4002004 (trigger check error); "
                        "backing off 15s; check FrontOrderType or price tick alignment",
                        self.symbol,
                    )
                    return False
                raise
            order_id = str(response.get("OrderId") or response.get("ID") or "")
            if not order_id:
                logger.warning("entry rejected for %s: %s", self.symbol, response)
                return False

        self.working_order = WorkingOrder(
            order_id=order_id,
            purpose="entry",
            side=direction,
            qty=qty,
            price=decision.price,
            is_market=decision.is_market,
            sent_ts_ns=now_ns,
            reason=reason,
            mode=mode.value,
        )
        self._order_ledger.add(WorkingOrderRecord(
            order_id=order_id,
            symbol=self.symbol,
            side=direction,
            qty=qty,
            price=decision.price,
        ))
        self._order_ledger.mark_working(order_id)
        self.stats["sent_orders"] += 1
        logger.info(
            "entry order sent symbol=%s side=%+d qty=%d price=%.3f market=%s mode=%s dry_run=%s",
            self.symbol,
            direction,
            qty,
            decision.price,
            decision.is_market,
            mode.value,
            self.dry_run,
        )

        if self.dry_run and decision.is_market:
            fill_price = snapshot.ask if direction > 0 else snapshot.bid
            self._apply_fill(qty=qty, fill_price=fill_price, fill_ts_ns=now_ns)
            self._finalize_working_order(final_status="filled")
        elif self.dry_run and self.queue_model and self.working_order is not None:
            # Estimate queue position: we arrive at the back of the existing best-level queue.
            self.working_order.queue_ahead_qty = (
                snapshot.bid_size if direction > 0 else snapshot.ask_size
            )
        return True

    async def close(
        self,
        *,
        snapshot: BoardSnapshot,
        score: float,
        reason: str,
        force: bool,
        target_price: float | None = None,
    ) -> bool:
        now_ns = self._clock.time_ns()
        if self.inventory.qty <= 0 or self.working_order is not None:
            return False
        if now_ns < self._exit_blocked_until_ns:
            return False

        decision = self.selector.exit(
            position_side=self.inventory.side,
            snapshot=snapshot,
            score=score,
            force=force,
        )
        if target_price is not None and not decision.is_market:
            exit_side = -self.inventory.side
            decision = PriceDecision(
                price=self._align_price_to_tick(target_price, side=exit_side),
                is_market=False,
                edge_ticks=decision.edge_ticks,
            )
        qty = self.inventory.qty
        self.stats["close_attempts"] += 1
        if self.dry_run:
            order_id = self._next_paper_order_id()
        else:
            if self.rest_client is None:
                raise RuntimeError("rest_client is required for live trading (dry_run=False)")
            try:
                response = await self.rest_client.send_exit_order(
                    symbol=self.symbol,
                    exchange=self.exchange,
                    position_side=self.inventory.side,
                    qty=qty,
                    price=decision.price,
                    is_market=decision.is_market,
                    profile=self.order_profile,
                )
            except KabuApiError as exc:
                if "not enough inventory" in str(exc):
                    # 本地 inventory 与 broker 端出现偏差。
                    # 从 API 获取实际持仓并同步，防止无限循环。
                    await self._sync_inventory_from_api()
                    return False
                error_code = _extract_error_code(exc.payload)
                if error_code == 8:
                    # Set backoff FIRST so the loop stops even if _sync_inventory_from_api raises.
                    _code8_blocked_until = now_ns + 15_000_000_000  # 15s
                    self._exit_blocked_until_ns = _code8_blocked_until
                    logger.warning(
                        "exit rejected symbol=%s code=8 (locked lot / margin spec error); "
                        "backing off 15s — check for pending manual orders",
                        self.symbol,
                    )
                    await self._sync_inventory_from_api()
                    # sync_broker_position may clear _exit_blocked_until_ns when inventory
                    # looks clean; re-apply the code=8 backoff so we don't retry immediately.
                    self._exit_blocked_until_ns = max(self._exit_blocked_until_ns, _code8_blocked_until)
                    return False
                raise
            order_id = str(response.get("OrderId") or response.get("ID") or "")
            if not order_id:
                logger.warning("exit rejected for %s: %s", self.symbol, response)
                return False

        self.working_order = WorkingOrder(
            order_id=order_id,
            purpose="exit",
            side=-self.inventory.side,
            qty=qty,
            price=decision.price,
            is_market=decision.is_market,
            sent_ts_ns=now_ns,
            reason=reason,
        )
        self._order_ledger.add(WorkingOrderRecord(
            order_id=order_id,
            symbol=self.symbol,
            side=-self.inventory.side,
            qty=qty,
            price=decision.price,
        ))
        self._order_ledger.mark_working(order_id)
        self.stats["sent_orders"] += 1
        logger.info(
            "exit order sent symbol=%s side=%+d qty=%d price=%.3f market=%s dry_run=%s reason=%s",
            self.symbol,
            -self.inventory.side,
            qty,
            decision.price,
            decision.is_market,
            self.dry_run,
            reason,
        )

        if self.dry_run and decision.is_market:
            fill_price = snapshot.bid if self.inventory.side > 0 else snapshot.ask
            self._apply_fill(qty=qty, fill_price=fill_price, fill_ts_ns=now_ns)
            self._finalize_working_order(final_status="filled")
        return True

    async def cancel_working(self, *, reason: str) -> bool:
        if self.working_order is None or self.working_order.cancel_requested:
            return False

        order_id = self.working_order.order_id
        self.working_order.cancel_requested = True
        self.paper_last_fill_reason = reason
        if self.dry_run:
            self._finalize_working_order(final_status="cancelled")
        else:
            if self.rest_client is None:
                raise RuntimeError("rest_client is required for live trading (dry_run=False)")
            try:
                await self.rest_client.cancel_order(order_id)
            except KabuApiError as exc:
                error_code = _extract_error_code(exc.payload)
                if exc.status == 500 and error_code == 43:
                    # kabu: cancel can race with exchange fill and return code=43
                    # ("already filled"). Treat as idempotent success and wait for
                    # reconcile to finalize local order state.
                    self.stats["cancel_orders"] += 1
                    logger.info(
                        "cancel ignored symbol=%s order_id=%s code=43 already_filled reason=%s",
                        self.symbol,
                        order_id,
                        reason,
                    )
                    return True
                if self.working_order is not None and self.working_order.order_id == order_id:
                    self.working_order.cancel_requested = False
                raise
            except Exception:
                if self.working_order is not None and self.working_order.order_id == order_id:
                    self.working_order.cancel_requested = False
                raise
        self.stats["cancel_orders"] += 1
        logger.info("cancel requested symbol=%s order_id=%s reason=%s", self.symbol, order_id, reason)
        return True

    def can_requote(self, now_ns: int) -> bool:
        """Check if requote budget allows another cancel+re-place. Does NOT consume budget."""
        return self.requotes.allow(now_ns)

    def consume_requote(self, now_ns: int) -> None:
        """Record one requote usage. Call after cancel_working() returns True."""
        self.requotes.consume(now_ns)

    async def check_timeout(self, now_ns: int) -> bool:
        if self.working_order is None:
            return False
        if self.working_order.purpose == "exit":
            # Keep close quotes on book; strategy manages exit life-cycle.
            return False
        if self.working_age_ns(now_ns) <= self.max_pending_ns:
            return False
        return await self.cancel_working(reason="pending_timeout")

    def sync_order_snapshot(self, snapshot: OrderSnapshot) -> None:
        if self.working_order is None or snapshot.order_id != self.working_order.order_id:
            return

        new_qty = snapshot.cum_qty - self.working_order.cum_qty
        if new_qty < 0:
            logger.warning(
                "cum_qty regression symbol=%s order_id=%s local=%d broker=%d; skipping fill",
                self.symbol,
                self.working_order.order_id,
                self.working_order.cum_qty,
                snapshot.cum_qty,
            )
            new_qty = 0
        if new_qty > 0:
            fill_price = self._incremental_fill_price(
                prev_qty=self.working_order.cum_qty,
                prev_avg=self.working_order.avg_fill_price,
                new_qty=snapshot.cum_qty,
                new_avg=snapshot.avg_fill_price or snapshot.price,
            )
            fill_ts_ns = snapshot.fill_ts_ns if snapshot.fill_ts_ns > 0 else self._clock.time_ns()
            self._apply_fill(qty=new_qty, fill_price=fill_price, fill_ts_ns=fill_ts_ns)
            self.working_order.cum_qty = snapshot.cum_qty
            self.working_order.avg_fill_price = snapshot.avg_fill_price or fill_price

        if snapshot.is_final:
            self._finalize_working_order(final_status=snapshot.status)

    def sync_paper_board(self, snapshot: BoardSnapshot) -> None:
        if not self.dry_run or self.working_order is None:
            return
        if self.working_order.is_market:
            return

        if self.working_order.side > 0 and snapshot.ask <= self.working_order.price:
            if self.queue_model and self.working_order.queue_ahead_qty > 0:
                # Quote crossed through our level but we haven't burned the queue yet.
                # Assume all existing depth at our level is consumed on a quote-cross.
                self.working_order.queue_ahead_qty = 0
            else:
                self._paper_fill(limit_price=min(self.working_order.price, snapshot.ask), reason="quote_cross")
        elif self.working_order.side < 0 and snapshot.bid >= self.working_order.price:
            if self.queue_model and self.working_order.queue_ahead_qty > 0:
                self.working_order.queue_ahead_qty = 0
            else:
                self._paper_fill(limit_price=max(self.working_order.price, snapshot.bid), reason="quote_cross")

    def sync_paper_trade(self, trade: TradePrint) -> None:
        if not self.dry_run or self.working_order is None:
            return
        if self.working_order.is_market:
            return

        if self.working_order.side > 0 and trade.price <= self.working_order.price:
            if self.queue_model:
                self.working_order.queue_ahead_qty = max(0, self.working_order.queue_ahead_qty - trade.size)
                if self.working_order.queue_ahead_qty > 0:
                    return  # Still waiting in queue
            self._paper_fill(limit_price=min(self.working_order.price, trade.price), reason="trade_through")
        elif self.working_order.side < 0 and trade.price >= self.working_order.price:
            if self.queue_model:
                self.working_order.queue_ahead_qty = max(0, self.working_order.queue_ahead_qty - trade.size)
                if self.working_order.queue_ahead_qty > 0:
                    return  # Still waiting in queue
            self._paper_fill(limit_price=max(self.working_order.price, trade.price), reason="trade_through")

    def drain_round_trips(self) -> list[RoundTrip]:
        completed = list(self.closed_trades)
        self.closed_trades.clear()
        return completed

    def reconcile_with_broker(self, broker_snapshot: OrderSnapshot) -> None:
        """Apply broker order truth to the OMS ledger and log any divergence."""
        local = self._order_ledger.get(broker_snapshot.order_id)
        if local is None:
            return
        _, issue = reconcile_order_state(local, broker_snapshot)
        if issue is not None:
            logger.warning(
                "reconciliation issue symbol=%s order_id=%s severity=%s msg=%s local=%s broker=%s",
                self.symbol,
                issue.order_id,
                issue.severity,
                issue.message,
                issue.local_status,
                issue.broker_status,
            )

    def snapshot(self) -> dict[str, object]:
        return {
            "state": self.state.value,
            "inventory_side": self.inventory.side,
            "inventory_qty": self.inventory.qty,
            "inventory_price": self.inventory.avg_price,
            "broker_hold_qty": self.broker_hold_qty,
            "broker_closable_qty": self.broker_closable_qty,
            "manual_close_lock": self._manual_close_lock,
            "external_inventory": self.has_external_inventory,
            "entry_blocked_until_ns": self._entry_blocked_until_ns,
            "exit_blocked_until_ns": self._exit_blocked_until_ns,
            "working_order_id": self.current_order_id,
            "working_order_side": self.working_order.side if self.working_order else 0,
            "working_order_price": self.working_order.price if self.working_order else 0.0,
            "working_order_mode": self.working_order.mode if self.working_order else "",
            "stats": dict(self.stats),
            "ledger": self._order_ledger.snapshot(),
        }

    def _next_paper_order_id(self) -> str:
        self.paper_order_counter += 1
        return f"PAPER-{self.symbol}-{self.paper_order_counter}"

    def _align_price_to_tick(self, price: float, *, side: int) -> float:
        """Snap price to valid tick grid.

        side > 0 (buy): floor to avoid paying more than intended.
        side < 0 (sell): ceil to avoid selling below intended target.
        """
        tick = max(self.selector.tick_size, 1e-9)
        steps = price / tick
        if side > 0:
            snapped_steps = math.floor(steps + 1e-9)
        else:
            snapped_steps = math.ceil(steps - 1e-9)
        return max(snapped_steps * tick, tick)

    def _paper_fill(self, *, limit_price: float, reason: str) -> None:
        if self.working_order is None:
            return
        now_ns = self._clock.time_ns()
        self.paper_last_fill_reason = reason
        self._apply_fill(qty=self.working_order.qty - self.working_order.cum_qty, fill_price=limit_price, fill_ts_ns=now_ns)
        self._finalize_working_order(final_status="filled")

    def _apply_fill(self, *, qty: int, fill_price: float, fill_ts_ns: int) -> None:
        if qty <= 0 or self.working_order is None:
            return

        self._order_ledger.apply_fill(self.working_order.order_id, qty, fill_price)
        self.stats["fills"] += 1
        if self.working_order.purpose == "entry":
            prev_qty = self.inventory.qty
            new_qty = prev_qty + qty
            self.inventory.avg_price = (
                (self.inventory.avg_price * prev_qty + fill_price * qty) / new_qty if new_qty > 0 else 0.0
            )
            self.inventory.qty = new_qty
            self.inventory.side = self.working_order.side
            if self.inventory.opened_ts_ns == 0:
                self.inventory.opened_ts_ns = fill_ts_ns
            self.inventory.entry_qty += qty
        else:
            self.inventory.exit_qty += qty
            self.inventory.exit_value += fill_price * qty
            self.inventory.qty = max(0, self.inventory.qty - qty)
            if self.inventory.qty == 0:
                exit_price = self.inventory.exit_value / max(self.inventory.exit_qty, 1)
                # 使用 exit_qty（实际成交平仓量）而非 entry_qty，防止 _sync_inventory_from_api
                # 修正持仓数量后 entry_qty 与实际平仓量不一致导致 P&L 失真。
                realized_pnl = self.inventory.side * (exit_price - self.inventory.avg_price) * self.inventory.exit_qty
                self.closed_trades.append(
                    RoundTrip(
                        symbol=self.symbol,
                        side=self.inventory.side,
                        qty=self.inventory.exit_qty,
                        entry_price=self.inventory.avg_price,
                        exit_price=exit_price,
                        entry_ts_ns=self.inventory.opened_ts_ns,
                        exit_ts_ns=fill_ts_ns,
                        realized_pnl=realized_pnl,
                        exit_reason=self.working_order.reason,
                    )
                )

    def _finalize_working_order(self, *, final_status: str) -> None:
        if self.working_order is None:
            return

        purpose = self.working_order.purpose
        if purpose == "entry" and self.inventory.qty == 0:
            self._reset_inventory()
        elif purpose == "entry" and final_status == "cancelled" and self.inventory.qty > 0:
            # Entry was partially filled before cancel — inventory is stranded.
            # Flag for strategy to force-close on next signal loop iteration.
            self.has_stranded_partial = True
            logger.warning(
                "stranded partial fill symbol=%s order_id=%s partial_qty=%d — will force close",
                self.symbol,
                self.working_order.order_id,
                self.inventory.qty,
            )
        if purpose == "exit" and self.inventory.qty == 0:
            self._reset_inventory()

        order_id = self.working_order.order_id
        if final_status == "filled":
            self._order_ledger.mark_filled(order_id)
        elif final_status == "cancelled":
            self._order_ledger.mark_canceled(order_id)
        elif final_status == "rejected":
            self._order_ledger.mark_rejected(order_id)

        logger.info(
            "order finalized symbol=%s order_id=%s purpose=%s status=%s inventory_qty=%d",
            self.symbol,
            order_id,
            purpose,
            final_status,
            self.inventory.qty,
        )
        self.working_order = None

    @staticmethod
    def _position_side_totals(positions: list[dict], *, symbol: str, side: str) -> tuple[int, int]:
        hold_qty = 0
        closable_qty = 0
        for position in positions:
            if str(position.get("Symbol")) != str(symbol) or str(position.get("Side")) != side:
                continue
            leaves_raw = position.get("LeavesQty")
            hold = int(float(leaves_raw if leaves_raw not in (None, "") else (position.get("Qty") or 0)))
            locked = int(float(position.get("HoldQty") or 0))
            closable_raw = position.get("ClosableQty")
            closable = int(float(closable_raw)) if closable_raw not in (None, "") else max(hold - locked, 0)
            hold_qty += hold
            closable_qty += max(closable, 0)
        return hold_qty, closable_qty

    async def sync_broker_position(self, *, force: bool = False) -> None:
        if self.rest_client is None:
            logger.error("sync_broker_position: rest_client is None, cannot sync")
            return

        now_ns = self._clock.time_ns()
        if not force and now_ns - self._last_position_sync_ns < self._position_sync_interval_ns:
            return
        self._last_position_sync_ns = now_ns

        try:
            positions = await self.rest_client.get_positions(self.symbol)
        except Exception as exc:
            logger.error(
                "sync_broker_position: get_positions failed: type=%s repr=%s",
                type(exc).__name__,
                repr(exc),
            )
            return

        long_hold, long_closable = self._position_side_totals(positions, symbol=self.symbol, side="2")
        short_hold, short_closable = self._position_side_totals(positions, symbol=self.symbol, side="1")

        if self.inventory.side > 0:
            broker_hold_qty = long_hold
            broker_closable_qty = long_closable
        elif self.inventory.side < 0:
            broker_hold_qty = short_hold
            broker_closable_qty = short_closable
        else:
            broker_hold_qty = long_hold + short_hold
            broker_closable_qty = long_closable + short_closable

        self.broker_hold_qty = broker_hold_qty
        self.broker_closable_qty = broker_closable_qty

        if self.inventory.qty == 0:
            has_external = broker_hold_qty > 0
            if has_external and not self.has_external_inventory:
                logger.warning(
                    "external inventory detected symbol=%s broker_hold=%d broker_closable=%d; blocking new entries until manual position is cleared",
                    self.symbol,
                    broker_hold_qty,
                    broker_closable_qty,
                )
            elif not has_external and self.has_external_inventory:
                logger.info("external inventory cleared symbol=%s", self.symbol)
            self.has_external_inventory = has_external
            self._manual_close_lock = False
            if not has_external:
                self._exit_blocked_until_ns = 0
            return

        if self.working_order is None and broker_hold_qty == 0:
            logger.warning(
                "broker position disappeared symbol=%s local_qty=%d; resetting local inventory (manual close likely)",
                self.symbol,
                self.inventory.qty,
            )
            self.has_external_inventory = False
            self._manual_close_lock = False
            self._exit_blocked_until_ns = 0
            self._reset_inventory()
            self.broker_hold_qty = 0
            self.broker_closable_qty = 0
            return

        qty_mismatch = broker_hold_qty != self.inventory.qty
        if qty_mismatch and self.working_order is None:
            if not self.has_external_inventory:
                logger.warning(
                    "broker inventory drift symbol=%s local_qty=%d broker_hold=%d broker_closable=%d; manual intervention assumed",
                    self.symbol,
                    self.inventory.qty,
                    broker_hold_qty,
                    broker_closable_qty,
                )
            self.has_external_inventory = True
            self._exit_blocked_until_ns = max(self._exit_blocked_until_ns, now_ns + 15_000_000_000)
        else:
            self.has_external_inventory = False

        locked = broker_hold_qty > 0 and broker_closable_qty < min(broker_hold_qty, self.inventory.qty)
        if locked and not self._manual_close_lock:
            logger.warning(
                "close inventory locked symbol=%s hold=%d closable=%d; external/manual repayment order is likely working",
                self.symbol,
                broker_hold_qty,
                broker_closable_qty,
            )
        elif not locked and self._manual_close_lock:
            logger.info(
                "close inventory lock cleared symbol=%s hold=%d closable=%d",
                self.symbol,
                broker_hold_qty,
                broker_closable_qty,
            )
        self._manual_close_lock = locked
        if locked:
            self._exit_blocked_until_ns = max(self._exit_blocked_until_ns, now_ns + 15_000_000_000)
        elif not self.has_external_inventory:
            self._exit_blocked_until_ns = 0

    async def _sync_inventory_from_api(self) -> None:
        await self.sync_broker_position(force=True)

    def _reset_inventory(self) -> None:
        self.inventory = Inventory()

    @staticmethod
    def _incremental_fill_price(*, prev_qty: int, prev_avg: float, new_qty: int, new_avg: float) -> float:
        incremental_qty = max(new_qty - prev_qty, 0)
        if incremental_qty == 0:
            return new_avg
        prev_value = prev_qty * prev_avg
        new_value = new_qty * new_avg
        return max((new_value - prev_value) / incremental_qty, 0.0)
