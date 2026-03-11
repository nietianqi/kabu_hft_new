from __future__ import annotations

import logging
import time
from collections import deque
from dataclasses import dataclass
from enum import Enum

from kabu_hft.config import OrderProfile
from kabu_hft.gateway import BoardSnapshot, KabuRestClient, OrderSnapshot, TradePrint

logger = logging.getLogger("kabu.execution")


class ExecutionState(str, Enum):
    FLAT = "FLAT"
    OPENING = "OPENING"
    OPEN = "OPEN"
    CLOSING = "CLOSING"


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
    cum_qty: int = 0
    avg_fill_price: float = 0.0
    cancel_requested: bool = False


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

    def allow(self, now_ns: int) -> bool:
        window_ns = 60 * 1_000_000_000
        while self.timestamps and now_ns - self.timestamps[0] > window_ns:
            self.timestamps.popleft()
        if len(self.timestamps) >= self.max_requotes_per_minute:
            return False
        self.timestamps.append(now_ns)
        return True


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

    def _improve_price(self, direction: int, snapshot: BoardSnapshot) -> float:
        if direction > 0:
            return min(snapshot.bid + self.tick_size, snapshot.ask - self.tick_size)
        return max(snapshot.ask - self.tick_size, snapshot.bid + self.tick_size)

    def entry(self, *, direction: int, snapshot: BoardSnapshot, score: float, microprice: float) -> PriceDecision:
        if direction > 0:
            price = snapshot.bid
            if score >= self.strong_threshold and snapshot.spread >= 2 * self.tick_size:
                price = self._improve_price(direction, snapshot)
            is_market = self.allow_aggressive_entry and score >= self.strong_threshold * 1.5
            if is_market:
                price = snapshot.ask
            edge_ticks = (microprice - price) / max(self.tick_size, 1e-9)
        else:
            price = snapshot.ask
            if score >= self.strong_threshold and snapshot.spread >= 2 * self.tick_size:
                price = self._improve_price(direction, snapshot)
            is_market = self.allow_aggressive_entry and score >= self.strong_threshold * 1.5
            if is_market:
                price = snapshot.bid
            edge_ticks = (price - microprice) / max(self.tick_size, 1e-9)
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
        rest_client: KabuRestClient,
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

        self.working_order: WorkingOrder | None = None
        self.inventory = Inventory()
        self.closed_trades: deque[RoundTrip] = deque()
        self.paper_order_counter = 0
        self.paper_last_fill_reason = ""
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

    def working_age_ns(self, now_ns: int) -> int:
        if self.working_order is None:
            return 0
        return max(0, now_ns - self.working_order.sent_ts_ns)

    def preview_entry(self, *, direction: int, snapshot: BoardSnapshot, score: float, microprice: float) -> PriceDecision:
        return self.selector.entry(
            direction=direction,
            snapshot=snapshot,
            score=score,
            microprice=microprice,
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
    ) -> bool:
        if self.state is not ExecutionState.FLAT or qty <= 0:
            return False

        decision = self.preview_entry(
            direction=direction,
            snapshot=snapshot,
            score=score,
            microprice=microprice,
        )
        if not decision.is_market and decision.edge_ticks < self.selector.min_edge_ticks:
            return False

        self.stats["open_attempts"] += 1
        now_ns = time.time_ns()
        if self.dry_run:
            order_id = self._next_paper_order_id()
        else:
            response = await self.rest_client.send_entry_order(
                symbol=self.symbol,
                exchange=self.exchange,
                side=direction,
                qty=qty,
                price=decision.price,
                is_market=decision.is_market,
                profile=self.order_profile,
            )
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
        )
        self.stats["sent_orders"] += 1
        logger.info(
            "entry order sent symbol=%s side=%+d qty=%d price=%.3f market=%s dry_run=%s",
            self.symbol,
            direction,
            qty,
            decision.price,
            decision.is_market,
            self.dry_run,
        )

        if self.dry_run and decision.is_market:
            fill_price = snapshot.ask if direction > 0 else snapshot.bid
            self._apply_fill(qty=qty, fill_price=fill_price, fill_ts_ns=now_ns)
            self._finalize_working_order(final_status="filled")
        return True

    async def close(
        self,
        *,
        snapshot: BoardSnapshot,
        score: float,
        reason: str,
        force: bool,
    ) -> bool:
        if self.inventory.qty <= 0 or self.working_order is not None:
            return False

        decision = self.selector.exit(
            position_side=self.inventory.side,
            snapshot=snapshot,
            score=score,
            force=force,
        )
        qty = self.inventory.qty
        self.stats["close_attempts"] += 1
        now_ns = time.time_ns()
        if self.dry_run:
            order_id = self._next_paper_order_id()
        else:
            response = await self.rest_client.send_exit_order(
                symbol=self.symbol,
                exchange=self.exchange,
                position_side=self.inventory.side,
                qty=qty,
                price=decision.price,
                is_market=decision.is_market,
                profile=self.order_profile,
            )
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
            await self.rest_client.cancel_order(order_id)
        self.stats["cancel_orders"] += 1
        logger.info("cancel requested symbol=%s order_id=%s reason=%s", self.symbol, order_id, reason)
        return True

    def can_requote(self, now_ns: int) -> bool:
        return self.requotes.allow(now_ns)

    async def check_timeout(self, now_ns: int) -> bool:
        if self.working_order is None:
            return False
        if self.working_age_ns(now_ns) <= self.max_pending_ns:
            return False
        return await self.cancel_working(reason="pending_timeout")

    def sync_order_snapshot(self, snapshot: OrderSnapshot) -> None:
        if self.working_order is None or snapshot.order_id != self.working_order.order_id:
            return

        new_qty = max(snapshot.cum_qty - self.working_order.cum_qty, 0)
        if new_qty > 0:
            fill_price = self._incremental_fill_price(
                prev_qty=self.working_order.cum_qty,
                prev_avg=self.working_order.avg_fill_price,
                new_qty=snapshot.cum_qty,
                new_avg=snapshot.avg_fill_price or snapshot.price,
            )
            self._apply_fill(qty=new_qty, fill_price=fill_price, fill_ts_ns=time.time_ns())
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
            self._paper_fill(limit_price=min(self.working_order.price, snapshot.ask), reason="quote_cross")
        elif self.working_order.side < 0 and snapshot.bid >= self.working_order.price:
            self._paper_fill(limit_price=max(self.working_order.price, snapshot.bid), reason="quote_cross")

    def sync_paper_trade(self, trade: TradePrint) -> None:
        if not self.dry_run or self.working_order is None:
            return
        if self.working_order.is_market:
            return

        if self.working_order.side > 0 and trade.price <= self.working_order.price:
            self._paper_fill(limit_price=min(self.working_order.price, trade.price), reason="trade_through")
        elif self.working_order.side < 0 and trade.price >= self.working_order.price:
            self._paper_fill(limit_price=max(self.working_order.price, trade.price), reason="trade_through")

    def drain_round_trips(self) -> list[RoundTrip]:
        completed = list(self.closed_trades)
        self.closed_trades.clear()
        return completed

    def snapshot(self) -> dict[str, object]:
        return {
            "state": self.state.value,
            "inventory_side": self.inventory.side,
            "inventory_qty": self.inventory.qty,
            "inventory_price": self.inventory.avg_price,
            "working_order_id": self.current_order_id,
            "working_order_side": self.working_order.side if self.working_order else 0,
            "working_order_price": self.working_order.price if self.working_order else 0.0,
            "stats": dict(self.stats),
        }

    def _next_paper_order_id(self) -> str:
        self.paper_order_counter += 1
        return f"PAPER-{self.symbol}-{self.paper_order_counter}"

    def _paper_fill(self, *, limit_price: float, reason: str) -> None:
        if self.working_order is None:
            return
        now_ns = time.time_ns()
        self.paper_last_fill_reason = reason
        self._apply_fill(qty=self.working_order.qty - self.working_order.cum_qty, fill_price=limit_price, fill_ts_ns=now_ns)
        self._finalize_working_order(final_status="filled")

    def _apply_fill(self, *, qty: int, fill_price: float, fill_ts_ns: int) -> None:
        if qty <= 0 or self.working_order is None:
            return

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
                realized_pnl = self.inventory.side * (exit_price - self.inventory.avg_price) * self.inventory.entry_qty
                self.closed_trades.append(
                    RoundTrip(
                        symbol=self.symbol,
                        side=self.inventory.side,
                        qty=self.inventory.entry_qty,
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
        if purpose == "exit" and self.inventory.qty == 0:
            self._reset_inventory()

        logger.info(
            "order finalized symbol=%s order_id=%s purpose=%s status=%s inventory_qty=%d",
            self.symbol,
            self.working_order.order_id,
            purpose,
            final_status,
            self.inventory.qty,
        )
        self.working_order = None

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
