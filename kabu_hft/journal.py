from __future__ import annotations

import asyncio
import csv
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from kabu_hft.execution import RoundTrip
    from kabu_hft.signals import SignalPacket

logger = logging.getLogger("kabu.journal")

JST = timezone(timedelta(hours=9))

_TRADE_FIELDS = [
    "ts_jst",
    "symbol",
    "side",
    "qty",
    "entry_price",
    "exit_price",
    "realized_pnl",
    "hold_ms",
    "exit_reason",
    "obi_z",
    "lob_ofi_z",
    "tape_ofi_z",
    "micro_momentum_z",
    "microprice_tilt_z",
    "composite",
]

_MARKOUT_FIELDS = [
    "ts_jst",
    "symbol",
    "side",
    "qty",
    "entry_price",
    "exit_price",
    "realized_pnl",
    "exit_reason",
    "markout_seconds",
    "markout_mid",
    "markout_pnl",
]


@dataclass(slots=True)
class _MarkoutTask:
    symbol: str
    side: int
    qty: int
    entry_price: float
    exit_price: float
    realized_pnl: float
    exit_reason: str
    markout_seconds: int
    ts_jst: str
    mid_snapshot_ref: list[float]


class TradeJournal:
    """Writes every completed round-trip to a CSV file and optionally records
    a markout P&L at a fixed horizon after the trade closes."""

    def __init__(self, trade_path: str | Path, markout_seconds: int = 30) -> None:
        self.trade_path = Path(trade_path)
        self.markout_path = self.trade_path.with_suffix(".markout.csv")
        self.markout_seconds = markout_seconds
        self._trade_writer: csv.DictWriter | None = None
        self._markout_writer: csv.DictWriter | None = None
        self._trade_file = None
        self._markout_file = None

    def open(self) -> None:
        trade_is_new = not self.trade_path.exists() or os.path.getsize(self.trade_path) == 0
        self._trade_file = open(self.trade_path, "a", newline="", encoding="utf-8")
        self._trade_writer = csv.DictWriter(self._trade_file, fieldnames=_TRADE_FIELDS)
        if trade_is_new:
            self._trade_writer.writeheader()

        if self.markout_seconds > 0:
            markout_is_new = not self.markout_path.exists() or os.path.getsize(self.markout_path) == 0
            self._markout_file = open(self.markout_path, "a", newline="", encoding="utf-8")
            self._markout_writer = csv.DictWriter(self._markout_file, fieldnames=_MARKOUT_FIELDS)
            if markout_is_new:
                self._markout_writer.writeheader()

        logger.info("journal opened trade=%s markout=%s", self.trade_path, self.markout_path)

    def close(self) -> None:
        if self._trade_file is not None:
            self._trade_file.flush()
            self._trade_file.close()
            self._trade_file = None
        if self._markout_file is not None:
            self._markout_file.flush()
            self._markout_file.close()
            self._markout_file = None

    def log_trade(self, trade: "RoundTrip", signal: "SignalPacket | None") -> None:
        if self._trade_writer is None:
            return
        hold_ms = max(0.0, (trade.exit_ts_ns - trade.entry_ts_ns) / 1_000_000)
        ts_jst = datetime.fromtimestamp(trade.exit_ts_ns / 1e9, tz=JST).isoformat()
        row = {
            "ts_jst": ts_jst,
            "symbol": trade.symbol,
            "side": trade.side,
            "qty": trade.qty,
            "entry_price": f"{trade.entry_price:.4f}",
            "exit_price": f"{trade.exit_price:.4f}",
            "realized_pnl": f"{trade.realized_pnl:.2f}",
            "hold_ms": f"{hold_ms:.1f}",
            "exit_reason": trade.exit_reason,
            "obi_z": f"{signal.obi_z:.4f}" if signal else "",
            "lob_ofi_z": f"{signal.lob_ofi_z:.4f}" if signal else "",
            "tape_ofi_z": f"{signal.tape_ofi_z:.4f}" if signal else "",
            "micro_momentum_z": f"{signal.micro_momentum_z:.4f}" if signal else "",
            "microprice_tilt_z": f"{signal.microprice_tilt_z:.4f}" if signal else "",
            "composite": f"{signal.composite:.4f}" if signal else "",
        }
        self._trade_writer.writerow(row)
        self._trade_file.flush()

    def schedule_markout(
        self,
        *,
        trade: "RoundTrip",
        mid_ref: list[float],
    ) -> None:
        """Schedule a markout P&L capture `markout_seconds` after this call.

        `mid_ref` is a single-element list; the caller updates `mid_ref[0]`
        with the current mid on every board update so that when the timer fires
        we read the latest mid available.
        """
        if self.markout_seconds <= 0 or self._markout_writer is None:
            return

        ts_jst = datetime.fromtimestamp(trade.exit_ts_ns / 1e9, tz=JST).isoformat()
        task = _MarkoutTask(
            symbol=trade.symbol,
            side=trade.side,
            qty=trade.qty,
            entry_price=trade.entry_price,
            exit_price=trade.exit_price,
            realized_pnl=trade.realized_pnl,
            exit_reason=trade.exit_reason,
            markout_seconds=self.markout_seconds,
            ts_jst=ts_jst,
            mid_snapshot_ref=mid_ref,
        )

        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            return

        loop.call_later(self.markout_seconds, self._write_markout, task)

    def _write_markout(self, task: _MarkoutTask) -> None:
        if self._markout_writer is None or self._markout_file is None:
            return
        markout_mid = task.mid_snapshot_ref[0] if task.mid_snapshot_ref else 0.0
        markout_pnl = task.side * (markout_mid - task.exit_price) * task.qty
        row = {
            "ts_jst": task.ts_jst,
            "symbol": task.symbol,
            "side": task.side,
            "qty": task.qty,
            "entry_price": f"{task.entry_price:.4f}",
            "exit_price": f"{task.exit_price:.4f}",
            "realized_pnl": f"{task.realized_pnl:.2f}",
            "exit_reason": task.exit_reason,
            "markout_seconds": task.markout_seconds,
            "markout_mid": f"{markout_mid:.4f}",
            "markout_pnl": f"{markout_pnl:.2f}",
        }
        self._markout_writer.writerow(row)
        self._markout_file.flush()
        logger.debug("markout written symbol=%s markout_pnl=%.2f", task.symbol, markout_pnl)
