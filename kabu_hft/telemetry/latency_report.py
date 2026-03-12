from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Iterable


TIME_RE = re.compile(r"(?P<h>\d{2}):(?P<m>\d{2}):(?P<s>\d{2})\.(?P<ms>\d{3})")
LAT_WARN_RE = re.compile(
    r"market data latency (?P<lat>-?\d+(?:\.\d+)?)ms for (?P<symbol>\S+)"
    r"(?: \(source=(?P<source>[a-z_]+) bid=(?P<bid>-?\d+(?:\.\d+)?)ms "
    r"ask=(?P<ask>-?\d+(?:\.\d+)?)ms current=(?P<current>-?\d+(?:\.\d+)?)ms\))?"
)
LAT_STATS_RE = re.compile(
    r"latency stats symbol=(?P<symbol>\S+) samples=(?P<samples>\d+) "
    r"p50=(?P<p50>-?\d+(?:\.\d+)?)ms p90=(?P<p90>-?\d+(?:\.\d+)?)ms "
    r"p99=(?P<p99>-?\d+(?:\.\d+)?)ms max=(?P<max>-?\d+(?:\.\d+)?)ms"
)
ENTRY_RE = re.compile(r"entry order sent symbol=(?P<symbol>\S+)")
CANCEL_RE = re.compile(r"cancel requested symbol=(?P<symbol>\S+) .* reason=(?P<reason>\S+)")
DISCONNECT_RE = re.compile(r"websocket disconnected")
CONNECT_RE = re.compile(r"websocket connected")


@dataclass(slots=True)
class WeightedLatencyWindow:
    samples: int
    p50_ms: float
    p90_ms: float
    p99_ms: float
    max_ms: float


@dataclass(slots=True)
class SymbolMetrics:
    symbol: str
    warning_latencies_ms: list[float] = field(default_factory=list)
    weighted_windows: list[WeightedLatencyWindow] = field(default_factory=list)
    source_counts: Counter[str] = field(default_factory=Counter)
    entries: int = 0
    cancels: int = 0
    stale_cancels: int = 0
    cancel_reasons: Counter[str] = field(default_factory=Counter)

    def est_latency(self) -> dict[str, float | int]:
        if self.weighted_windows:
            total = max(sum(window.samples for window in self.weighted_windows), 1)
            p50 = sum(window.p50_ms * window.samples for window in self.weighted_windows) / total
            p90 = sum(window.p90_ms * window.samples for window in self.weighted_windows) / total
            p99 = sum(window.p99_ms * window.samples for window in self.weighted_windows) / total
            p99_worst = max(window.p99_ms for window in self.weighted_windows)
            max_seen = max(window.max_ms for window in self.weighted_windows)
            return {
                "samples": total,
                "p50_ms": round(p50, 1),
                "p90_ms": round(p90, 1),
                "p99_ms": round(p99, 1),
                "p99_worst_ms": round(p99_worst, 1),
                "max_ms": round(max_seen, 1),
                "method": "weighted_window",
            }
        if self.warning_latencies_ms:
            values = sorted(self.warning_latencies_ms)
            return {
                "samples": len(values),
                "p50_ms": round(percentile(values, 0.50), 1),
                "p90_ms": round(percentile(values, 0.90), 1),
                "p99_ms": round(percentile(values, 0.99), 1),
                "p99_worst_ms": round(percentile(values, 0.99), 1),
                "max_ms": round(values[-1], 1),
                "method": "warning_only",
            }
        return {
            "samples": 0,
            "p50_ms": 0.0,
            "p90_ms": 0.0,
            "p99_ms": 0.0,
            "p99_worst_ms": 0.0,
            "max_ms": 0.0,
            "method": "none",
        }

    def stale_cancel_rate(self) -> float:
        if self.cancels <= 0:
            return 0.0
        return 100.0 * self.stale_cancels / self.cancels


@dataclass(slots=True)
class RunMetrics:
    label: str
    path: str
    symbols: dict[str, SymbolMetrics] = field(default_factory=dict)
    disconnects: int = 0
    connects: int = 0
    start_sod: float | None = None
    end_sod: float | None = None
    stale_trade_exits: int = 0
    total_trade_rows: int = 0
    stale_trade_exit_rate: float = 0.0

    def ensure_symbol(self, symbol: str) -> SymbolMetrics:
        if symbol not in self.symbols:
            self.symbols[symbol] = SymbolMetrics(symbol=symbol)
        return self.symbols[symbol]

    def duration_seconds(self) -> float:
        if self.start_sod is None or self.end_sod is None:
            return 0.0
        if self.end_sod >= self.start_sod:
            return self.end_sod - self.start_sod
        return self.end_sod + 24 * 3600.0 - self.start_sod

    def disconnects_per_hour(self) -> float:
        duration = self.duration_seconds()
        if duration <= 0:
            return 0.0
        return self.disconnects * 3600.0 / duration

    def total_entries(self) -> int:
        return sum(symbol.entries for symbol in self.symbols.values())

    def total_cancels(self) -> int:
        return sum(symbol.cancels for symbol in self.symbols.values())

    def total_stale_cancels(self) -> int:
        return sum(symbol.stale_cancels for symbol in self.symbols.values())

    def stale_cancel_rate(self) -> float:
        total_cancels = self.total_cancels()
        if total_cancels <= 0:
            return 0.0
        return 100.0 * self.total_stale_cancels() / total_cancels

    def snapshot(self) -> dict[str, object]:
        symbol_rows: list[dict[str, object]] = []
        for symbol in sorted(self.symbols):
            symbol_metrics = self.symbols[symbol]
            lat = symbol_metrics.est_latency()
            top_reason = ""
            top_reason_count = 0
            if symbol_metrics.cancel_reasons:
                top_reason, top_reason_count = symbol_metrics.cancel_reasons.most_common(1)[0]
            symbol_rows.append(
                {
                    "symbol": symbol,
                    "latency": lat,
                    "entries": symbol_metrics.entries,
                    "cancels": symbol_metrics.cancels,
                    "stale_cancels": symbol_metrics.stale_cancels,
                    "stale_cancel_rate_pct": round(symbol_metrics.stale_cancel_rate(), 2),
                    "top_cancel_reason": top_reason,
                    "top_cancel_reason_count": top_reason_count,
                    "source_counts": dict(symbol_metrics.source_counts),
                }
            )
        return {
            "label": self.label,
            "path": self.path,
            "duration_seconds": round(self.duration_seconds(), 1),
            "disconnects": self.disconnects,
            "connects": self.connects,
            "disconnects_per_hour": round(self.disconnects_per_hour(), 2),
            "entries": self.total_entries(),
            "cancels": self.total_cancels(),
            "stale_cancels": self.total_stale_cancels(),
            "stale_cancel_rate_pct": round(self.stale_cancel_rate(), 2),
            "stale_trade_exits": self.stale_trade_exits,
            "stale_trade_exit_rate_pct": round(self.stale_trade_exit_rate, 2),
            "symbols": symbol_rows,
        }


def percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    idx = int(round((len(values) - 1) * q))
    idx = max(0, min(idx, len(values) - 1))
    return values[idx]


def _parse_time_of_day_seconds(line: str) -> float | None:
    match = TIME_RE.search(line)
    if not match:
        return None
    hour = int(match.group("h"))
    minute = int(match.group("m"))
    second = int(match.group("s"))
    millis = int(match.group("ms"))
    return hour * 3600 + minute * 60 + second + millis / 1000.0


def analyze_log(path: Path, *, label: str | None = None) -> RunMetrics:
    run = RunMetrics(label=label or path.stem, path=str(path))
    with path.open("r", encoding="utf-8", errors="ignore") as handle:
        for line in handle:
            ts_sod = _parse_time_of_day_seconds(line)
            if ts_sod is not None:
                if run.start_sod is None:
                    run.start_sod = ts_sod
                run.end_sod = ts_sod

            if DISCONNECT_RE.search(line):
                run.disconnects += 1
            if CONNECT_RE.search(line):
                run.connects += 1

            lat_window = LAT_STATS_RE.search(line)
            if lat_window:
                symbol_metrics = run.ensure_symbol(lat_window.group("symbol"))
                symbol_metrics.weighted_windows.append(
                    WeightedLatencyWindow(
                        samples=int(lat_window.group("samples")),
                        p50_ms=float(lat_window.group("p50")),
                        p90_ms=float(lat_window.group("p90")),
                        p99_ms=float(lat_window.group("p99")),
                        max_ms=float(lat_window.group("max")),
                    )
                )
                continue

            lat_warn = LAT_WARN_RE.search(line)
            if lat_warn:
                symbol = lat_warn.group("symbol")
                symbol_metrics = run.ensure_symbol(symbol)
                symbol_metrics.warning_latencies_ms.append(float(lat_warn.group("lat")))
                source = lat_warn.group("source")
                if source:
                    symbol_metrics.source_counts[source] += 1
                continue

            entry = ENTRY_RE.search(line)
            if entry:
                run.ensure_symbol(entry.group("symbol")).entries += 1
                continue

            cancel = CANCEL_RE.search(line)
            if cancel:
                symbol_metrics = run.ensure_symbol(cancel.group("symbol"))
                reason = cancel.group("reason")
                symbol_metrics.cancels += 1
                symbol_metrics.cancel_reasons[reason] += 1
                if "stale_quote" in reason:
                    symbol_metrics.stale_cancels += 1
                continue

    return run


def attach_trade_stale_exit_rate(run: RunMetrics, trades_csv: Path) -> None:
    if not trades_csv.exists():
        return
    with trades_csv.open("r", encoding="utf-8", errors="ignore", newline="") as handle:
        reader = csv.DictReader(handle)
        total = 0
        stale = 0
        for row in reader:
            total += 1
            reason = str(row.get("exit_reason") or "")
            if "stale_quote" in reason:
                stale += 1
        run.total_trade_rows = total
        run.stale_trade_exits = stale
        run.stale_trade_exit_rate = (100.0 * stale / total) if total else 0.0


def render_report(runs: Iterable[RunMetrics]) -> str:
    lines: list[str] = []
    for run in runs:
        snapshot = run.snapshot()
        lines.append(f"=== {snapshot['label']} ===")
        lines.append(f"log: {snapshot['path']}")
        lines.append(
            "duration={:.1f}s connect={} disconnect={} disconnect/h={:.2f}".format(
                snapshot["duration_seconds"],
                snapshot["connects"],
                snapshot["disconnects"],
                snapshot["disconnects_per_hour"],
            )
        )
        lines.append(
            "entries={} cancels={} stale_cancels={} stale_cancel_rate={:.2f}%".format(
                snapshot["entries"],
                snapshot["cancels"],
                snapshot["stale_cancels"],
                snapshot["stale_cancel_rate_pct"],
            )
        )
        if snapshot["stale_trade_exits"] or snapshot["stale_trade_exit_rate_pct"]:
            lines.append(
                "trade_stale_exits={} stale_trade_exit_rate={:.2f}%".format(
                    snapshot["stale_trade_exits"],
                    snapshot["stale_trade_exit_rate_pct"],
                )
            )
        lines.append("symbol  samples  p50ms  p90ms  p99ms  maxms  method         stale_cancel%  top_cancel_reason")
        for symbol_row in snapshot["symbols"]:
            latency = symbol_row["latency"]
            lines.append(
                "{:<6} {:>7} {:>6} {:>6} {:>6} {:>6} {:<13} {:>13}  {}".format(
                    symbol_row["symbol"],
                    latency["samples"],
                    latency["p50_ms"],
                    latency["p90_ms"],
                    latency["p99_ms"],
                    latency["max_ms"],
                    latency["method"],
                    f"{symbol_row['stale_cancel_rate_pct']:.2f}",
                    symbol_row["top_cancel_reason"] or "-",
                )
            )
        lines.append("")
    return "\n".join(lines).strip()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze kabu_hft runtime logs for latency, disconnects, and stale behavior."
    )
    parser.add_argument(
        "--log",
        action="append",
        required=True,
        help="Path to a runtime log file. Provide multiple --log entries to compare environments.",
    )
    parser.add_argument(
        "--label",
        action="append",
        default=[],
        help="Optional label for each --log (same order).",
    )
    parser.add_argument(
        "--trades-csv",
        action="append",
        default=[],
        help="Optional trades.csv file(s) to add stale exit rate. If one file is provided, it applies to all logs.",
    )
    parser.add_argument(
        "--json-out",
        default="",
        help="Optional path to write JSON report.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logs = [Path(path) for path in args.log]
    labels = list(args.label)
    runs: list[RunMetrics] = []
    for index, log_path in enumerate(logs):
        label = labels[index] if index < len(labels) else log_path.stem
        run = analyze_log(log_path, label=label)
        runs.append(run)

    if args.trades_csv:
        trade_paths = [Path(path) for path in args.trades_csv]
        if len(trade_paths) == 1:
            for run in runs:
                attach_trade_stale_exit_rate(run, trade_paths[0])
        else:
            for index, run in enumerate(runs):
                if index < len(trade_paths):
                    attach_trade_stale_exit_rate(run, trade_paths[index])

    print(render_report(runs))

    if args.json_out:
        payload = [run.snapshot() for run in runs]
        out = Path(args.json_out)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()

