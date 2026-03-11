from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Iterator

from kabu_hft.gateway import BoardSnapshot, KabuAdapter, TradePrint

logger = logging.getLogger("kabu.replay.loader")

try:
    import orjson as _orjson

    def _loads(s: str | bytes) -> Any:
        return _orjson.loads(s)

except ImportError:
    def _loads(s: str | bytes) -> Any:
        return json.loads(s)


class ReplayLoader:
    """Reads one or more JSONL board recording files and yields a stream of
    ``(recv_ns, BoardSnapshot, TradePrint | None)`` tuples in file order.

    Files written by :class:`~kabu_hft.replay.recorder.BoardRecorder` contain
    one JSON record per line::

        {"recv_ns": <int>, "data": <raw kabu board dict>}

    The loader applies the same :class:`~kabu_hft.gateway.KabuAdapter`
    normalization used by the live WebSocket feed, so the replay stream is
    semantically identical to what the live strategy sees.

    Example::

        loader = ReplayLoader(["data/board_20260311.jsonl"])
        for recv_ns, snap, trade in loader.stream():
            signals = stack.on_board(snap)
    """

    def __init__(self, paths: list[str | Path] | str | Path) -> None:
        if isinstance(paths, (str, Path)):
            paths = [paths]
        self._paths: list[Path] = [Path(p) for p in paths]

    def stream(
        self,
        *,
        symbol_filter: str | None = None,
    ) -> Iterator[tuple[int, BoardSnapshot, TradePrint | None]]:
        """Yield ``(recv_ns, snapshot, trade_or_None)`` in time order.

        Args:
            symbol_filter: If given, only yield records for this symbol.
        """
        prev_snapshots: dict[str, BoardSnapshot] = {}
        prev_volumes: dict[str, int] = {}
        last_trade_price: dict[str, float] = {}

        for path in self._paths:
            if not path.exists():
                logger.warning("replay file not found: %s", path)
                continue

            logger.info("loading replay file: %s", path)
            line_num = 0
            skipped = 0

            with open(path, encoding="utf-8") as fh:
                for raw_line in fh:
                    line_num += 1
                    raw_line = raw_line.strip()
                    if not raw_line:
                        continue
                    try:
                        record = _loads(raw_line)
                    except Exception as exc:
                        logger.warning("line %d parse error in %s: %s", line_num, path, exc)
                        continue

                    recv_ns: int = record.get("recv_ns", 0)
                    data: dict[str, Any] = record.get("data", {})
                    symbol = str(data.get("Symbol", ""))

                    if symbol_filter and symbol != symbol_filter:
                        skipped += 1
                        continue

                    prev = prev_snapshots.get(symbol)
                    snapshot = KabuAdapter.board(data, prev)
                    if snapshot is None:
                        skipped += 1
                        continue

                    prev_vol = prev_volumes.get(symbol, snapshot.volume)
                    trade = KabuAdapter.trade(
                        data,
                        prev,
                        prev_volume=prev_vol,
                        last_trade_price=last_trade_price.get(symbol),
                    )

                    prev_snapshots[symbol] = snapshot
                    prev_volumes[symbol] = snapshot.volume
                    if trade is not None:
                        last_trade_price[symbol] = trade.price

                    yield recv_ns, snapshot, trade

            logger.info(
                "loaded %s: %d lines, %d valid snapshots, %d skipped",
                path.name,
                line_num,
                line_num - skipped,
                skipped,
            )

    @staticmethod
    def glob(pattern: str) -> "ReplayLoader":
        """Create a loader from a glob pattern, e.g. ``'data/board_*.jsonl'``."""
        import glob as _glob

        paths = sorted(_glob.glob(pattern))
        if not paths:
            logger.warning("no files matched pattern: %s", pattern)
        return ReplayLoader(paths)
