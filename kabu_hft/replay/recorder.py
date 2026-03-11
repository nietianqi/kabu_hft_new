from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger("kabu.recorder")

JST = timezone(timedelta(hours=9))

try:
    import orjson as _orjson

    def _dumps(obj: Any) -> str:
        return _orjson.dumps(obj, option=_orjson.OPT_NON_STR_KEYS).decode()

except ImportError:
    def _dumps(obj: Any) -> str:
        return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


class BoardRecorder:
    """Writes raw kabu PUSH board JSON to daily-rotated JSONL files.

    Each line is a JSON object::

        {"recv_ns": <int nanoseconds>, "data": <raw kabu board dict>}

    Files are named ``board_YYYYMMDD.jsonl`` and written to ``base_dir``.
    A new file is opened automatically when the JST date changes.

    Usage (integrated in KabuWebSocket via ``on_raw`` callback)::

        recorder = BoardRecorder("data/")
        recorder.open()
        # ... later, on each raw board message:
        recorder.on_board(symbol, raw_dict, recv_ns)
        recorder.close()
    """

    def __init__(self, base_dir: str | Path) -> None:
        self._base = Path(base_dir)
        self._current_date: str = ""
        self._fh = None  # type: ignore[assignment]

    def open(self) -> None:
        self._base.mkdir(parents=True, exist_ok=True)
        logger.info("recorder base dir: %s", self._base.resolve())

    def on_board(self, symbol: str, raw: dict[str, Any], recv_ns: int) -> None:
        """Save a raw board JSON record.  Must be called before normalization
        so the original kabu field names (BidPrice / AskPrice etc.) are preserved.
        """
        date_str = datetime.now(JST).strftime("%Y%m%d")
        if date_str != self._current_date:
            self._rotate(date_str)
        line = _dumps({"recv_ns": recv_ns, "data": raw})
        self._fh.write(line)
        self._fh.write("\n")

    def flush(self) -> None:
        if self._fh is not None:
            self._fh.flush()

    def close(self) -> None:
        if self._fh is not None:
            self._fh.flush()
            self._fh.close()
            self._fh = None
            self._current_date = ""
            logger.info("recorder closed")

    def _rotate(self, date_str: str) -> None:
        if self._fh is not None:
            self._fh.flush()
            self._fh.close()
        path = self._base / f"board_{date_str}.jsonl"
        self._fh = open(path, "a", encoding="utf-8", buffering=1)  # line-buffered
        self._current_date = date_str
        logger.info("recorder rotated to %s", path)
