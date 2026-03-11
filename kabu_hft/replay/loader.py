from __future__ import annotations

import json
from pathlib import Path
from typing import Iterator


class JsonlReplayLoader:
    def __init__(self, path: str | Path):
        self.path = Path(path)

    def iter_events(self) -> Iterator[dict]:
        with self.path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                payload = json.loads(line)
                if isinstance(payload, dict):
                    yield payload
