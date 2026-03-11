from __future__ import annotations

import time
from typing import Protocol, runtime_checkable


@runtime_checkable
class Clock(Protocol):
    """Injectable clock abstraction. Swap LiveClock for SimulatedClock in replay/tests."""

    def time_ns(self) -> int: ...


class LiveClock:
    """Wall-clock implementation backed by time.time_ns()."""

    def time_ns(self) -> int:
        return time.time_ns()


class SimulatedClock:
    """Deterministic clock for backtesting and unit tests.

    Call ``set(ns)`` on each replay event to advance simulation time.
    """

    def __init__(self, start_ns: int = 0) -> None:
        self._now_ns = start_ns

    def set(self, ns: int) -> None:
        """Set the current simulated time (absolute nanoseconds)."""
        self._now_ns = ns

    def advance(self, ns: int) -> None:
        """Advance the clock by *ns* nanoseconds."""
        self._now_ns += ns

    def time_ns(self) -> int:
        return self._now_ns
