from __future__ import annotations

import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from .models import RequestWindow

MonotonicClock = Callable[[], float]
UtcClock = Callable[[], datetime]


def _utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class CyclePlan:
    sequence: int
    planned_at_utc: datetime


class CycleScheduler:
    """Use monotonic time for cadence and UTC only for request planning metadata."""

    def __init__(
        self,
        interval_seconds: float,
        *,
        monotonic: MonotonicClock = time.monotonic,
        utc_now: UtcClock = _utc_now,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")
        self._interval_seconds = interval_seconds
        self._monotonic = monotonic
        self._utc_now = utc_now
        self._next_due = monotonic()
        self._active_sequence: int | None = None
        self._sequence = 0

    def try_start(self) -> CyclePlan | None:
        now_monotonic = self._monotonic()
        if self._active_sequence is not None or now_monotonic < self._next_due:
            return None
        self._sequence += 1
        self._active_sequence = self._sequence
        self._next_due = now_monotonic + self._interval_seconds
        planned = self._utc_now()
        if planned.tzinfo is None or planned.utcoffset() != timedelta(0):
            raise ValueError("UTC planning clock must return timezone-aware UTC")
        return CyclePlan(self._sequence, planned.astimezone(UTC))

    def finish(self, sequence: int) -> None:
        if sequence != self._active_sequence:
            raise ValueError("sequence does not identify the active cycle")
        self._active_sequence = None


class FairSymbolOrder:
    def __init__(self, symbols: Sequence[str]) -> None:
        self._symbols = tuple(symbols)
        if not self._symbols or len(set(self._symbols)) != len(self._symbols):
            raise ValueError("symbols must be non-empty and distinct")
        self._offset = 0

    def next_cycle(self) -> tuple[str, ...]:
        ordered = self._symbols[self._offset :] + self._symbols[: self._offset]
        self._offset = (self._offset + 1) % len(self._symbols)
        return ordered


def plan_overlap_window(
    *,
    now_utc: datetime,
    accounted_through_utc: datetime | None,
    overlap: timedelta = timedelta(hours=2),
    initial_lookback: timedelta = timedelta(days=1),
) -> RequestWindow:
    if now_utc.tzinfo is None or now_utc.utcoffset() != timedelta(0):
        raise ValueError("now_utc must be timezone-aware UTC")
    if accounted_through_utc is not None and (
        accounted_through_utc.tzinfo is None or accounted_through_utc.utcoffset() != timedelta(0)
    ):
        raise ValueError("accounted_through_utc must be timezone-aware UTC")
    if overlap <= timedelta(0) or initial_lookback <= timedelta(0):
        raise ValueError("overlap and initial_lookback must be positive")
    start = (
        accounted_through_utc - overlap
        if accounted_through_utc is not None
        else now_utc - initial_lookback
    )
    return RequestWindow(start.astimezone(UTC), now_utc.astimezone(UTC))
