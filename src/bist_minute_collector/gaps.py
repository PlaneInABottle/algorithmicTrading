from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum

from .calendar_engine import BistCalendar, CalendarConfidence
from .models import RequestWindow
from .store import WriterStore


class GapState(StrEnum):
    PENDING = "pending"
    RECOVERED = "recovered"
    PROVIDER_EMPTY = "provider-empty/unverifiable"
    OUTSIDE_SESSION = "outside-session"
    UNRECOVERABLE = "unrecoverable-beyond-retention"


class InvalidGapTransition(ValueError):
    pass


_ALLOWED_TRANSITIONS: dict[GapState, frozenset[GapState]] = {
    GapState.PENDING: frozenset(
        {
            GapState.PENDING,
            GapState.RECOVERED,
            GapState.PROVIDER_EMPTY,
            GapState.OUTSIDE_SESSION,
            GapState.UNRECOVERABLE,
        }
    ),
    GapState.PROVIDER_EMPTY: frozenset(
        {GapState.PROVIDER_EMPTY, GapState.RECOVERED, GapState.UNRECOVERABLE}
    ),
    GapState.RECOVERED: frozenset(),
    GapState.OUTSIDE_SESSION: frozenset(),
    GapState.UNRECOVERABLE: frozenset(),
}


@dataclass(frozen=True, slots=True)
class GapRange:
    source: str
    exchange_code: str
    interval_seconds: int
    start_utc: datetime
    end_utc: datetime

    def __post_init__(self) -> None:
        RequestWindow(self.start_utc, self.end_utc)
        if not self.source or not self.exchange_code.endswith(".E"):
            raise ValueError("source and canonical exchange code are required")
        if self.interval_seconds != 60:
            raise ValueError("only one-minute gaps are supported")
        if self.start_utc == self.end_utc or int(self.start_utc.timestamp()) % 60:
            raise ValueError("gap must be non-empty and minute-aligned")
        if int(self.end_utc.timestamp()) % 60:
            raise ValueError("gap end must be minute-aligned")

    @property
    def gap_id(self) -> str:
        identity = "|".join(
            (
                self.source,
                self.exchange_code,
                str(self.interval_seconds),
                self.start_utc.isoformat(),
                self.end_utc.isoformat(),
            )
        )
        return hashlib.sha256(identity.encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class GapCurrent:
    gap: GapRange
    state: GapState
    attempt_count: int
    first_seen_utc: datetime
    last_seen_utc: datetime
    recovery_cutoff_utc: datetime | None
    cause: str
    evidence_id: str


@dataclass(frozen=True, slots=True)
class GapEvent:
    event_id: int
    gap_id: str
    from_state: GapState | None
    to_state: GapState
    cause: str
    evidence_id: str
    observed_at_utc: datetime
    attempt_count: int


@dataclass(frozen=True, slots=True)
class PlannedGap:
    gap: GapRange
    state: GapState
    cause: str
    evidence_id: str
    recovery_cutoff_utc: datetime


@dataclass(frozen=True, slots=True)
class RecoveryTask:
    window: RequestWindow
    gaps: tuple[GapRange, ...]
    calendar_snapshot_id: str
    calendar_confidence: CalendarConfidence


@dataclass(frozen=True, slots=True)
class RecoveryPlan:
    gaps: tuple[PlannedGap, ...]
    tasks: tuple[RecoveryTask, ...]
    recovery_cutoff_utc: datetime


class GapLedger:
    def __init__(self, store: WriterStore) -> None:
        self.store = store

    @staticmethod
    def _current_from_row(row: dict[str, object]) -> GapCurrent:
        cutoff = row["recovery_cutoff_utc"]
        return GapCurrent(
            GapRange(
                str(row["source"]),
                str(row["exchange_code"]),
                int(str(row["interval_seconds"])),
                datetime.fromisoformat(str(row["start_utc"]).replace("Z", "+00:00")).astimezone(
                    UTC
                ),
                datetime.fromisoformat(str(row["end_utc"]).replace("Z", "+00:00")).astimezone(UTC),
            ),
            GapState(str(row["state"])),
            int(str(row["attempt_count"])),
            datetime.fromisoformat(str(row["first_seen_utc"]).replace("Z", "+00:00")).astimezone(
                UTC
            ),
            datetime.fromisoformat(str(row["last_seen_utc"]).replace("Z", "+00:00")).astimezone(
                UTC
            ),
            (
                datetime.fromisoformat(str(cutoff).replace("Z", "+00:00")).astimezone(UTC)
                if cutoff is not None
                else None
            ),
            str(row["cause"]),
            str(row["evidence_id"]),
        )

    def record(
        self,
        gap: GapRange,
        state: GapState,
        cause: str,
        evidence_id: str,
        observed_at_utc: datetime,
        *,
        recovery_cutoff_utc: datetime | None = None,
    ) -> bool:
        existing = self.store.gap_current_row(gap.gap_id)
        if existing is not None:
            current = GapState(str(existing["state"]))
            if current is not state:
                return self.transition(
                    gap,
                    state,
                    cause,
                    evidence_id,
                    observed_at_utc,
                    recovery_cutoff_utc=recovery_cutoff_utc,
                )
            if (
                str(existing["cause"]) == cause
                and str(existing["evidence_id"]) == evidence_id
                and str(existing["last_seen_utc"])
                == observed_at_utc.astimezone(UTC).isoformat().replace("+00:00", "Z")
            ):
                return False
        return self.store.record_gap_event(
            gap_id=gap.gap_id,
            source=gap.source,
            exchange_code=gap.exchange_code,
            interval_seconds=gap.interval_seconds,
            start_utc=gap.start_utc,
            end_utc=gap.end_utc,
            to_state=state.value,
            cause=cause,
            evidence_id=evidence_id,
            observed_at_utc=observed_at_utc,
            recovery_cutoff_utc=recovery_cutoff_utc,
            increment_attempt=False,
        )

    def record_attempt(self, gap: GapRange, evidence_id: str, observed_at_utc: datetime) -> int:
        current = self.current(gap)
        if current.state not in {GapState.PENDING, GapState.PROVIDER_EMPTY}:
            raise InvalidGapTransition(f"cannot attempt terminal gap {current.state.value}")
        self.store.record_gap_event(
            gap_id=gap.gap_id,
            source=gap.source,
            exchange_code=gap.exchange_code,
            interval_seconds=gap.interval_seconds,
            start_utc=gap.start_utc,
            end_utc=gap.end_utc,
            to_state=current.state.value,
            cause="provider-attempt",
            evidence_id=evidence_id,
            observed_at_utc=observed_at_utc,
            recovery_cutoff_utc=current.recovery_cutoff_utc,
            increment_attempt=True,
        )
        return self.current(gap).attempt_count

    def transition(
        self,
        gap: GapRange,
        state: GapState,
        cause: str,
        evidence_id: str,
        observed_at_utc: datetime,
        *,
        recovery_cutoff_utc: datetime | None = None,
    ) -> bool:
        current = self.current(gap)
        if state not in _ALLOWED_TRANSITIONS[current.state]:
            raise InvalidGapTransition(f"{current.state.value} cannot transition to {state.value}")
        return self.store.record_gap_event(
            gap_id=gap.gap_id,
            source=gap.source,
            exchange_code=gap.exchange_code,
            interval_seconds=gap.interval_seconds,
            start_utc=gap.start_utc,
            end_utc=gap.end_utc,
            to_state=state.value,
            cause=cause,
            evidence_id=evidence_id,
            observed_at_utc=observed_at_utc,
            recovery_cutoff_utc=recovery_cutoff_utc or current.recovery_cutoff_utc,
            increment_attempt=False,
        )

    def current(self, gap: GapRange) -> GapCurrent:
        row = self.store.gap_current_row(gap.gap_id)
        if row is None:
            raise KeyError(gap.gap_id)
        return self._current_from_row(row)

    def list_current(self, states: frozenset[GapState] | None = None) -> tuple[GapCurrent, ...]:
        wanted = None if states is None else frozenset(state.value for state in states)
        return tuple(self._current_from_row(row) for row in self.store.gap_current_rows(wanted))

    def history(self, gap: GapRange) -> tuple[GapEvent, ...]:
        return tuple(
            GapEvent(
                int(str(row["event_id"])),
                str(row["gap_id"]),
                GapState(str(row["from_state"])) if row["from_state"] is not None else None,
                GapState(str(row["to_state"])),
                str(row["cause"]),
                str(row["evidence_id"]),
                datetime.fromisoformat(
                    str(row["observed_at_utc"]).replace("Z", "+00:00")
                ).astimezone(UTC),
                int(str(row["attempt_count"])),
            )
            for row in self.store.gap_event_rows(gap.gap_id)
        )


class RecoveryPlanner:
    def __init__(self, calendar: BistCalendar, *, retention_days: int = 28) -> None:
        if retention_days <= 0:
            raise ValueError("retention_days must be positive")
        self.calendar = calendar
        self.retention = timedelta(days=retention_days)

    def plan(
        self,
        *,
        source: str,
        exchange_code: str,
        start_utc: datetime,
        end_utc: datetime,
        now_utc: datetime,
    ) -> RecoveryPlan:
        RequestWindow(start_utc, end_utc)
        if now_utc.tzinfo is None or now_utc.utcoffset() != timedelta(0):
            raise ValueError("now_utc must be timezone-aware UTC")
        cutoff = now_utc - self.retention
        gaps: list[PlannedGap] = []
        tasks: list[RecoveryTask] = []
        for session in self.calendar.sessions_between(start_utc, end_utc):
            window = session.query_window
            if window is None:
                continue
            clipped_start = max(start_utc, window.start_utc)
            clipped_end = min(end_utc, window.end_utc)
            if clipped_start >= clipped_end:
                continue
            if int(clipped_start.timestamp()) % 60:
                clipped_start = datetime.fromtimestamp(
                    (int(clipped_start.timestamp()) // 60 + 1) * 60, UTC
                )
            clipped_end = datetime.fromtimestamp(int(clipped_end.timestamp()) // 60 * 60, UTC)
            if clipped_start >= clipped_end:
                continue
            pending: list[GapRange] = []
            cursor = clipped_start
            while cursor < clipped_end:
                gap = GapRange(source, exchange_code, 60, cursor, cursor + timedelta(minutes=1))
                if gap.end_utc <= cutoff:
                    state = GapState.UNRECOVERABLE
                    cause = "older-than-conservative-28-day-retention"
                    evidence = cutoff.isoformat()
                else:
                    state = GapState.PENDING
                    cause = "uncovered-versioned-query-slot"
                    evidence = session.calendar_snapshot_id
                    pending.append(gap)
                gaps.append(PlannedGap(gap, state, cause, evidence, cutoff))
                cursor = gap.end_utc
            if pending:
                tasks.append(
                    RecoveryTask(
                        RequestWindow(pending[0].start_utc, pending[-1].end_utc),
                        tuple(pending),
                        session.calendar_snapshot_id,
                        session.confidence,
                    )
                )
        return RecoveryPlan(
            tuple(gaps),
            tuple(sorted(tasks, key=lambda item: item.window.start_utc)),
            cutoff,
        )
