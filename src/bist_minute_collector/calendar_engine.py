from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from enum import StrEnum
from zoneinfo import ZoneInfo

from .models import RequestWindow
from .snapshots import CalendarSnapshot, SessionTransition


class CalendarConfidence(StrEnum):
    AUTHORITATIVE = "authoritative-versioned-snapshot"
    UNVERIFIED = "unverified-weekday-fallback"


class SlotClassification(StrEnum):
    CLOSED = "closed"
    FULL_DAY = "full-day"
    HALF_DAY = "half-day"
    UNVERIFIED_WEEKDAY = "unverified-weekday"
    QUERY_SLOT = "query-slot"
    OUTSIDE_SESSION = "outside-session"


@dataclass(frozen=True, slots=True)
class SessionDay:
    session_date: date
    classification: SlotClassification
    confidence: CalendarConfidence
    query_window: RequestWindow | None
    calendar_snapshot_id: str
    reason: str


class BistCalendar:
    """Versioned BIST query-calendar interpretation; it never asserts a minute traded."""

    def __init__(self, snapshot: CalendarSnapshot) -> None:
        self.snapshot = snapshot
        self.timezone = ZoneInfo(snapshot.timezone)
        self._holidays = {item.date: item for item in snapshot.holiday_events}

    @staticmethod
    def _transition_time(transitions: tuple[SessionTransition, ...], state: str) -> time:
        try:
            return next(item.at for item in transitions if item.state == state)
        except StopIteration as error:
            raise ValueError(f"calendar snapshot is missing transition {state!r}") from error

    def _window(
        self, session_date: date, transitions: tuple[SessionTransition, ...]
    ) -> RequestWindow:
        start = self._transition_time(transitions, "opening_auction_order_collection")
        end = self._transition_time(transitions, "end_of_day")
        start_local = datetime.combine(session_date, start, self.timezone)
        end_local = datetime.combine(session_date, end, self.timezone)
        return RequestWindow(start_local.astimezone(UTC), end_local.astimezone(UTC))

    def session(self, session_date: date) -> SessionDay:
        authoritative = self.snapshot.effective_from <= session_date <= self.snapshot.effective_to
        if authoritative:
            holiday = self._holidays.get(session_date)
            if session_date.weekday() >= 5 or (holiday is not None and holiday.state == "closed"):
                return SessionDay(
                    session_date,
                    SlotClassification.CLOSED,
                    CalendarConfidence.AUTHORITATIVE,
                    None,
                    self.snapshot.snapshot_id,
                    holiday.name if holiday is not None else "official-weekend",
                )
            if holiday is not None and holiday.state.startswith("half_day"):
                return SessionDay(
                    session_date,
                    SlotClassification.HALF_DAY,
                    CalendarConfidence.AUTHORITATIVE,
                    self._window(session_date, self.snapshot.half_day_transitions),
                    self.snapshot.snapshot_id,
                    holiday.name,
                )
            return SessionDay(
                session_date,
                SlotClassification.FULL_DAY,
                CalendarConfidence.AUTHORITATIVE,
                self._window(session_date, self.snapshot.full_day_transitions),
                self.snapshot.snapshot_id,
                "verified-standard-session",
            )

        if session_date.weekday() >= 5:
            return SessionDay(
                session_date,
                SlotClassification.CLOSED,
                CalendarConfidence.UNVERIFIED,
                None,
                self.snapshot.snapshot_id,
                "unverified-weekend-fallback",
            )
        return SessionDay(
            session_date,
            SlotClassification.UNVERIFIED_WEEKDAY,
            CalendarConfidence.UNVERIFIED,
            self._window(session_date, self.snapshot.full_day_transitions),
            self.snapshot.snapshot_id,
            "outside-calendar-validity-weekday-fallback",
        )

    def classify_minute(self, instant_utc: datetime) -> SlotClassification:
        local = self.to_istanbul(instant_utc)
        session = self.session(local.date())
        if session.confidence is CalendarConfidence.UNVERIFIED:
            return session.classification
        if session.query_window is None:
            return SlotClassification.OUTSIDE_SESSION
        if session.query_window.start_utc <= instant_utc < session.query_window.end_utc:
            return SlotClassification.QUERY_SLOT
        return SlotClassification.OUTSIDE_SESSION

    def sessions_between(self, start_utc: datetime, end_utc: datetime) -> tuple[SessionDay, ...]:
        RequestWindow(start_utc, end_utc)
        if start_utc == end_utc:
            return ()
        first = self.to_istanbul(start_utc).date()
        last = self.to_istanbul(end_utc - timedelta(microseconds=1)).date()
        sessions: list[SessionDay] = []
        current = first
        while current <= last:
            sessions.append(self.session(current))
            current += timedelta(days=1)
        return tuple(sessions)

    def to_istanbul(self, instant_utc: datetime) -> datetime:
        if instant_utc.tzinfo is None or instant_utc.utcoffset() != timedelta(0):
            raise ValueError("instant_utc must be timezone-aware UTC")
        return instant_utc.astimezone(self.timezone)

    @staticmethod
    def to_utc(instant: datetime) -> datetime:
        if instant.tzinfo is None:
            raise ValueError("instant must be timezone-aware")
        return instant.astimezone(UTC)
