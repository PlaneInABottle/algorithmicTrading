from __future__ import annotations

import json
from dataclasses import FrozenInstanceError, replace
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest

from bist_minute_collector.calendar_engine import (
    BistCalendar,
    CalendarConfidence,
    SlotClassification,
)
from bist_minute_collector.snapshots import SnapshotValidationError, load_calendar_snapshot

ROOT = Path(__file__).parents[2]
SNAPSHOT = ROOT / "config/calendars/bist/2026.json"


def _calendar_json() -> dict[str, Any]:
    return json.loads(SNAPSHOT.read_text(encoding="utf-8"))  # type: ignore[no-any-return]


def _write_snapshot(tmp_path: Path, data: dict[str, Any]) -> Path:
    path = tmp_path / "calendar.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    return path


def test_current_calendar_snapshot_is_sourced_checksummed_and_limited() -> None:
    snapshot = load_calendar_snapshot(SNAPSHOT)

    assert snapshot.timezone == "Europe/Istanbul"
    assert snapshot.effective_from.isoformat() == "2026-01-05"
    assert snapshot.compute_transcription_sha256() == snapshot.transcription_sha256
    assert len(snapshot.sources) == 2
    assert len(snapshot.holiday_events) == 17
    assert "historical sessions" in " ".join(snapshot.limitations)
    assert "suspensions" in " ".join(snapshot.limitations)
    assert "point-in-time membership" in " ".join(snapshot.limitations)
    assert "different boundaries" in " ".join(snapshot.limitations)


def test_calendar_models_are_immutable() -> None:
    snapshot = load_calendar_snapshot(SNAPSHOT)

    with pytest.raises(FrozenInstanceError):
        snapshot.timezone = "UTC"  # type: ignore[misc]


@pytest.mark.parametrize("field", ["effective_from", "effective_to", "retrieved_at_utc"])
def test_calendar_requires_effective_and_retrieval_metadata(tmp_path: Path, field: str) -> None:
    data = _calendar_json()
    del data[field]

    with pytest.raises(SnapshotValidationError, match=field):
        load_calendar_snapshot(_write_snapshot(tmp_path, data))


def test_calendar_rejects_missing_source_checksum(tmp_path: Path) -> None:
    data = _calendar_json()
    del data["sources"][0]["sha256"]

    with pytest.raises(SnapshotValidationError, match="sha256"):
        load_calendar_snapshot(_write_snapshot(tmp_path, data))


def test_calendar_rejects_transcription_checksum_mismatch(tmp_path: Path) -> None:
    data = _calendar_json()
    data["half_day_transitions"][0]["at"] = "09:41:00"

    with pytest.raises(SnapshotValidationError, match="transcription_sha256"):
        load_calendar_snapshot(_write_snapshot(tmp_path, data))


def test_session_transitions_do_not_claim_expected_bars() -> None:
    snapshot = load_calendar_snapshot(SNAPSHOT)

    assert "not guaranteed bar times" in snapshot.session_scope
    assert snapshot.full_day_transitions[0].state == "opening_auction_order_collection"
    assert snapshot.full_day_transitions[-1].state == "end_of_day"


def test_calendar_classifies_weekend_holiday_full_day_and_half_day() -> None:
    calendar = BistCalendar(load_calendar_snapshot(SNAPSHOT))

    assert calendar.session(date(2026, 8, 1)).classification is SlotClassification.CLOSED
    assert calendar.session(date(2026, 7, 15)).classification is SlotClassification.CLOSED
    full = calendar.session(date(2026, 8, 3))
    assert full.confidence is CalendarConfidence.AUTHORITATIVE
    assert full.query_window is not None
    assert full.query_window.start_utc == datetime(2026, 8, 3, 6, 40, tzinfo=UTC)
    assert full.query_window.end_utc == datetime(2026, 8, 3, 15, 19, tzinfo=UTC)
    half = calendar.session(date(2026, 10, 28))
    assert half.classification is SlotClassification.HALF_DAY
    assert half.query_window is not None
    assert half.query_window.end_utc == datetime(2026, 10, 28, 9, 49, tzinfo=UTC)


def test_unverified_fallback_never_proves_outside_session() -> None:
    calendar = BistCalendar(load_calendar_snapshot(SNAPSHOT))
    fallback = calendar.session(date(2027, 1, 4))

    assert fallback.confidence is CalendarConfidence.UNVERIFIED
    assert fallback.classification is SlotClassification.UNVERIFIED_WEEKDAY
    assert fallback.query_window is not None
    assert calendar.classify_minute(datetime(2027, 1, 4, 2, 0, tzinfo=UTC)) is not (
        SlotClassification.OUTSIDE_SESSION
    )


def test_historical_istanbul_dst_and_current_utc_round_trip() -> None:
    calendar = BistCalendar(load_calendar_snapshot(SNAPSHOT))

    before_2015_fall_change = datetime(2015, 10, 25, 1, 30, tzinfo=UTC)
    after_2015_fall_change = datetime(2015, 11, 8, 1, 30, tzinfo=UTC)
    assert calendar.to_utc(calendar.to_istanbul(before_2015_fall_change)) == before_2015_fall_change
    assert calendar.to_utc(calendar.to_istanbul(after_2015_fall_change)) == after_2015_fall_change
    current = datetime(2026, 8, 3, 10, 0, tzinfo=UTC)
    offset = calendar.to_istanbul(current).utcoffset()
    assert offset is not None and offset.total_seconds() == 10_800
    assert calendar.to_utc(calendar.to_istanbul(current)) == current


def test_calendar_rejects_malformed_holiday_and_missing_required_segment(
    tmp_path: Path,
) -> None:
    malformed = _calendar_json()
    malformed["holiday_events"][0]["date"] = "not-a-date"
    with pytest.raises(SnapshotValidationError, match="holiday date"):
        load_calendar_snapshot(_write_snapshot(tmp_path, malformed))

    snapshot = load_calendar_snapshot(SNAPSHOT)
    snapshot = replace(
        snapshot,
        full_day_transitions=tuple(
            item for item in snapshot.full_day_transitions if item.state != "end_of_day"
        ),
    )
    with pytest.raises(ValueError, match="end_of_day"):
        BistCalendar(snapshot).session(date(2026, 8, 3))
