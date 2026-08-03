from __future__ import annotations

import json
from dataclasses import FrozenInstanceError
from pathlib import Path
from typing import Any

import pytest

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
