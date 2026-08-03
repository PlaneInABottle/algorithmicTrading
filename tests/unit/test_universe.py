from __future__ import annotations

import json
from dataclasses import FrozenInstanceError
from pathlib import Path
from typing import Any, cast

import pytest

from bist_minute_collector.snapshots import (
    SnapshotValidationError,
    compare_source_members,
    load_universe_snapshot,
)

ROOT = Path(__file__).parents[2]
SNAPSHOT = ROOT / "config/universes/bist50/2026q3.json"
EXPECTED_YAHOO_SHA256 = "3b3b37c63273ae81129fd93ad20ea7edcf39e6dc94b1dfcdc2aa2934991b70f5"
EXPECTED_EXCHANGE_SHA256 = "785e877d116b699e71bbd23a4af353ac6c82d00b77b67973cfa0082928bb059e"


def _snapshot_json() -> dict[str, Any]:
    return json.loads(SNAPSHOT.read_text(encoding="utf-8"))  # type: ignore[no-any-return]


def _write_snapshot(tmp_path: Path, data: dict[str, Any]) -> Path:
    path = tmp_path / "candidate.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    return path


def test_current_snapshot_has_exactly_50_distinct_members_and_approved_hashes() -> None:
    snapshot = load_universe_snapshot(SNAPSHOT)

    assert snapshot.index_code == "XU050"
    assert len(snapshot.members) == 50
    assert len({member.exchange_code for member in snapshot.members}) == 50
    assert len({member.alias_for("yahoo_chart") for member in snapshot.members}) == 50
    assert snapshot.canonical_exchange_sha256 == EXPECTED_EXCHANGE_SHA256
    assert snapshot.canonical_yahoo_sha256 == EXPECTED_YAHOO_SHA256
    assert snapshot.compute_exchange_sha256() == EXPECTED_EXCHANGE_SHA256
    assert snapshot.compute_alias_sha256("yahoo_chart") == EXPECTED_YAHOO_SHA256


def test_exchange_identity_is_separate_from_yahoo_alias() -> None:
    snapshot = load_universe_snapshot(SNAPSHOT)
    asels = next(member for member in snapshot.members if member.exchange_code == "ASELS.E")

    assert asels.exchange_code == "ASELS.E"
    assert asels.alias_for("yahoo_chart") == "ASELS.IS"


def test_snapshot_models_are_immutable() -> None:
    snapshot = load_universe_snapshot(SNAPSHOT)

    with pytest.raises(FrozenInstanceError):
        snapshot.index_code = "OTHER"  # type: ignore[misc]
    with pytest.raises(TypeError):
        cast(dict[str, str], snapshot.members[0].provider_aliases)["yahoo_chart"] = "OTHER.IS"


def test_duplicate_exchange_member_is_rejected(tmp_path: Path) -> None:
    data = _snapshot_json()
    data["members"].append(data["members"][0])
    data["expected_member_count"] = 51

    with pytest.raises(SnapshotValidationError, match="duplicate exchange code"):
        load_universe_snapshot(_write_snapshot(tmp_path, data))


def test_duplicate_yahoo_alias_is_rejected(tmp_path: Path) -> None:
    data = _snapshot_json()
    data["members"][1]["provider_aliases"]["yahoo_chart"] = "AEFES.IS"

    with pytest.raises(SnapshotValidationError, match="duplicate provider alias"):
        load_universe_snapshot(_write_snapshot(tmp_path, data))


def test_unknown_provider_alias_is_rejected(tmp_path: Path) -> None:
    data = _snapshot_json()
    data["members"][0]["provider_aliases"]["unapproved_provider"] = "AEFES"

    with pytest.raises(SnapshotValidationError, match="unknown provider alias"):
        load_universe_snapshot(_write_snapshot(tmp_path, data))


@pytest.mark.parametrize("field", ["effective_from", "effective_to", "retrieved_at_utc"])
def test_required_snapshot_metadata_is_rejected_when_missing(tmp_path: Path, field: str) -> None:
    data = _snapshot_json()
    del data[field]

    with pytest.raises(SnapshotValidationError, match=field):
        load_universe_snapshot(_write_snapshot(tmp_path, data))


@pytest.mark.parametrize("field", ["url", "retrieved_at_utc", "sha256", "role"])
def test_required_source_metadata_is_rejected_when_missing(tmp_path: Path, field: str) -> None:
    data = _snapshot_json()
    del data["sources"][0][field]

    with pytest.raises(SnapshotValidationError, match=field):
        load_universe_snapshot(_write_snapshot(tmp_path, data))


def test_checksum_mismatch_is_rejected(tmp_path: Path) -> None:
    data = _snapshot_json()
    data["canonical_yahoo_sha256"] = "0" * 64

    with pytest.raises(SnapshotValidationError, match="canonical_yahoo_sha256"):
        load_universe_snapshot(_write_snapshot(tmp_path, data))


def test_source_list_disagreement_is_explicit() -> None:
    snapshot = load_universe_snapshot(SNAPSHOT)
    source_codes = {member.exchange_code for member in snapshot.members}
    source_codes.remove("ASELS.E")
    source_codes.add("NOTREAL.E")

    comparison = compare_source_members(snapshot, source_codes)

    assert comparison.agrees is False
    assert comparison.missing_from_source == ("ASELS.E",)
    assert comparison.unexpected_in_source == ("NOTREAL.E",)
