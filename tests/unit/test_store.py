from __future__ import annotations

import sqlite3
from dataclasses import replace
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from bist_minute_collector.models import FetchResult, OutcomeCode, ProviderOutcome
from bist_minute_collector.store import (
    CursorUpdate,
    IngestEnvelope,
    ProvenanceConflictError,
    WriterAlreadyOpenError,
    WriterStore,
)
from tests.helpers import BASE_OPEN, corrected_result, fetch_result, minute_bar


def envelope(
    fetch_id: str,
    *,
    result: FetchResult | None = None,
    body: bytes = b'{"chart":"synthetic"}',
    cursor_at: datetime | None = None,
) -> IngestEnvelope:
    selected = result or fetch_result()
    return IngestEnvelope(
        fetch_id=fetch_id,
        exchange_code="ASELS.E",
        result=selected,
        retained_response=body,
        cursor=(
            CursorUpdate(
                accounted_through_utc=cursor_at,
                last_complete_bar_open_utc=selected.bars[-1].bar_open_utc,
            )
            if cursor_at is not None
            else None
        ),
    )


def test_duplicate_envelope_and_identical_fetch_are_idempotent(tmp_path: Path) -> None:
    with WriterStore(tmp_path) as store:
        first = envelope("fetch-1")
        assert store.ingest(first).inserted_bars == 1
        assert store.ingest(first).duplicate_envelope
        assert store.ingest(envelope("fetch-2")).identical_bars == 1

        assert store.active_bar_count() == 1
        assert store.revision_count() == 0
        assert store.provenance_count() == 2


def test_duplicate_fetch_id_with_different_envelope_is_rejected(tmp_path: Path) -> None:
    with WriterStore(tmp_path) as store:
        store.ingest(envelope("fetch-1"))
        changed = envelope(
            "fetch-1",
            result=corrected_result(close="101", observed_minutes=20, response_byte="c"),
        )
        with pytest.raises(ProvenanceConflictError):
            store.ingest(changed)
        assert store.active_bar("yahoo-chart", "ASELS.E", 60, BASE_OPEN).close == "100.25"


def test_out_of_order_observations_converge_to_latest_active_value(tmp_path: Path) -> None:
    old = corrected_result(close="100.25", observed_minutes=15, response_byte="a")
    new = corrected_result(close="101.75", observed_minutes=20, response_byte="b")

    with WriterStore(tmp_path / "ordered") as store:
        store.ingest(envelope("old", result=old))
        store.ingest(envelope("new", result=new))
        ordered = store.active_bar("yahoo-chart", "ASELS.E", 60, BASE_OPEN)

    with WriterStore(tmp_path / "late") as store:
        store.ingest(envelope("new", result=new))
        report = store.ingest(envelope("old", result=old))
        late = store.active_bar("yahoo-chart", "ASELS.E", 60, BASE_OPEN)
        assert report.ignored_older_bars == 1

    assert ordered == late
    assert ordered.close == "101.75"


def test_fractional_observation_timestamp_orders_after_exact_second(tmp_path: Path) -> None:
    exact = corrected_result(close="100.25", observed_minutes=20, response_byte="a")
    exact_bar = exact.bars[0]
    fractional_time = exact_bar.observed_at_utc + timedelta(microseconds=500_000)
    fractional_bar = replace(
        exact_bar,
        close=Decimal("101.75"),
        high=Decimal("101.75"),
        observed_at_utc=fractional_time,
    )
    fractional = replace(
        exact,
        retrieved_at_utc=fractional_time,
        response_sha256="b" * 64,
        bars=(fractional_bar,),
    )

    with WriterStore(tmp_path) as store:
        store.ingest(envelope("exact", result=exact))
        report = store.ingest(envelope("fractional", result=fractional))

        assert report.corrected_bars == 1
        assert store.active_bar("yahoo-chart", "ASELS.E", 60, BASE_OPEN).close == "101.75"


def test_exact_second_cursor_cannot_regress_fractional_cursor(tmp_path: Path) -> None:
    exact = BASE_OPEN + timedelta(hours=1)
    fractional = exact + timedelta(microseconds=500_000)

    with WriterStore(tmp_path) as store:
        store.ingest(envelope("fractional", cursor_at=fractional))
        store.ingest(envelope("exact", cursor_at=exact))

        cursor = store.cursor("yahoo-chart", "ASELS.E", 60)
        assert cursor is not None
        assert cursor.accounted_through_utc == fractional
        assert cursor.last_fetch_id == "fractional"


def test_changed_complete_value_creates_one_revision_before_replace(tmp_path: Path) -> None:
    with WriterStore(tmp_path) as store:
        store.ingest(envelope("old"))
        report = store.ingest(
            envelope(
                "correction",
                result=corrected_result(close="101.75", observed_minutes=20, response_byte="b"),
                body=b'{"chart":"corrected"}',
            )
        )
        assert report.corrected_bars == 1
        assert store.revision_count() == 1
        revision = store.revisions()[0]
        active = store.active_bar("yahoo-chart", "ASELS.E", 60, BASE_OPEN)
        assert revision.before_close == "100.25"
        assert revision.after_close == "101.75"
        assert revision.before_fetch_id == "old"
        assert revision.after_fetch_id == "correction"
        assert active.close == "101.75"


def test_cursor_and_data_roll_back_together_before_commit(tmp_path: Path) -> None:
    def fail(point: str) -> None:
        if point == "before_commit":
            raise RuntimeError("injected precommit failure")

    with WriterStore(tmp_path, fault_injector=fail) as store:
        with pytest.raises(RuntimeError, match="precommit"):
            store.ingest(envelope("fetch-1", cursor_at=BASE_OPEN + timedelta(hours=1)))
        assert store.active_bar_count() == 0
        assert store.provenance_count() == 0
        assert store.cursor("yahoo-chart", "ASELS.E", 60) is None


def test_failure_between_revision_and_replace_rolls_back_entire_correction(
    tmp_path: Path,
) -> None:
    with WriterStore(tmp_path) as seed:
        seed.ingest(envelope("old"))

    def fail(point: str) -> None:
        if point == "after_revision_before_replace":
            raise RuntimeError("injected revision boundary failure")

    with WriterStore(tmp_path, fault_injector=fail) as store:
        with pytest.raises(RuntimeError, match="revision boundary"):
            store.ingest(
                envelope(
                    "correction",
                    result=corrected_result(close="101.75", observed_minutes=20, response_byte="b"),
                )
            )
        assert store.revision_count() == 0
        assert store.provenance_count() == 1
        assert store.active_bar("yahoo-chart", "ASELS.E", 60, BASE_OPEN).close == "100.25"


def test_sqlite_uniqueness_wal_reopen_rollback_and_quick_check(tmp_path: Path) -> None:
    store = WriterStore(tmp_path)
    store.ingest(envelope("fetch-1"))
    assert store.journal_mode() == "wal"
    assert store.quick_check() == "ok"
    database_path = store.database_path
    store.close()

    with WriterStore(tmp_path) as reopened:
        assert reopened.active_bar_count() == 1
        assert reopened.quick_check() == "ok"
        assert reopened.schema_version() == 1
        assert reopened.migration_count() == 1
        reopened.apply_migrations()
        assert reopened.migration_count() == 1
        with pytest.raises(WriterAlreadyOpenError):
            WriterStore(tmp_path)

    connection = sqlite3.connect(database_path)
    index_rows = connection.execute("PRAGMA index_list('bars_hot')").fetchall()
    connection.close()
    assert any(row[2] == 1 for row in index_rows)


def test_version_zero_database_upgrades_additively_and_reader_does_not_mutate(
    tmp_path: Path,
) -> None:
    state = tmp_path / "state"
    state.mkdir(parents=True)
    database = state / "collector.sqlite3"
    connection = sqlite3.connect(database)
    connection.execute("CREATE TABLE phase_zero_sentinel (value TEXT NOT NULL)")
    connection.execute("INSERT INTO phase_zero_sentinel VALUES ('preserve-me')")
    connection.commit()
    connection.close()

    with WriterStore(tmp_path) as store:
        assert store.scalar("SELECT value FROM phase_zero_sentinel") == "preserve-me"
        signature_before = store.schema_signature()
    with WriterStore.open_reader(tmp_path) as reader:
        assert reader.quick_check() == "ok"
        assert reader.schema_signature() == signature_before
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            reader.scalar("DELETE FROM bars_hot")


def test_retention_prunes_ordinary_bodies_but_never_anomaly_or_correction(
    tmp_path: Path,
) -> None:
    with WriterStore(tmp_path, response_budget_bytes=80) as store:
        store.ingest(envelope("ordinary-1", body=b"x" * 50))
        store.ingest(
            envelope(
                "correction",
                result=corrected_result(close="102", observed_minutes=25, response_byte="d"),
                body=b"c" * 50,
            )
        )
        anomaly = fetch_result(bars=(), outcome=OutcomeCode.SOURCE_EMPTY, response_sha256="c" * 64)
        store.ingest(envelope("anomaly", result=anomaly, body=b"z" * 50))

        second_bar = minute_bar(bar_open_utc=BASE_OPEN + timedelta(minutes=1))
        store.ingest(envelope("ordinary-2", result=fetch_result(bar=second_bar), body=b"y" * 50))
        third_bar = minute_bar(bar_open_utc=BASE_OPEN + timedelta(minutes=2))
        store.ingest(envelope("ordinary-3", result=fetch_result(bar=third_bar), body=b"q" * 50))

        retained = {item.fetch_id: item for item in store.retained_responses()}
        assert retained["anomaly"].retention_class == "anomaly"
        assert retained["anomaly"].protected
        assert retained["correction"].retention_class == "correction"
        assert retained["correction"].protected
        assert retained["ordinary-1"].retention_class == "correction"
        assert retained["ordinary-1"].protected
        assert (
            sum(item.byte_count for item in retained.values() if item.retention_class == "ordinary")
            <= 80
        )
        assert store.body_retention("ordinary-2") == (
            False,
            "ordinary response ring byte budget",
        )


def test_success_with_routine_null_row_issue_remains_evictable(tmp_path: Path) -> None:
    result = fetch_result()
    mixed = replace(
        result,
        outcomes=(
            *result.outcomes,
            ProviderOutcome(OutcomeCode.NULL_ROW, "synthetic empty slot", row_index=1),
        ),
    )

    with WriterStore(tmp_path, response_budget_bytes=40) as store:
        store.ingest(envelope("mixed", result=mixed, body=b"m" * 30))
        store.ingest(
            envelope(
                "later",
                result=fetch_result(bar=minute_bar(bar_open_utc=BASE_OPEN + timedelta(minutes=1))),
                body=b"l" * 30,
            )
        )

        assert store.body_retention("mixed") == (
            False,
            "ordinary response ring byte budget",
        )
        assert {item.fetch_id for item in store.retained_responses()} == {"later"}
