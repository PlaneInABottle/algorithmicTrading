from __future__ import annotations

import hashlib
import os
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq  # type: ignore[import-untyped]
import pytest

from bist_minute_collector.archive import ArchiveCompactor
from bist_minute_collector.store import IngestEnvelope, WriterStore
from tests.helpers import corrected_result, fetch_result, minute_bar


def seed(root: Path) -> None:
    with WriterStore(root) as store:
        store.ingest(
            IngestEnvelope(
                fetch_id="seed",
                exchange_code="ASELS.E",
                result=fetch_result(),
                retained_response=b'{"seed":true}',
            )
        )


@pytest.mark.parametrize(
    "kill_point",
    ["after_temp_write", "after_pending_manifest", "after_final_rename"],
)
def test_reopen_reconciles_each_publication_crash_boundary(tmp_path: Path, kill_point: str) -> None:
    seed(tmp_path)

    def kill(point: str) -> None:
        if point == kill_point:
            raise RuntimeError(f"simulated process death at {point}")

    with WriterStore(tmp_path) as store:
        with pytest.raises(RuntimeError, match="simulated process death"):
            ArchiveCompactor(tmp_path, store, fault_injector=kill).compact_all()
        assert store.active_manifest_count() == 0

    with WriterStore(tmp_path) as reopened:
        recovered = ArchiveCompactor(tmp_path, reopened).reconcile()
        assert recovered.recovered == 1
        assert recovered.failed == 0
        assert reopened.active_manifest_count() == 1
        active = reopened.active_manifests()[0]
        assert active.final_path is not None
        path = tmp_path / active.final_path
        assert path.is_file()
        assert hashlib.sha256(path.read_bytes()).hexdigest() == active.content_sha256
        assert reopened.quick_check() == "ok"


def test_zero_byte_writing_manifest_is_rebuilt_from_committed_bars(tmp_path: Path) -> None:
    source_value = Decimal("1.2100000381469727")
    bar = replace(
        minute_bar(),
        open=source_value,
        high=source_value,
        low=source_value,
        close=source_value,
    )
    with WriterStore(tmp_path) as store:
        store.ingest(
            IngestEnvelope(
                fetch_id="committed-high-scale-source-value",
                exchange_code="CANTE.E",
                result=fetch_result(bar=bar),
            )
        )
        temp = (
            tmp_path
            / "archive"
            / "source=yahoo-chart"
            / "interval=60"
            / "session_date=2026-08-03"
            / ".interrupted.parquet.tmp"
        )
        temp.parent.mkdir(parents=True)
        temp.touch()
        store.create_manifest(
            "interrupted",
            "yahoo-chart",
            60,
            "2026-08-03",
            temp.relative_to(tmp_path).as_posix(),
        )

    with WriterStore(tmp_path) as reopened:
        report = ArchiveCompactor(tmp_path, reopened).reconcile()

        assert report.recovered == 1
        assert report.failed == 0
        assert reopened.active_manifest_count() == 1
        failed = reopened.manifests(("failed",))
        assert len(failed) == 1
        assert failed[0].attempt_id == "interrupted"
        assert failed[0].error == "empty writing temp rebuilt from committed SQLite bars"
        assert temp.exists()
        assert temp.stat().st_size == 0
        active = reopened.active_manifests()[0]
        assert active.final_path is not None
        table = pq.read_table(tmp_path / active.final_path, columns=["close"])
        assert table.column("close").to_pylist() == [source_value]
        assert reopened.quick_check() == "ok"


@pytest.mark.parametrize("corruption", ["truncated", "wrong-hash"])
def test_corrupt_pending_publication_fails_closed_and_preserves_prior_active(
    tmp_path: Path, corruption: str
) -> None:
    seed(tmp_path)
    with WriterStore(tmp_path) as store:
        original = ArchiveCompactor(tmp_path, store).compact_all()[0]
        store.ingest(
            IngestEnvelope(
                fetch_id="correction",
                exchange_code="ASELS.E",
                result=corrected_result(close="101.75", observed_minutes=20, response_byte="c"),
                retained_response=b'{"corrected":true}',
            )
        )

        def kill(point: str) -> None:
            if point == "after_pending_manifest":
                raise RuntimeError("leave pending")

        with pytest.raises(RuntimeError, match="leave pending"):
            ArchiveCompactor(tmp_path, store, fault_injector=kill).compact_all()
        pending = store.pending_manifests()[0]
        temp = tmp_path / pending.temp_path
        if corruption == "truncated":
            temp.write_bytes(temp.read_bytes()[:16])
        else:
            store.override_manifest_hash_for_test(pending.attempt_id, "0" * 64)

    with WriterStore(tmp_path) as reopened:
        result = ArchiveCompactor(tmp_path, reopened).reconcile()
        assert result.failed == 1
        assert reopened.active_partition_hashes() == [original.content_sha256]
        assert original.path.read_bytes()
        assert temp.exists()
        assert reopened.quick_check() == "ok"


def test_failed_temp_retention_is_bounded_accounted_and_preserves_active_evidence(
    tmp_path: Path,
) -> None:
    seed(tmp_path)
    fixed_now = datetime(2026, 8, 20, tzinfo=UTC)
    with WriterStore(tmp_path) as store:
        original = ArchiveCompactor(tmp_path, store).compact_all()[0]
        temp_paths: list[Path] = []
        for index in range(2):
            temp = tmp_path / "archive" / f".failed-{index}.parquet.tmp"
            temp.parent.mkdir(parents=True, exist_ok=True)
            temp.write_bytes(f"failed-{index}".encode())
            old_timestamp = (fixed_now - timedelta(days=8, minutes=index)).timestamp()
            os.utime(temp, (old_timestamp, old_timestamp))
            attempt_id = f"failed-{index}"
            store.create_manifest(
                attempt_id,
                "yahoo-chart",
                60,
                "2026-08-03",
                temp.relative_to(tmp_path).as_posix(),
            )
            store.fail_manifest(attempt_id, "synthetic retained failure")
            temp_paths.append(temp)

        report = ArchiveCompactor(
            tmp_path,
            store,
            failed_temp_retention=timedelta(days=7),
            max_failed_temp_files=1,
            utc_now=lambda: fixed_now,
        ).reconcile()

        assert report.cleaned_failed_temps == 2
        assert not any(path.exists() for path in temp_paths)
        failed = store.manifests(("failed",))
        assert len(failed) == 2
        assert all(item.temp_removed_at_utc == fixed_now for item in failed)
        assert all(item.temp_removal_reason == "failed-temp-retention-policy" for item in failed)
        assert original.path.is_file()
        assert store.active_partition_hashes() == [original.content_sha256]
