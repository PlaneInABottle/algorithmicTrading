from __future__ import annotations

import errno
import os
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

import pyarrow.parquet as pq  # type: ignore[import-untyped]
import pytest

from bist_minute_collector.archive import ArchiveCompactor, LowDiskError, PublicationError
from bist_minute_collector.store import IngestEnvelope, WriterStore
from tests.helpers import BASE_OPEN, fetch_result, minute_bar, with_symbol


def seeded_store(root: Path) -> WriterStore:
    store = WriterStore(root)
    store.ingest(
        IngestEnvelope(
            fetch_id="seed",
            exchange_code="ASELS.E",
            result=fetch_result(),
            retained_response=b'{"seed":true}',
        )
    )
    return store


def test_byte_for_byte_rerun_has_same_active_partition_hash(tmp_path: Path) -> None:
    with seeded_store(tmp_path) as store:
        compactor = ArchiveCompactor(tmp_path, store)
        first = compactor.compact_all()
        first_bytes = first[0].path.read_bytes()
        second = compactor.compact_all()
        assert second[0].content_sha256 == first[0].content_sha256
        assert second[0].path == first[0].path
        assert second[0].path.read_bytes() == first_bytes
        assert store.active_manifest_count() == 1


def test_partition_has_explicit_schema_zstd_and_deterministic_exchange_time_order(
    tmp_path: Path,
) -> None:
    with seeded_store(tmp_path) as store:
        thyao = with_symbol(
            fetch_result(
                bar=minute_bar(bar_open_utc=BASE_OPEN - timedelta(minutes=1), symbol="THYAO.IS")
            ),
            "THYAO.IS",
        )
        asels_later = fetch_result(
            bar=replace(
                minute_bar(bar_open_utc=BASE_OPEN + timedelta(minutes=1)),
                observed_at_utc=BASE_OPEN + timedelta(minutes=16),
            )
        )
        store.ingest(IngestEnvelope(fetch_id="thyao", exchange_code="THYAO.E", result=thyao))
        store.ingest(
            IngestEnvelope(fetch_id="asels-later", exchange_code="ASELS.E", result=asels_later)
        )
        published = ArchiveCompactor(tmp_path, store).compact_all()[0]

        parquet = pq.ParquetFile(published.path)
        assert parquet.metadata.num_rows == 3
        assert {
            parquet.metadata.row_group(0).column(index).compression
            for index in range(parquet.metadata.num_columns)
        } == {"ZSTD"}
        table = parquet.read(columns=["exchange_code", "bar_open_utc"])
        rows = list(
            zip(
                table.column("exchange_code").to_pylist(),
                table.column("bar_open_utc").to_pylist(),
                strict=True,
            )
        )
        assert rows == sorted(rows)
        assert table.schema.metadata == {
            b"collector_schema": b"bist-minute-bars-v1",
            b"transformation_version": b"phase2-v1",
        }


def test_low_disk_preflight_writes_nothing_and_preserves_committed_data(tmp_path: Path) -> None:
    with seeded_store(tmp_path) as store:
        before = store.active_bar_count()
        compactor = ArchiveCompactor(tmp_path, store, free_bytes=lambda _: 1, min_free_bytes=2)
        with pytest.raises(LowDiskError):
            compactor.compact_all()
        assert store.active_bar_count() == before
        assert store.manifest_count() == 0
        assert not list((tmp_path / "archive").rglob("*.parquet"))


def test_mocked_enospc_preserves_existing_active_partition(tmp_path: Path) -> None:
    with seeded_store(tmp_path) as store:
        compactor = ArchiveCompactor(tmp_path, store)
        active = compactor.compact_all()[0]

        def fail(point: str) -> None:
            if point == "before_temp_write":
                raise OSError(errno.ENOSPC, "synthetic disk full")

        broken = ArchiveCompactor(tmp_path, store, fault_injector=fail)
        with pytest.raises(PublicationError, match="disk full"):
            broken.compact_all()
        assert active.path.read_bytes()
        assert store.active_partition_hashes() == [active.content_sha256]


@pytest.mark.skipif(os.name == "nt", reason="POSIX permission semantics required")
def test_read_only_partition_directory_preserves_committed_data(tmp_path: Path) -> None:
    with seeded_store(tmp_path) as store:
        compactor = ArchiveCompactor(tmp_path, store)
        active = compactor.compact_all()[0]
        partition = active.path.parent
        partition.chmod(0o500)
        try:
            with pytest.raises(PublicationError):
                compactor.compact_all(force_rewrite=True)
        finally:
            partition.chmod(0o700)
        assert active.path.read_bytes()
        assert store.active_partition_hashes() == [active.content_sha256]
