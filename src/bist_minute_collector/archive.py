from __future__ import annotations

import errno
import hashlib
import os
import shutil
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import cast
from zoneinfo import ZoneInfo

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from .store import FaultInjector, StoredBar, StoredManifest, WriterStore

FreeBytesProbe = Callable[[Path], int]
DEFAULT_MIN_FREE_BYTES = 10 * 1024 * 1024 * 1024
_ISTANBUL = ZoneInfo("Europe/Istanbul")


class ArchiveError(RuntimeError):
    """Base error for archive publication failures."""


class LowDiskError(ArchiveError):
    """Raised before publication when durable headroom is insufficient."""


class PublicationError(ArchiveError):
    """Raised when publication fails closed after preserving committed state."""


@dataclass(frozen=True, slots=True)
class PublishedPartition:
    source: str
    interval_seconds: int
    session_date: str
    content_sha256: str
    row_count: int
    path: Path
    attempt_id: str


@dataclass(frozen=True, slots=True)
class ReconcileReport:
    recovered: int = 0
    failed: int = 0
    cleaned_failed_temps: int = 0


@dataclass(frozen=True, slots=True)
class HeadroomProjection:
    projected_temp_bytes: int
    projected_wal_bytes: int
    projected_publication_bytes: int
    required_bytes: int


PARQUET_SCHEMA = pa.schema(
    [
        pa.field("source", pa.string(), nullable=False),
        pa.field("exchange_code", pa.string(), nullable=False),
        pa.field("provider_symbol", pa.string(), nullable=False),
        pa.field("interval_seconds", pa.int32(), nullable=False),
        pa.field("bar_open_utc", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("raw_source_timestamp", pa.int64(), nullable=False),
        pa.field("open", pa.decimal128(38, 12), nullable=False),
        pa.field("high", pa.decimal128(38, 12), nullable=False),
        pa.field("low", pa.decimal128(38, 12), nullable=False),
        pa.field("close", pa.decimal128(38, 12), nullable=False),
        pa.field("volume", pa.int64(), nullable=False),
        pa.field("observed_at_utc", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("active_fetch_id", pa.string(), nullable=False),
    ],
    metadata={
        b"collector_schema": b"bist-minute-bars-v1",
        b"transformation_version": b"phase2-v1",
    },
)


def _default_free_bytes(path: Path) -> int:
    probe = path
    while not probe.exists() and probe != probe.parent:
        probe = probe.parent
    return shutil.disk_usage(probe).free


def _table(bars: tuple[StoredBar, ...]) -> pa.Table:
    ordered = sorted(
        bars,
        key=lambda bar: (
            bar.source,
            bar.exchange_code,
            bar.interval_seconds,
            bar.bar_open_utc,
        ),
    )
    columns: dict[str, list[object]] = {field.name: [] for field in PARQUET_SCHEMA}
    for bar in ordered:
        columns["source"].append(bar.source)
        columns["exchange_code"].append(bar.exchange_code)
        columns["provider_symbol"].append(bar.provider_symbol)
        columns["interval_seconds"].append(bar.interval_seconds)
        columns["bar_open_utc"].append(bar.bar_open_utc)
        columns["raw_source_timestamp"].append(bar.raw_source_timestamp)
        columns["open"].append(Decimal(bar.open))
        columns["high"].append(Decimal(bar.high))
        columns["low"].append(Decimal(bar.low))
        columns["close"].append(Decimal(bar.close))
        columns["volume"].append(bar.volume)
        columns["observed_at_utc"].append(bar.observed_at_utc)
        columns["active_fetch_id"].append(bar.active_fetch_id)
    return pa.Table.from_pydict(columns, schema=PARQUET_SCHEMA)


def _write_deterministic_parquet(path: Path, bars: tuple[StoredBar, ...]) -> None:
    with path.open("xb") as output:
        pq.write_table(
            _table(bars),
            output,
            compression="zstd",
            compression_level=9,
            use_dictionary=["source", "exchange_code", "provider_symbol", "active_fetch_id"],
            write_statistics=True,
            row_group_size=65_536,
            data_page_version="1.0",
            use_compliant_nested_type=True,
            store_schema=True,
        )
        output.flush()
        os.fsync(output.fileno())


def _validate_parquet(path: Path, expected_rows: int | None = None) -> int:
    table = pq.read_table(path)
    if not table.schema.equals(PARQUET_SCHEMA, check_metadata=True):
        raise PublicationError(f"unexpected Parquet schema for {path}")
    if expected_rows is not None and table.num_rows != expected_rows:
        raise PublicationError(
            f"Parquet row count mismatch for {path}: {table.num_rows} != {expected_rows}"
        )
    return cast(int, table.num_rows)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


class ArchiveCompactor:
    """Publish deterministic daily snapshots without weakening SQLite as source of truth."""

    def __init__(
        self,
        data_root: Path,
        store: WriterStore,
        *,
        free_bytes: FreeBytesProbe = _default_free_bytes,
        min_free_bytes: int = DEFAULT_MIN_FREE_BYTES,
        fault_injector: FaultInjector | None = None,
        failed_temp_retention: timedelta = timedelta(days=7),
        max_failed_temp_files: int = 64,
        utc_now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if min_free_bytes < 0:
            raise ValueError("min_free_bytes must be nonnegative")
        if failed_temp_retention < timedelta(0) or max_failed_temp_files < 0:
            raise ValueError("failed-temp retention and cap must be nonnegative")
        self.data_root = Path(data_root).resolve()
        self.archive_root = self.data_root / "archive"
        self.store = store
        self._free_bytes = free_bytes
        self._min_free_bytes = min_free_bytes
        self._fault = fault_injector or (lambda _point: None)
        self._failed_temp_retention = failed_temp_retention
        self._max_failed_temp_files = max_failed_temp_files
        self._utc_now = utc_now

    def compact_all(self) -> tuple[PublishedPartition, ...]:
        keys = sorted(
            {
                (
                    bar.source,
                    bar.interval_seconds,
                    bar.bar_open_utc.astimezone(_ISTANBUL).date().isoformat(),
                )
                for bar in self.store.active_bars()
            }
        )
        if not keys:
            return ()
        self._guard_space()
        return tuple(self._compact_partition(*key) for key in keys)

    def _guard_space(self) -> None:
        available = self._free_bytes(self.data_root)
        projection = self.projected_headroom()
        required = max(self._min_free_bytes, projection.required_bytes)
        if available < required:
            raise LowDiskError(
                f"free space {available} is below dynamic headroom {required} "
                f"(absolute floor {self._min_free_bytes})"
            )

    def projected_headroom(self) -> HeadroomProjection:
        projected_temp = max(1 * 1024 * 1024, self.store.active_bar_count() * 512)
        database_bytes = (
            self.store.database_path.stat().st_size if self.store.database_path.exists() else 0
        )
        wal = self.store.database_path.with_name(f"{self.store.database_path.name}-wal")
        current_wal_bytes = wal.stat().st_size if wal.exists() else 0
        projected_wal = max(4 * 1024 * 1024, database_bytes + current_wal_bytes)
        active_sizes = [
            (self.data_root / manifest.final_path).stat().st_size
            for manifest in self.store.active_manifests()
            if manifest.final_path is not None and (self.data_root / manifest.final_path).is_file()
        ]
        projected_publication = max([projected_temp, *active_sizes])
        required = 2 * (projected_temp + projected_wal + projected_publication)
        return HeadroomProjection(
            projected_temp,
            projected_wal,
            projected_publication,
            required,
        )

    def _compact_partition(
        self, source: str, interval_seconds: int, session_date: str
    ) -> PublishedPartition:
        bars = self.store.partition_bars(source, interval_seconds, session_date)
        if not bars:
            raise PublicationError("refusing to publish an empty partition")
        partition = (
            self.archive_root
            / f"source={source}"
            / f"interval={interval_seconds}"
            / f"session_date={session_date}"
        )
        partition.mkdir(parents=True, exist_ok=True)
        attempt_id = uuid.uuid4().hex
        temp = partition / f".{attempt_id}.parquet.tmp"
        self.store.create_manifest(
            attempt_id,
            source,
            interval_seconds,
            session_date,
            temp.relative_to(self.data_root).as_posix(),
        )
        try:
            self._fault("before_temp_write")
            _write_deterministic_parquet(temp, bars)
            self._fault("after_temp_write")
            row_count = _validate_parquet(temp, len(bars))
            content_sha256 = _sha256(temp)
            final = partition / f"part-{content_sha256}.parquet"
            self.store.mark_manifest_pending(
                attempt_id,
                final.relative_to(self.data_root).as_posix(),
                content_sha256,
                row_count,
            )
            self._fault("after_pending_manifest")
            self._publish_temp(temp, final, content_sha256)
            self._fault("after_final_rename")
            self.store.activate_manifest(attempt_id)
            return PublishedPartition(
                source,
                interval_seconds,
                session_date,
                content_sha256,
                row_count,
                final,
                attempt_id,
            )
        except OSError as error:
            self.store.fail_manifest(attempt_id, f"{error.__class__.__name__}: {error}")
            if error.errno == errno.ENOSPC:
                raise PublicationError(f"disk full while publishing {session_date}") from error
            raise PublicationError(f"archive publication failed: {error}") from error
        except PublicationError as error:
            self.store.fail_manifest(attempt_id, str(error))
            raise

    @staticmethod
    def _publish_temp(temp: Path, final: Path, content_sha256: str) -> None:
        if final.exists():
            if _sha256(final) != content_sha256:
                raise PublicationError(f"content-address collision at {final}")
            temp.unlink()
        else:
            os.replace(temp, final)
        directory_fd = os.open(final.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)

    def reconcile(self) -> ReconcileReport:
        recovered = failed = 0
        for manifest in self.store.manifests(("writing", "pending")):
            try:
                self._reconcile_manifest(manifest)
                recovered += 1
            except (OSError, PublicationError, ValueError) as error:
                self.store.fail_manifest(
                    manifest.attempt_id, f"{error.__class__.__name__}: {error}"
                )
                failed += 1
        cleaned = self._cleanup_failed_temps()
        return ReconcileReport(recovered, failed, cleaned)

    def _cleanup_failed_temps(self) -> int:
        now = self._utc_now()
        if now.tzinfo is None or now.utcoffset() != timedelta(0):
            raise ValueError("utc_now must return timezone-aware UTC")
        candidates: list[tuple[float, StoredManifest, Path]] = []
        archive_root = self.archive_root.resolve()
        for manifest in self.store.manifests(("failed",)):
            if manifest.temp_removed_at_utc is not None:
                continue
            temp = (self.data_root / manifest.temp_path).resolve()
            if (
                not temp.is_file()
                or not temp.is_relative_to(archive_root)
                or not temp.name.startswith(".")
                or not temp.name.endswith(".parquet.tmp")
            ):
                continue
            candidates.append((temp.stat().st_mtime, manifest, temp))
        candidates.sort(key=lambda item: (item[0], item[1].attempt_id))
        overflow = max(0, len(candidates) - self._max_failed_temp_files)
        cutoff = (now - self._failed_temp_retention).timestamp()
        cleaned = 0
        for index, (modified_at, manifest, temp) in enumerate(candidates):
            if index >= overflow and modified_at > cutoff:
                continue
            try:
                temp.unlink()
            except OSError:
                continue
            self.store.mark_manifest_temp_removed(
                manifest.attempt_id,
                now.astimezone(UTC),
                "failed-temp-retention-policy",
            )
            cleaned += 1
        return cleaned

    def _reconcile_manifest(self, manifest: StoredManifest) -> None:
        temp = self.data_root / manifest.temp_path
        final = self.data_root / manifest.final_path if manifest.final_path else None
        expected_hash = manifest.content_sha256
        expected_rows = manifest.row_count
        if manifest.state == "writing":
            if not temp.is_file():
                raise PublicationError("writing manifest has no retained temp file")
            expected_rows = _validate_parquet(temp)
            expected_hash = _sha256(temp)
            final = temp.parent / f"part-{expected_hash}.parquet"
            self.store.mark_manifest_pending(
                manifest.attempt_id,
                final.relative_to(self.data_root).as_posix(),
                expected_hash,
                expected_rows,
            )
        if final is None or expected_hash is None:
            raise PublicationError("pending manifest lacks publication identity")
        candidate = final if final.is_file() else temp
        if not candidate.is_file():
            raise PublicationError("pending publication has neither final nor temp file")
        _validate_parquet(candidate, expected_rows)
        if _sha256(candidate) != expected_hash:
            raise PublicationError("pending publication checksum mismatch")
        if candidate == temp:
            self._publish_temp(temp, final, expected_hash)
        self.store.activate_manifest(manifest.attempt_id)
