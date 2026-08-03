from __future__ import annotations

import errno
import hashlib
import os
import shutil
import uuid
from collections.abc import Callable
from dataclasses import dataclass
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
    ) -> None:
        self.data_root = Path(data_root).resolve()
        self.archive_root = self.data_root / "archive"
        self.store = store
        self._free_bytes = free_bytes
        self._min_free_bytes = min_free_bytes
        self._fault = fault_injector or (lambda _point: None)

    def compact_all(self, *, force_rewrite: bool = False) -> tuple[PublishedPartition, ...]:
        del force_rewrite
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
        if available < self._min_free_bytes:
            raise LowDiskError(
                f"free space {available} is below compaction floor {self._min_free_bytes}"
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
        return ReconcileReport(recovered, failed)

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
