from __future__ import annotations

import fcntl
import hashlib
import json
import sqlite3
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import TracebackType
from typing import Self
from zoneinfo import ZoneInfo

from .models import FetchResult, MinuteBar, OutcomeCode

SCHEMA_VERSION = 1
FaultInjector = Callable[[str], None]


class StoreError(RuntimeError):
    """Base error for durable-store contract violations."""


class WriterAlreadyOpenError(StoreError):
    """Raised when a second writer attempts to own the same data root."""


class ProvenanceConflictError(StoreError):
    """Raised when a fetch identifier is replayed with different contents."""


@dataclass(frozen=True, slots=True)
class CursorUpdate:
    accounted_through_utc: datetime
    last_complete_bar_open_utc: datetime | None


@dataclass(frozen=True, slots=True)
class IngestEnvelope:
    fetch_id: str
    exchange_code: str
    result: FetchResult
    retained_response: bytes | None = None
    cursor: CursorUpdate | None = None


@dataclass(frozen=True, slots=True)
class IngestReport:
    inserted_bars: int = 0
    corrected_bars: int = 0
    identical_bars: int = 0
    ignored_older_bars: int = 0
    duplicate_envelope: bool = False


@dataclass(frozen=True, slots=True)
class StoredBar:
    source: str
    exchange_code: str
    provider_symbol: str
    interval_seconds: int
    bar_open_utc: datetime
    raw_source_timestamp: int
    open: str
    high: str
    low: str
    close: str
    volume: int
    observed_at_utc: datetime
    active_fetch_id: str


@dataclass(frozen=True, slots=True)
class Revision:
    revision_id: int
    source: str
    exchange_code: str
    interval_seconds: int
    bar_open_utc: datetime
    before_close: str
    after_close: str
    before_fetch_id: str
    after_fetch_id: str


@dataclass(frozen=True, slots=True)
class StoredCursor:
    accounted_through_utc: datetime
    last_complete_bar_open_utc: datetime | None
    last_fetch_id: str


@dataclass(frozen=True, slots=True)
class RetainedResponse:
    fetch_id: str
    retention_class: str
    protected: bool
    byte_count: int
    body_sha256: str


@dataclass(frozen=True, slots=True)
class StoredManifest:
    attempt_id: str
    source: str
    interval_seconds: int
    session_date: str
    state: str
    temp_path: str
    final_path: str | None
    content_sha256: str | None
    row_count: int | None
    error: str | None


def _utc_text(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("timestamp must be timezone-aware UTC")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _from_utc_text(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def _decimal_text(value: Decimal) -> str:
    return format(value, "f")


def _bar_payload(bar: MinuteBar) -> dict[str, object]:
    return {
        "source": bar.source,
        "symbol": bar.symbol,
        "interval_seconds": bar.interval_seconds,
        "bar_open_utc": _utc_text(bar.bar_open_utc),
        "raw_source_timestamp": bar.raw_source_timestamp,
        "open": _decimal_text(bar.open),
        "high": _decimal_text(bar.high),
        "low": _decimal_text(bar.low),
        "close": _decimal_text(bar.close),
        "volume": bar.volume,
        "observed_at_utc": _utc_text(bar.observed_at_utc),
    }


def _envelope_sha256(envelope: IngestEnvelope) -> str:
    result = envelope.result
    metadata: dict[str, object] | None = None
    if result.metadata is not None:
        metadata = {
            "symbol": result.metadata.symbol,
            "currency": result.metadata.currency,
            "exchange_name": result.metadata.exchange_name,
            "full_exchange_name": result.metadata.full_exchange_name,
            "instrument_type": result.metadata.instrument_type,
            "first_trade_date": result.metadata.first_trade_date,
            "regular_market_time": result.metadata.regular_market_time,
            "gmt_offset_seconds": result.metadata.gmt_offset_seconds,
            "timezone_abbreviation": result.metadata.timezone_abbreviation,
            "exchange_timezone_name": result.metadata.exchange_timezone_name,
            "data_granularity": result.metadata.data_granularity,
            "raw_metadata_sha256": result.metadata.raw_metadata_sha256,
        }
    payload = {
        "fetch_id": envelope.fetch_id,
        "exchange_code": envelope.exchange_code,
        "result": {
            "source": result.source,
            "symbol": result.symbol,
            "interval": result.interval,
            "request_start": _utc_text(result.request_window.start_utc),
            "request_end": _utc_text(result.request_window.end_utc),
            "request_semantics": result.request_window_semantics.value,
            "retrieved_at": _utc_text(result.retrieved_at_utc),
            "status_code": result.status_code,
            "attempts": result.attempts,
            "response_sha256": result.response_sha256,
            "metadata": metadata,
            "bars": [_bar_payload(bar) for bar in result.bars],
            "outcomes": [
                {
                    "code": outcome.code.value,
                    "message": outcome.message,
                    "raw_source_timestamp": outcome.raw_source_timestamp,
                    "row_index": outcome.row_index,
                    "retry_after": outcome.retry_after,
                }
                for outcome in result.outcomes
            ],
        },
        "retained_response_sha256": (
            hashlib.sha256(envelope.retained_response).hexdigest()
            if envelope.retained_response is not None
            else None
        ),
        "cursor": (
            {
                "accounted_through": _utc_text(envelope.cursor.accounted_through_utc),
                "last_complete": (
                    _utc_text(envelope.cursor.last_complete_bar_open_utc)
                    if envelope.cursor.last_complete_bar_open_utc is not None
                    else None
                ),
            }
            if envelope.cursor is not None
            else None
        ),
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(canonical).hexdigest()


MIGRATION_V1 = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at_utc TEXT NOT NULL,
    migration_sha256 TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS fetch_provenance (
    fetch_id TEXT PRIMARY KEY,
    envelope_sha256 TEXT NOT NULL,
    source TEXT NOT NULL,
    provider_symbol TEXT NOT NULL,
    exchange_code TEXT NOT NULL,
    interval_seconds INTEGER NOT NULL,
    request_start_utc TEXT NOT NULL,
    request_end_utc TEXT NOT NULL,
    request_semantics TEXT NOT NULL,
    retrieved_at_utc TEXT NOT NULL,
    status_code INTEGER,
    attempts INTEGER NOT NULL CHECK (attempts >= 1),
    response_sha256 TEXT,
    metadata_sha256 TEXT,
    body_retained INTEGER NOT NULL CHECK (body_retained IN (0,1)),
    body_retention_reason TEXT NOT NULL,
    created_at_utc TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS provider_outcomes (
    fetch_id TEXT NOT NULL REFERENCES fetch_provenance(fetch_id),
    outcome_index INTEGER NOT NULL,
    code TEXT NOT NULL,
    message TEXT NOT NULL,
    raw_source_timestamp INTEGER,
    row_index INTEGER,
    retry_after TEXT,
    PRIMARY KEY (fetch_id, outcome_index)
);
CREATE TABLE IF NOT EXISTS bar_observations (
    fetch_id TEXT NOT NULL REFERENCES fetch_provenance(fetch_id),
    source TEXT NOT NULL,
    exchange_code TEXT NOT NULL,
    provider_symbol TEXT NOT NULL,
    interval_seconds INTEGER NOT NULL,
    bar_open_utc TEXT NOT NULL,
    raw_source_timestamp INTEGER NOT NULL,
    open TEXT NOT NULL,
    high TEXT NOT NULL,
    low TEXT NOT NULL,
    close TEXT NOT NULL,
    volume INTEGER NOT NULL CHECK (volume >= 0),
    observed_at_utc TEXT NOT NULL,
    PRIMARY KEY (fetch_id, source, exchange_code, interval_seconds, bar_open_utc)
);
CREATE TABLE IF NOT EXISTS bars_hot (
    source TEXT NOT NULL,
    exchange_code TEXT NOT NULL,
    provider_symbol TEXT NOT NULL,
    interval_seconds INTEGER NOT NULL,
    bar_open_utc TEXT NOT NULL,
    raw_source_timestamp INTEGER NOT NULL,
    open TEXT NOT NULL,
    high TEXT NOT NULL,
    low TEXT NOT NULL,
    close TEXT NOT NULL,
    volume INTEGER NOT NULL CHECK (volume >= 0),
    observed_at_utc TEXT NOT NULL,
    active_fetch_id TEXT NOT NULL REFERENCES fetch_provenance(fetch_id),
    PRIMARY KEY (source, exchange_code, interval_seconds, bar_open_utc)
);
CREATE TABLE IF NOT EXISTS bar_revisions (
    revision_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    exchange_code TEXT NOT NULL,
    interval_seconds INTEGER NOT NULL,
    bar_open_utc TEXT NOT NULL,
    before_raw_source_timestamp INTEGER NOT NULL,
    before_open TEXT NOT NULL,
    before_high TEXT NOT NULL,
    before_low TEXT NOT NULL,
    before_close TEXT NOT NULL,
    before_volume INTEGER NOT NULL,
    before_observed_at_utc TEXT NOT NULL,
    before_fetch_id TEXT NOT NULL,
    after_raw_source_timestamp INTEGER NOT NULL,
    after_open TEXT NOT NULL,
    after_high TEXT NOT NULL,
    after_low TEXT NOT NULL,
    after_close TEXT NOT NULL,
    after_volume INTEGER NOT NULL,
    after_observed_at_utc TEXT NOT NULL,
    after_fetch_id TEXT NOT NULL,
    recorded_at_utc TEXT NOT NULL,
    UNIQUE (source, exchange_code, interval_seconds, bar_open_utc, before_fetch_id, after_fetch_id)
);
CREATE TABLE IF NOT EXISTS cursor_state (
    source TEXT NOT NULL,
    exchange_code TEXT NOT NULL,
    interval_seconds INTEGER NOT NULL,
    accounted_through_utc TEXT NOT NULL,
    last_complete_bar_open_utc TEXT,
    last_fetch_id TEXT NOT NULL REFERENCES fetch_provenance(fetch_id),
    PRIMARY KEY (source, exchange_code, interval_seconds)
);
CREATE TABLE IF NOT EXISTS response_evidence (
    fetch_id TEXT PRIMARY KEY REFERENCES fetch_provenance(fetch_id),
    body BLOB NOT NULL,
    body_sha256 TEXT NOT NULL,
    byte_count INTEGER NOT NULL CHECK (byte_count >= 0),
    retention_class TEXT NOT NULL CHECK (retention_class IN ('ordinary','anomaly','correction')),
    protected INTEGER NOT NULL CHECK (protected IN (0,1)),
    protection_reason TEXT,
    created_at_utc TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS compaction_manifests (
    attempt_id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    interval_seconds INTEGER NOT NULL,
    session_date TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('writing','pending','active','superseded','failed')),
    temp_path TEXT NOT NULL,
    final_path TEXT,
    content_sha256 TEXT,
    row_count INTEGER,
    error TEXT,
    created_at_utc TEXT NOT NULL,
    activated_at_utc TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS one_active_partition
ON compaction_manifests(source, interval_seconds, session_date)
WHERE state = 'active';
CREATE INDEX IF NOT EXISTS observations_by_key
ON bar_observations(source, exchange_code, interval_seconds, bar_open_utc);
CREATE INDEX IF NOT EXISTS bars_hot_by_partition
ON bars_hot(source, interval_seconds, bar_open_utc, exchange_code);
"""


def _migration_statements(sql: str) -> Iterator[str]:
    statement = ""
    for line in sql.splitlines():
        statement += f"{line}\n"
        if sqlite3.complete_statement(statement):
            cleaned = statement.strip()
            if cleaned:
                yield cleaned
            statement = ""
    if statement.strip():
        raise StoreError("incomplete migration SQL")


class StoreReader(AbstractContextManager["StoreReader"]):
    def __init__(self, database_path: Path) -> None:
        self.database_path = database_path
        uri = f"file:{database_path.as_posix()}?mode=ro"
        self._connection = sqlite3.connect(uri, uri=True)
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA query_only=ON")

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        self._connection.close()

    def scalar(self, query: str, parameters: tuple[object, ...] = ()) -> object | None:
        row = self._connection.execute(query, parameters).fetchone()
        return None if row is None else row[0]

    def integer_scalar(self, query: str, parameters: tuple[object, ...] = ()) -> int:
        value = self.scalar(query, parameters)
        if not isinstance(value, int):
            raise StoreError(f"query did not return an integer: {query}")
        return value

    def quick_check(self) -> str:
        value = self.scalar("PRAGMA quick_check")
        return str(value)

    def schema_signature(self) -> tuple[tuple[str, str], ...]:
        rows = self._connection.execute(
            "SELECT type, name, sql FROM sqlite_schema "
            "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
        ).fetchall()
        return tuple((str(row["name"]), str(row["sql"])) for row in rows)


class WriterStore(StoreReader):
    """The sole transactional writer for one collector data root."""

    def __init__(
        self,
        data_root: Path,
        *,
        response_budget_bytes: int = 64 * 1024 * 1024,
        fault_injector: FaultInjector | None = None,
    ) -> None:
        self.data_root = Path(data_root).resolve()
        self.state_directory = self.data_root / "state"
        self.state_directory.mkdir(parents=True, exist_ok=True)
        self.database_path = self.state_directory / "collector.sqlite3"
        self._lock_path = self.state_directory / "writer.lock"
        self._lock_file = self._lock_path.open("a+b")
        try:
            fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            self._lock_file.close()
            raise WriterAlreadyOpenError(f"writer already open for {self.data_root}") from error
        self._connection = sqlite3.connect(self.database_path, isolation_level=None)
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA foreign_keys=ON")
        self._connection.execute("PRAGMA busy_timeout=5000")
        self._connection.execute("PRAGMA synchronous=FULL")
        self._connection.execute("PRAGMA journal_mode=WAL")
        self.response_budget_bytes = response_budget_bytes
        self._fault = fault_injector or (lambda _point: None)
        self._closed = False
        self.apply_migrations()

    @classmethod
    def open_reader(cls, data_root: Path) -> StoreReader:
        return StoreReader(Path(data_root).resolve() / "state" / "collector.sqlite3")

    def close(self) -> None:
        if self._closed:
            return
        self._connection.close()
        fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_UN)
        self._lock_file.close()
        self._closed = True

    def apply_migrations(self) -> None:
        current = int(self._connection.execute("PRAGMA user_version").fetchone()[0])
        if current > SCHEMA_VERSION:
            raise StoreError(f"database schema {current} is newer than supported {SCHEMA_VERSION}")
        if current == SCHEMA_VERSION:
            return
        checksum = hashlib.sha256(MIGRATION_V1.encode()).hexdigest()
        self._connection.execute("BEGIN EXCLUSIVE")
        try:
            for statement in _migration_statements(MIGRATION_V1):
                self._connection.execute(statement)
            self._connection.execute(
                "INSERT INTO schema_migrations(version, applied_at_utc, migration_sha256) "
                "VALUES (?, ?, ?)",
                (1, _utc_text(datetime.now(UTC)), checksum),
            )
            self._connection.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
            self._connection.commit()
        except BaseException:
            self._connection.rollback()
            raise

    def schema_version(self) -> int:
        return self.integer_scalar("PRAGMA user_version")

    def migration_count(self) -> int:
        return self.integer_scalar("SELECT COUNT(*) FROM schema_migrations")

    def journal_mode(self) -> str:
        return str(self.scalar("PRAGMA journal_mode"))

    def ingest(self, envelope: IngestEnvelope) -> IngestReport:
        self._validate_envelope(envelope)
        digest = _envelope_sha256(envelope)
        result = envelope.result
        interval_seconds = 60
        self._connection.execute("BEGIN IMMEDIATE")
        try:
            existing = self._connection.execute(
                "SELECT envelope_sha256 FROM fetch_provenance WHERE fetch_id=?",
                (envelope.fetch_id,),
            ).fetchone()
            if existing is not None:
                if str(existing["envelope_sha256"]) != digest:
                    raise ProvenanceConflictError(
                        f"fetch_id {envelope.fetch_id!r} has different contents"
                    )
                self._connection.rollback()
                return IngestReport(duplicate_envelope=True)

            self._insert_provenance(envelope, digest, interval_seconds)
            self._insert_outcomes(envelope)
            self._retain_response(envelope)
            inserted = corrected = identical = ignored = 0
            for bar in result.bars:
                self._insert_observation(envelope, bar)
                action = self._upsert_active(envelope, bar)
                if action == "inserted":
                    inserted += 1
                elif action == "corrected":
                    corrected += 1
                elif action == "identical":
                    identical += 1
                else:
                    ignored += 1

            if envelope.cursor is not None:
                self._upsert_cursor(envelope, interval_seconds)
            self._prune_responses()
            self._fault("before_commit")
            self._connection.commit()
            return IngestReport(inserted, corrected, identical, ignored)
        except BaseException:
            self._connection.rollback()
            raise

    def _validate_envelope(self, envelope: IngestEnvelope) -> None:
        if not envelope.fetch_id or not envelope.exchange_code.endswith(".E"):
            raise ValueError("fetch_id and canonical .E exchange_code are required")
        if envelope.result.interval != "1m":
            raise ValueError("Phase 2 accepts only one-minute results")
        _utc_text(envelope.result.retrieved_at_utc)
        if envelope.cursor is not None:
            _utc_text(envelope.cursor.accounted_through_utc)
            if envelope.cursor.last_complete_bar_open_utc is not None:
                _utc_text(envelope.cursor.last_complete_bar_open_utc)
        for bar in envelope.result.bars:
            if (
                bar.source != envelope.result.source
                or bar.symbol != envelope.result.symbol
                or bar.interval_seconds != 60
            ):
                raise ValueError("bar identity does not match fetch result")
            _utc_text(bar.bar_open_utc)
            _utc_text(bar.observed_at_utc)
            if int(bar.bar_open_utc.timestamp()) % 60 or bar.raw_source_timestamp != int(
                bar.bar_open_utc.timestamp()
            ):
                raise ValueError("bar timestamp must be source-retained and minute aligned")
            if (
                bar.volume < 0
                or bar.low > min(bar.open, bar.close)
                or bar.high < max(bar.open, bar.close)
            ):
                raise ValueError("bar violates complete OHLCV invariants")

    def _insert_provenance(
        self, envelope: IngestEnvelope, digest: str, interval_seconds: int
    ) -> None:
        result = envelope.result
        self._connection.execute(
            "INSERT INTO fetch_provenance VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                envelope.fetch_id,
                digest,
                result.source,
                result.symbol,
                envelope.exchange_code,
                interval_seconds,
                _utc_text(result.request_window.start_utc),
                _utc_text(result.request_window.end_utc),
                result.request_window_semantics.value,
                _utc_text(result.retrieved_at_utc),
                result.status_code,
                result.attempts,
                result.response_sha256,
                result.metadata.raw_metadata_sha256 if result.metadata is not None else None,
                int(envelope.retained_response is not None),
                (
                    "retained in response evidence ring"
                    if envelope.retained_response is not None
                    else "exact response body not supplied to storage"
                ),
                _utc_text(datetime.now(UTC)),
            ),
        )

    def _insert_outcomes(self, envelope: IngestEnvelope) -> None:
        for index, outcome in enumerate(envelope.result.outcomes):
            self._connection.execute(
                "INSERT INTO provider_outcomes VALUES (?,?,?,?,?,?,?)",
                (
                    envelope.fetch_id,
                    index,
                    outcome.code.value,
                    outcome.message,
                    outcome.raw_source_timestamp,
                    outcome.row_index,
                    outcome.retry_after,
                ),
            )

    def _retain_response(self, envelope: IngestEnvelope) -> None:
        body = envelope.retained_response
        if body is None:
            return
        routine_codes = {
            OutcomeCode.SUCCESS,
            OutcomeCode.NULL_ROW,
            OutcomeCode.PARTIAL_ROW,
            OutcomeCode.NON_MINUTE_ALIGNED_PROVISIONAL_TAIL,
        }
        codes = envelope.result.codes
        anomaly = OutcomeCode.SUCCESS not in codes or any(
            code not in routine_codes for code in codes
        )
        retention_class = "anomaly" if anomaly else "ordinary"
        self._connection.execute(
            "INSERT INTO response_evidence VALUES (?,?,?,?,?,?,?,?)",
            (
                envelope.fetch_id,
                body,
                hashlib.sha256(body).hexdigest(),
                len(body),
                retention_class,
                int(anomaly),
                "provider outcome requires audit" if anomaly else None,
                _utc_text(datetime.now(UTC)),
            ),
        )

    def _insert_observation(self, envelope: IngestEnvelope, bar: MinuteBar) -> None:
        self._connection.execute(
            "INSERT INTO bar_observations VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                envelope.fetch_id,
                bar.source,
                envelope.exchange_code,
                bar.symbol,
                bar.interval_seconds,
                _utc_text(bar.bar_open_utc),
                bar.raw_source_timestamp,
                _decimal_text(bar.open),
                _decimal_text(bar.high),
                _decimal_text(bar.low),
                _decimal_text(bar.close),
                bar.volume,
                _utc_text(bar.observed_at_utc),
            ),
        )

    @staticmethod
    def _values_from_row(row: sqlite3.Row) -> tuple[object, ...]:
        return (
            row["raw_source_timestamp"],
            row["open"],
            row["high"],
            row["low"],
            row["close"],
            row["volume"],
        )

    @staticmethod
    def _values_from_bar(bar: MinuteBar) -> tuple[object, ...]:
        return (
            bar.raw_source_timestamp,
            _decimal_text(bar.open),
            _decimal_text(bar.high),
            _decimal_text(bar.low),
            _decimal_text(bar.close),
            bar.volume,
        )

    def _upsert_active(self, envelope: IngestEnvelope, bar: MinuteBar) -> str:
        key = (
            bar.source,
            envelope.exchange_code,
            bar.interval_seconds,
            _utc_text(bar.bar_open_utc),
        )
        active = self._connection.execute(
            "SELECT * FROM bars_hot WHERE source=? AND exchange_code=? "
            "AND interval_seconds=? AND bar_open_utc=?",
            key,
        ).fetchone()
        if active is None:
            self._connection.execute(
                "INSERT INTO bars_hot VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    bar.source,
                    envelope.exchange_code,
                    bar.symbol,
                    bar.interval_seconds,
                    _utc_text(bar.bar_open_utc),
                    bar.raw_source_timestamp,
                    _decimal_text(bar.open),
                    _decimal_text(bar.high),
                    _decimal_text(bar.low),
                    _decimal_text(bar.close),
                    bar.volume,
                    _utc_text(bar.observed_at_utc),
                    envelope.fetch_id,
                ),
            )
            return "inserted"
        if self._values_from_row(active) == self._values_from_bar(bar):
            return "identical"
        candidate_version = (bar.observed_at_utc, envelope.fetch_id)
        active_version = (
            _from_utc_text(str(active["observed_at_utc"])),
            str(active["active_fetch_id"]),
        )
        if candidate_version <= active_version:
            return "ignored"

        self._connection.execute(
            "INSERT INTO bar_revisions VALUES (NULL,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                bar.source,
                envelope.exchange_code,
                bar.interval_seconds,
                _utc_text(bar.bar_open_utc),
                active["raw_source_timestamp"],
                active["open"],
                active["high"],
                active["low"],
                active["close"],
                active["volume"],
                active["observed_at_utc"],
                active["active_fetch_id"],
                bar.raw_source_timestamp,
                _decimal_text(bar.open),
                _decimal_text(bar.high),
                _decimal_text(bar.low),
                _decimal_text(bar.close),
                bar.volume,
                _utc_text(bar.observed_at_utc),
                envelope.fetch_id,
                _utc_text(datetime.now(UTC)),
            ),
        )
        self._connection.execute(
            "UPDATE response_evidence SET retention_class='correction', protected=1, "
            "protection_reason='bar correction chain' WHERE fetch_id IN (?,?)",
            (active["active_fetch_id"], envelope.fetch_id),
        )
        self._fault("after_revision_before_replace")
        self._connection.execute(
            "UPDATE bars_hot SET provider_symbol=?, raw_source_timestamp=?, open=?, high=?, "
            "low=?, close=?, volume=?, observed_at_utc=?, active_fetch_id=? "
            "WHERE source=? AND exchange_code=? AND interval_seconds=? AND bar_open_utc=?",
            (
                bar.symbol,
                bar.raw_source_timestamp,
                _decimal_text(bar.open),
                _decimal_text(bar.high),
                _decimal_text(bar.low),
                _decimal_text(bar.close),
                bar.volume,
                _utc_text(bar.observed_at_utc),
                envelope.fetch_id,
                *key,
            ),
        )
        return "corrected"

    def _upsert_cursor(self, envelope: IngestEnvelope, interval_seconds: int) -> None:
        assert envelope.cursor is not None
        cursor = envelope.cursor
        existing = self._connection.execute(
            "SELECT accounted_through_utc FROM cursor_state WHERE source=? AND exchange_code=? "
            "AND interval_seconds=?",
            (envelope.result.source, envelope.exchange_code, interval_seconds),
        ).fetchone()
        accounted = _utc_text(cursor.accounted_through_utc)
        if existing is not None and cursor.accounted_through_utc < _from_utc_text(
            str(existing["accounted_through_utc"])
        ):
            return
        self._connection.execute(
            "INSERT INTO cursor_state VALUES (?,?,?,?,?,?) "
            "ON CONFLICT(source,exchange_code,interval_seconds) DO UPDATE SET "
            "accounted_through_utc=excluded.accounted_through_utc, "
            "last_complete_bar_open_utc=excluded.last_complete_bar_open_utc, "
            "last_fetch_id=excluded.last_fetch_id",
            (
                envelope.result.source,
                envelope.exchange_code,
                interval_seconds,
                accounted,
                (
                    _utc_text(cursor.last_complete_bar_open_utc)
                    if cursor.last_complete_bar_open_utc is not None
                    else None
                ),
                envelope.fetch_id,
            ),
        )

    def _prune_responses(self) -> None:
        total = self.integer_scalar(
            "SELECT COALESCE(SUM(byte_count),0) FROM response_evidence "
            "WHERE retention_class='ordinary'"
        )
        while total > self.response_budget_bytes:
            candidate = self._connection.execute(
                "SELECT fetch_id, byte_count FROM response_evidence r "
                "WHERE retention_class='ordinary' AND protected=0 "
                "ORDER BY created_at_utc, fetch_id LIMIT 1"
            ).fetchone()
            if candidate is None:
                return
            self._connection.execute(
                "UPDATE fetch_provenance SET body_retained=0, "
                "body_retention_reason='ordinary response ring byte budget' WHERE fetch_id=?",
                (candidate["fetch_id"],),
            )
            self._connection.execute(
                "DELETE FROM response_evidence WHERE fetch_id=?", (candidate["fetch_id"],)
            )
            total -= int(candidate["byte_count"])

    def active_bar_count(self) -> int:
        return self.integer_scalar("SELECT COUNT(*) FROM bars_hot")

    def provenance_count(self) -> int:
        return self.integer_scalar("SELECT COUNT(*) FROM fetch_provenance")

    def revision_count(self) -> int:
        return self.integer_scalar("SELECT COUNT(*) FROM bar_revisions")

    def active_bar(
        self, source: str, exchange_code: str, interval_seconds: int, bar_open_utc: datetime
    ) -> StoredBar:
        row = self._connection.execute(
            "SELECT * FROM bars_hot WHERE source=? AND exchange_code=? "
            "AND interval_seconds=? AND bar_open_utc=?",
            (source, exchange_code, interval_seconds, _utc_text(bar_open_utc)),
        ).fetchone()
        if row is None:
            raise KeyError((source, exchange_code, interval_seconds, bar_open_utc))
        return self._stored_bar(row)

    @staticmethod
    def _stored_bar(row: sqlite3.Row) -> StoredBar:
        return StoredBar(
            source=str(row["source"]),
            exchange_code=str(row["exchange_code"]),
            provider_symbol=str(row["provider_symbol"]),
            interval_seconds=int(row["interval_seconds"]),
            bar_open_utc=_from_utc_text(str(row["bar_open_utc"])),
            raw_source_timestamp=int(row["raw_source_timestamp"]),
            open=str(row["open"]),
            high=str(row["high"]),
            low=str(row["low"]),
            close=str(row["close"]),
            volume=int(row["volume"]),
            observed_at_utc=_from_utc_text(str(row["observed_at_utc"])),
            active_fetch_id=str(row["active_fetch_id"]),
        )

    def active_bars(self) -> tuple[StoredBar, ...]:
        rows = self._connection.execute(
            "SELECT * FROM bars_hot ORDER BY source, interval_seconds, bar_open_utc, exchange_code"
        ).fetchall()
        return tuple(self._stored_bar(row) for row in rows)

    def revisions(self) -> tuple[Revision, ...]:
        rows = self._connection.execute(
            "SELECT revision_id,source,exchange_code,interval_seconds,bar_open_utc,before_close,"
            "after_close,before_fetch_id,after_fetch_id FROM bar_revisions ORDER BY revision_id"
        ).fetchall()
        return tuple(
            Revision(
                int(row["revision_id"]),
                str(row["source"]),
                str(row["exchange_code"]),
                int(row["interval_seconds"]),
                _from_utc_text(str(row["bar_open_utc"])),
                str(row["before_close"]),
                str(row["after_close"]),
                str(row["before_fetch_id"]),
                str(row["after_fetch_id"]),
            )
            for row in rows
        )

    def cursor(self, source: str, exchange_code: str, interval_seconds: int) -> StoredCursor | None:
        row = self._connection.execute(
            "SELECT * FROM cursor_state WHERE source=? AND exchange_code=? AND interval_seconds=?",
            (source, exchange_code, interval_seconds),
        ).fetchone()
        if row is None:
            return None
        last = row["last_complete_bar_open_utc"]
        return StoredCursor(
            _from_utc_text(str(row["accounted_through_utc"])),
            _from_utc_text(str(last)) if last is not None else None,
            str(row["last_fetch_id"]),
        )

    def retained_responses(self) -> tuple[RetainedResponse, ...]:
        rows = self._connection.execute(
            "SELECT fetch_id,retention_class,protected,byte_count,body_sha256 "
            "FROM response_evidence ORDER BY fetch_id"
        ).fetchall()
        return tuple(
            RetainedResponse(
                str(row["fetch_id"]),
                str(row["retention_class"]),
                bool(row["protected"]),
                int(row["byte_count"]),
                str(row["body_sha256"]),
            )
            for row in rows
        )

    def body_retention(self, fetch_id: str) -> tuple[bool, str]:
        row = self._connection.execute(
            "SELECT body_retained,body_retention_reason FROM fetch_provenance WHERE fetch_id=?",
            (fetch_id,),
        ).fetchone()
        if row is None:
            raise KeyError(fetch_id)
        return bool(row["body_retained"]), str(row["body_retention_reason"])

    # Compaction manifest operations remain on the one writer.
    def create_manifest(
        self,
        attempt_id: str,
        source: str,
        interval_seconds: int,
        session_date: str,
        temp_path: str,
    ) -> None:
        self._connection.execute(
            "INSERT INTO compaction_manifests VALUES "
            "(?,?,?,?, 'writing', ?,NULL,NULL,NULL,NULL,?,NULL)",
            (
                attempt_id,
                source,
                interval_seconds,
                session_date,
                temp_path,
                _utc_text(datetime.now(UTC)),
            ),
        )

    def mark_manifest_pending(
        self, attempt_id: str, final_path: str, content_sha256: str, row_count: int
    ) -> None:
        self._connection.execute(
            "UPDATE compaction_manifests SET state='pending',final_path=?,content_sha256=?,"
            "row_count=?,error=NULL WHERE attempt_id=?",
            (final_path, content_sha256, row_count, attempt_id),
        )

    def activate_manifest(self, attempt_id: str) -> None:
        row = self._connection.execute(
            "SELECT source,interval_seconds,session_date FROM compaction_manifests "
            "WHERE attempt_id=?",
            (attempt_id,),
        ).fetchone()
        if row is None:
            raise KeyError(attempt_id)
        self._connection.execute("BEGIN IMMEDIATE")
        try:
            self._connection.execute(
                "UPDATE compaction_manifests SET state='superseded' "
                "WHERE source=? AND interval_seconds=? AND session_date=? AND state='active' "
                "AND attempt_id<>?",
                (row["source"], row["interval_seconds"], row["session_date"], attempt_id),
            )
            self._connection.execute(
                "UPDATE compaction_manifests SET state='active',activated_at_utc=?,error=NULL "
                "WHERE attempt_id=?",
                (_utc_text(datetime.now(UTC)), attempt_id),
            )
            self._connection.commit()
        except BaseException:
            self._connection.rollback()
            raise

    def fail_manifest(self, attempt_id: str, error: str) -> None:
        self._connection.execute(
            "UPDATE compaction_manifests SET state='failed',error=? WHERE attempt_id=?",
            (error, attempt_id),
        )

    def manifests(self, states: tuple[str, ...] = ()) -> tuple[StoredManifest, ...]:
        query = "SELECT * FROM compaction_manifests"
        parameters: tuple[object, ...] = ()
        if states:
            placeholders = ",".join("?" for _ in states)
            query += f" WHERE state IN ({placeholders})"
            parameters = states
        query += " ORDER BY created_at_utc,attempt_id"
        rows = self._connection.execute(query, parameters).fetchall()
        return tuple(
            StoredManifest(
                str(row["attempt_id"]),
                str(row["source"]),
                int(row["interval_seconds"]),
                str(row["session_date"]),
                str(row["state"]),
                str(row["temp_path"]),
                str(row["final_path"]) if row["final_path"] is not None else None,
                (str(row["content_sha256"]) if row["content_sha256"] is not None else None),
                int(row["row_count"]) if row["row_count"] is not None else None,
                str(row["error"]) if row["error"] is not None else None,
            )
            for row in rows
        )

    def pending_manifests(self) -> tuple[StoredManifest, ...]:
        return self.manifests(("pending",))

    def active_manifests(self) -> tuple[StoredManifest, ...]:
        return self.manifests(("active",))

    def manifest_count(self) -> int:
        return self.integer_scalar("SELECT COUNT(*) FROM compaction_manifests")

    def active_manifest_count(self) -> int:
        return self.integer_scalar("SELECT COUNT(*) FROM compaction_manifests WHERE state='active'")

    def active_partition_hashes(self) -> list[str]:
        rows = self._connection.execute(
            "SELECT content_sha256 FROM compaction_manifests WHERE state='active' "
            "ORDER BY source,interval_seconds,session_date"
        ).fetchall()
        return [str(row["content_sha256"]) for row in rows]

    def override_manifest_hash_for_test(self, attempt_id: str, content_sha256: str) -> None:
        self._connection.execute(
            "UPDATE compaction_manifests SET content_sha256=? WHERE attempt_id=?",
            (content_sha256, attempt_id),
        )

    def partition_bars(
        self, source: str, interval_seconds: int, session_date: str
    ) -> tuple[StoredBar, ...]:
        # UTC bounds are deliberately broad; authoritative partition membership is Istanbul date.
        return tuple(
            bar
            for bar in self.active_bars()
            if bar.source == source
            and bar.interval_seconds == interval_seconds
            and bar.bar_open_utc.astimezone(_ISTANBUL).date().isoformat() == session_date
        )


_ISTANBUL = ZoneInfo("Europe/Istanbul")
