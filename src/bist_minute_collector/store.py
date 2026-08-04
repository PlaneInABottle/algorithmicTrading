from __future__ import annotations

import fcntl
import hashlib
import json
import sqlite3
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import TracebackType
from typing import TYPE_CHECKING, Self
from zoneinfo import ZoneInfo

from .models import FetchResult, MinuteBar, OutcomeCode

if TYPE_CHECKING:
    from .snapshots import UniverseSnapshot

SCHEMA_VERSION = 5
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
    temp_removed_at_utc: datetime | None
    temp_removal_reason: str | None


@dataclass(frozen=True, slots=True)
class RequestReservation:
    allowed: bool
    reason: str
    attempt_id: int | None = None


@dataclass(frozen=True, slots=True)
class StoredProviderRuntime:
    source: str
    cooldown_until_utc: datetime | None
    circuit_open_until_utc: datetime | None
    throttle_streak: int
    paused_reason: str | None
    paused_until_utc: datetime | None


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

MIGRATION_V2 = """
CREATE TABLE IF NOT EXISTS provider_runtime_state (
    source TEXT PRIMARY KEY,
    cooldown_until_utc TEXT,
    circuit_open_until_utc TEXT,
    throttle_streak INTEGER NOT NULL DEFAULT 0 CHECK (throttle_streak >= 0),
    paused_reason TEXT,
    updated_at_utc TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS request_attempts (
    attempt_id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id TEXT NOT NULL UNIQUE,
    source TEXT NOT NULL,
    provider_symbol TEXT NOT NULL,
    attempted_at_utc TEXT NOT NULL,
    attempted_at_epoch_us INTEGER NOT NULL,
    session_date TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('started','completed','cancelled')),
    outcome_code TEXT,
    completed_at_utc TEXT
);
CREATE INDEX IF NOT EXISTS request_attempts_budget
ON request_attempts(source, attempted_at_epoch_us, session_date);
CREATE INDEX IF NOT EXISTS request_attempts_outcome
ON request_attempts(source, outcome_code, attempted_at_epoch_us);
CREATE TABLE IF NOT EXISTS provider_schema_drift (
    source TEXT NOT NULL,
    signature TEXT NOT NULL,
    provider_symbol TEXT NOT NULL,
    observed_at_utc TEXT NOT NULL,
    PRIMARY KEY (source, signature, provider_symbol)
);
ALTER TABLE compaction_manifests ADD COLUMN temp_removed_at_utc TEXT;
ALTER TABLE compaction_manifests ADD COLUMN temp_removal_reason TEXT;
"""

MIGRATION_V3 = """
ALTER TABLE provider_runtime_state ADD COLUMN paused_until_utc TEXT;
"""

MIGRATION_V4 = """
CREATE TABLE IF NOT EXISTS gap_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_key TEXT NOT NULL UNIQUE,
    gap_id TEXT NOT NULL,
    source TEXT NOT NULL,
    exchange_code TEXT NOT NULL,
    interval_seconds INTEGER NOT NULL,
    start_utc TEXT NOT NULL,
    end_utc TEXT NOT NULL,
    from_state TEXT,
    to_state TEXT NOT NULL,
    cause TEXT NOT NULL,
    evidence_id TEXT NOT NULL,
    observed_at_utc TEXT NOT NULL,
    attempt_count INTEGER NOT NULL,
    recovery_cutoff_utc TEXT
);
CREATE INDEX IF NOT EXISTS gap_events_history ON gap_events(gap_id,event_id);
CREATE TABLE IF NOT EXISTS gap_current (
    gap_id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    exchange_code TEXT NOT NULL,
    interval_seconds INTEGER NOT NULL,
    start_utc TEXT NOT NULL,
    end_utc TEXT NOT NULL,
    state TEXT NOT NULL,
    attempt_count INTEGER NOT NULL,
    first_seen_utc TEXT NOT NULL,
    last_seen_utc TEXT NOT NULL,
    recovery_cutoff_utc TEXT,
    cause TEXT NOT NULL,
    evidence_id TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS gap_current_state_time
ON gap_current(state,start_utc,exchange_code);
CREATE TABLE IF NOT EXISTS universe_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    canonical_exchange_sha256 TEXT NOT NULL,
    canonical_yahoo_sha256 TEXT NOT NULL,
    effective_from TEXT NOT NULL,
    effective_to TEXT NOT NULL,
    retrieved_at_utc TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS universe_members (
    snapshot_id TEXT NOT NULL REFERENCES universe_snapshots(snapshot_id),
    exchange_code TEXT NOT NULL,
    yahoo_alias TEXT NOT NULL,
    PRIMARY KEY(snapshot_id,exchange_code),
    UNIQUE(snapshot_id,yahoo_alias)
);
CREATE TABLE IF NOT EXISTS universe_activations (
    activation_id TEXT PRIMARY KEY,
    snapshot_id TEXT NOT NULL REFERENCES universe_snapshots(snapshot_id),
    activated_at_utc TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS universe_activations_time
ON universe_activations(activated_at_utc,activation_id);
"""

MIGRATION_V5 = """
CREATE TABLE IF NOT EXISTS collector_runs (
    run_id TEXT PRIMARY KEY,
    session_date TEXT NOT NULL,
    calendar_snapshot_id TEXT NOT NULL,
    universe_snapshot_id TEXT NOT NULL,
    attempted_at_utc TEXT NOT NULL,
    completed_at_utc TEXT,
    status TEXT NOT NULL CHECK (status IN (
        'started','succeeded','skipped-closed','skipped-pre-close',
        'failed-unverified-calendar','failed','interrupted'
    )),
    reason TEXT NOT NULL,
    request_count INTEGER NOT NULL DEFAULT 0 CHECK (request_count >= 0),
    successful_symbols INTEGER NOT NULL DEFAULT 0 CHECK (successful_symbols >= 0),
    denied_symbols INTEGER NOT NULL DEFAULT 0 CHECK (denied_symbols >= 0),
    recovered_gaps INTEGER NOT NULL DEFAULT 0 CHECK (recovered_gaps >= 0),
    unresolved_gaps INTEGER NOT NULL DEFAULT 0 CHECK (unresolved_gaps >= 0),
    partition_count INTEGER NOT NULL DEFAULT 0 CHECK (partition_count >= 0),
    quick_check TEXT,
    health_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS collector_runs_attempted
ON collector_runs(attempted_at_utc,run_id);
CREATE INDEX IF NOT EXISTS collector_runs_status_attempted
ON collector_runs(status,attempted_at_utc,run_id);
"""

MIGRATIONS = {
    1: MIGRATION_V1,
    2: MIGRATION_V2,
    3: MIGRATION_V3,
    4: MIGRATION_V4,
    5: MIGRATION_V5,
}


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
        self._connection.execute("PRAGMA busy_timeout=5000")
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

    def gap_current_row(self, gap_id: str) -> dict[str, object] | None:
        row = self._connection.execute(
            "SELECT * FROM gap_current WHERE gap_id=?", (gap_id,)
        ).fetchone()
        return None if row is None else {str(key): row[key] for key in row.keys()}

    def gap_current_rows(
        self, states: frozenset[str] | None = None
    ) -> tuple[dict[str, object], ...]:
        if states:
            placeholders = ",".join("?" for _ in states)
            rows = self._connection.execute(
                f"SELECT * FROM gap_current WHERE state IN ({placeholders}) "
                "ORDER BY start_utc,exchange_code,gap_id",
                tuple(sorted(states)),
            ).fetchall()
        else:
            rows = self._connection.execute(
                "SELECT * FROM gap_current ORDER BY start_utc,exchange_code,gap_id"
            ).fetchall()
        return tuple({str(key): row[key] for key in row.keys()} for row in rows)

    def gap_event_rows(self, gap_id: str) -> tuple[dict[str, object], ...]:
        rows = self._connection.execute(
            "SELECT * FROM gap_events WHERE gap_id=? ORDER BY event_id", (gap_id,)
        ).fetchall()
        return tuple({str(key): row[key] for key in row.keys()} for row in rows)

    def gap_event_count(self) -> int:
        return self.integer_scalar("SELECT COUNT(*) FROM gap_events")

    def active_universe_members(self) -> tuple[tuple[str, str], ...]:
        row = self._connection.execute(
            "SELECT snapshot_id FROM universe_activations "
            "ORDER BY activated_at_utc DESC,activation_id DESC LIMIT 1"
        ).fetchone()
        if row is None:
            return ()
        members = self._connection.execute(
            "SELECT exchange_code,yahoo_alias FROM universe_members WHERE snapshot_id=? "
            "ORDER BY exchange_code",
            (str(row["snapshot_id"]),),
        ).fetchall()
        return tuple((str(item["exchange_code"]), str(item["yahoo_alias"])) for item in members)

    def active_universe_snapshot_id(self) -> str | None:
        row = self._connection.execute(
            "SELECT snapshot_id FROM universe_activations "
            "ORDER BY activated_at_utc DESC,activation_id DESC LIMIT 1"
        ).fetchone()
        return None if row is None else str(row["snapshot_id"])

    def universe_activation_count(self) -> int:
        return self.integer_scalar("SELECT COUNT(*) FROM universe_activations")

    def universe_membership_count(self, snapshot_id: str) -> int:
        return self.integer_scalar(
            "SELECT COUNT(*) FROM universe_members WHERE snapshot_id=?", (snapshot_id,)
        )

    @staticmethod
    def _run_row(row: sqlite3.Row | None) -> dict[str, object] | None:
        if row is None:
            return None
        result = {str(key): row[key] for key in row.keys()}
        try:
            result["health"] = json.loads(str(result.pop("health_json")))
        except json.JSONDecodeError as error:
            raise StoreError("collector run has malformed health JSON") from error
        return result

    def latest_collector_run(self) -> dict[str, object] | None:
        row = self._connection.execute(
            "SELECT * FROM collector_runs ORDER BY attempted_at_utc DESC,run_id DESC LIMIT 1"
        ).fetchone()
        return self._run_row(row)

    def collector_run_status(self) -> dict[str, dict[str, object] | None]:
        def latest(where: str = "1=1") -> dict[str, object] | None:
            row = self._connection.execute(
                "SELECT * FROM collector_runs WHERE "
                f"{where} ORDER BY attempted_at_utc DESC,run_id DESC LIMIT 1"
            ).fetchone()
            return self._run_row(row)

        return {
            "latest": latest(),
            "last_attempt": latest(),
            "last_success": latest("status IN ('succeeded','skipped-closed','skipped-pre-close')"),
            "last_completed_cycle": latest("status='succeeded'"),
        }


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
        for version in range(current + 1, SCHEMA_VERSION + 1):
            migration = MIGRATIONS[version]
            checksum = hashlib.sha256(migration.encode()).hexdigest()
            self._connection.execute("BEGIN EXCLUSIVE")
            try:
                for statement in _migration_statements(migration):
                    self._connection.execute(statement)
                self._connection.execute(
                    "INSERT INTO schema_migrations(version, applied_at_utc, migration_sha256) "
                    "VALUES (?, ?, ?)",
                    (version, _utc_text(datetime.now(UTC)), checksum),
                )
                self._connection.execute(f"PRAGMA user_version={version}")
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

    def provider_runtime(self, source: str) -> StoredProviderRuntime:
        row = self._connection.execute(
            "SELECT * FROM provider_runtime_state WHERE source=?", (source,)
        ).fetchone()
        if row is None:
            return StoredProviderRuntime(source, None, None, 0, None, None)
        cooldown = row["cooldown_until_utc"]
        circuit = row["circuit_open_until_utc"]
        paused_until = row["paused_until_utc"]
        return StoredProviderRuntime(
            source=str(row["source"]),
            cooldown_until_utc=(_from_utc_text(str(cooldown)) if cooldown is not None else None),
            circuit_open_until_utc=(_from_utc_text(str(circuit)) if circuit is not None else None),
            throttle_streak=int(row["throttle_streak"]),
            paused_reason=(str(row["paused_reason"]) if row["paused_reason"] is not None else None),
            paused_until_utc=(
                _from_utc_text(str(paused_until)) if paused_until is not None else None
            ),
        )

    def save_provider_runtime(
        self,
        source: str,
        *,
        cooldown_until_utc: datetime | None,
        circuit_open_until_utc: datetime | None,
        throttle_streak: int,
        paused_reason: str | None,
        paused_until_utc: datetime | None,
        updated_at_utc: datetime,
    ) -> None:
        if throttle_streak < 0:
            raise ValueError("throttle_streak must be nonnegative")
        for value in (
            cooldown_until_utc,
            circuit_open_until_utc,
            paused_until_utc,
            updated_at_utc,
        ):
            if value is not None:
                _utc_text(value)
        self._connection.execute(
            "INSERT INTO provider_runtime_state "
            "(source,cooldown_until_utc,circuit_open_until_utc,throttle_streak,"
            "paused_reason,updated_at_utc,paused_until_utc) VALUES (?,?,?,?,?,?,?) "
            "ON CONFLICT(source) DO UPDATE SET "
            "cooldown_until_utc=excluded.cooldown_until_utc,"
            "circuit_open_until_utc=excluded.circuit_open_until_utc,"
            "throttle_streak=excluded.throttle_streak,"
            "paused_reason=excluded.paused_reason,updated_at_utc=excluded.updated_at_utc,"
            "paused_until_utc=excluded.paused_until_utc",
            (
                source,
                _utc_text(cooldown_until_utc) if cooldown_until_utc is not None else None,
                (_utc_text(circuit_open_until_utc) if circuit_open_until_utc is not None else None),
                throttle_streak,
                paused_reason,
                _utc_text(updated_at_utc),
                _utc_text(paused_until_utc) if paused_until_utc is not None else None,
            ),
        )

    def start_collector_run(
        self,
        run_id: str,
        *,
        session_date: date,
        attempted_at_utc: datetime,
        calendar_snapshot_id: str,
        universe_snapshot_id: str,
    ) -> None:
        if not run_id or not calendar_snapshot_id or not universe_snapshot_id:
            raise ValueError("run and snapshot identities are required")
        self._connection.execute(
            "INSERT INTO collector_runs "
            "(run_id,session_date,calendar_snapshot_id,universe_snapshot_id,"
            "attempted_at_utc,status,reason) VALUES (?,?,?,?,?,'started','started')",
            (
                run_id,
                session_date.isoformat(),
                calendar_snapshot_id,
                universe_snapshot_id,
                _utc_text(attempted_at_utc),
            ),
        )

    def finish_collector_run(
        self,
        run_id: str,
        *,
        status: str,
        reason: str,
        completed_at_utc: datetime,
        request_count: int = 0,
        successful_symbols: int = 0,
        denied_symbols: int = 0,
        recovered_gaps: int = 0,
        unresolved_gaps: int = 0,
        partition_count: int = 0,
        quick_check: str | None = None,
        health: dict[str, object] | None = None,
    ) -> None:
        if status == "started":
            raise ValueError("a finished run cannot remain started")
        counts = (
            request_count,
            successful_symbols,
            denied_symbols,
            recovered_gaps,
            unresolved_gaps,
            partition_count,
        )
        if any(item < 0 for item in counts):
            raise ValueError("run accounting cannot be negative")
        encoded_health = json.dumps(health or {}, sort_keys=True, separators=(",", ":"))
        cursor = self._connection.execute(
            "UPDATE collector_runs SET completed_at_utc=?,status=?,reason=?,request_count=?,"
            "successful_symbols=?,denied_symbols=?,recovered_gaps=?,unresolved_gaps=?,"
            "partition_count=?,quick_check=?,health_json=? "
            "WHERE run_id=? AND status='started'",
            (
                _utc_text(completed_at_utc),
                status,
                reason[:500],
                *counts,
                quick_check,
                encoded_health,
                run_id,
            ),
        )
        if cursor.rowcount != 1:
            raise StoreError(f"collector run {run_id!r} is not in started state")

    def reconcile_started_runs(self, observed_at_utc: datetime) -> int:
        cursor = self._connection.execute(
            "UPDATE collector_runs SET completed_at_utc=?,status='interrupted',"
            "reason='process-exited-before-terminal-status' WHERE status='started'",
            (_utc_text(observed_at_utc),),
        )
        return cursor.rowcount

    def reserve_request(
        self,
        request_id: str,
        source: str,
        provider_symbol: str,
        attempted_at_utc: datetime,
        *,
        hourly_cap: int,
        session_cap: int,
    ) -> RequestReservation:
        if not request_id or hourly_cap <= 0 or session_cap <= 0:
            raise ValueError("request_id and positive request caps are required")
        _utc_text(attempted_at_utc)
        epoch_us = int(attempted_at_utc.timestamp() * 1_000_000)
        hour_cutoff_us = epoch_us - 3_600_000_000
        session_date = attempted_at_utc.astimezone(_ISTANBUL).date().isoformat()
        self._connection.execute("BEGIN IMMEDIATE")
        try:
            existing = self._connection.execute(
                "SELECT attempt_id FROM request_attempts WHERE request_id=?", (request_id,)
            ).fetchone()
            if existing is not None:
                self._connection.rollback()
                return RequestReservation(False, "duplicate-request-id")
            runtime = self.provider_runtime(source)
            if runtime.paused_reason is not None:
                if runtime.paused_until_utc is None or attempted_at_utc < runtime.paused_until_utc:
                    self._connection.rollback()
                    return RequestReservation(False, "provider-paused")
                self._connection.execute(
                    "UPDATE provider_runtime_state SET paused_reason=NULL,"
                    "paused_until_utc=NULL,updated_at_utc=? WHERE source=?",
                    (_utc_text(attempted_at_utc), source),
                )
            if (
                runtime.circuit_open_until_utc is not None
                and attempted_at_utc < runtime.circuit_open_until_utc
            ):
                self._connection.rollback()
                return RequestReservation(False, "provider-circuit-open")
            if (
                runtime.cooldown_until_utc is not None
                and attempted_at_utc < runtime.cooldown_until_utc
            ):
                self._connection.rollback()
                return RequestReservation(False, "provider-cooldown")
            hourly = int(
                self._connection.execute(
                    "SELECT COUNT(*) FROM request_attempts WHERE source=? "
                    "AND attempted_at_epoch_us>=? AND attempted_at_epoch_us<=?",
                    (source, hour_cutoff_us, epoch_us),
                ).fetchone()[0]
            )
            if hourly >= hourly_cap:
                self._connection.rollback()
                return RequestReservation(False, "rolling-hour-cap")
            session = int(
                self._connection.execute(
                    "SELECT COUNT(*) FROM request_attempts WHERE source=? AND session_date=?",
                    (source, session_date),
                ).fetchone()[0]
            )
            if session >= session_cap:
                self._connection.rollback()
                return RequestReservation(False, "exchange-session-cap")
            cursor = self._connection.execute(
                "INSERT INTO request_attempts "
                "(request_id,source,provider_symbol,attempted_at_utc,attempted_at_epoch_us,"
                "session_date,state) VALUES (?,?,?,?,?,?,'started')",
                (
                    request_id,
                    source,
                    provider_symbol,
                    _utc_text(attempted_at_utc),
                    epoch_us,
                    session_date,
                ),
            )
            if cursor.lastrowid is None:
                raise StoreError("request reservation did not return an attempt identifier")
            attempt_id = int(cursor.lastrowid)
            self._connection.commit()
            return RequestReservation(True, "allowed", attempt_id)
        except BaseException:
            self._connection.rollback()
            raise

    def complete_request(
        self, request_id: str, outcome_code: str, completed_at_utc: datetime
    ) -> None:
        _utc_text(completed_at_utc)
        cursor = self._connection.execute(
            "UPDATE request_attempts SET state='completed',outcome_code=?,completed_at_utc=? "
            "WHERE request_id=? AND state='started'",
            (outcome_code, _utc_text(completed_at_utc), request_id),
        )
        if cursor.rowcount != 1:
            raise StoreError(f"request {request_id!r} is not in started state")

    def cancel_request(self, request_id: str, completed_at_utc: datetime) -> None:
        _utc_text(completed_at_utc)
        cursor = self._connection.execute(
            "UPDATE request_attempts SET state='cancelled',outcome_code='cancelled',"
            "completed_at_utc=? WHERE request_id=? AND state='started'",
            (_utc_text(completed_at_utc), request_id),
        )
        if cursor.rowcount != 1:
            raise StoreError(f"request {request_id!r} is not in started state")

    def request_attempt_count(self) -> int:
        return self.integer_scalar("SELECT COUNT(*) FROM request_attempts")

    def request_attempt_state(self, attempt_id: int) -> str:
        row = self._connection.execute(
            "SELECT state FROM request_attempts WHERE attempt_id=?", (attempt_id,)
        ).fetchone()
        if row is None:
            raise KeyError(attempt_id)
        return str(row["state"])

    def request_provider_symbol(self, request_id: str) -> str:
        row = self._connection.execute(
            "SELECT provider_symbol FROM request_attempts WHERE request_id=?", (request_id,)
        ).fetchone()
        if row is None:
            raise KeyError(request_id)
        return str(row["provider_symbol"])

    def outcome_count_since(self, source: str, outcome_code: str, since_utc: datetime) -> int:
        _utc_text(since_utc)
        cutoff_us = int(since_utc.timestamp() * 1_000_000)
        return self.integer_scalar(
            "SELECT COUNT(*) FROM request_attempts WHERE source=? AND outcome_code=? "
            "AND attempted_at_epoch_us>=?",
            (source, outcome_code, cutoff_us),
        )

    def record_schema_drift(
        self, source: str, signature: str, provider_symbol: str, observed_at_utc: datetime
    ) -> int:
        _utc_text(observed_at_utc)
        self._connection.execute(
            "INSERT OR IGNORE INTO provider_schema_drift VALUES (?,?,?,?)",
            (source, signature, provider_symbol, _utc_text(observed_at_utc)),
        )
        return self.integer_scalar(
            "SELECT COUNT(*) FROM provider_schema_drift WHERE source=? AND signature=?",
            (source, signature),
        )

    def record_gap_event(
        self,
        *,
        gap_id: str,
        source: str,
        exchange_code: str,
        interval_seconds: int,
        start_utc: datetime,
        end_utc: datetime,
        to_state: str,
        cause: str,
        evidence_id: str,
        observed_at_utc: datetime,
        recovery_cutoff_utc: datetime | None,
        increment_attempt: bool,
    ) -> bool:
        if not gap_id or not source or not exchange_code.endswith(".E"):
            raise ValueError("gap identity is required")
        for instant in (start_utc, end_utc, observed_at_utc, recovery_cutoff_utc):
            if instant is not None:
                _utc_text(instant)
        if end_utc <= start_utc or interval_seconds != 60:
            raise ValueError("one-minute non-empty gap range is required")
        self._connection.execute("BEGIN IMMEDIATE")
        try:
            current = self._connection.execute(
                "SELECT * FROM gap_current WHERE gap_id=?", (gap_id,)
            ).fetchone()
            from_state = str(current["state"]) if current is not None else None
            current_attempts = int(current["attempt_count"]) if current is not None else 0
            if increment_attempt and current is None:
                raise StoreError("cannot record an attempt before gap discovery")
            attempts = current_attempts + int(increment_attempt)
            event_identity = "|".join(
                (
                    gap_id,
                    from_state or "",
                    to_state,
                    cause,
                    evidence_id,
                    _utc_text(observed_at_utc),
                    str(attempts),
                )
            )
            event_key = hashlib.sha256(event_identity.encode()).hexdigest()
            cursor = self._connection.execute(
                "INSERT OR IGNORE INTO gap_events "
                "(event_key,gap_id,source,exchange_code,interval_seconds,start_utc,end_utc,"
                "from_state,to_state,cause,evidence_id,observed_at_utc,attempt_count,"
                "recovery_cutoff_utc) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    event_key,
                    gap_id,
                    source,
                    exchange_code,
                    interval_seconds,
                    _utc_text(start_utc),
                    _utc_text(end_utc),
                    from_state,
                    to_state,
                    cause,
                    evidence_id,
                    _utc_text(observed_at_utc),
                    attempts,
                    (_utc_text(recovery_cutoff_utc) if recovery_cutoff_utc is not None else None),
                ),
            )
            if cursor.rowcount == 0:
                self._connection.rollback()
                return False
            first_seen = (
                str(current["first_seen_utc"])
                if current is not None
                else _utc_text(observed_at_utc)
            )
            self._connection.execute(
                "INSERT INTO gap_current "
                "(gap_id,source,exchange_code,interval_seconds,start_utc,end_utc,state,"
                "attempt_count,first_seen_utc,last_seen_utc,recovery_cutoff_utc,cause,evidence_id) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(gap_id) DO UPDATE SET state=excluded.state,"
                "attempt_count=excluded.attempt_count,last_seen_utc=excluded.last_seen_utc,"
                "recovery_cutoff_utc=excluded.recovery_cutoff_utc,cause=excluded.cause,"
                "evidence_id=excluded.evidence_id",
                (
                    gap_id,
                    source,
                    exchange_code,
                    interval_seconds,
                    _utc_text(start_utc),
                    _utc_text(end_utc),
                    to_state,
                    attempts,
                    first_seen,
                    _utc_text(observed_at_utc),
                    (_utc_text(recovery_cutoff_utc) if recovery_cutoff_utc is not None else None),
                    cause,
                    evidence_id,
                ),
            )
            self._connection.commit()
            return True
        except BaseException:
            self._connection.rollback()
            raise

    def activate_universe(
        self,
        snapshot: UniverseSnapshot,
        *,
        activated_at_utc: datetime,
        activation_id: str | None = None,
    ) -> str:
        _utc_text(activated_at_utc)
        resolved_activation = (
            activation_id
            or hashlib.sha256(
                f"{snapshot.snapshot_id}|{_utc_text(activated_at_utc)}".encode()
            ).hexdigest()
        )
        self._connection.execute("BEGIN IMMEDIATE")
        try:
            self._connection.execute(
                "INSERT OR IGNORE INTO universe_snapshots VALUES (?,?,?,?,?,?)",
                (
                    snapshot.snapshot_id,
                    snapshot.canonical_exchange_sha256,
                    snapshot.canonical_yahoo_sha256,
                    snapshot.effective_from.isoformat(),
                    snapshot.effective_to.isoformat(),
                    _utc_text(snapshot.retrieved_at_utc),
                ),
            )
            stored = self._connection.execute(
                "SELECT * FROM universe_snapshots WHERE snapshot_id=?", (snapshot.snapshot_id,)
            ).fetchone()
            assert stored is not None
            if (
                str(stored["canonical_exchange_sha256"]) != snapshot.canonical_exchange_sha256
                or str(stored["canonical_yahoo_sha256"]) != snapshot.canonical_yahoo_sha256
            ):
                raise StoreError("universe snapshot id conflicts with stored checksums")
            for member in snapshot.members:
                self._connection.execute(
                    "INSERT OR IGNORE INTO universe_members VALUES (?,?,?)",
                    (
                        snapshot.snapshot_id,
                        member.exchange_code,
                        member.alias_for("yahoo_chart"),
                    ),
                )
            observed_count = int(
                self._connection.execute(
                    "SELECT COUNT(*) FROM universe_members WHERE snapshot_id=?",
                    (snapshot.snapshot_id,),
                ).fetchone()[0]
            )
            if observed_count != len(snapshot.members):
                raise StoreError("stored universe membership does not match immutable snapshot")
            self._connection.execute(
                "INSERT OR IGNORE INTO universe_activations VALUES (?,?,?)",
                (resolved_activation, snapshot.snapshot_id, _utc_text(activated_at_utc)),
            )
            self._connection.commit()
            return resolved_activation
        except BaseException:
            self._connection.rollback()
            raise

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
            "INSERT INTO compaction_manifests "
            "(attempt_id,source,interval_seconds,session_date,state,temp_path,created_at_utc) "
            "VALUES (?,?,?,?,'writing',?,?)",
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
                (
                    _from_utc_text(str(row["temp_removed_at_utc"]))
                    if row["temp_removed_at_utc"] is not None
                    else None
                ),
                (
                    str(row["temp_removal_reason"])
                    if row["temp_removal_reason"] is not None
                    else None
                ),
            )
            for row in rows
        )

    def mark_manifest_temp_removed(
        self, attempt_id: str, removed_at_utc: datetime, reason: str
    ) -> None:
        _utc_text(removed_at_utc)
        cursor = self._connection.execute(
            "UPDATE compaction_manifests SET temp_removed_at_utc=?,temp_removal_reason=? "
            "WHERE attempt_id=? AND state='failed' AND temp_removed_at_utc IS NULL",
            (_utc_text(removed_at_utc), reason, attempt_id),
        )
        if cursor.rowcount != 1:
            raise StoreError(f"failed manifest {attempt_id!r} cannot record temp removal")

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
