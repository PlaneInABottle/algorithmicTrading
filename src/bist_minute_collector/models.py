from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from enum import StrEnum


class OutcomeCode(StrEnum):
    """Typed facts observed while obtaining or parsing one provider response."""

    SUCCESS = "success"
    SOURCE_EMPTY = "source-empty"
    RETENTION_LIMIT = "retention-422"
    THROTTLED = "throttled-429"
    SERVER_ERROR = "server-error-5xx"
    CLIENT_ERROR = "client-error"
    TIMEOUT = "timeout"
    TRANSPORT_ERROR = "transport-error"
    MALFORMED_JSON = "malformed-json"
    ARRAY_LENGTH_MISMATCH = "array-length-mismatch"
    MISSING_METADATA = "missing-metadata"
    UNEXPECTED_GRANULARITY = "unexpected-granularity"
    UNEXPECTED_TIMEZONE = "unexpected-timezone"
    UNEXPECTED_CURRENCY = "unexpected-currency"
    SYMBOL_MISMATCH = "symbol-mismatch"
    PARTIAL_ROW = "partial-row"
    NULL_ROW = "null-row"
    NON_MINUTE_ALIGNED_PROVISIONAL_TAIL = "non-minute-aligned-provisional-tail"
    INVALID_ROW = "invalid-row"


class RequestWindowSemantics(StrEnum):
    """Whether the UTC request window is exact or a labeled provider approximation."""

    EXACT_UTC_WINDOW = "exact-utc-window"
    PROVIDER_RANGE_1D_APPROXIMATE = "provider-range-1d-approximate"


@dataclass(frozen=True, slots=True)
class RequestWindow:
    start_utc: datetime
    end_utc: datetime

    def __post_init__(self) -> None:
        for name, value in (("start_utc", self.start_utc), ("end_utc", self.end_utc)):
            if value.tzinfo is None or value.utcoffset() != timedelta(0):
                raise ValueError(f"{name} must be timezone-aware UTC")
        if self.end_utc < self.start_utc:
            raise ValueError("end_utc must not be before start_utc")

    @property
    def duration(self) -> timedelta:
        return self.end_utc - self.start_utc


@dataclass(frozen=True, slots=True)
class RawHttpResponse:
    source: str
    symbol: str
    interval: str
    request_window: RequestWindow
    retrieved_at_utc: datetime
    status_code: int
    headers: Mapping[str, str]
    body: bytes
    attempts: int
    request_window_semantics: RequestWindowSemantics = RequestWindowSemantics.EXACT_UTC_WINDOW


@dataclass(frozen=True, slots=True)
class ProviderMetadata:
    symbol: str
    currency: str
    exchange_name: str | None
    full_exchange_name: str | None
    instrument_type: str | None
    first_trade_date: int | None
    regular_market_time: int | None
    gmt_offset_seconds: int
    timezone_abbreviation: str | None
    exchange_timezone_name: str
    data_granularity: str
    raw_metadata_sha256: str


@dataclass(frozen=True, slots=True)
class MinuteBar:
    source: str
    symbol: str
    interval_seconds: int
    bar_open_utc: datetime
    raw_source_timestamp: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    observed_at_utc: datetime


@dataclass(frozen=True, slots=True)
class ProviderOutcome:
    code: OutcomeCode
    message: str
    raw_source_timestamp: int | None = None
    row_index: int | None = None
    retry_after: str | None = None


@dataclass(frozen=True, slots=True)
class FetchResult:
    source: str
    symbol: str
    interval: str
    request_window: RequestWindow
    request_window_semantics: RequestWindowSemantics
    retrieved_at_utc: datetime
    status_code: int | None
    attempts: int
    response_sha256: str | None
    metadata: ProviderMetadata | None
    bars: tuple[MinuteBar, ...]
    outcomes: tuple[ProviderOutcome, ...]

    @property
    def codes(self) -> tuple[OutcomeCode, ...]:
        return tuple(dict.fromkeys(outcome.code for outcome in self.outcomes))


@dataclass(frozen=True, slots=True)
class BatchFetchResult:
    symbols: tuple[str, ...]
    results: tuple[FetchResult, ...]
    request_count: int
    max_observed_concurrency: int
