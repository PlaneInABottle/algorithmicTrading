from __future__ import annotations

import asyncio
import hashlib
import json
import re
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Protocol, cast

import httpx

from .models import (
    BatchFetchResult,
    FetchResult,
    MinuteBar,
    OutcomeCode,
    ProviderMetadata,
    ProviderOutcome,
    RawHttpResponse,
    RequestWindow,
    RequestWindowSemantics,
)

YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
YAHOO_SYMBOL_PATTERN = re.compile(r"^[A-Z0-9]+\.IS$")
REQUIRED_METADATA = (
    "symbol",
    "currency",
    "exchangeTimezoneName",
    "gmtoffset",
    "dataGranularity",
)
QUOTE_FIELDS = ("open", "high", "low", "close", "volume")


class OneMinuteProvider(Protocol):
    async def fetch_many_one_day(
        self, symbols: Sequence[str], *, max_concurrency: int
    ) -> BatchFetchResult: ...


def _result(
    response: RawHttpResponse,
    *outcomes: ProviderOutcome,
    metadata: ProviderMetadata | None = None,
    bars: tuple[MinuteBar, ...] = (),
) -> FetchResult:
    return FetchResult(
        source=response.source,
        symbol=response.symbol,
        interval=response.interval,
        request_window=response.request_window,
        request_window_semantics=response.request_window_semantics,
        retrieved_at_utc=response.retrieved_at_utc,
        status_code=response.status_code,
        attempts=response.attempts,
        response_sha256=hashlib.sha256(response.body).hexdigest(),
        metadata=metadata,
        bars=bars,
        outcomes=outcomes,
    )


def _header(headers: Any, name: str) -> str | None:
    wanted = name.casefold()
    for key, value in headers.items():
        if str(key).casefold() == wanted:
            return str(value)
    return None


def _error_description(body: bytes) -> str:
    try:
        payload = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return ""
    if not isinstance(payload, dict):
        return ""
    chart = payload.get("chart")
    if not isinstance(chart, dict):
        return ""
    error = chart.get("error")
    if not isinstance(error, dict):
        return ""
    return str(error.get("description", ""))


def _terminal_http_result(response: RawHttpResponse) -> FetchResult | None:
    if response.status_code == 429:
        return _result(
            response,
            ProviderOutcome(
                OutcomeCode.THROTTLED,
                "provider returned HTTP 429",
                retry_after=_header(response.headers, "Retry-After"),
            ),
        )
    if 500 <= response.status_code <= 599:
        return _result(
            response,
            ProviderOutcome(
                OutcomeCode.SERVER_ERROR, f"provider returned HTTP {response.status_code}"
            ),
        )
    if response.status_code == 422:
        description = _error_description(response.body)
        if "last 30 days" in description or "8 days" in description:
            return _result(response, ProviderOutcome(OutcomeCode.RETENTION_LIMIT, description))
    if response.status_code != 200:
        return _result(
            response,
            ProviderOutcome(
                OutcomeCode.CLIENT_ERROR, f"provider returned HTTP {response.status_code}"
            ),
        )
    return None


def _metadata(meta: dict[str, Any], response: RawHttpResponse) -> ProviderMetadata | FetchResult:
    missing = [field for field in REQUIRED_METADATA if field not in meta or meta[field] is None]
    if missing:
        return _result(
            response,
            ProviderOutcome(
                OutcomeCode.MISSING_METADATA,
                f"missing required metadata: {','.join(missing)}",
            ),
        )
    if str(meta["symbol"]) != response.symbol:
        return _result(
            response,
            ProviderOutcome(
                OutcomeCode.SYMBOL_MISMATCH,
                f"response symbol {meta['symbol']!s} does not match {response.symbol}",
            ),
        )
    if str(meta["dataGranularity"]) != "1m":
        return _result(
            response,
            ProviderOutcome(
                OutcomeCode.UNEXPECTED_GRANULARITY,
                f"expected 1m, received {meta['dataGranularity']!s}",
            ),
        )
    if str(meta["exchangeTimezoneName"]) != "Europe/Istanbul" or int(meta["gmtoffset"]) != 10800:
        return _result(
            response,
            ProviderOutcome(
                OutcomeCode.UNEXPECTED_TIMEZONE,
                "expected Europe/Istanbul with UTC+03:00 provider metadata",
            ),
        )
    if str(meta["currency"]) != "TRY":
        return _result(
            response,
            ProviderOutcome(
                OutcomeCode.UNEXPECTED_CURRENCY,
                f"expected TRY, received {meta['currency']!s}",
            ),
        )
    checksum = hashlib.sha256(
        json.dumps(meta, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()
    return ProviderMetadata(
        symbol=str(meta["symbol"]),
        currency=str(meta["currency"]),
        exchange_name=str(meta["exchangeName"]) if meta.get("exchangeName") is not None else None,
        full_exchange_name=(
            str(meta["fullExchangeName"]) if meta.get("fullExchangeName") is not None else None
        ),
        instrument_type=(
            str(meta["instrumentType"]) if meta.get("instrumentType") is not None else None
        ),
        first_trade_date=(
            int(meta["firstTradeDate"]) if meta.get("firstTradeDate") is not None else None
        ),
        regular_market_time=(
            int(meta["regularMarketTime"]) if meta.get("regularMarketTime") is not None else None
        ),
        gmt_offset_seconds=int(meta["gmtoffset"]),
        timezone_abbreviation=(str(meta["timezone"]) if meta.get("timezone") is not None else None),
        exchange_timezone_name=str(meta["exchangeTimezoneName"]),
        data_granularity=str(meta["dataGranularity"]),
        raw_metadata_sha256=checksum,
    )


def _decimal(value: Any) -> Decimal:
    if isinstance(value, bool):
        raise InvalidOperation
    return value if isinstance(value, Decimal) else Decimal(str(value))


def parse_chart_response(response: RawHttpResponse) -> FetchResult:
    """Parse one Yahoo Chart response without guessing, aligning, or filling source rows."""

    terminal = _terminal_http_result(response)
    if terminal is not None:
        return terminal
    try:
        payload = json.loads(response.body, parse_float=Decimal)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return _result(
            response, ProviderOutcome(OutcomeCode.MALFORMED_JSON, "response is not valid JSON")
        )
    if not isinstance(payload, dict) or not isinstance(payload.get("chart"), dict):
        return _result(
            response, ProviderOutcome(OutcomeCode.MALFORMED_JSON, "missing chart object")
        )
    chart = cast(dict[str, Any], payload["chart"])
    results = chart.get("result")
    if not isinstance(results, list) or not results:
        message = _error_description(response.body) or "provider returned no chart result"
        return _result(response, ProviderOutcome(OutcomeCode.SOURCE_EMPTY, message))
    if not isinstance(results[0], dict):
        return _result(
            response, ProviderOutcome(OutcomeCode.MALFORMED_JSON, "chart result is not an object")
        )
    result = cast(dict[str, Any], results[0])
    raw_meta = result.get("meta")
    if not isinstance(raw_meta, dict):
        return _result(
            response, ProviderOutcome(OutcomeCode.MISSING_METADATA, "missing required metadata")
        )
    metadata = _metadata(cast(dict[str, Any], raw_meta), response)
    if isinstance(metadata, FetchResult):
        return metadata

    timestamps = result.get("timestamp")
    indicators = result.get("indicators")
    if not isinstance(timestamps, list) or not isinstance(indicators, dict):
        return _result(
            response,
            ProviderOutcome(
                OutcomeCode.SOURCE_EMPTY, "provider returned no timestamp/quote arrays"
            ),
            metadata=metadata,
        )
    quote_list = indicators.get("quote")
    if not isinstance(quote_list, list) or not quote_list or not isinstance(quote_list[0], dict):
        return _result(
            response,
            ProviderOutcome(OutcomeCode.SOURCE_EMPTY, "provider returned no quote array"),
            metadata=metadata,
        )
    quote = cast(dict[str, Any], quote_list[0])
    arrays = [quote.get(field) for field in QUOTE_FIELDS]
    if any(not isinstance(values, list) for values in arrays):
        return _result(
            response,
            ProviderOutcome(OutcomeCode.ARRAY_LENGTH_MISMATCH, "missing OHLCV array"),
            metadata=metadata,
        )
    typed_arrays = cast(list[list[Any]], arrays)
    lengths = {len(timestamps), *(len(values) for values in typed_arrays)}
    if len(lengths) != 1:
        return _result(
            response,
            ProviderOutcome(
                OutcomeCode.ARRAY_LENGTH_MISMATCH,
                f"timestamp/OHLCV array lengths differ: {sorted(lengths)}",
            ),
            metadata=metadata,
        )

    bars: list[MinuteBar] = []
    outcomes: list[ProviderOutcome] = []
    for index, raw_timestamp_value in enumerate(timestamps):
        if isinstance(raw_timestamp_value, bool) or not isinstance(raw_timestamp_value, int):
            outcomes.append(
                ProviderOutcome(
                    OutcomeCode.INVALID_ROW, "timestamp is not an integer", row_index=index
                )
            )
            continue
        raw_timestamp = raw_timestamp_value
        values = [array[index] for array in typed_arrays]
        if all(value is None for value in values):
            outcomes.append(
                ProviderOutcome(
                    OutcomeCode.NULL_ROW,
                    "provider supplied a timestamped all-null OHLCV row",
                    raw_source_timestamp=raw_timestamp,
                    row_index=index,
                )
            )
            continue
        if any(value is None for value in values):
            outcomes.append(
                ProviderOutcome(
                    OutcomeCode.PARTIAL_ROW,
                    "provider supplied a partial OHLCV row",
                    raw_source_timestamp=raw_timestamp,
                    row_index=index,
                )
            )
            continue
        if raw_timestamp % 60 != 0:
            outcomes.append(
                ProviderOutcome(
                    OutcomeCode.NON_MINUTE_ALIGNED_PROVISIONAL_TAIL,
                    "complete source observation is not minute-aligned and was quarantined",
                    raw_source_timestamp=raw_timestamp,
                    row_index=index,
                )
            )
            continue
        try:
            open_, high, low, close = (_decimal(value) for value in values[:4])
            volume_decimal = _decimal(values[4])
            if not all(value.is_finite() for value in (open_, high, low, close, volume_decimal)):
                raise InvalidOperation
            if volume_decimal != volume_decimal.to_integral_value():
                raise InvalidOperation
            volume = int(volume_decimal)
        except (InvalidOperation, ValueError, TypeError):
            outcomes.append(
                ProviderOutcome(
                    OutcomeCode.INVALID_ROW,
                    "OHLCV values are not valid finite numeric values",
                    raw_source_timestamp=raw_timestamp,
                    row_index=index,
                )
            )
            continue
        if volume < 0 or low > high or not (low <= open_ <= high) or not (low <= close <= high):
            outcomes.append(
                ProviderOutcome(
                    OutcomeCode.INVALID_ROW,
                    "OHLCV invariants failed",
                    raw_source_timestamp=raw_timestamp,
                    row_index=index,
                )
            )
            continue
        bars.append(
            MinuteBar(
                source=response.source,
                symbol=response.symbol,
                interval_seconds=60,
                bar_open_utc=datetime.fromtimestamp(raw_timestamp, tz=UTC),
                raw_source_timestamp=raw_timestamp,
                open=open_,
                high=high,
                low=low,
                close=close,
                volume=volume,
                observed_at_utc=response.retrieved_at_utc,
            )
        )

    if bars:
        outcomes.insert(0, ProviderOutcome(OutcomeCode.SUCCESS, f"accepted {len(bars)} bars"))
    else:
        outcomes.append(
            ProviderOutcome(
                OutcomeCode.SOURCE_EMPTY,
                "no complete aligned one-minute OHLCV observation was accepted",
            )
        )
    return _result(response, *outcomes, metadata=metadata, bars=tuple(bars))


def timeout_result(
    *,
    source: str,
    symbol: str,
    interval: str,
    request_window: RequestWindow,
    retrieved_at_utc: datetime,
    message: str,
    request_window_semantics: RequestWindowSemantics = RequestWindowSemantics.EXACT_UTC_WINDOW,
) -> FetchResult:
    return FetchResult(
        source=source,
        symbol=symbol,
        interval=interval,
        request_window=request_window,
        request_window_semantics=request_window_semantics,
        retrieved_at_utc=retrieved_at_utc,
        status_code=None,
        attempts=1,
        response_sha256=None,
        metadata=None,
        bars=(),
        outcomes=(ProviderOutcome(OutcomeCode.TIMEOUT, message),),
    )


def _transport_result(
    *,
    symbol: str,
    request_window: RequestWindow,
    retrieved_at_utc: datetime,
    message: str,
    request_window_semantics: RequestWindowSemantics,
) -> FetchResult:
    return FetchResult(
        source="yahoo_chart",
        symbol=symbol,
        interval="1m",
        request_window=request_window,
        request_window_semantics=request_window_semantics,
        retrieved_at_utc=retrieved_at_utc,
        status_code=None,
        attempts=1,
        response_sha256=None,
        metadata=None,
        bars=(),
        outcomes=(ProviderOutcome(OutcomeCode.TRANSPORT_ERROR, message),),
    )


class YahooChartProvider:
    """Unofficial personal-research adapter; no completeness, retention, or SLA claim."""

    def __init__(self, *, timeout_seconds: float = 15.0) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self._timeout_seconds = timeout_seconds

    async def fetch_many_one_day(
        self, symbols: Sequence[str], *, max_concurrency: int
    ) -> BatchFetchResult:
        ordered_symbols = tuple(symbols)
        if not ordered_symbols or len(set(ordered_symbols)) != len(ordered_symbols):
            raise ValueError("symbols must be non-empty and distinct")
        if any(not YAHOO_SYMBOL_PATTERN.fullmatch(symbol) for symbol in ordered_symbols):
            raise ValueError("every symbol must be an uppercase Yahoo .IS alias")
        if not 1 <= max_concurrency <= 2:
            raise ValueError("max_concurrency must be between one and two")

        semaphore = asyncio.Semaphore(max_concurrency)
        counter_lock = asyncio.Lock()
        active = 0
        maximum = 0

        async with httpx.AsyncClient(
            timeout=httpx.Timeout(self._timeout_seconds),
            follow_redirects=False,
            headers={
                "Accept": "application/json",
                "User-Agent": "bist-minute-collector-phase1/0.1 (personal research)",
            },
        ) as client:

            async def fetch(symbol: str) -> FetchResult:
                nonlocal active, maximum
                async with semaphore:
                    async with counter_lock:
                        active += 1
                        maximum = max(maximum, active)
                    try:
                        retrieved_at = datetime.now(UTC)
                        window = RequestWindow(retrieved_at - timedelta(days=1), retrieved_at)
                        try:
                            response = await client.get(
                                YAHOO_CHART_URL.format(symbol=symbol),
                                params={
                                    "range": "1d",
                                    "interval": "1m",
                                    "includePrePost": "false",
                                    "events": "div,splits",
                                },
                            )
                        except httpx.TimeoutException:
                            return timeout_result(
                                source="yahoo_chart",
                                symbol=symbol,
                                interval="1m",
                                request_window=window,
                                retrieved_at_utc=datetime.now(UTC),
                                message=(
                                    f"request exceeded {self._timeout_seconds:g} second timeout"
                                ),
                                request_window_semantics=(
                                    RequestWindowSemantics.PROVIDER_RANGE_1D_APPROXIMATE
                                ),
                            )
                        except httpx.RequestError as exc:
                            return _transport_result(
                                symbol=symbol,
                                request_window=window,
                                retrieved_at_utc=datetime.now(UTC),
                                message=f"{type(exc).__name__}: bounded request failed",
                                request_window_semantics=(
                                    RequestWindowSemantics.PROVIDER_RANGE_1D_APPROXIMATE
                                ),
                            )
                        raw = RawHttpResponse(
                            source="yahoo_chart",
                            symbol=symbol,
                            interval="1m",
                            request_window=window,
                            retrieved_at_utc=datetime.now(UTC),
                            status_code=response.status_code,
                            headers=response.headers,
                            body=response.content,
                            attempts=1,
                            request_window_semantics=(
                                RequestWindowSemantics.PROVIDER_RANGE_1D_APPROXIMATE
                            ),
                        )
                        return parse_chart_response(raw)
                    finally:
                        async with counter_lock:
                            active -= 1

            results = tuple(await asyncio.gather(*(fetch(symbol) for symbol in ordered_symbols)))
        return BatchFetchResult(
            symbols=ordered_symbols,
            results=results,
            request_count=len(ordered_symbols),
            max_observed_concurrency=maximum,
        )
