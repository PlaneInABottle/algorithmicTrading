from __future__ import annotations

import asyncio
import copy
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx
import pytest

from bist_minute_collector.models import (
    OutcomeCode,
    RawHttpResponse,
    RequestWindow,
    RequestWindowSemantics,
)
from bist_minute_collector.provider import YahooChartProvider, parse_chart_response, timeout_result

ROOT = Path(__file__).parents[2]
FIXTURES = ROOT / "tests/fixtures/yahoo_chart"
RETRIEVED = datetime(2026, 8, 3, 14, 42, tzinfo=UTC)
WINDOW = RequestWindow(RETRIEVED - timedelta(days=1), RETRIEVED)


def _fixture(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def _payload() -> dict[str, Any]:
    return json.loads(_fixture("ASELS.IS-1d-1m-2026-08-03.sanitized.json"))  # type: ignore[no-any-return]


def _response(
    body: bytes,
    *,
    status: int = 200,
    headers: dict[str, str] | None = None,
    symbol: str = "ASELS.IS",
) -> RawHttpResponse:
    return RawHttpResponse(
        source="yahoo_chart",
        symbol=symbol,
        interval="1m",
        request_window=WINDOW,
        retrieved_at_utc=RETRIEVED,
        status_code=status,
        headers=headers or {},
        body=body,
        attempts=1,
    )


def _codes(result_codes: tuple[OutcomeCode, ...]) -> set[OutcomeCode]:
    return set(result_codes)


def test_complete_aligned_rows_parse_and_preserve_raw_timestamp_and_metadata() -> None:
    result = parse_chart_response(_response(_fixture("ASELS.IS-1d-1m-2026-08-03.sanitized.json")))

    assert OutcomeCode.SUCCESS in _codes(result.codes)
    assert len(result.bars) == 2
    first = result.bars[0]
    assert first.raw_source_timestamp == 1785740100
    assert first.bar_open_utc == datetime.fromtimestamp(1785740100, tz=UTC)
    assert first.open == Decimal("344.0")
    assert first.volume == 0  # An explicit complete provider row, not a synthetic empty bar.
    assert result.metadata is not None
    assert result.metadata.exchange_timezone_name == "Europe/Istanbul"
    assert result.metadata.data_granularity == "1m"
    assert result.response_sha256


def test_all_five_sanitized_success_fixtures_preserve_symbol_identity() -> None:
    names = sorted(FIXTURES.glob("*.IS-1d-1m-2026-08-03.sanitized.json"))

    assert len(names) == 5
    for path in names:
        symbol = path.name.split("-1d", maxsplit=1)[0]
        result = parse_chart_response(_response(path.read_bytes(), symbol=symbol))
        assert result.metadata is not None
        assert result.metadata.symbol == symbol
        assert result.bars


def test_null_row_and_nonaligned_tail_create_issues_not_bars() -> None:
    result = parse_chart_response(_response(_fixture("ASELS.IS-1d-1m-2026-08-03.sanitized.json")))

    assert OutcomeCode.NULL_ROW in _codes(result.codes)
    assert OutcomeCode.NON_MINUTE_ALIGNED_PROVISIONAL_TAIL in _codes(result.codes)
    assert {bar.raw_source_timestamp for bar in result.bars} == {1785740100, 1785740400}
    issue_timestamps = {issue.raw_source_timestamp for issue in result.outcomes}
    assert 1785740160 in issue_timestamps
    assert 1785767214 in issue_timestamps


def test_source_empty_does_not_create_zero_volume_or_synthetic_bar() -> None:
    payload = _payload()
    quote = payload["chart"]["result"][0]["indicators"]["quote"][0]
    payload["chart"]["result"][0]["timestamp"] = [1785740160]
    for field in ("open", "high", "low", "close", "volume"):
        quote[field] = [None]

    result = parse_chart_response(_response(json.dumps(payload).encode()))

    assert result.bars == ()
    assert OutcomeCode.SOURCE_EMPTY in _codes(result.codes)
    assert OutcomeCode.NULL_ROW in _codes(result.codes)


def test_partial_row_is_rejected_without_synthetic_bar() -> None:
    payload = _payload()
    payload["chart"]["result"][0]["indicators"]["quote"][0]["high"][1] = None

    result = parse_chart_response(_response(json.dumps(payload).encode()))

    assert OutcomeCode.PARTIAL_ROW in _codes(result.codes)
    assert 1785740400 not in {bar.raw_source_timestamp for bar in result.bars}


def test_array_length_mismatch_is_a_typed_terminal_outcome() -> None:
    payload = _payload()
    payload["chart"]["result"][0]["indicators"]["quote"][0]["volume"].pop()

    result = parse_chart_response(_response(json.dumps(payload).encode()))

    assert result.bars == ()
    assert result.codes == (OutcomeCode.ARRAY_LENGTH_MISMATCH,)


@pytest.mark.parametrize(
    "field", ["symbol", "currency", "exchangeTimezoneName", "gmtoffset", "dataGranularity"]
)
def test_missing_required_metadata_is_typed(field: str) -> None:
    payload = _payload()
    del payload["chart"]["result"][0]["meta"][field]

    result = parse_chart_response(_response(json.dumps(payload).encode()))

    assert result.bars == ()
    assert result.codes == (OutcomeCode.MISSING_METADATA,)


def test_unexpected_granularity_and_timezone_are_typed() -> None:
    granularity = _payload()
    granularity["chart"]["result"][0]["meta"]["dataGranularity"] = "5m"
    timezone = _payload()
    timezone["chart"]["result"][0]["meta"]["exchangeTimezoneName"] = "UTC"

    granularity_result = parse_chart_response(_response(json.dumps(granularity).encode()))
    timezone_result = parse_chart_response(_response(json.dumps(timezone).encode()))

    assert granularity_result.codes == (OutcomeCode.UNEXPECTED_GRANULARITY,)
    assert timezone_result.codes == (OutcomeCode.UNEXPECTED_TIMEZONE,)
    assert not granularity_result.bars
    assert not timezone_result.bars


def test_corrupted_fixture_bytes_are_malformed_json() -> None:
    result = parse_chart_response(_response(_fixture("corrupted.json")))

    assert result.codes == (OutcomeCode.MALFORMED_JSON,)
    assert not result.bars


def test_retention_422_is_not_misreported_as_empty_or_delisted() -> None:
    result = parse_chart_response(_response(_fixture("retention-422.sanitized.json"), status=422))

    assert result.codes == (OutcomeCode.RETENTION_LIMIT,)
    assert OutcomeCode.SOURCE_EMPTY not in result.codes
    assert "last 30 days" in result.outcomes[0].message


def test_per_request_eight_day_422_is_typed_as_retention_limit() -> None:
    result = parse_chart_response(
        _response(_fixture("retention-per-request-422.sanitized.json"), status=422)
    )

    assert result.codes == (OutcomeCode.RETENTION_LIMIT,)
    assert "8 days" in result.outcomes[0].message


@pytest.mark.parametrize("retry_after", [None, "120"])
def test_429_is_typed_with_optional_retry_after(retry_after: str | None) -> None:
    headers = {} if retry_after is None else {"Retry-After": retry_after}

    result = parse_chart_response(_response(b"", status=429, headers=headers))

    assert result.codes == (OutcomeCode.THROTTLED,)
    assert result.outcomes[0].retry_after == retry_after
    assert result.attempts == 1


@pytest.mark.parametrize("status", [500, 502, 503])
def test_5xx_is_typed_without_parsing_a_bar(status: int) -> None:
    result = parse_chart_response(_response(b"upstream failure", status=status))

    assert result.codes == (OutcomeCode.SERVER_ERROR,)
    assert not result.bars


def test_timeout_is_typed_and_has_one_attempt() -> None:
    result = timeout_result(
        source="yahoo_chart",
        symbol="ASELS.IS",
        interval="1m",
        request_window=WINDOW,
        retrieved_at_utc=RETRIEVED,
        message="bounded 15 second timeout",
    )

    assert result.codes == (OutcomeCode.TIMEOUT,)
    assert result.attempts == 1
    assert not result.bars


def test_parser_does_not_mutate_input_payload() -> None:
    payload = _payload()
    before = copy.deepcopy(payload)

    parse_chart_response(_response(json.dumps(payload).encode()))

    assert payload == before


def test_non_finite_json_number_is_rejected_as_invalid_row() -> None:
    payload = _payload()
    payload["chart"]["result"][0]["indicators"]["quote"][0]["open"][0] = float("inf")

    result = parse_chart_response(_response(json.dumps(payload).encode()))

    assert OutcomeCode.INVALID_ROW in result.codes
    assert 1785740100 not in {bar.raw_source_timestamp for bar in result.bars}


def test_fixture_requests_have_exact_window_semantics() -> None:
    result = parse_chart_response(_response(_fixture("ASELS.IS-1d-1m-2026-08-03.sanitized.json")))

    assert result.request_window_semantics is RequestWindowSemantics.EXACT_UTC_WINDOW


def test_historical_fetch_honors_exact_bounded_period_window() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            content=_fixture("ASELS.IS-1d-1m-2026-08-03.sanitized.json"),
            request=request,
        )

    window = RequestWindow(
        datetime(2026, 8, 3, 6, 40, tzinfo=UTC),
        datetime(2026, 8, 3, 15, 19, tzinfo=UTC),
    )
    result = asyncio.run(
        YahooChartProvider(transport=httpx.MockTransport(handler)).fetch_one("ASELS.IS", window)
    )

    assert len(requests) == 1
    query = requests[0].url.params
    assert query["period1"] == str(int(window.start_utc.timestamp()))
    assert query["period2"] == str(int(window.end_utc.timestamp()))
    assert "range" not in query
    assert result.request_window == window
    assert result.request_window_semantics is RequestWindowSemantics.EXACT_UTC_WINDOW


def test_historical_fetch_rejects_more_than_seven_days_without_request() -> None:
    requested = False

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal requested
        requested = True
        return httpx.Response(500, request=request)

    start = datetime(2026, 8, 1, tzinfo=UTC)
    with pytest.raises(ValueError, match="seven days"):
        asyncio.run(
            YahooChartProvider(transport=httpx.MockTransport(handler)).fetch_one(
                "ASELS.IS", RequestWindow(start, start + timedelta(days=8))
            )
        )
    assert not requested
