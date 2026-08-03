from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from bist_minute_collector.models import (
    FetchResult,
    MinuteBar,
    OutcomeCode,
    ProviderMetadata,
    ProviderOutcome,
    RequestWindow,
    RequestWindowSemantics,
)

BASE_OPEN = datetime(2026, 8, 3, 7, 0, tzinfo=UTC)


def minute_bar(
    *,
    bar_open_utc: datetime = BASE_OPEN,
    observed_at_utc: datetime | None = None,
    close: str = "100.25",
    symbol: str = "ASELS.IS",
) -> MinuteBar:
    observed = observed_at_utc or bar_open_utc + timedelta(minutes=15)
    close_value = Decimal(close)
    return MinuteBar(
        source="yahoo-chart",
        symbol=symbol,
        interval_seconds=60,
        bar_open_utc=bar_open_utc,
        raw_source_timestamp=int(bar_open_utc.timestamp()),
        open=Decimal("100.00"),
        high=max(Decimal("100.50"), close_value),
        low=min(Decimal("99.75"), close_value),
        close=close_value,
        volume=1250,
        observed_at_utc=observed,
    )


def fetch_result(
    *,
    bar: MinuteBar | None = None,
    bars: tuple[MinuteBar, ...] | None = None,
    retrieved_at_utc: datetime | None = None,
    outcome: OutcomeCode = OutcomeCode.SUCCESS,
    response_sha256: str = "a" * 64,
) -> FetchResult:
    selected = bars if bars is not None else ((bar or minute_bar()),)
    retrieved = retrieved_at_utc or max(
        (item.observed_at_utc for item in selected), default=BASE_OPEN + timedelta(minutes=15)
    )
    symbol = selected[0].symbol if selected else "ASELS.IS"
    return FetchResult(
        source="yahoo-chart",
        symbol=symbol,
        interval="1m",
        request_window=RequestWindow(BASE_OPEN, BASE_OPEN + timedelta(days=1)),
        request_window_semantics=RequestWindowSemantics.EXACT_UTC_WINDOW,
        retrieved_at_utc=retrieved,
        status_code=200,
        attempts=1,
        response_sha256=response_sha256,
        metadata=ProviderMetadata(
            symbol=symbol,
            currency="TRY",
            exchange_name="IST",
            full_exchange_name="Istanbul",
            instrument_type="EQUITY",
            first_trade_date=0,
            regular_market_time=int(retrieved.timestamp()),
            gmt_offset_seconds=10800,
            timezone_abbreviation="TRT",
            exchange_timezone_name="Europe/Istanbul",
            data_granularity="1m",
            raw_metadata_sha256="b" * 64,
        ),
        bars=selected,
        outcomes=(ProviderOutcome(outcome, f"synthetic {outcome.value}"),),
    )


def corrected_result(*, close: str, observed_minutes: int, response_byte: str) -> FetchResult:
    bar = minute_bar(
        close=close,
        observed_at_utc=BASE_OPEN + timedelta(minutes=observed_minutes),
    )
    return fetch_result(
        bar=bar,
        retrieved_at_utc=bar.observed_at_utc,
        response_sha256=response_byte * 64,
    )


def with_symbol(result: FetchResult, symbol: str) -> FetchResult:
    bars = tuple(replace(bar, symbol=symbol) for bar in result.bars)
    assert result.metadata is not None
    return replace(
        result,
        symbol=symbol,
        bars=bars,
        metadata=replace(result.metadata, symbol=symbol),
    )
