from __future__ import annotations

import asyncio
import json

import pytest

from bist_minute_collector.models import OutcomeCode, RequestWindowSemantics
from bist_minute_collector.provider import YahooChartProvider

SMOKE_SYMBOLS = ("ASELS.IS", "AKBNK.IS", "THYAO.IS", "TUPRS.IS", "DSTKF.IS")


@pytest.mark.network
def test_exact_five_symbol_one_day_smoke_is_bounded_and_truthful() -> None:
    batch = asyncio.run(
        YahooChartProvider(timeout_seconds=15.0).fetch_many_one_day(
            SMOKE_SYMBOLS,
            max_concurrency=2,
        )
    )

    assert batch.request_count == 5
    assert batch.symbols == SMOKE_SYMBOLS
    assert batch.max_observed_concurrency <= 2
    assert all(result.attempts == 1 for result in batch.results)
    assert all(result.symbol in SMOKE_SYMBOLS for result in batch.results)
    assert all(
        result.request_window_semantics is RequestWindowSemantics.PROVIDER_RANGE_1D_APPROXIMATE
        for result in batch.results
    )
    for result in batch.results:
        if OutcomeCode.SUCCESS in result.codes:
            assert result.metadata is not None
            assert result.metadata.data_granularity == "1m"
            assert result.metadata.exchange_timezone_name == "Europe/Istanbul"
        assert OutcomeCode.UNEXPECTED_GRANULARITY not in result.codes
        assert OutcomeCode.UNEXPECTED_TIMEZONE not in result.codes

    summary = [
        {
            "symbol": result.symbol,
            "status": result.status_code,
            "outcomes": [code.value for code in result.codes],
            "bar_count": len(result.bars),
            "latest_raw_timestamp": (result.bars[-1].raw_source_timestamp if result.bars else None),
        }
        for result in batch.results
    ]
    print(json.dumps(summary, sort_keys=True))
