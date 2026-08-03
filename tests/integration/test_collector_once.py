from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from bist_minute_collector.cli import main
from bist_minute_collector.collector import (
    CollectionReport,
    Collector,
    CollectorConfig,
    SymbolRoute,
)
from bist_minute_collector.models import FetchResult, OutcomeCode, ProviderOutcome, RequestWindow
from bist_minute_collector.store import WriterStore
from tests.helpers import BASE_OPEN, corrected_result, fetch_result, minute_bar, with_symbol

ROUTES = (
    SymbolRoute("ASELS.E", "ASELS.IS"),
    SymbolRoute("THYAO.E", "THYAO.IS"),
    SymbolRoute("AKBNK.E", "AKBNK.IS"),
    SymbolRoute("TUPRS.E", "TUPRS.IS"),
)
COLLECTION_TIME = datetime(2026, 8, 5, 15, 0, tzinfo=UTC)


def symbol_result(symbol: str, *, close: str = "100.25") -> FetchResult:
    result = fetch_result(bar=minute_bar(symbol=symbol, close=close))
    return with_symbol(result, symbol)


class SequenceProvider:
    source = "yahoo-chart"

    def __init__(
        self,
        results: Mapping[str, Sequence[FetchResult]],
        *,
        delays: dict[str, float] | None = None,
    ) -> None:
        self._results = {symbol: list(items) for symbol, items in results.items()}
        self._delays = delays or {}
        self.calls: list[tuple[str, RequestWindow]] = []
        self.events: list[tuple[str, str]] = []
        self.active = 0
        self.max_active = 0

    async def fetch_one(self, symbol: str, window: RequestWindow) -> FetchResult:
        self.calls.append((symbol, window))
        self.events.append(("start", symbol))
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            await asyncio.sleep(self._delays.get(symbol, 0))
            result = self._results[symbol].pop(0)
            self.events.append(("finish", symbol))
            return result
        finally:
            self.active -= 1


def test_collect_once_bounds_concurrency_uses_one_writer_and_replays_overlap(
    tmp_path: Path,
) -> None:
    first = {route.provider_symbol: [symbol_result(route.provider_symbol)] for route in ROUTES}
    second = {
        route.provider_symbol: [
            (
                with_symbol(
                    corrected_result(close="101.75", observed_minutes=20, response_byte="c"),
                    route.provider_symbol,
                )
                if route.provider_symbol == "ASELS.IS"
                else symbol_result(route.provider_symbol)
            )
        ]
        for route in ROUTES
    }
    merged = {symbol: [*first[symbol], *second[symbol]] for symbol in first}
    provider = SequenceProvider(merged, delays={"ASELS.IS": 0.04, "THYAO.IS": 0.001})

    with WriterStore(tmp_path) as store:
        collector = Collector(provider, store, ROUTES, config=CollectorConfig(max_concurrency=2))
        first_report = asyncio.run(collector.collect_once(now_utc=COLLECTION_TIME))
        second_report = asyncio.run(
            collector.collect_once(now_utc=COLLECTION_TIME + timedelta(minutes=10))
        )

        assert first_report.completed_symbols == tuple(route.provider_symbol for route in ROUTES)
        assert first_report.successful_symbols == tuple(route.provider_symbol for route in ROUTES)
        assert first_report.max_fetch_concurrency <= 2
        assert first_report.max_writer_concurrency == 1
        assert provider.max_active <= 2
        assert store.active_bar_count() == 4
        assert store.revision_count() == 1
        assert store.active_bar("yahoo-chart", "ASELS.E", 60, BASE_OPEN).close == "101.75"
        assert second_report.request_attempts == 4

        first_cycle = provider.calls[:4]
        second_cycle = provider.calls[4:]
        assert tuple(symbol for symbol, _ in first_cycle) == tuple(
            route.provider_symbol for route in ROUTES
        )
        assert second_cycle[0][0] == "THYAO.IS"
        cursor = store.cursor("yahoo-chart", "THYAO.E", 60)
        assert cursor is not None
        assert second_cycle[0][1].start_utc == cursor.accounted_through_utc - timedelta(hours=2)

    third_start = provider.events.index(("start", "AKBNK.IS"))
    slow_finish = provider.events.index(("finish", "ASELS.IS"))
    assert third_start < slow_finish


def test_retry_budget_is_three_attempts_and_uses_bounded_delays(tmp_path: Path) -> None:
    server_error = fetch_result(bars=(), outcome=OutcomeCode.SERVER_ERROR)
    timeout = fetch_result(bars=(), outcome=OutcomeCode.TIMEOUT)
    provider = SequenceProvider({"ASELS.IS": [server_error, timeout, symbol_result("ASELS.IS")]})
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    with WriterStore(tmp_path) as store:
        collector = Collector(
            provider,
            store,
            ROUTES[:1],
            config=CollectorConfig(max_concurrency=2),
            sleep=fake_sleep,
        )
        report = asyncio.run(collector.collect_once(now_utc=COLLECTION_TIME))

        assert report.request_attempts == 3
        assert sleeps == [2.0, 10.0]
        assert len(provider.calls) == 3
        assert store.request_attempt_count() == 3
        assert store.provenance_count() == 1
        assert store.cursor("yahoo-chart", "ASELS.E", 60) is not None


@pytest.mark.parametrize("terminal_code", [OutcomeCode.SERVER_ERROR, OutcomeCode.TIMEOUT])
def test_retry_exhaustion_stops_after_three_without_advancing_cursor(
    tmp_path: Path, terminal_code: OutcomeCode
) -> None:
    failed = fetch_result(bars=(), outcome=terminal_code)
    provider = SequenceProvider({"ASELS.IS": [failed, failed, failed]})

    async def no_wait(_seconds: float) -> None:
        return None

    with WriterStore(tmp_path) as store:
        report = asyncio.run(
            Collector(provider, store, ROUTES[:1], sleep=no_wait).collect_once(
                now_utc=COLLECTION_TIME
            )
        )

        assert report.request_attempts == 3
        assert len(provider.calls) == 3
        assert store.request_attempt_count() == 3
        assert store.provenance_count() == 1
        assert store.cursor("yahoo-chart", "ASELS.E", 60) is None
        assert report.completed_symbols == ("ASELS.IS",)
        assert report.successful_symbols == ()


class BlockingProvider:
    source = "yahoo-chart"

    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.cancelled = False

    async def fetch_one(self, symbol: str, window: RequestWindow) -> FetchResult:
        del symbol, window
        self.started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cancelled = True
            raise
        raise AssertionError("blocking provider unexpectedly returned")


def test_shutdown_is_deadline_bounded_and_cancelled_fetch_does_not_advance_cursor(
    tmp_path: Path,
) -> None:
    async def scenario(store: WriterStore) -> tuple[float, CollectionReport]:
        loop = asyncio.get_running_loop()
        provider = BlockingProvider()
        collector = Collector(
            provider,
            store,
            ROUTES[:1],
            config=CollectorConfig(request_timeout_seconds=30),
        )
        task = asyncio.create_task(collector.collect_once(now_utc=COLLECTION_TIME))
        await asyncio.wait_for(provider.started.wait(), timeout=1)
        started = loop.time()
        await collector.shutdown(timeout_seconds=0.5)
        report = await asyncio.wait_for(task, timeout=0.5)
        assert provider.cancelled
        return loop.time() - started, report

    with WriterStore(tmp_path) as store:
        elapsed, report = asyncio.run(scenario(store))
        assert elapsed < 0.5
        assert report.cancelled_requests == 1
        assert store.request_attempt_state(1) == "cancelled"
        assert store.cursor("yahoo-chart", "ASELS.E", 60) is None
        assert store.provenance_count() == 0


def test_full_writer_queue_applies_backpressure_without_dropping_results(tmp_path: Path) -> None:
    routes = tuple(SymbolRoute(f"SYM{index}.E", f"SYM{index}.IS") for index in range(6))
    provider = SequenceProvider(
        {route.provider_symbol: [symbol_result(route.provider_symbol)] for route in routes}
    )

    with WriterStore(tmp_path) as store:
        collector = Collector(
            provider,
            store,
            routes,
            config=CollectorConfig(
                max_concurrency=2,
                writer_queue_size=1,
                writer_delay_seconds=0.01,
            ),
        )
        report = asyncio.run(collector.collect_once(now_utc=COLLECTION_TIME))

        assert report.queue_full_events > 0
        assert report.completed_symbols == tuple(route.provider_symbol for route in routes)
        assert store.active_bar_count() == 6
        assert store.provenance_count() == 6
        assert store.request_attempt_count() == 6


def test_per_symbol_invalid_row_is_isolated_without_pausing_other_symbols(tmp_path: Path) -> None:
    accepted = symbol_result("ASELS.IS")
    isolated = replace(
        accepted,
        outcomes=(
            *accepted.outcomes,
            ProviderOutcome(OutcomeCode.INVALID_ROW, "symbol-specific malformed row"),
        ),
    )
    provider = SequenceProvider(
        {
            "ASELS.IS": [isolated],
            "THYAO.IS": [symbol_result("THYAO.IS")],
        }
    )

    with WriterStore(tmp_path) as store:
        report = asyncio.run(
            Collector(provider, store, ROUTES[:2]).collect_once(now_utc=COLLECTION_TIME)
        )

        assert report.adapter_paused_reason is None
        assert report.isolated_symbols == ("ASELS.IS",)
        assert report.completed_symbols == ("ASELS.IS", "THYAO.IS")
        assert store.active_bar_count() == 2


def test_common_schema_drift_pauses_adapter_before_third_symbol(tmp_path: Path) -> None:
    malformed = fetch_result(
        bars=(),
        outcome=OutcomeCode.MISSING_METADATA,
        retrieved_at_utc=COLLECTION_TIME,
    )
    provider = SequenceProvider(
        {
            "ASELS.IS": [malformed],
            "THYAO.IS": [malformed],
            "AKBNK.IS": [malformed],
        }
    )

    with WriterStore(tmp_path) as store:
        report = asyncio.run(
            Collector(
                provider,
                store,
                ROUTES[:3],
                config=CollectorConfig(max_concurrency=1),
            ).collect_once(now_utc=COLLECTION_TIME)
        )

        assert len(provider.calls) == 2
        assert report.completed_symbols == ("ASELS.IS", "THYAO.IS")
        assert report.denied_symbols == ("AKBNK.IS",)
        assert report.adapter_paused_reason == "schema-drift:missing-metadata"
        assert store.request_attempt_count() == 2


def test_collect_once_dry_run_is_network_free_and_does_not_create_state(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    data_root = tmp_path / "dry-run-data"
    result = main(
        [
            "collect-once",
            "--data-root",
            str(data_root),
            "--symbols",
            "ASELS.IS,THYAO.IS",
            "--dry-run",
        ]
    )
    assert result == 0
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["dry_run"] is True
    assert payload["symbols"] == ["ASELS.IS", "THYAO.IS"]
    assert payload["planned_requests"] == 2
    assert not data_root.exists()
