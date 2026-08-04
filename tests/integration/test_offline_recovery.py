from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from bist_minute_collector.calendar_engine import BistCalendar
from bist_minute_collector.collector import SymbolRoute
from bist_minute_collector.gaps import GapLedger, GapState, RecoveryPlanner
from bist_minute_collector.models import FetchResult, OutcomeCode, ProviderOutcome, RequestWindow
from bist_minute_collector.recovery import RecoveryEngine, startup_recovery
from bist_minute_collector.snapshots import load_calendar_snapshot
from bist_minute_collector.store import WriterStore
from tests.helpers import fetch_result, minute_bar, with_symbol

ROOT = Path(__file__).parents[2]
CALENDAR = ROOT / "config/calendars/bist/2026.json"
ROUTE = SymbolRoute("ASELS.E", "ASELS.IS")


class ExactRecoveryProvider:
    source = "yahoo_chart"

    def __init__(self, *, empty: bool = False) -> None:
        self.empty = empty
        self.calls: list[RequestWindow] = []

    async def fetch_one(self, symbol: str, window: RequestWindow) -> FetchResult:
        self.calls.append(window)
        if self.empty:
            base = fetch_result(
                bars=(),
                outcome=OutcomeCode.SOURCE_EMPTY,
                retrieved_at_utc=window.end_utc,
            )
            return replace(base, symbol=symbol, request_window=window)
        bars = []
        cursor = window.start_utc
        while cursor < window.end_utc:
            bars.append(
                minute_bar(
                    symbol=symbol,
                    bar_open_utc=cursor,
                    observed_at_utc=window.end_utc,
                )
            )
            cursor += timedelta(minutes=1)
        return replace(
            with_symbol(fetch_result(bars=tuple(bars)), symbol),
            request_window=window,
            retrieved_at_utc=window.end_utc,
        )


def test_three_day_offline_recovery_processes_oldest_first_and_repairs_returned_bars(
    tmp_path: Path,
) -> None:
    calendar = BistCalendar(load_calendar_snapshot(CALENDAR))
    provider = ExactRecoveryProvider()
    with WriterStore(tmp_path) as store:
        engine = RecoveryEngine(store, provider, calendar)
        report = asyncio.run(
            engine.recover(
                ROUTE,
                start_utc=datetime(2026, 8, 3, 0, 0, tzinfo=UTC),
                end_utc=datetime(2026, 8, 6, 0, 0, tzinfo=UTC),
                now_utc=datetime(2026, 8, 6, 12, 0, tzinfo=UTC),
            )
        )

        assert report.requests == len(provider.calls) > 0
        assert provider.calls == sorted(provider.calls, key=lambda item: item.start_utc)
        assert store.active_bar_count() == report.recovered_gaps > len(provider.calls)
        assert all(item.state is GapState.RECOVERED for item in GapLedger(store).list_current())


def test_35_day_offline_recovery_records_unrecoverable_without_provider_call(
    tmp_path: Path,
) -> None:
    calendar = BistCalendar(load_calendar_snapshot(CALENDAR))
    provider = ExactRecoveryProvider()
    with WriterStore(tmp_path) as store:
        report = asyncio.run(
            RecoveryEngine(store, provider, calendar).recover(
                ROUTE,
                start_utc=datetime(2026, 6, 22, 0, 0, tzinfo=UTC),
                end_utc=datetime(2026, 6, 30, 0, 0, tzinfo=UTC),
                now_utc=datetime(2026, 8, 4, 16, 0, tzinfo=UTC),
            )
        )

        assert report.requests == 0
        assert not provider.calls
        assert report.unrecoverable_gaps > 0
        assert store.active_bar_count() == 0


def test_recovery_interruption_keeps_pending_evidence_and_replay_is_idempotent(
    tmp_path: Path,
) -> None:
    fail_once = True

    def fail(point: str) -> None:
        nonlocal fail_once
        if point == "before_commit" and fail_once:
            fail_once = False
            raise RuntimeError("injected backfill interruption")

    calendar = BistCalendar(load_calendar_snapshot(CALENDAR))
    provider = ExactRecoveryProvider()
    start = datetime(2026, 8, 3, 6, 40, tzinfo=UTC)
    end = start + timedelta(minutes=1)
    with WriterStore(tmp_path, fault_injector=fail) as store:
        with pytest.raises(RuntimeError, match="backfill interruption"):
            asyncio.run(
                RecoveryEngine(store, provider, calendar).recover(
                    ROUTE, start_utc=start, end_utc=end, now_utc=end + timedelta(days=1)
                )
            )
        assert store.active_bar_count() == 0
        assert GapLedger(store).list_current()[0].state is GapState.PENDING

    with WriterStore(tmp_path) as reopened:
        report = asyncio.run(
            RecoveryEngine(reopened, provider, calendar).recover(
                ROUTE, start_utc=start, end_utc=end, now_utc=end + timedelta(days=1)
            )
        )
        assert report.recovered_gaps == 1
        assert reopened.active_bar_count() == 1
        assert reopened.quick_check() == "ok"


def test_retention_422_closes_gap_truthfully_and_429_leaves_pending(tmp_path: Path) -> None:
    calendar = BistCalendar(load_calendar_snapshot(CALENDAR))
    planner = RecoveryPlanner(calendar)
    window = (
        planner.plan(
            source="yahoo_chart",
            exchange_code="ASELS.E",
            start_utc=datetime(2026, 8, 3, 6, 40, tzinfo=UTC),
            end_utc=datetime(2026, 8, 3, 6, 41, tzinfo=UTC),
            now_utc=datetime(2026, 8, 4, 16, 0, tzinfo=UTC),
        )
        .tasks[0]
        .window
    )

    class TerminalProvider(ExactRecoveryProvider):
        def __init__(self, code: OutcomeCode) -> None:
            super().__init__()
            self.code = code

        async def fetch_one(self, symbol: str, requested: RequestWindow) -> FetchResult:
            self.calls.append(requested)
            result = fetch_result(bars=(), outcome=self.code, retrieved_at_utc=requested.end_utc)
            return replace(
                result,
                symbol=symbol,
                request_window=requested,
                outcomes=(ProviderOutcome(self.code, self.code.value),),
            )

    with WriterStore(tmp_path / "retention") as store:
        report = asyncio.run(
            RecoveryEngine(store, TerminalProvider(OutcomeCode.RETENTION_LIMIT), calendar).recover(
                ROUTE,
                start_utc=window.start_utc,
                end_utc=window.end_utc,
                now_utc=datetime(2026, 8, 4, 16, 0, tzinfo=UTC),
            )
        )
        assert report.unrecoverable_gaps == 1

    with WriterStore(tmp_path / "throttled") as store:
        report = asyncio.run(
            RecoveryEngine(store, TerminalProvider(OutcomeCode.THROTTLED), calendar).recover(
                ROUTE,
                start_utc=window.start_utc,
                end_utc=window.end_utc,
                now_utc=datetime(2026, 8, 4, 16, 0, tzinfo=UTC),
            )
        )
        assert report.pending_gaps == 1


def test_three_empty_post_session_observations_can_later_recover(tmp_path: Path) -> None:
    calendar = BistCalendar(load_calendar_snapshot(CALENDAR))
    start = datetime(2026, 8, 3, 6, 40, tzinfo=UTC)
    end = start + timedelta(minutes=1)
    with WriterStore(tmp_path) as store:
        empty = ExactRecoveryProvider(empty=True)
        for _ in range(3):
            report = asyncio.run(
                RecoveryEngine(store, empty, calendar).recover(
                    ROUTE,
                    start_utc=start,
                    end_utc=end,
                    now_utc=datetime(2026, 8, 4, 16, 0, tzinfo=UTC),
                )
            )
        assert report.provider_empty_gaps == 1
        assert store.active_bar_count() == 0

        recovered = asyncio.run(
            RecoveryEngine(store, ExactRecoveryProvider(), calendar).recover(
                ROUTE,
                start_utc=start,
                end_utc=end,
                now_utc=datetime(2026, 8, 4, 16, 1, tzinfo=UTC),
            )
        )
        assert recovered.recovered_gaps == 1
        assert store.active_bar_count() == 1


def test_startup_recovery_resumes_from_durable_cursor_without_replaying_closed_time(
    tmp_path: Path,
) -> None:
    calendar = BistCalendar(load_calendar_snapshot(CALENDAR))
    provider = ExactRecoveryProvider()
    with WriterStore(tmp_path) as store:
        first = asyncio.run(
            RecoveryEngine(store, provider, calendar).recover(
                ROUTE,
                start_utc=datetime(2026, 8, 3, 6, 40, tzinfo=UTC),
                end_utc=datetime(2026, 8, 3, 15, 19, tzinfo=UTC),
                now_utc=datetime(2026, 8, 3, 16, 30, tzinfo=UTC),
            )
        )
        assert first.requests == 1
        provider.calls.clear()

        reports = asyncio.run(
            startup_recovery(
                store,
                provider,
                calendar,
                (ROUTE,),
                now_utc=datetime(2026, 8, 5, 5, 0, tzinfo=UTC),
            )
        )
        assert sum(item.requests for item in reports) == 1
        assert provider.calls[0].start_utc == datetime(2026, 8, 4, 6, 40, tzinfo=UTC)
        assert provider.calls[0].end_utc == datetime(2026, 8, 4, 15, 19, tzinfo=UTC)


def test_partial_window_return_recovers_only_matching_minutes(tmp_path: Path) -> None:
    class PartialProvider(ExactRecoveryProvider):
        async def fetch_one(self, symbol: str, window: RequestWindow) -> FetchResult:
            self.calls.append(window)
            bar = minute_bar(
                symbol=symbol,
                bar_open_utc=window.start_utc,
                observed_at_utc=window.end_utc,
            )
            return replace(
                with_symbol(fetch_result(bar=bar), symbol),
                request_window=window,
                retrieved_at_utc=window.end_utc,
            )

    start = datetime(2026, 8, 3, 6, 40, tzinfo=UTC)
    with WriterStore(tmp_path) as store:
        report = asyncio.run(
            RecoveryEngine(
                store,
                PartialProvider(),
                BistCalendar(load_calendar_snapshot(CALENDAR)),
            ).recover(
                ROUTE,
                start_utc=start,
                end_utc=start + timedelta(minutes=2),
                now_utc=datetime(2026, 8, 4, 16, 0, tzinfo=UTC),
            )
        )

        assert report.recovered_gaps == 1
        assert report.pending_gaps == 1
        assert store.active_bar_count() == 1
