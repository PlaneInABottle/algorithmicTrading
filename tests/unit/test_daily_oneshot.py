from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime
from pathlib import Path

from bist_minute_collector.calendar_engine import BistCalendar
from bist_minute_collector.collector import (
    CollectionReport,
    CollectorProvider,
    SymbolRoute,
)
from bist_minute_collector.models import FetchResult, RequestWindow
from bist_minute_collector.oneshot import (
    DailyOneShot,
    DailyRunStatus,
    RecoverySummary,
    decide_daily_run,
)
from bist_minute_collector.snapshots import load_calendar_snapshot, load_universe_snapshot
from bist_minute_collector.store import WriterStore

ROOT = Path(__file__).resolve().parents[2]
CALENDAR = ROOT / "config" / "calendars" / "bist" / "2026.json"
UNIVERSE = ROOT / "config" / "universes" / "bist50" / "2026q3.json"
UNIVERSE_100 = ROOT / "config" / "universes" / "bist100" / "2026q3.json"


class NoNetworkProvider:
    source = "yahoo-chart"

    def __init__(self) -> None:
        self.calls = 0

    async def fetch_one(self, symbol: str, window: RequestWindow) -> FetchResult:
        self.calls += 1
        raise AssertionError(f"unexpected provider call for {symbol}: {window}")


def test_daily_decision_uses_authoritative_calendar_and_post_close_cutoff() -> None:
    calendar = BistCalendar(load_calendar_snapshot(CALENDAR))

    open_day = decide_daily_run(calendar, datetime(2026, 8, 4, 16, 31, tzinfo=UTC))
    pre_close = decide_daily_run(calendar, datetime(2026, 8, 4, 16, 0, tzinfo=UTC))
    holiday = decide_daily_run(calendar, datetime(2026, 5, 19, 16, 31, tzinfo=UTC))
    outside_snapshot = decide_daily_run(calendar, datetime(2027, 1, 4, 16, 31, tzinfo=UTC))

    assert open_day.status is DailyRunStatus.READY
    assert open_day.session_date == date(2026, 8, 4)
    assert open_day.query_window is not None
    assert pre_close.status is DailyRunStatus.SKIPPED_PRE_CLOSE
    assert holiday.status is DailyRunStatus.SKIPPED_CLOSED
    assert outside_snapshot.status is DailyRunStatus.FAILED_UNVERIFIED_CALENDAR


def test_closed_day_and_pre_close_runs_make_zero_provider_calls(tmp_path: Path) -> None:
    calendar = BistCalendar(load_calendar_snapshot(CALENDAR))
    universe = load_universe_snapshot(UNIVERSE)
    provider = NoNetworkProvider()
    runner = DailyOneShot(tmp_path, provider, calendar, universe)

    closed = asyncio.run(runner.run(now_utc=datetime(2026, 5, 19, 16, 31, tzinfo=UTC)))
    early = asyncio.run(runner.run(now_utc=datetime(2026, 8, 4, 16, 0, tzinfo=UTC)))

    assert closed.status is DailyRunStatus.SKIPPED_CLOSED
    assert early.status is DailyRunStatus.SKIPPED_PRE_CLOSE
    assert provider.calls == 0
    with WriterStore.open_reader(tmp_path) as reader:
        latest = reader.latest_collector_run()
        assert latest is not None
        assert latest["status"] == DailyRunStatus.SKIPPED_PRE_CLOSE.value
        assert reader.integer_scalar("SELECT COUNT(*) FROM request_attempts") == 0


def test_daily_runner_accepts_the_active_bist100_universe(tmp_path: Path) -> None:
    calendar = BistCalendar(load_calendar_snapshot(CALENDAR))
    universe = load_universe_snapshot(UNIVERSE_100)
    runner = DailyOneShot(tmp_path, NoNetworkProvider(), calendar, universe)

    report = asyncio.run(
        runner.run(now_utc=datetime(2026, 8, 4, 16, 31, tzinfo=UTC), dry_run=True)
    )

    assert report.status is DailyRunStatus.DRY_RUN
    assert report.planned_symbols == 100


def test_daily_workflow_orders_reconcile_recovery_sweep_compaction_and_health(
    tmp_path: Path,
) -> None:
    calendar = BistCalendar(load_calendar_snapshot(CALENDAR))
    universe = load_universe_snapshot(UNIVERSE)
    provider = NoNetworkProvider()
    events: list[str] = []

    def reconcile(store: WriterStore) -> int:
        events.append("reconcile")
        return 0

    async def recover(
        store: WriterStore,
        provider_arg: CollectorProvider,
        calendar_arg: BistCalendar,
        routes: tuple[SymbolRoute, ...],
        now_utc: datetime,
        recover_through_utc: datetime,
    ) -> RecoverySummary:
        assert provider_arg is provider
        assert calendar_arg is calendar
        assert len(routes) == 50
        assert recover_through_utc == datetime(2026, 8, 4, 6, 40, tzinfo=UTC)
        events.append("recover")
        return RecoverySummary(requests=3, recovered_gaps=2, unresolved_gaps=1)

    async def sweep(
        store: WriterStore,
        provider_arg: CollectorProvider,
        routes: tuple[SymbolRoute, ...],
        query_window: RequestWindow,
        now_utc: datetime,
    ) -> CollectionReport:
        assert provider_arg is provider
        assert len(routes) == 50
        assert query_window is not None
        events.append("sweep")
        symbols = tuple(member.alias_for("yahoo_chart") for member in universe.members)
        return CollectionReport(
            completed_symbols=symbols,
            successful_symbols=symbols,
            isolated_symbols=(),
            denied_symbols=(),
            request_attempts=50,
            cancelled_requests=0,
            queue_full_events=0,
            max_fetch_concurrency=2,
            max_writer_concurrency=1,
            adapter_paused_reason=None,
        )

    def compact(store: WriterStore, through_date: date) -> int:
        assert through_date == date(2026, 8, 4)
        events.append("compact")
        return 4

    runner = DailyOneShot(
        tmp_path,
        provider,
        calendar,
        universe,
        reconcile=reconcile,
        recover=recover,
        sweep=sweep,
        compact=compact,
    )
    result = asyncio.run(runner.run(now_utc=datetime(2026, 8, 4, 16, 31, tzinfo=UTC)))

    assert result.status is DailyRunStatus.SUCCEEDED
    assert result.requests == 53
    assert result.successful_symbols == 50
    assert result.partitions == 4
    assert result.quick_check == "ok"
    assert events == ["reconcile", "recover", "sweep", "compact"]
    with WriterStore.open_reader(tmp_path) as reader:
        assert reader.active_universe_snapshot_id() == universe.snapshot_id
        status = reader.collector_run_status()
        assert status["last_attempt"] is not None
        assert status["last_success"] is not None
        assert status["last_completed_cycle"] is not None


def test_failed_daily_run_is_durable_and_never_claims_completed_cycle(tmp_path: Path) -> None:
    calendar = BistCalendar(load_calendar_snapshot(CALENDAR))
    universe = load_universe_snapshot(UNIVERSE)

    def fail_reconcile(store: WriterStore) -> int:
        raise RuntimeError("injected pending-manifest failure")

    runner = DailyOneShot(
        tmp_path,
        NoNetworkProvider(),
        calendar,
        universe,
        reconcile=fail_reconcile,
    )
    try:
        asyncio.run(runner.run(now_utc=datetime(2026, 8, 4, 16, 31, tzinfo=UTC)))
    except RuntimeError as error:
        assert str(error) == "injected pending-manifest failure"
    else:
        raise AssertionError("injected failure should propagate")

    with WriterStore.open_reader(tmp_path) as reader:
        status = reader.collector_run_status()
        assert status["latest"] is not None
        assert status["latest"]["status"] == DailyRunStatus.FAILED.value
        assert status["last_success"] is None
        assert status["last_completed_cycle"] is None


def test_startup_marks_abandoned_started_run_interrupted(tmp_path: Path) -> None:
    started = datetime(2026, 8, 3, 16, 31, tzinfo=UTC)
    with WriterStore(tmp_path) as store:
        store.start_collector_run(
            "abandoned",
            session_date=date(2026, 8, 3),
            attempted_at_utc=started,
            calendar_snapshot_id="calendar",
            universe_snapshot_id="universe",
        )
    with WriterStore(tmp_path) as store:
        assert store.reconcile_started_runs(datetime(2026, 8, 4, 16, 31, tzinfo=UTC)) == 1
    with WriterStore.open_reader(tmp_path) as reader:
        latest = reader.latest_collector_run()
        assert latest is not None
        assert latest["status"] == DailyRunStatus.INTERRUPTED.value
