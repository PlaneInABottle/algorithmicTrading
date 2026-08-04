from __future__ import annotations

import asyncio
import hashlib
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

from bist_minute_collector.calendar_engine import BistCalendar
from bist_minute_collector.models import FetchResult, OutcomeCode, RequestWindow
from bist_minute_collector.oneshot import DailyOneShot, DailyRunStatus
from bist_minute_collector.snapshots import load_calendar_snapshot, load_universe_snapshot
from bist_minute_collector.store import WriterStore
from tests.helpers import fetch_result, minute_bar, with_symbol

ROOT = Path(__file__).resolve().parents[2]
CALENDAR = ROOT / "config" / "calendars" / "bist" / "2026.json"
UNIVERSE = ROOT / "config" / "universes" / "bist50" / "2026q3.json"
NOW = datetime(2026, 8, 4, 16, 31, tzinfo=UTC)


class CompleteProvider:
    source = "yahoo-chart"

    def __init__(self, throttled_symbol: str | None = None) -> None:
        self.throttled_symbol = throttled_symbol
        self.calls: list[tuple[str, RequestWindow]] = []
        self.active = 0
        self.max_active = 0

    async def fetch_one(self, symbol: str, window: RequestWindow) -> FetchResult:
        self.calls.append((symbol, window))
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        await asyncio.sleep(0)
        self.active -= 1
        digest = hashlib.sha256(symbol.encode()).hexdigest()
        if symbol == self.throttled_symbol:
            return replace(
                with_symbol(fetch_result(bars=()), symbol),
                request_window=window,
                retrieved_at_utc=NOW,
                status_code=429,
                response_sha256=digest,
                outcomes=(fetch_result(outcome=OutcomeCode.THROTTLED).outcomes[0],),
            )
        bar = minute_bar(
            symbol=symbol,
            bar_open_utc=window.start_utc,
            observed_at_utc=NOW,
        )
        return replace(
            with_symbol(fetch_result(bar=bar, response_sha256=digest), symbol),
            request_window=window,
            retrieved_at_utc=NOW,
        )


def test_fresh_daily_run_makes_one_exact_sweep_and_publishes_closed_session(
    tmp_path: Path,
) -> None:
    calendar = BistCalendar(load_calendar_snapshot(CALENDAR))
    universe = load_universe_snapshot(UNIVERSE)
    provider = CompleteProvider()

    report = asyncio.run(DailyOneShot(tmp_path, provider, calendar, universe).run(now_utc=NOW))

    assert report.status is DailyRunStatus.SUCCEEDED
    assert report.requests == 50
    assert len(provider.calls) == 50
    assert provider.max_active <= 2
    assert {window for _, window in provider.calls} == {
        calendar.session(NOW.astimezone(calendar.timezone).date()).query_window
    }
    with WriterStore.open_reader(tmp_path) as reader:
        assert reader.quick_check() == "ok"
        assert reader.integer_scalar("PRAGMA user_version") == 5
        assert reader.integer_scalar("SELECT COUNT(*) FROM bars_hot") == 50
        assert reader.integer_scalar("SELECT COUNT(*) FROM fetch_provenance") == 50
        assert reader.integer_scalar("SELECT COUNT(*) FROM request_attempts") == 50
        assert reader.integer_scalar("SELECT COUNT(*) FROM cursor_state") == 50
        assert reader.integer_scalar("SELECT COUNT(*) FROM gap_current") == 0
        assert (
            reader.integer_scalar("SELECT COUNT(*) FROM compaction_manifests WHERE state='active'")
            == 1
        )


def test_consecutive_day_recovers_only_before_current_session_then_sweeps_once(
    tmp_path: Path,
) -> None:
    calendar = BistCalendar(load_calendar_snapshot(CALENDAR))
    universe = load_universe_snapshot(UNIVERSE)
    provider = CompleteProvider()
    runner = DailyOneShot(tmp_path, provider, calendar, universe)

    first_now = datetime(2026, 8, 3, 16, 31, tzinfo=UTC)
    first = asyncio.run(runner.run(now_utc=first_now))
    assert first.status is DailyRunStatus.SUCCEEDED
    assert len(provider.calls) == 50

    provider.calls.clear()
    second = asyncio.run(runner.run(now_utc=NOW))

    assert second.status is DailyRunStatus.SUCCEEDED
    assert second.requests == 50
    assert second.recovered_gaps == 0
    assert len(provider.calls) == 50
    current_window = calendar.session(NOW.astimezone(calendar.timezone).date()).query_window
    assert current_window is not None
    assert all(window == current_window for _, window in provider.calls)


def test_one_throttled_symbol_is_terminal_failed_run_without_retry_storm(
    tmp_path: Path,
) -> None:
    calendar = BistCalendar(load_calendar_snapshot(CALENDAR))
    universe = load_universe_snapshot(UNIVERSE)
    provider = CompleteProvider("ASELS.IS")

    report = asyncio.run(DailyOneShot(tmp_path, provider, calendar, universe).run(now_utc=NOW))

    assert report.status is DailyRunStatus.FAILED
    assert 1 <= report.requests <= 50
    assert len(provider.calls) == report.requests
    assert sum(symbol == "ASELS.IS" for symbol, _ in provider.calls) == 1
    assert report.successful_symbols == report.requests - 1
    assert report.denied_symbols == 50 - report.requests
    assert report.partitions == 0
    with WriterStore.open_reader(tmp_path) as reader:
        assert (
            reader.integer_scalar("SELECT COUNT(*) FROM request_attempts WHERE state='completed'")
            == report.requests
        )
        assert (
            reader.integer_scalar(
                "SELECT COUNT(*) FROM request_attempts WHERE outcome_code='throttled-429'"
            )
            == 1
        )
        assert reader.integer_scalar("SELECT COUNT(*) FROM cursor_state") == report.requests - 1
        latest = reader.latest_collector_run()
        assert latest is not None
        assert latest["status"] == DailyRunStatus.FAILED.value
