from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from bist_minute_collector.calendar_engine import BistCalendar
from bist_minute_collector.cli import main
from bist_minute_collector.gaps import (
    GapLedger,
    GapRange,
    GapState,
    InvalidGapTransition,
    RecoveryPlanner,
)
from bist_minute_collector.snapshots import load_calendar_snapshot, load_universe_snapshot
from bist_minute_collector.store import WriterStore

ROOT = Path(__file__).parents[2]
CALENDAR = ROOT / "config/calendars/bist/2026.json"
UNIVERSE = ROOT / "config/universes/bist50/2026q3.json"
NOW = datetime(2026, 8, 4, 16, 0, tzinfo=UTC)


def one_minute_gap() -> GapRange:
    start = datetime(2026, 8, 3, 7, 0, tzinfo=UTC)
    return GapRange("yahoo_chart", "ASELS.E", 60, start, start + timedelta(minutes=1))


def test_all_five_gap_states_and_allowed_transitions_are_append_only(tmp_path: Path) -> None:
    with WriterStore(tmp_path) as store:
        ledger = GapLedger(store)
        gap = one_minute_gap()
        assert ledger.record(gap, GapState.PENDING, "discovered", "calendar-v1", NOW)
        assert ledger.record_attempt(gap, "fetch-one", NOW + timedelta(seconds=1)) == 1
        assert ledger.transition(
            gap,
            GapState.PROVIDER_EMPTY,
            "three-empty-overlaps-post-session",
            "fetch-three",
            NOW + timedelta(seconds=2),
        )
        assert ledger.transition(
            gap,
            GapState.RECOVERED,
            "complete-bar-returned",
            "fetch-four",
            NOW + timedelta(seconds=3),
        )
        assert ledger.current(gap).state is GapState.RECOVERED
        assert [event.to_state for event in ledger.history(gap)] == [
            GapState.PENDING,
            GapState.PENDING,
            GapState.PROVIDER_EMPTY,
            GapState.RECOVERED,
        ]
        with pytest.raises(InvalidGapTransition):
            ledger.transition(
                gap,
                GapState.PENDING,
                "illegal-reopen",
                "operator",
                NOW + timedelta(seconds=4),
            )

        outside = GapRange(
            "yahoo_chart",
            "ASELS.E",
            60,
            datetime(2026, 8, 2, 7, 0, tzinfo=UTC),
            datetime(2026, 8, 2, 7, 1, tzinfo=UTC),
        )
        expired = GapRange(
            "yahoo_chart",
            "ASELS.E",
            60,
            datetime(2026, 6, 1, 7, 0, tzinfo=UTC),
            datetime(2026, 6, 1, 7, 1, tzinfo=UTC),
        )
        ledger.record(outside, GapState.OUTSIDE_SESSION, "official-weekend", "calendar-v1", NOW)
        ledger.record(
            expired,
            GapState.UNRECOVERABLE,
            "older-than-provider-retention",
            "cutoff-28d",
            NOW,
        )
        assert {item.state for item in ledger.list_current()} == {
            GapState.RECOVERED,
            GapState.OUTSIDE_SESSION,
            GapState.UNRECOVERABLE,
        }
        assert store.gap_event_count() == 6


def test_identical_gap_evidence_is_idempotent(tmp_path: Path) -> None:
    with WriterStore(tmp_path) as store:
        ledger = GapLedger(store)
        gap = one_minute_gap()
        assert ledger.record(gap, GapState.PENDING, "discovered", "calendar-v1", NOW)
        assert not ledger.record(gap, GapState.PENDING, "discovered", "calendar-v1", NOW)
        assert store.gap_event_count() == 1


def test_three_day_recovery_is_oldest_first_and_seven_day_bounded() -> None:
    planner = RecoveryPlanner(BistCalendar(load_calendar_snapshot(CALENDAR)))
    plan = planner.plan(
        source="yahoo_chart",
        exchange_code="ASELS.E",
        start_utc=datetime(2026, 8, 3, 0, 0, tzinfo=UTC),
        end_utc=datetime(2026, 8, 6, 0, 0, tzinfo=UTC),
        now_utc=datetime(2026, 8, 6, 12, 0, tzinfo=UTC),
    )

    assert plan.tasks
    assert tuple(task.window.start_utc for task in plan.tasks) == tuple(
        sorted(task.window.start_utc for task in plan.tasks)
    )
    assert all(task.window.duration <= timedelta(days=7) for task in plan.tasks)
    assert all(gap.state is GapState.PENDING for gap in plan.gaps)


def test_older_than_28_days_is_truthful_and_never_scheduled() -> None:
    planner = RecoveryPlanner(BistCalendar(load_calendar_snapshot(CALENDAR)))
    plan = planner.plan(
        source="yahoo_chart",
        exchange_code="ASELS.E",
        start_utc=datetime(2026, 6, 22, 0, 0, tzinfo=UTC),
        end_utc=datetime(2026, 6, 30, 0, 0, tzinfo=UTC),
        now_utc=NOW,
    )

    assert not plan.tasks
    assert plan.gaps
    assert all(gap.state is GapState.UNRECOVERABLE for gap in plan.gaps)


def test_provider_empty_can_recover_without_deleting_an_existing_bar(tmp_path: Path) -> None:
    gap = one_minute_gap()
    with WriterStore(tmp_path) as store:
        ledger = GapLedger(store)
        ledger.record(gap, GapState.PENDING, "discovered", "calendar-v1", NOW)
        for index in range(3):
            ledger.record_attempt(gap, f"empty-{index}", NOW + timedelta(seconds=index + 1))
        ledger.transition(
            gap,
            GapState.PROVIDER_EMPTY,
            "three-empty-overlaps-post-session",
            "empty-2",
            NOW + timedelta(seconds=4),
        )
        active_before = store.active_bar_count()
        ledger.transition(
            gap,
            GapState.RECOVERED,
            "late-complete-bar",
            "late-fetch",
            NOW + timedelta(seconds=5),
        )
        assert store.active_bar_count() == active_before
        assert ledger.current(gap).state is GapState.RECOVERED


def test_active_universe_switch_preserves_old_membership(tmp_path: Path) -> None:
    snapshot = load_universe_snapshot(UNIVERSE)
    with WriterStore(tmp_path) as store:
        store.activate_universe(snapshot, activated_at_utc=NOW)
        original = store.active_universe_members()
        store.activate_universe(
            snapshot,
            activated_at_utc=NOW + timedelta(days=1),
            activation_id="future-reactivation",
        )

        assert len(original) == 50
        assert store.active_universe_members() == original
        assert store.universe_activation_count() == 2
        assert store.universe_membership_count(snapshot.snapshot_id) == 50


def test_backfill_is_dry_run_by_default_and_does_not_create_state(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    data_root = tmp_path / "must-not-exist"
    result = main(
        [
            "backfill",
            "--data-root",
            str(data_root),
            "--symbols",
            "ASELS.IS,THYAO.IS",
            "--from",
            "2026-08-01T00:00:00Z",
            "--to",
            "2026-08-03T00:00:00Z",
        ]
    )

    assert result == 0
    assert not data_root.exists()
    assert '"dry_run": true' in capsys.readouterr().out


def test_gaps_json_filters_current_states_without_mutating_store(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    with WriterStore(tmp_path) as store:
        GapLedger(store).record(
            one_minute_gap(), GapState.PENDING, "discovered", "calendar-v1", NOW
        )
        before = store.gap_event_count()

    result = main(
        [
            "gaps",
            "--data-root",
            str(tmp_path),
            "--state",
            "pending,unrecoverable-beyond-retention",
            "--json",
        ]
    )

    assert result == 0
    assert '"pending": 1' in capsys.readouterr().out
    with WriterStore.open_reader(tmp_path) as reader:
        assert reader.gap_event_count() == before
