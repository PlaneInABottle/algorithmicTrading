from __future__ import annotations

from datetime import UTC, datetime, timedelta

from bist_minute_collector.scheduler import CycleScheduler, FairSymbolOrder, plan_overlap_window


class FakeClock:
    def __init__(self) -> None:
        self.monotonic_value = 0.0
        self.utc_value = datetime(2026, 8, 3, 7, 0, tzinfo=UTC)

    def monotonic(self) -> float:
        return self.monotonic_value

    def utc_now(self) -> datetime:
        return self.utc_value


def test_monotonic_schedule_emits_one_due_cycle_and_never_overlaps() -> None:
    clock = FakeClock()
    scheduler = CycleScheduler(
        interval_seconds=600,
        monotonic=clock.monotonic,
        utc_now=clock.utc_now,
    )

    first = scheduler.try_start()
    assert first is not None
    assert first.sequence == 1
    assert first.planned_at_utc == clock.utc_value

    clock.monotonic_value = 1_000
    clock.utc_value += timedelta(days=7)
    assert scheduler.try_start() is None

    scheduler.finish(first.sequence)
    second = scheduler.try_start()
    assert second is not None
    assert second.sequence == 2
    assert second.planned_at_utc == clock.utc_value
    scheduler.finish(second.sequence)

    clock.utc_value -= timedelta(days=30)
    clock.monotonic_value = 1_001
    assert scheduler.try_start() is None


def test_scheduler_rejects_wrong_finish_token() -> None:
    clock = FakeClock()
    scheduler = CycleScheduler(600, monotonic=clock.monotonic, utc_now=clock.utc_now)
    cycle = scheduler.try_start()
    assert cycle is not None

    try:
        scheduler.finish(cycle.sequence + 1)
    except ValueError as error:
        assert "active cycle" in str(error)
    else:
        raise AssertionError("wrong cycle token must not unlock the scheduler")


def test_overlap_planner_uses_two_hours_and_utc_half_open_windows() -> None:
    now = datetime(2026, 8, 3, 15, 0, tzinfo=UTC)
    accounted = datetime(2026, 8, 3, 12, 0, tzinfo=UTC)

    existing = plan_overlap_window(now_utc=now, accounted_through_utc=accounted)
    initial = plan_overlap_window(now_utc=now, accounted_through_utc=None)

    assert existing.start_utc == accounted - timedelta(hours=2)
    assert existing.end_utc == now
    assert initial.start_utc == now - timedelta(days=1)
    assert initial.end_utc == now


def test_fair_symbol_order_rotates_without_duplicates() -> None:
    order = FairSymbolOrder(("ASELS.IS", "THYAO.IS", "AKBNK.IS"))

    assert order.next_cycle() == ("ASELS.IS", "THYAO.IS", "AKBNK.IS")
    assert order.next_cycle() == ("THYAO.IS", "AKBNK.IS", "ASELS.IS")
    assert order.next_cycle() == ("AKBNK.IS", "ASELS.IS", "THYAO.IS")
