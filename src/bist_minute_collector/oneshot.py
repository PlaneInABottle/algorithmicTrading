from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from enum import StrEnum
from pathlib import Path

from .archive import ArchiveCompactor, ArchiveError
from .calendar_engine import BistCalendar, CalendarConfidence, SlotClassification
from .collector import CollectionReport, Collector, CollectorConfig, CollectorProvider, SymbolRoute
from .models import RequestWindow
from .recovery import RecoveryReport, startup_recovery
from .snapshots import UniverseSnapshot
from .store import StoreError, WriterStore

_POST_CLOSE_LOCAL = time(19, 30)


class DailyRunStatus(StrEnum):
    READY = "ready"
    DRY_RUN = "dry-run"
    SUCCEEDED = "succeeded"
    SKIPPED_CLOSED = "skipped-closed"
    SKIPPED_PRE_CLOSE = "skipped-pre-close"
    FAILED_UNVERIFIED_CALENDAR = "failed-unverified-calendar"
    FAILED = "failed"
    INTERRUPTED = "interrupted"


@dataclass(frozen=True, slots=True)
class DailyRunDecision:
    status: DailyRunStatus
    session_date: date
    reason: str
    query_window: RequestWindow | None


@dataclass(frozen=True, slots=True)
class RecoverySummary:
    requests: int = 0
    recovered_gaps: int = 0
    unresolved_gaps: int = 0


@dataclass(frozen=True, slots=True)
class DailyRunReport:
    run_id: str
    status: DailyRunStatus
    session_date: date
    reason: str
    requests: int = 0
    successful_symbols: int = 0
    denied_symbols: int = 0
    recovered_gaps: int = 0
    unresolved_gaps: int = 0
    partitions: int = 0
    quick_check: str | None = None
    planned_symbols: int = 0


RecoveryRunner = Callable[
    [
        WriterStore,
        CollectorProvider,
        BistCalendar,
        tuple[SymbolRoute, ...],
        datetime,
        datetime,
    ],
    Awaitable[RecoverySummary],
]
SweepRunner = Callable[
    [
        WriterStore,
        CollectorProvider,
        tuple[SymbolRoute, ...],
        RequestWindow,
        datetime,
    ],
    Awaitable[CollectionReport],
]
ReconcileRunner = Callable[[WriterStore], int]
CompactRunner = Callable[[WriterStore, date], int]


def decide_daily_run(calendar: BistCalendar, now_utc: datetime) -> DailyRunDecision:
    if now_utc.tzinfo is None or now_utc.utcoffset() != timedelta(0):
        raise ValueError("now_utc must be timezone-aware UTC")
    local_now = calendar.to_istanbul(now_utc)
    session = calendar.session(local_now.date())
    if session.confidence is not CalendarConfidence.AUTHORITATIVE:
        return DailyRunDecision(
            DailyRunStatus.FAILED_UNVERIFIED_CALENDAR,
            local_now.date(),
            session.reason,
            None,
        )
    if session.classification is SlotClassification.CLOSED:
        return DailyRunDecision(
            DailyRunStatus.SKIPPED_CLOSED,
            local_now.date(),
            session.reason,
            None,
        )
    if local_now.timetz().replace(tzinfo=None) < _POST_CLOSE_LOCAL:
        return DailyRunDecision(
            DailyRunStatus.SKIPPED_PRE_CLOSE,
            local_now.date(),
            "before-19:30-Europe/Istanbul",
            session.query_window,
        )
    if session.query_window is None:
        return DailyRunDecision(
            DailyRunStatus.FAILED_UNVERIFIED_CALENDAR,
            local_now.date(),
            "authoritative-open-session-has-no-query-window",
            None,
        )
    return DailyRunDecision(
        DailyRunStatus.READY,
        local_now.date(),
        session.reason,
        session.query_window,
    )


async def _default_recover(
    store: WriterStore,
    provider: CollectorProvider,
    calendar: BistCalendar,
    routes: tuple[SymbolRoute, ...],
    now_utc: datetime,
    recover_through_utc: datetime,
) -> RecoverySummary:
    reports: tuple[RecoveryReport, ...] = await startup_recovery(
        store,
        provider,
        calendar,
        routes,
        now_utc=now_utc,
        recover_through_utc=recover_through_utc,
    )
    return RecoverySummary(
        requests=sum(item.requests for item in reports),
        recovered_gaps=sum(item.recovered_gaps for item in reports),
        unresolved_gaps=sum(
            item.pending_gaps + item.provider_empty_gaps + item.unrecoverable_gaps
            for item in reports
        ),
    )


async def _default_sweep(
    store: WriterStore,
    provider: CollectorProvider,
    routes: tuple[SymbolRoute, ...],
    query_window: RequestWindow,
    now_utc: datetime,
) -> CollectionReport:
    return await Collector(
        provider,
        store,
        routes,
        config=CollectorConfig(max_concurrency=2),
    ).collect_once(now_utc=now_utc, request_window=query_window)


def _default_reconcile(store: WriterStore) -> int:
    report = ArchiveCompactor(store.data_root, store).reconcile()
    if report.failed:
        raise ArchiveError(f"{report.failed} pending archive publication(s) failed reconciliation")
    return report.recovered


def _default_compact(store: WriterStore, through_date: date) -> int:
    return len(ArchiveCompactor(store.data_root, store).compact_through(through_date))


class DailyOneShot:
    """One daily post-close workflow; every invocation owns one writer and then exits."""

    def __init__(
        self,
        data_root: Path,
        provider: CollectorProvider,
        calendar: BistCalendar,
        universe: UniverseSnapshot,
        *,
        reconcile: ReconcileRunner = _default_reconcile,
        recover: RecoveryRunner = _default_recover,
        sweep: SweepRunner = _default_sweep,
        compact: CompactRunner = _default_compact,
    ) -> None:
        self.data_root = Path(data_root)
        self.provider = provider
        self.calendar = calendar
        self.universe = universe
        self._reconcile = reconcile
        self._recover = recover
        self._sweep = sweep
        self._compact = compact
        self.routes = tuple(
            SymbolRoute(member.exchange_code, member.alias_for("yahoo_chart"))
            for member in universe.members
        )
        if not self.routes:
            raise ValueError("the daily one-shot requires a non-empty immutable universe")

    async def run(self, *, now_utc: datetime, dry_run: bool = False) -> DailyRunReport:
        decision = decide_daily_run(self.calendar, now_utc)
        if dry_run:
            return DailyRunReport(
                "dry-run",
                DailyRunStatus.DRY_RUN,
                decision.session_date,
                decision.reason,
                planned_symbols=len(self.routes),
            )

        run_id = uuid.uuid4().hex
        with WriterStore(self.data_root) as store:
            store.reconcile_started_runs(now_utc)
            store.start_collector_run(
                run_id,
                session_date=decision.session_date,
                attempted_at_utc=now_utc,
                calendar_snapshot_id=self.calendar.snapshot.snapshot_id,
                universe_snapshot_id=self.universe.snapshot_id,
            )
            try:
                self._reconcile(store)
                store.activate_universe(
                    self.universe,
                    activated_at_utc=now_utc,
                    activation_id=(
                        f"daily-{self.universe.snapshot_id}-{decision.session_date.isoformat()}"
                    ),
                )
                if decision.status is not DailyRunStatus.READY:
                    quick_check = store.quick_check()
                    store.finish_collector_run(
                        run_id,
                        status=decision.status.value,
                        reason=decision.reason,
                        completed_at_utc=datetime.now(UTC),
                        quick_check=quick_check,
                        health={"provider_calls": 0},
                    )
                    return DailyRunReport(
                        run_id,
                        decision.status,
                        decision.session_date,
                        decision.reason,
                        quick_check=quick_check,
                        planned_symbols=len(self.routes),
                    )

                assert decision.query_window is not None
                recovery = await self._recover(
                    store,
                    self.provider,
                    self.calendar,
                    self.routes,
                    now_utc,
                    decision.query_window.start_utc,
                )
                collection = await self._sweep(
                    store,
                    self.provider,
                    self.routes,
                    decision.query_window,
                    now_utc,
                )
                accounted = len(collection.completed_symbols) + len(collection.denied_symbols)
                complete = (
                    accounted == len(self.routes)
                    and len(collection.successful_symbols) == len(self.routes)
                    and not collection.cancelled_requests
                    and collection.max_fetch_concurrency <= 2
                    and collection.max_writer_concurrency <= 1
                )
                partitions = self._compact(store, decision.session_date) if complete else 0
                quick_check = store.quick_check()
                status = (
                    DailyRunStatus.SUCCEEDED
                    if complete and quick_check == "ok"
                    else DailyRunStatus.FAILED
                )
                reason = (
                    "daily-post-close-cycle-complete"
                    if status is DailyRunStatus.SUCCEEDED
                    else "incomplete-daily-sweep"
                )
                total_requests = recovery.requests + collection.request_attempts
                store.finish_collector_run(
                    run_id,
                    status=status.value,
                    reason=reason,
                    completed_at_utc=datetime.now(UTC),
                    request_count=total_requests,
                    successful_symbols=len(collection.successful_symbols),
                    denied_symbols=len(collection.denied_symbols),
                    recovered_gaps=recovery.recovered_gaps,
                    unresolved_gaps=recovery.unresolved_gaps,
                    partition_count=partitions,
                    quick_check=quick_check,
                    health={
                        "accounted_symbols": accounted,
                        "cancelled_requests": collection.cancelled_requests,
                        "max_fetch_concurrency": collection.max_fetch_concurrency,
                        "max_writer_concurrency": collection.max_writer_concurrency,
                    },
                )
                return DailyRunReport(
                    run_id,
                    status,
                    decision.session_date,
                    reason,
                    requests=total_requests,
                    successful_symbols=len(collection.successful_symbols),
                    denied_symbols=len(collection.denied_symbols),
                    recovered_gaps=recovery.recovered_gaps,
                    unresolved_gaps=recovery.unresolved_gaps,
                    partitions=partitions,
                    quick_check=quick_check,
                    planned_symbols=len(self.routes),
                )
            except BaseException as error:
                try:
                    store.finish_collector_run(
                        run_id,
                        status=DailyRunStatus.FAILED.value,
                        reason=f"{error.__class__.__name__}: {error}",
                        completed_at_utc=datetime.now(UTC),
                        quick_check=store.quick_check(),
                        health={"exception_type": error.__class__.__name__},
                    )
                except StoreError:
                    pass
                raise
