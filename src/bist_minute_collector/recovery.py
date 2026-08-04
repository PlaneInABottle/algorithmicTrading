from __future__ import annotations

import asyncio
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from datetime import datetime, timedelta

from .backoff import DurableRequestPolicy, RetryPolicy
from .calendar_engine import BistCalendar
from .collector import CollectorProvider, SymbolRoute
from .gaps import GapLedger, GapState, RecoveryPlanner, RecoveryTask
from .models import OutcomeCode, RequestWindowSemantics
from .store import CursorUpdate, IngestEnvelope, WriterStore

AsyncSleep = Callable[[float], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class RecoveryReport:
    requests: int
    recovered_gaps: int
    pending_gaps: int
    provider_empty_gaps: int
    unrecoverable_gaps: int
    denied_reason: str | None


class RecoveryEngine:
    """Bounded oldest-first recovery through the existing one-writer store."""

    def __init__(
        self,
        store: WriterStore,
        provider: CollectorProvider,
        calendar: BistCalendar,
        *,
        retry_policy: RetryPolicy | None = None,
        sleep: AsyncSleep = asyncio.sleep,
    ) -> None:
        self.store = store
        self.provider = provider
        self.calendar = calendar
        self.retry_policy = retry_policy or RetryPolicy()
        self.sleep = sleep

    async def recover(
        self,
        route: SymbolRoute,
        *,
        start_utc: datetime,
        end_utc: datetime,
        now_utc: datetime,
    ) -> RecoveryReport:
        planner = RecoveryPlanner(self.calendar)
        plan = planner.plan(
            source=self.provider.source,
            exchange_code=route.exchange_code,
            start_utc=start_utc,
            end_utc=end_utc,
            now_utc=now_utc,
        )
        ledger = GapLedger(self.store)
        for item in plan.gaps:
            existing = self.store.gap_current_row(item.gap.gap_id)
            if existing is not None:
                continue
            ledger.record(
                item.gap,
                item.state,
                item.cause,
                item.evidence_id,
                now_utc,
                recovery_cutoff_utc=item.recovery_cutoff_utc,
            )

        policy = DurableRequestPolicy(self.store, source=self.provider.source)
        request_count = 0
        denied_reason: str | None = None

        def is_active(gap_id: str) -> bool:
            row = self.store.gap_current_row(gap_id)
            if row is None:
                raise RuntimeError(f"planned gap {gap_id} has no durable state")
            return GapState(str(row["state"])) in {
                GapState.PENDING,
                GapState.PROVIDER_EMPTY,
            }

        for task in plan.tasks:
            active_gaps = tuple(gap for gap in task.gaps if is_active(gap.gap_id))
            if not active_gaps:
                continue
            final_result = None
            final_request_id = None
            for completed_attempt in range(1, self.retry_policy.max_attempts + 1):
                request_id = uuid.uuid4().hex
                attempted_at = now_utc + timedelta(microseconds=request_count)
                reservation = policy.reserve(request_id, route.provider_symbol, attempted_at)
                if not reservation.allowed:
                    denied_reason = reservation.reason
                    break
                request_count += 1
                for gap in active_gaps:
                    ledger.record_attempt(gap, request_id, attempted_at)
                result = await self.provider.fetch_one(route.provider_symbol, task.window)
                policy.observe(request_id, result, result.retrieved_at_utc)
                final_result = result
                final_request_id = request_id
                delay = self.retry_policy.retry_delay(result, completed_attempt=completed_attempt)
                if delay is None:
                    break
                await self.sleep(delay)
            if denied_reason is not None:
                break
            if final_result is None or final_request_id is None:
                continue
            final_result = replace(final_result, attempts=completed_attempt)
            exact = final_result.request_window_semantics is RequestWindowSemantics.EXACT_UTC_WINDOW
            successful = OutcomeCode.SUCCESS in final_result.codes
            cursor = None
            if exact and successful:
                cursor = CursorUpdate(
                    task.window.end_utc,
                    (
                        max(bar.bar_open_utc for bar in final_result.bars)
                        if final_result.bars
                        else None
                    ),
                )
            fetch_id = f"recovery-{final_request_id}"
            self.store.ingest(
                IngestEnvelope(
                    fetch_id=fetch_id,
                    exchange_code=route.exchange_code,
                    result=final_result,
                    cursor=cursor,
                )
            )
            self._apply_result(
                ledger,
                task,
                fetch_id,
                final_result.codes,
                frozenset(bar.bar_open_utc for bar in final_result.bars),
                now_utc,
            )
            if OutcomeCode.THROTTLED in final_result.codes:
                break

        current = tuple(
            item
            for item in ledger.list_current()
            if item.gap.source == self.provider.source
            and item.gap.exchange_code == route.exchange_code
            and start_utc <= item.gap.start_utc < end_utc
        )
        return RecoveryReport(
            request_count,
            sum(item.state is GapState.RECOVERED for item in current),
            sum(item.state is GapState.PENDING for item in current),
            sum(item.state is GapState.PROVIDER_EMPTY for item in current),
            sum(item.state is GapState.UNRECOVERABLE for item in current),
            denied_reason,
        )

    @staticmethod
    def _apply_result(
        ledger: GapLedger,
        task: RecoveryTask,
        fetch_id: str,
        codes: tuple[OutcomeCode, ...],
        bar_opens: frozenset[datetime],
        observed_at_utc: datetime,
    ) -> None:
        for gap in task.gaps:
            current = ledger.current(gap)
            if current.state not in {GapState.PENDING, GapState.PROVIDER_EMPTY}:
                continue
            if gap.start_utc in bar_opens:
                ledger.transition(
                    gap,
                    GapState.RECOVERED,
                    "complete-aligned-bar-returned",
                    fetch_id,
                    observed_at_utc,
                )
            elif OutcomeCode.RETENTION_LIMIT in codes:
                ledger.transition(
                    gap,
                    GapState.UNRECOVERABLE,
                    "provider-explicit-retention-rejection",
                    fetch_id,
                    observed_at_utc,
                )
            elif (
                (OutcomeCode.SOURCE_EMPTY in codes or OutcomeCode.SUCCESS in codes)
                and current.attempt_count >= 3
                and observed_at_utc >= task.window.end_utc + timedelta(minutes=45)
            ):
                ledger.transition(
                    gap,
                    GapState.PROVIDER_EMPTY,
                    "three-empty-overlaps-post-session",
                    fetch_id,
                    observed_at_utc,
                )


async def startup_recovery(
    store: WriterStore,
    provider: CollectorProvider,
    calendar: BistCalendar,
    routes: tuple[SymbolRoute, ...],
    *,
    now_utc: datetime,
) -> tuple[RecoveryReport, ...]:
    """Resume only uncovered time after each durable cursor, bounded by 28 days."""

    reports: list[RecoveryReport] = []
    engine = RecoveryEngine(store, provider, calendar)
    for route in routes:
        cursor = store.cursor(provider.source, route.exchange_code, 60)
        start = cursor.accounted_through_utc if cursor is not None else now_utc - timedelta(days=1)
        if start >= now_utc:
            continue
        reports.append(
            await engine.recover(
                route,
                start_utc=start,
                end_utc=now_utc,
                now_utc=now_utc,
            )
        )
    return tuple(reports)
