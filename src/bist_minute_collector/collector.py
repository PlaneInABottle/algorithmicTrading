from __future__ import annotations

import asyncio
import uuid
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import Protocol, cast

from .backoff import DurableRequestPolicy, PolicySnapshot, RetryPolicy
from .models import FetchResult, OutcomeCode, RequestWindow
from .provider import timeout_result
from .scheduler import FairSymbolOrder, plan_overlap_window
from .store import CursorUpdate, IngestEnvelope, IngestReport, RequestReservation, WriterStore

AsyncSleep = Callable[[float], Awaitable[None]]


class CollectorProvider(Protocol):
    source: str

    async def fetch_one(self, symbol: str, window: RequestWindow) -> FetchResult: ...


@dataclass(frozen=True, slots=True)
class SymbolRoute:
    exchange_code: str
    provider_symbol: str

    def __post_init__(self) -> None:
        if not self.exchange_code.endswith(".E") or not self.provider_symbol.endswith(".IS"):
            raise ValueError("canonical .E and provider .IS symbols are required")


@dataclass(frozen=True, slots=True)
class CollectorConfig:
    max_concurrency: int = 2
    writer_queue_size: int = 16
    request_timeout_seconds: float = 15.0
    shutdown_timeout_seconds: float = 45.0
    hourly_request_cap: int = 360
    session_request_cap: int = 3_000
    writer_delay_seconds: float = 0.0

    def __post_init__(self) -> None:
        if not 1 <= self.max_concurrency <= 2:
            raise ValueError("max_concurrency must be one or two")
        if self.writer_queue_size <= 0:
            raise ValueError("writer_queue_size must be positive")
        if (
            self.request_timeout_seconds <= 0
            or self.shutdown_timeout_seconds <= 0
            or self.hourly_request_cap <= 0
            or self.session_request_cap <= 0
            or self.writer_delay_seconds < 0
        ):
            raise ValueError("timeouts/caps must be positive and writer delay nonnegative")


@dataclass(frozen=True, slots=True)
class CollectionReport:
    completed_symbols: tuple[str, ...]
    successful_symbols: tuple[str, ...]
    isolated_symbols: tuple[str, ...]
    denied_symbols: tuple[str, ...]
    request_attempts: int
    cancelled_requests: int
    queue_full_events: int
    max_fetch_concurrency: int
    max_writer_concurrency: int
    adapter_paused_reason: str | None


@dataclass(slots=True)
class _Authorize:
    request_id: str
    symbol: str
    attempted_at_utc: datetime
    future: asyncio.Future[RequestReservation]


@dataclass(slots=True)
class _Observe:
    request_id: str
    result: FetchResult
    observed_at_utc: datetime
    future: asyncio.Future[PolicySnapshot]


@dataclass(slots=True)
class _Cancel:
    request_id: str
    observed_at_utc: datetime
    future: asyncio.Future[None]


@dataclass(slots=True)
class _Ingest:
    envelope: IngestEnvelope
    future: asyncio.Future[IngestReport]


_WriterCommand = _Authorize | _Observe | _Cancel | _Ingest
_STOP = object()


class Collector:
    """One bounded collection cycle with two fetchers and exactly one writer actor."""

    def __init__(
        self,
        provider: CollectorProvider,
        store: WriterStore,
        routes: Sequence[SymbolRoute],
        *,
        config: CollectorConfig | None = None,
        retry_policy: RetryPolicy | None = None,
        sleep: AsyncSleep = asyncio.sleep,
    ) -> None:
        self.provider = provider
        self.store = store
        self.routes = tuple(routes)
        if not self.routes or len({route.provider_symbol for route in self.routes}) != len(
            self.routes
        ):
            raise ValueError("routes must be non-empty with distinct provider symbols")
        self.config = config or CollectorConfig()
        self.retry_policy = retry_policy or RetryPolicy()
        self._sleep = sleep
        self._fair_order = FairSymbolOrder(tuple(route.provider_symbol for route in self.routes))
        self._route_by_symbol = {route.provider_symbol: route for route in self.routes}
        self._running = False
        self._shutdown_requested = False
        self._worker_tasks: tuple[asyncio.Task[None], ...] = ()
        self._cycle_done: asyncio.Event | None = None

    def request_shutdown(self) -> None:
        self._shutdown_requested = True
        for task in self._worker_tasks:
            task.cancel()

    async def shutdown(self, *, timeout_seconds: float | None = None) -> None:
        self.request_shutdown()
        cycle_done = self._cycle_done
        if cycle_done is None:
            return
        timeout = timeout_seconds or self.config.shutdown_timeout_seconds
        await asyncio.wait_for(cycle_done.wait(), timeout=timeout)

    async def collect_once(self, *, now_utc: datetime | None = None) -> CollectionReport:
        if self._running:
            raise RuntimeError("a collection cycle is already active")
        self._running = True
        self._shutdown_requested = False
        self._cycle_done = asyncio.Event()
        cycle_now = now_utc or datetime.now(UTC)
        if cycle_now.tzinfo is None or cycle_now.utcoffset() != timedelta(0):
            self._running = False
            self._cycle_done.set()
            raise ValueError("now_utc must be timezone-aware UTC")

        ordered_symbols = self._fair_order.next_cycle()
        writer_queue: asyncio.Queue[_WriterCommand | object] = asyncio.Queue(
            maxsize=self.config.writer_queue_size
        )
        policy = DurableRequestPolicy(
            self.store,
            source=self.provider.source,
            hourly_cap=self.config.hourly_request_cap,
            session_cap=self.config.session_request_cap,
        )
        completed: set[str] = set()
        successful: set[str] = set()
        isolated: set[str] = set()
        denied: set[str] = set()
        request_attempts = 0
        cancelled_requests = 0
        queue_full_events = 0
        active_fetches = 0
        max_fetches = 0
        active_writers = 0
        max_writers = 0

        async def ask(command: _WriterCommand) -> object:
            nonlocal queue_full_events
            if writer_queue.full():
                queue_full_events += 1
            await writer_queue.put(command)
            return await command.future

        async def writer_actor() -> None:
            nonlocal active_writers, max_writers
            while True:
                command = await writer_queue.get()
                try:
                    if command is _STOP:
                        return
                    typed = cast(_WriterCommand, command)
                    active_writers += 1
                    max_writers = max(max_writers, active_writers)
                    try:
                        if self.config.writer_delay_seconds:
                            await asyncio.sleep(self.config.writer_delay_seconds)
                        if isinstance(typed, _Authorize):
                            result: object = policy.reserve(
                                typed.request_id,
                                typed.symbol,
                                typed.attempted_at_utc,
                            )
                        elif isinstance(typed, _Observe):
                            result = policy.observe(
                                typed.request_id,
                                typed.result,
                                typed.observed_at_utc,
                            )
                        elif isinstance(typed, _Cancel):
                            policy.cancel(typed.request_id, typed.observed_at_utc)
                            result = None
                        else:
                            result = self.store.ingest(typed.envelope)
                        if not typed.future.done():
                            typed.future.set_result(result)  # type: ignore[arg-type]
                    except BaseException as error:
                        if not typed.future.done():
                            typed.future.set_exception(error)
                    finally:
                        active_writers -= 1
                finally:
                    writer_queue.task_done()

        work_queue: asyncio.Queue[SymbolRoute | None] = asyncio.Queue()
        for symbol in ordered_symbols:
            work_queue.put_nowait(self._route_by_symbol[symbol])
        for _ in range(self.config.max_concurrency):
            work_queue.put_nowait(None)

        async def collect_route(route: SymbolRoute) -> None:
            nonlocal active_fetches, max_fetches, request_attempts, cancelled_requests
            cursor = self.store.cursor(self.provider.source, route.exchange_code, 60)
            window = plan_overlap_window(
                now_utc=cycle_now,
                accounted_through_utc=(
                    cursor.accounted_through_utc if cursor is not None else None
                ),
            )
            final_result: FetchResult | None = None
            final_request_id: str | None = None
            for completed_attempt in range(1, self.retry_policy.max_attempts + 1):
                if self._shutdown_requested:
                    return
                request_id = uuid.uuid4().hex
                attempted_at = cycle_now + timedelta(microseconds=request_attempts)
                reservation = cast(
                    RequestReservation,
                    await ask(
                        _Authorize(
                            request_id,
                            route.provider_symbol,
                            attempted_at,
                            asyncio.get_running_loop().create_future(),
                        )
                    ),
                )
                if not reservation.allowed:
                    denied.add(route.provider_symbol)
                    return
                request_attempts += 1
                active_fetches += 1
                max_fetches = max(max_fetches, active_fetches)
                try:
                    try:
                        result = await asyncio.wait_for(
                            self.provider.fetch_one(route.provider_symbol, window),
                            timeout=self.config.request_timeout_seconds,
                        )
                    except TimeoutError:
                        result = timeout_result(
                            source=self.provider.source,
                            symbol=route.provider_symbol,
                            interval="1m",
                            request_window=window,
                            retrieved_at_utc=attempted_at,
                            message=(
                                f"request exceeded {self.config.request_timeout_seconds:g} "
                                "second collector timeout"
                            ),
                        )
                except asyncio.CancelledError:
                    cancelled_requests += 1
                    await ask(
                        _Cancel(
                            request_id,
                            attempted_at,
                            asyncio.get_running_loop().create_future(),
                        )
                    )
                    return
                finally:
                    active_fetches -= 1

                await ask(
                    _Observe(
                        request_id,
                        result,
                        result.retrieved_at_utc,
                        asyncio.get_running_loop().create_future(),
                    )
                )
                final_result = result
                final_request_id = request_id
                delay = self.retry_policy.retry_delay(result, completed_attempt=completed_attempt)
                if delay is None:
                    break
                try:
                    await self._sleep(delay)
                except asyncio.CancelledError:
                    return

            if final_result is None or final_request_id is None or self._shutdown_requested:
                return
            final_result = replace(final_result, attempts=completed_attempt)
            if any(
                code in {OutcomeCode.INVALID_ROW, OutcomeCode.PARTIAL_ROW}
                for code in final_result.codes
            ):
                isolated.add(route.provider_symbol)
            cursor_update = None
            if OutcomeCode.SUCCESS in final_result.codes and final_result.bars:
                cursor_update = CursorUpdate(
                    accounted_through_utc=final_result.request_window.end_utc,
                    last_complete_bar_open_utc=max(bar.bar_open_utc for bar in final_result.bars),
                )
            await ask(
                _Ingest(
                    IngestEnvelope(
                        fetch_id=f"fetch-{final_request_id}",
                        exchange_code=route.exchange_code,
                        result=final_result,
                        cursor=cursor_update,
                    ),
                    asyncio.get_running_loop().create_future(),
                )
            )
            completed.add(route.provider_symbol)
            if OutcomeCode.SUCCESS in final_result.codes:
                successful.add(route.provider_symbol)

        async def fetch_worker() -> None:
            while True:
                route = await work_queue.get()
                try:
                    if route is None or self._shutdown_requested:
                        return
                    await collect_route(route)
                finally:
                    work_queue.task_done()

        actor = asyncio.create_task(writer_actor())
        self._worker_tasks = tuple(
            asyncio.create_task(fetch_worker()) for _ in range(self.config.max_concurrency)
        )
        try:
            await asyncio.gather(*self._worker_tasks)
            await writer_queue.put(_STOP)
            await actor
            snapshot = policy.snapshot(cycle_now)
            return CollectionReport(
                completed_symbols=tuple(
                    symbol for symbol in ordered_symbols if symbol in completed
                ),
                successful_symbols=tuple(
                    symbol for symbol in ordered_symbols if symbol in successful
                ),
                isolated_symbols=tuple(symbol for symbol in ordered_symbols if symbol in isolated),
                denied_symbols=tuple(symbol for symbol in ordered_symbols if symbol in denied),
                request_attempts=request_attempts,
                cancelled_requests=cancelled_requests,
                queue_full_events=queue_full_events,
                max_fetch_concurrency=max_fetches,
                max_writer_concurrency=max_writers,
                adapter_paused_reason=snapshot.paused_reason,
            )
        finally:
            for task in self._worker_tasks:
                if not task.done():
                    task.cancel()
            if not actor.done():
                try:
                    writer_queue.put_nowait(_STOP)
                except asyncio.QueueFull:
                    actor.cancel()
                await asyncio.gather(actor, return_exceptions=True)
            self._worker_tasks = ()
            self._running = False
            assert self._cycle_done is not None
            self._cycle_done.set()
