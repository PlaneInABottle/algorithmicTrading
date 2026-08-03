from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime

from .models import FetchResult, OutcomeCode
from .store import RequestReservation, WriterStore

_RETRYABLE_CODES = frozenset(
    {OutcomeCode.SERVER_ERROR, OutcomeCode.TIMEOUT, OutcomeCode.TRANSPORT_ERROR}
)
_SCHEMA_DRIFT_CODES = frozenset(
    {
        OutcomeCode.MALFORMED_JSON,
        OutcomeCode.ARRAY_LENGTH_MISMATCH,
        OutcomeCode.MISSING_METADATA,
        OutcomeCode.UNEXPECTED_GRANULARITY,
        OutcomeCode.UNEXPECTED_TIMEZONE,
        OutcomeCode.UNEXPECTED_CURRENCY,
        OutcomeCode.SYMBOL_MISMATCH,
    }
)
_THROTTLE_COOLDOWNS = (60, 120, 240, 480, 900)
_SCHEMA_DRIFT_PAUSE = timedelta(minutes=30)


@dataclass(frozen=True, slots=True)
class PolicySnapshot:
    cooldown_until_utc: datetime | None
    circuit_open_until_utc: datetime | None
    throttle_streak: int
    paused_reason: str | None
    paused_until_utc: datetime | None


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_attempts: int = 3
    delays_seconds: tuple[float, ...] = (2.0, 10.0)

    def retry_delay(self, result: FetchResult, *, completed_attempt: int) -> float | None:
        if completed_attempt <= 0:
            raise ValueError("completed_attempt must be positive")
        if completed_attempt >= self.max_attempts:
            return None
        if not any(code in _RETRYABLE_CODES for code in result.codes):
            return None
        index = completed_attempt - 1
        if index >= len(self.delays_seconds):
            return None
        return self.delays_seconds[index]


def parse_retry_after(value: str | None, *, now_utc: datetime) -> datetime | None:
    if value is None:
        return None
    if now_utc.tzinfo is None or now_utc.utcoffset() != timedelta(0):
        raise ValueError("now_utc must be timezone-aware UTC")
    stripped = value.strip()
    try:
        seconds = int(stripped)
    except ValueError:
        try:
            parsed = parsedate_to_datetime(stripped)
        except (TypeError, ValueError, OverflowError):
            return None
        if parsed.tzinfo is None:
            return None
        return max(now_utc, parsed.astimezone(UTC))
    if seconds < 0:
        return None
    return now_utc + timedelta(seconds=seconds)


class DurableRequestPolicy:
    """Persist request budgets and provider-wide safety state through one WriterStore."""

    def __init__(
        self,
        store: WriterStore,
        *,
        source: str,
        hourly_cap: int = 360,
        session_cap: int = 3_000,
    ) -> None:
        if not source:
            raise ValueError("source is required")
        self.store = store
        self.source = source
        self.hourly_cap = hourly_cap
        self.session_cap = session_cap

    def reserve(
        self, request_id: str, provider_symbol: str, attempted_at_utc: datetime
    ) -> RequestReservation:
        return self.store.reserve_request(
            request_id,
            self.source,
            provider_symbol,
            attempted_at_utc,
            hourly_cap=self.hourly_cap,
            session_cap=self.session_cap,
        )

    def cancel(self, request_id: str, completed_at_utc: datetime) -> None:
        self.store.cancel_request(request_id, completed_at_utc)

    def observe(
        self, request_id: str, result: FetchResult, observed_at_utc: datetime
    ) -> PolicySnapshot:
        codes = result.codes
        outcome_code = codes[0].value if codes else "unknown"
        self.store.complete_request(request_id, outcome_code, observed_at_utc)
        current = self.store.provider_runtime(self.source)
        cooldown = current.cooldown_until_utc
        circuit = current.circuit_open_until_utc
        streak = current.throttle_streak
        paused = current.paused_reason
        paused_until = current.paused_until_utc

        if OutcomeCode.THROTTLED in codes:
            streak += 1
            supplied = next(
                (
                    parse_retry_after(outcome.retry_after, now_utc=observed_at_utc)
                    for outcome in result.outcomes
                    if outcome.code is OutcomeCode.THROTTLED
                ),
                None,
            )
            fallback_seconds = _THROTTLE_COOLDOWNS[min(streak - 1, len(_THROTTLE_COOLDOWNS) - 1)]
            candidate = supplied or observed_at_utc + timedelta(seconds=fallback_seconds)
            cooldown = max(filter(None, (cooldown, candidate)), default=candidate)
            recent = self.store.outcome_count_since(
                self.source,
                OutcomeCode.THROTTLED.value,
                observed_at_utc - timedelta(minutes=2),
            )
            if recent >= 3:
                candidate_circuit = observed_at_utc + timedelta(minutes=30)
                circuit = max(filter(None, (circuit, candidate_circuit)), default=candidate_circuit)
        elif OutcomeCode.SUCCESS in codes and (cooldown is None or observed_at_utc >= cooldown):
            streak = 0

        schema_code = next((code for code in codes if code in _SCHEMA_DRIFT_CODES), None)
        if schema_code is not None:
            affected = self.store.record_schema_drift(
                self.source,
                schema_code.value,
                self.store.request_provider_symbol(request_id),
                observed_at_utc,
            )
            if affected >= 2:
                paused = f"schema-drift:{schema_code.value}"
                paused_until = observed_at_utc + _SCHEMA_DRIFT_PAUSE

        self.store.save_provider_runtime(
            self.source,
            cooldown_until_utc=cooldown,
            circuit_open_until_utc=circuit,
            throttle_streak=streak,
            paused_reason=paused,
            paused_until_utc=paused_until,
            updated_at_utc=observed_at_utc,
        )
        return PolicySnapshot(cooldown, circuit, streak, paused, paused_until)

    def snapshot(self, now_utc: datetime) -> PolicySnapshot:
        if now_utc.tzinfo is None or now_utc.utcoffset() != timedelta(0):
            raise ValueError("now_utc must be timezone-aware UTC")
        current = self.store.provider_runtime(self.source)
        return PolicySnapshot(
            current.cooldown_until_utc,
            current.circuit_open_until_utc,
            current.throttle_streak,
            current.paused_reason,
            current.paused_until_utc,
        )
