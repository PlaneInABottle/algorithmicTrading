from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

from bist_minute_collector.backoff import DurableRequestPolicy, RetryPolicy, parse_retry_after
from bist_minute_collector.models import FetchResult, OutcomeCode, ProviderOutcome
from bist_minute_collector.store import WriterStore
from tests.helpers import fetch_result

NOW = datetime(2026, 8, 3, 10, 0, tzinfo=UTC)


def outcome_result(code: OutcomeCode, *, retry_after: str | None = None) -> FetchResult:
    base = fetch_result(bars=(), outcome=code)
    return replace(
        base,
        outcomes=(ProviderOutcome(code, f"synthetic {code.value}", retry_after=retry_after),),
    )


def test_retry_policy_bounds_5xx_timeout_and_transport_attempts() -> None:
    policy = RetryPolicy()

    for code in (OutcomeCode.SERVER_ERROR, OutcomeCode.TIMEOUT, OutcomeCode.TRANSPORT_ERROR):
        result = outcome_result(code)
        assert policy.retry_delay(result, completed_attempt=1) == 2.0
        assert policy.retry_delay(result, completed_attempt=2) == 10.0
        assert policy.retry_delay(result, completed_attempt=3) is None

    assert policy.retry_delay(outcome_result(OutcomeCode.THROTTLED), completed_attempt=1) is None
    assert policy.retry_delay(fetch_result(), completed_attempt=1) is None


def test_retry_after_accepts_seconds_and_http_date_without_guessing() -> None:
    assert parse_retry_after("120", now_utc=NOW) == NOW + timedelta(seconds=120)
    assert parse_retry_after("Mon, 03 Aug 2026 10:05:00 GMT", now_utc=NOW) == NOW + timedelta(
        minutes=5
    )
    assert parse_retry_after("nonsense", now_utc=NOW) is None


def test_retry_after_cooldown_and_circuit_survive_reopen(tmp_path: Path) -> None:
    with WriterStore(tmp_path) as store:
        policy = DurableRequestPolicy(store, source="yahoo_chart")
        request_ids = []
        for index, symbol in enumerate(("ASELS.IS", "THYAO.IS", "AKBNK.IS")):
            request_id = f"throttle-{index}"
            request_ids.append(request_id)
            assert policy.reserve(request_id, symbol, NOW + timedelta(seconds=index)).allowed
        for index, request_id in enumerate(request_ids):
            policy.observe(
                request_id,
                outcome_result(OutcomeCode.THROTTLED, retry_after="120"),
                NOW + timedelta(seconds=10 + index),
            )
        snapshot = policy.snapshot(NOW + timedelta(seconds=20))
        assert snapshot.circuit_open_until_utc == NOW + timedelta(minutes=30, seconds=12)
        assert snapshot.cooldown_until_utc is not None
        assert snapshot.cooldown_until_utc >= NOW + timedelta(seconds=132)

    with WriterStore(tmp_path) as reopened:
        policy = DurableRequestPolicy(reopened, source="yahoo_chart")
        denied = policy.reserve("after-restart", "TUPRS.IS", NOW + timedelta(minutes=1))
        assert not denied.allowed
        assert denied.reason == "provider-circuit-open"


def test_missing_retry_after_uses_persisted_bounded_fallback(tmp_path: Path) -> None:
    with WriterStore(tmp_path) as store:
        policy = DurableRequestPolicy(store, source="yahoo_chart")
        assert policy.reserve("throttle", "ASELS.IS", NOW).allowed
        snapshot = policy.observe(
            "throttle",
            outcome_result(OutcomeCode.THROTTLED),
            NOW + timedelta(seconds=1),
        )
        assert snapshot.cooldown_until_utc == NOW + timedelta(seconds=61)

    with WriterStore(tmp_path) as reopened:
        denied = DurableRequestPolicy(reopened, source="yahoo_chart").reserve(
            "during-fallback", "THYAO.IS", NOW + timedelta(seconds=30)
        )
        assert not denied.allowed
        assert denied.reason == "provider-cooldown"


def test_request_hour_and_session_caps_are_atomic_and_persisted(tmp_path: Path) -> None:
    with WriterStore(tmp_path) as store:
        policy = DurableRequestPolicy(
            store,
            source="yahoo_chart",
            hourly_cap=2,
            session_cap=3,
        )
        assert policy.reserve("one", "ASELS.IS", NOW).allowed
        assert policy.reserve("two", "THYAO.IS", NOW + timedelta(minutes=1)).allowed

    with WriterStore(tmp_path) as reopened:
        policy = DurableRequestPolicy(
            reopened,
            source="yahoo_chart",
            hourly_cap=2,
            session_cap=3,
        )
        denied_hour = policy.reserve("three", "AKBNK.IS", NOW + timedelta(minutes=2))
        assert not denied_hour.allowed
        assert denied_hour.reason == "rolling-hour-cap"
        assert reopened.request_attempt_count() == 2

        assert policy.reserve("three", "AKBNK.IS", NOW + timedelta(hours=2)).allowed
        denied_session = policy.reserve("four", "TUPRS.IS", NOW + timedelta(hours=2, minutes=1))
        assert not denied_session.allowed
        assert denied_session.reason == "exchange-session-cap"


def test_common_schema_drift_pauses_after_two_symbols_but_row_issue_does_not(
    tmp_path: Path,
) -> None:
    with WriterStore(tmp_path) as store:
        policy = DurableRequestPolicy(store, source="yahoo_chart")

        assert policy.reserve("bad-one", "ASELS.IS", NOW).allowed
        first = policy.observe(
            "bad-one",
            outcome_result(OutcomeCode.MISSING_METADATA),
            NOW + timedelta(seconds=1),
        )
        assert first.paused_reason is None

        assert policy.reserve("row-issue", "AKBNK.IS", NOW + timedelta(seconds=2)).allowed
        accepted = fetch_result()
        accepted_with_issue = replace(
            accepted,
            outcomes=(
                *accepted.outcomes,
                ProviderOutcome(OutcomeCode.INVALID_ROW, "one symbol-specific bad row"),
            ),
        )
        isolated = policy.observe("row-issue", accepted_with_issue, NOW + timedelta(seconds=3))
        assert isolated.paused_reason is None

        assert policy.reserve("bad-two", "THYAO.IS", NOW + timedelta(seconds=4)).allowed
        second = policy.observe(
            "bad-two",
            outcome_result(OutcomeCode.MISSING_METADATA),
            NOW + timedelta(seconds=5),
        )
        assert second.paused_reason == "schema-drift:missing-metadata"

        denied = policy.reserve("paused", "TUPRS.IS", NOW + timedelta(seconds=6))
        assert not denied.allowed
        assert denied.reason == "provider-paused"


def test_schema_drift_pause_expires_durably_and_allows_a_probe_after_reopen(
    tmp_path: Path,
) -> None:
    with WriterStore(tmp_path) as store:
        policy = DurableRequestPolicy(store, source="yahoo_chart")
        for index, symbol in enumerate(("ASELS.IS", "THYAO.IS")):
            request_id = f"schema-{index}"
            attempted_at = NOW + timedelta(seconds=index * 2)
            assert policy.reserve(request_id, symbol, attempted_at).allowed
            policy.observe(
                request_id,
                outcome_result(OutcomeCode.MISSING_METADATA),
                attempted_at + timedelta(seconds=1),
            )

    with WriterStore(tmp_path) as reopened:
        policy = DurableRequestPolicy(reopened, source="yahoo_chart")
        still_paused = policy.reserve("before-expiry", "AKBNK.IS", NOW + timedelta(minutes=29))
        assert not still_paused.allowed
        assert still_paused.reason == "provider-paused"

        probe = policy.reserve("after-expiry", "AKBNK.IS", NOW + timedelta(minutes=31))
        assert probe.allowed
        snapshot = policy.snapshot(NOW + timedelta(minutes=31))
        assert snapshot.paused_reason is None
        assert snapshot.paused_until_utc is None
