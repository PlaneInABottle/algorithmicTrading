# Phase 4 Focused Confirmation — M1

Reviewer session: `ses_03834f75effeZZa1No7tD39myV`
Model: `opencode-go/deepseek-v4-flash`
Agent: `plan`
Review mode: read-only; single focused confirmation

**Verdict: APPROVE.** No Critical, High, or Medium Phase 4 finding remains. The prior Medium M1 is correctly resolved test-first, and the fix does not regress recovery semantics.

## M1 verification

- **`fetch_one` honors the requested exact half-open UTC window:** `provider.py:432-458` validates the symbol, rejects non-positive/over-7-day windows (`provider.py:437-438`), then issues `period1=int(window.start_utc.timestamp())` and `period2=int(window.end_utc.timestamp())` with no `range` (`provider.py:452-453`); the returned `FetchResult` carries `request_window=window` and `EXACT_UTC_WINDOW` (`provider.py:481-488`). Test: `test_historical_fetch_honors_exact_bounded_period_window` (`test_provider_contract.py:244-269`) asserts both params, absence of `range`, and exact semantics via `MockTransport`.
- **Rejects more than seven days before any I/O:** the `ValueError` check at `provider.py:437-438` precedes client creation (`provider.py:439`); `test_historical_fetch_rejects_more_than_seven_days_without_request` (`test_provider_contract.py:272-287`) asserts the exception and `not requested`.
- **Bounded timeout/transport/outcome semantics retained:** `provider.py:459-467` (`httpx.TimeoutException` → `timeout_result` with the window), `provider.py:468-475` (`RequestError` → `_transport_result` with `EXACT_UTC_WINDOW`), and `provider.py:476-489` reuses the unchanged typed `parse_chart_response` (429/422/5xx/malformed/schema outcomes intact).
- **Enables past-session recovery without falsely recovering unmatched gaps:** `recovery.py:106` passes `task.window` (per-session, ≤7 days) to `fetch_one`; with `EXACT_UTC_WINDOW` semantics the cursor now advances for exact+success results (`recovery.py:119-130`, resolving the prior cursor-stagnation Low), and `_apply_result` transitions a gap to `RECOVERED` only when `gap.start_utc in bar_opens` (`recovery.py:180-187`), so unmatched minutes stay `pending`/`provider-empty` and are never falsely recovered. Recovery tests confirm oldest-first behavior, interruption idempotency, and startup cursor resume.
- **Bounded real-network evidence:** `test_one_symbol_historical_period_window_is_bounded_and_exact` makes one ASELS request over one session window, asserting exact semantics, one attempt, and no transport/timeout/retention outcome.

## Low observation

The confirmation noted no explicit strict-subset return test. `test_partial_window_return_recovers_only_matching_minutes` was added afterward and passes: one returned minute recovers while the unmatched minute stays pending, with exactly one stored bar.

Phase 4 is confirmed green with no remaining Critical/High/Medium finding. The reviewer did not review or recommend Phase 5 and performed no edits, migrations, network, or process actions.
