# Phase 3 Focused Confirmation

Reviewer session: `ses_03834f75effeZZa1No7tD39myV`
Model: `opencode-go/deepseek-v4-flash`
Agent: `plan`
Review mode: read-only; single focused confirmation

**Verdict: APPROVE.** All three prior corrections are correctly and minimally resolved. **No Critical, High, or Medium finding remains.**

## M1 — Schema-drift pause: bounded durable expiry/recovery, fail-closed preserved
- **Expiry is bounded and durable:** `backoff.py:25` `_SCHEMA_DRIFT_PAUSE = timedelta(minutes=30)`; `observe` sets `paused_until = observed_at_utc + 30min` when two symbols share a drift signature (`backoff.py:154-156`) and persists it via `save_provider_runtime` (`store.py:586-605`).
- **Fail-closed while unexpired:** `reserve_request` denies `provider-paused` whenever `paused_until_utc is None or attempted_at_utc < paused_until_utc` (`store.py:632-635`).
- **Durable recovery across reopen:** on expiry, the same `BEGIN IMMEDIATE` reservation transaction clears `paused_reason`/`paused_until_utc` before proceeding (`store.py:636-640`), so recovery is transactional, not a bypass.
- **Test evidence:** `test_schema_drift_pause_expires_durably_and_allows_a_probe_after_reopen` (`tests/unit/test_backoff.py:157-182`) — pause persisted; after reopen, denied at +29 min (`provider-paused`), allowed at +31 min, and `snapshot.paused_reason`/`paused_until_utc` are None. Present in collected tests; `lastfailed={}`.

## Additive migration from the committed Phase 1-2 schema
- `SCHEMA_VERSION = 3` (`store.py:19`); `MIGRATION_V3` is a single additive `ALTER TABLE provider_runtime_state ADD COLUMN paused_until_utc TEXT` (`store.py:405-407`); `apply_migrations` applies versions `range(current+1, 4)` sequentially (`store.py:518`).
- **Test evidence:** `test_version_one_database_upgrades_additively_and_repeatably` (`tests/unit/test_store.py:235-260`) — builds a v1 DB, upgrades to `user_version == 3` / `migration_count == 3`, preserves the sentinel table, verifies `request_attempts` exists, and confirms re-running `apply_migrations` is a no-op. Because the chain applies v2 then v3 in one transaction, the v3 `ALTER` runs against a v2-shaped schema — transitively covering the v2→v3 path from the committed foundation.

## L1 — collect-once distinguishes accounted/completed from successful
- `collector.py:345-346` adds a symbol to `successful` only when `OutcomeCode.SUCCESS in final_result.codes`; `CollectionReport.successful_symbols` is populated (`collector.py:371-373`), distinct from `completed_symbols`.
- `cli.py`: `ok = accounted == len(routes) and len(report.successful_symbols) == len(routes) and report.cancelled_requests == 0`, with `successful_symbols` surfaced in the JSON report — an all-error run yields `ok=False` and exit 1.
- **Test evidence:** `test_retry_exhaustion_stops_after_three_without_advancing_cursor` (`tests/integration/test_collector_once.py:146-169`) asserts `completed_symbols == ("ASELS.IS",)` but `successful_symbols == ()` for a fully-failed run; the normal run asserts `successful_symbols` equals all routes (`test_collector_once.py:95`).

All inspected code and tests are read-only evidence only; no files, processes, network, or Git actions were performed. The Phase 3 review finding set is now closed with no Critical/High/Medium items, and the CLI no longer reports success for a run whose fetches all failed.
