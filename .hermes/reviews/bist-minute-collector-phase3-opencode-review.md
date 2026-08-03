# Formal Phase 3 Implementation Review — `bist-minute-collector`

Reviewer session: `ses_03834f75effeZZa1No7tD39myV`
Model: `opencode-go/deepseek-v4-flash`
Agent: `plan`
Review mode: read-only

**Scope:** Phase 3 (monotonic scheduler, bounded concurrency, single-writer actor, overlap/backoff, durable request policy) plus the reconciled Phase 2 Low items. Evidence: `scheduler.py`, `collector.py`, `backoff.py`, updated `store.py`/`archive.py`/`provider.py`/`cli.py`, `test_scheduler.py`, `test_backoff.py`, `test_collector_once.py`, `test_process_lock.py`, `tests/support/phase3_process.py`, updated unit/integration tests, pyproject, Git state, pytest cache (`lastfailed={}`, all Phase 3 tests collected).

## Verdict: REVISE (targeted)

**One Medium finding remains (M1). No Critical or High findings remain.**

---

### Medium

**M1 — Confirmed operational dead-end: the schema-drift pause is permanent with no expiry or recovery path.**
- **Evidence:** `backoff.py:117` seeds `paused = current.paused_reason`; `backoff.py:143-152` only *sets* `paused` to a drift string (never clears it); `backoff.py:154-161` persists it via `save_provider_runtime` (`store.py:556-586`); `reserve_request` denies every reservation while `paused_reason` is non-null (`store.py:612-615`). No code path clears the pause, and `cli.py` has no reset command. Because reservations are denied while paused, `observe` can never run again to update state — the pause is a terminal self-denial until the DB is edited by hand.
- **Rationale:** Two malformed responses for two symbols permanently block every future `collect-once` run and all symbols, with no operator-facing recovery. This is conservative fail-closed behavior whose recovery path was never specified or implemented.
- **Smallest test-first correction:** RED test — after a schema-drift pause is persisted, a later time window (or an explicit `reset-policy` CLI action) must permit a reservation again; then implement a bounded pause (e.g., a `paused_expires_utc` column checked in `reserve_request`, or a `reset-policy` subcommand) and wire the test.

---

### Low (optional; none requires another review)

- **L1 — CLI `ok` conflates "accounted" with "successful".** `cli.py:159-160`: `ok = accounted == len(routes) and cancelled == 0`, where symbols whose retries all fail are added to `completed_symbols` after their failed provenance is ingested (`collector.py:342`). A fully network-down run returns exit 0 with zero bars. *Test-first:* RED test asserting a distinct success signal (or non-zero exit) when no symbol yields a SUCCESS outcome.
- **L2 — Crash-left `request_attempts` rows are never reconciled and inflate caps.** Rows in `started` state after a crash between `reserve` and `observe` persist forever and count toward the hourly/session caps (`store.py:628-646`, no state filter). Transient cap inflation self-corrects within an hour and no data/cursor is lost, but the audit trail accumulates stale `started` rows. *Test-first:* RED test simulating a kill after reserve, reopening, asserting stale rows are reconciled and caps are not inflated; add startup reconciliation (before unattended operation).
- **L3 — `fetch_one` discards the planner's overlap window.** `provider.py:426-431` (`del window`) always fetches `range=1d`; the collector's `plan_overlap_window` result (`collector.py:238-243`) is therefore advisory for the real provider, and two-hour overlap is satisfied only because `range=1d` returns the whole day. No data loss now. *Test-first:* assert a window-honoring provider receives the planned window, or document the window as advisory until a licensed window-scoped provider exists.
- **L4 — No raw response bodies are retained during collection.** `collector.py:331-341` passes `retained_response=None`, so plan §4.5 body-evidence is not exercised by the collector. Scope note for the later retention phase; not a Phase 3 defect.
- **L5 — Cap accounting uses synthetic attempt timestamps.** `collector.py:250` (`cycle_now + microseconds(request_attempts)`) drives the persisted hourly/session caps; timestamps are planned, not actual request times. Within-cycle error is negligible. Minor fidelity note.
- **L6 — Cancellation during observe/ingest is untested.** The `CancelledError` handler (`collector.py:286-295`) covers only the fetch await; cancellation while awaiting `_Observe`/`_Ingest` (`collector.py:299-306`, `331-341`) propagates without a terminal request state (ties to L2) and is not covered by `test_process_lock.py` (which cancels a blocking fetch). *Test-first:* extend the shutdown scenario to cancel during observe/ingest and assert terminal accounting.

---

### Verified correct (no finding)

Monotonic-cadence/UTC-metadata scheduling with no overlap and a correct finish token (`scheduler.py`); global fetch concurrency ≤ 2 with a single writer actor and FIFO futures (`collector.py`); full-writer-queue backpressure that drops no results; fair symbol rotation and two-hour/initial-one-day overlap planning; durable `Retry-After` cooldown, 30-minute circuit, and hourly/session caps that survive reopen (`backoff.py`/`store.py` + `test_backoff.py`); bounded 3-attempt 5xx/timeout/transport retries with 2/10 s delays; schema-drift pause after two symbols vs per-symbol row-issue isolation (`test_backoff.py:118-154`, `test_collector_once.py:242-293`); deadline-bounded shutdown with cancelled requests marked `cancelled` and no cursor/provenance advance; real subprocess two-process exclusion, SIGTERM graceful cancel, and SIGKILL mid-transaction rollback with `quick_check=ok` (`test_process_lock.py`, `tests/support/phase3_process.py`); collect-once request accounting (`request_attempt_count` matches attempts); additive v1→v2 migration with `ALTER`/`CREATE` and repeatable upgrade (`test_version_one_database_upgrades_additively_and_repeatably`); reader `busy_timeout=5000` (`store.py:426`); bounded failed-temp cleanup with `temp_removed_at_utc`/reason that preserves active evidence (`archive.py:321-356` + test); dynamic temp/WAL/publication headroom guard (`archive.py:204-234` + test); `force_rewrite` removed with a regression test; universe-validated, dry-run network-free `collect-once` CLI; no new dependencies beyond httpx+pyarrow; no secrets; Phase-3-only modules (no gaps engine, systemd unit, or daemon); no overengineering beyond the plan's RED-test-driven machinery.

**Git boundary note (factual):** `main` now has one foundation commit (`d098be5`); all Phase 3 work is uncommitted (modified + untracked); no remote. Nothing is staged or pushed.

Apply M1 (test-first) before unattended operation and optionally L1–L6; Phase 3 is otherwise correct and within scope. No Phase 4 implementation is recommended here.
