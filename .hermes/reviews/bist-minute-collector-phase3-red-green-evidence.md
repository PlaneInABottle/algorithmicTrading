# Phase 3 RED/GREEN evidence

Date: 2026-08-03
Scope: Phase 3 scheduler/single-cycle collection plus the explicitly required deferred Phase 2 Low items. No daemon, service, gap engine, persistent data root, or Phase 4 code was created.

## RED

1. The exact required first command, `uv run pytest tests/unit/test_scheduler.py tests/unit/test_backoff.py -q`, initially exited 4 because both required test files did not exist.
2. After writing the tests first, the same command failed during collection because `bist_minute_collector.scheduler` and `bist_minute_collector.backoff` did not exist.
3. Phase 3 integration tests initially failed because `bist_minute_collector.collector` did not exist.
4. Deferred Phase 2 Low-item tests initially produced five failures: schema v2/additive upgrade was absent, dynamic headroom was absent, `force_rewrite` remained in the public signature, failed-temp retention/accounting was absent, and OS-level process-kill behavior was not covered.
5. Review M1 RED: `test_schema_drift_pause_expires_durably_and_allows_a_probe_after_reopen` failed because a reopened store returned `provider-paused` after the intended expiry.
6. Review L1 RED: exhausted 5xx/timeout tests failed because `CollectionReport` had no distinct `successful_symbols` evidence.

## GREEN

- `uv sync --frozen`: 21 packages audited, lock unchanged.
- `uv run ruff format --check .`: 28 files formatted.
- `uv run ruff check .`: passed.
- `uv run mypy`: passed for 28 source files.
- `uv run pytest tests/unit/test_scheduler.py tests/unit/test_backoff.py -q`: 11 passed.
- `uv run pytest tests/integration/test_collector_once.py tests/integration/test_process_lock.py -q`: 12 passed.
- `uv run pytest tests/unit/test_store.py tests/unit/test_archive.py -q`: 20 passed before review; covered again by the final full suite.
- `uv run pytest tests/integration/test_crash_consistency.py -q`: 6 passed before review; covered again by the final full suite.
- `uv run pytest -m 'not network' -q`: 102 passed, 1 opt-in network test deselected.
- `uv run python3 -m compileall -q src tests`: passed.
- `git diff --check`: passed.

## Failure injection and process evidence

- Duplicate/out-of-order/corrected overlapping fetches converge through the Phase 2 idempotent single-writer store; corrections create revisions.
- 429 with and without `Retry-After`, 5xx, timeout, transport errors, malformed common schema, per-symbol invalid rows, slow symbols, and a full writer queue are deterministic test cases.
- Retry budget is three total attempts with 2 s and 10 s delays; 429 is not retried in-cycle.
- Two-process exclusion is exercised using a real subprocess.
- A real subprocess receives SIGTERM during a blocking fetch: the request becomes `cancelled`, and no provenance or cursor is advanced.
- A real subprocess receives SIGKILL inside an uncommitted SQLite transaction: reopen rolls back the write, leaves no bar/provenance/cursor, and `PRAGMA quick_check` returns `ok`.
- Failed Parquet temp retention is bounded/accounted, and dynamic disk headroom includes projected temp, WAL, and publication needs without deleting active evidence.

## Bounded real provider observation

Fresh task-owned root: `/tmp/bist-phase3-final.sl2NbG/data` (moved to the desktop trash after inspection; no project/live data retained).

- Dry run: exactly `ASELS.IS,THYAO.IS`, two planned requests, max concurrency two, no data root created by the dry-run path.
- Live foreground cycle: exactly two requests, one per symbol; both terminal `success`; two fetch provenance rows; 964 normalized bars; two cursors; zero `started` requests; max fetch concurrency two; max writer concurrency one; zero queue-full events; adapter not paused.
- CLI reported both `completed_symbols` and `successful_symbols`, `ok=true`, and two attempts.
- `doctor`: schema version 3, `quick_check=ok`, zero pending publications.
- No retry was used. No UI scraping, yfinance dependency, rate-limit evasion, background process, or availability/SLA claim occurred.

This observation proves bounded request accounting and adapter compatibility at one moment only. Yahoo remains an unofficial personal-research source with volatile retention/availability and no completeness or SLA claim.
