# Phase 4 RED/GREEN evidence

Date: 2026-08-04
Scope: calendar, append-only gaps, bounded recovery, immutable universe activation, dry-run backfill, read-only reporting, and additive schema v4. No service or timer was installed or started.

## RED evidence

1. The exact first command `uv run pytest tests/unit/test_calendar.py tests/unit/test_gaps.py -q` exited 4 because `tests/unit/test_gaps.py` did not exist.
2. After writing tests first, collection failed because `calendar_engine`, `gaps`, and `recovery` modules did not exist.
3. CLI tests failed because `backfill` and `gaps` were not registered.
4. The first idempotency run exposed duplicate initial gap evidence; the ledger was corrected to no-op identical evidence.
5. Review M1 tests failed because the Yahoo adapter accepted no injected transport and discarded historical windows.

## GREEN evidence

- Calendar/gap focused suite: 20 passed.
- Offline-recovery suite after the partial-return addition: 7 passed.
- Full offline suite: 124 passed, 2 opt-in network tests deselected.
- One bounded real historical-window test: 1 passed, exactly one ASELS request and no retry.
- Ruff formatting/lint, strict mypy, compile/whitespace checks: passed.
- Disposable v3→v4 migration preserved prior runtime state, was repeatable, and returned `quick_check=ok`.
- Disposable CLI root: schema v4, one append-only pending gap, one universe activation, 50 immutable members, read-only gap report, and `quick_check=ok`; safely moved to trash afterward.
- Canonical Aug 1–3 half-open dry run planned zero requests because it covered only the verified weekend.

## Truth boundaries

Yahoo remains an unofficial personal-research source with no SLA or completeness guarantee. Complete aligned returned rows are stored; missing or partial minutes remain evidenced gaps. No synthetic or forward-filled bars are created. The durable production root remained untouched at schema v3 throughout Phase 4 development and review.
