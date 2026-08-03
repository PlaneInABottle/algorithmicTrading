# Phase 2 RED/GREEN and verification evidence

Date: 2026-08-03

## RED evidence

1. Before Phase 2 modules existed, test collection failed on missing `bist_minute_collector.store` and `bist_minute_collector.archive` imports.
2. The first implementation run passed 13 of 17 focused tests. Three revision tests exposed an incorrect SQLite insert arity; one crash test exposed an invalid test premise where an already verified identical final file made a corrupt temporary retry irrelevant.
3. A bounded-retention RED test showed ordinary retained bodies could remain above their byte cap because active-bar provenance prevented eviction.
4. Review-finding RED command:

   ```text
   uv run pytest tests/unit/test_store.py -q -k 'fractional_observation or exact_second_cursor or routine_null_row'
   3 failed, 9 deselected
   ```

   The failures proved the fractional timestamp ordering defect, cursor regression, and routine-null-row retention overreach.

## GREEN evidence

- Focused review-finding rerun: `3 passed, 9 deselected`.
- Final store suite: `12 passed`.
- Final offline suite: `75 passed, 1 deselected` (the opt-in network smoke remains deselected).
- Final Phase 2 store/archive suites: `17 passed`.
- Final crash-consistency suite: `5 passed`.
- Ruff format/check and mypy: clean over 19 source files.
- `uv sync --frozen`: audited 21 locked packages.

## Deterministic archive evidence

A task-owned temporary root was seeded through `WriterStore.ingest` with one deterministic Phase 1 `FetchResult`, compacted twice, inspected, and removed:

```text
first_sha256=712f5fdbe3d614f390bd0106b4578d25f562d3af28d2a9cb792cb834813da9d3
second_sha256=712f5fdbe3d614f390bd0106b4578d25f562d3af28d2a9cb792cb834813da9d3
same_path=True
same_bytes=True
active_manifests=1
temporary_root_removed=true
```

## CLI/native-store evidence

The required CLI commands ran against a separate task-owned temporary root, seeded only through supported APIs:

```text
doctor before: ok=true, quick_check=ok, schema_version=1, active_partitions=0
compact: ok=true, recovered=0, failed=0, row_count=1
published sha256: 6fdf8956ca1018416d8abe58fdbcc6b9f37d8240fc07f803c5b2da163da1e7fb
doctor after: ok=true, quick_check=ok, schema_version=1, active_partitions=1
read-only SQLite: bars=1, fetches=1, active_manifests=1, quick_check=ok
temporary_root_removed=true
```

The published file hash matched the active SQLite manifest. No live collector data root was created.
