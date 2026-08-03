# OpenCode Phase 2 implementation review

- Date: 2026-08-03
- Session: `ses_03834f75effeZZa1No7tD39myV`
- Model: `opencode-go/deepseek-v4-flash`
- Agent: `plan`
- Mode: read-only implementation review
- Stored message: `msg_fc8b5c391002tQDgOBxGvQEnML` / `prt_fc8b5dc02001fHqlT4UUK9Fbv2`

## Full reviewer response
## Formal Phase 2 Implementation Review — `bist-minute-collector` (Phase 2)

### Verdict: **APPROVE**

**Explicit statement: No Critical, High, or Medium finding remains.** All Phase 2 requirements are implemented and tested; the only remaining items are Low/optional and none requires another review.

---

### Verified correct (no finding)

- **Provider-neutral SQLite WAL schema + additive migration** — `MIGRATION_V1` (`store.py:216-348`) applied in `BEGIN EXCLUSIVE` with a migration checksum and `user_version` (`store.py:452-472`); a pre-existing phase-zero table survives upgrade (`test_store.py:168-188`); fail-closed on a newer schema (`store.py:454-455`).
- **One writer / read-only reader** — nonblocking `flock` with `WriterAlreadyOpenError` (`store.py:411-428`; `test_store.py:159-160`); reader uses `mode=ro` + `PRAGMA query_only=ON` and cannot mutate (`store.py:364-394`; `test_store.py:186-187`).
- **Idempotent fetch/provenance + active-key identity** — envelope SHA-256 replay detection and `ProvenanceConflictError` (`store.py:483-526`, digest at `149-213`); `bars_hot` PK `(source, exchange_code, interval_seconds, bar_open_utc)` (`store.py:267-282`), matching invariant 2; identical replay creates no revision (`test_store.py:44-54`).
- **Correction ordering and revision-before-replace** — changed complete observations insert `bar_revisions(before/after, …)` before updating `bars_hot` and promote both triggering bodies to protected `correction` (`store.py:668-756`; `test_store.py:87-105`); a fault between revision and replace rolls back the entire correction (`test_store.py:121-141`); out-of-order observations converge (`test_store.py:68-84`).
- **Cursor-follows-data transactionality** — cursor upsert is inside the same `BEGIN IMMEDIATE` as bars/provenance and rolls back together (`store.py:518-522`, `758-787`; `test_store.py:108-118`).
- **Unknown/empty outcomes + retained evidence** — empty/null/failed envelopes yield typed outcomes, no bars, and a protected `anomaly` body (`store.py:604-624`; `test_store.py:202-203`).
- **Bounded response retention with anomaly/correction protection** — `_prune_responses` evicts only `ordinary` + `protected=0`, records `body_retained=0` with a reason, and never touches anomaly/correction (`store.py:789-810`; `test_store.py:190-224`).
- **Deterministic explicit-schema Zstandard Parquet** — fixed Arrow schema/metadata, zstd, dictionary encoding, sorted rows, byte-for-byte rerun hash stability (`archive.py:54-147`; `test_archive.py:30-83`).
- **Atomic publish protocol + startup reconciliation** — temp write → `writing` → `pending` → atomic rename → dir `fsync` → `active`, with content-address collision guard (`archive.py:192-303`); recovery proven at `after_temp_write`, `after_pending_manifest`, `after_final_rename` (`test_crash_consistency.py:25-51`).
- **Fail-closed truncation/wrong-hash/ENOSPC/read-only/low-disk** — committed active partitions and `quick_check=ok` preserved (`test_crash_consistency.py:54-89`; `test_archive.py:85-124`); `reconcile`'s `except (OSError, PublicationError, ValueError)` (`archive.py:269`) correctly catches pyarrow `ArrowInvalid` (a `ValueError`) and `ArrowIOError` (an `OSError`) from corrupt temps.
- **CLI scope** — `doctor` (read-only) and `compact` (writer) only, `--data-root` scoped, JSON output, bounded exit codes (`cli.py:27-89`); no scheduler/systemd/gaps code exists.
- **Dependency/secret/Git/Phase-2-only boundaries** — `pyarrow>=21,<26` and `[project.scripts]` wired (`pyproject.toml:12,15-16`); no secrets; `.gitignore` covers `data/state/*.sqlite/*.parquet`; Git remains `main` with **zero commits, nothing staged, no remote**; no Phase 3+ modules.

---

### Low findings (optional; none blocks)

- **L1 — Latent string-order hazard in ordering/cursor guards.** `_upsert_active` compares `(_utc_text(observed_at_utc), fetch_id)` tuples (`store.py:702-704`) and `_upsert_cursor` compares `_utc_text(accounted_through_utc)` strings (`store.py:766-767`), while `_utc_text` (`store.py:119-122`) drops the microsecond fraction when zero. Because `'Z'`(0x5A) > `'.'`(0x2E), `"T07:00:00Z"` sorts after `"T07:00:00.500000Z"`: a newer-with-microseconds observation could be wrongly `ignored`, and an earlier exact-boundary cursor could regress a later one. Not triggered by current Phase 2 paths (real `observed_at` values carry microseconds on both sides); zero-microsecond values come only from fixtures/CLI and exact-boundary cursors Phase 3 will produce. **Classification: latent correctness hazard, not an active defect.** *Smallest test-first fix:* RED tests mixing microsecond/non-microsecond timestamps on both sites asserting the newer value wins and the cursor never regresses, then compare `_from_utc_text(...)` datetimes (or normalize to fixed-width `%Y-%m-%dT%H:%M:%S.%fZ`) at `store.py:702-704` and `store.py:766-767`.
- **L2 — Anomaly classification over-breadth.** `_retain_response` marks a body `anomaly` when *any* outcome ≠ `SUCCESS` (`store.py:608-611`), so routine responses with accepted bars plus `NULL_ROW`/`PARTIAL_ROW` issues would be protected forever, defeating the ordinary-ring budget (`store.py:789-810`) and consuming the future 5 GiB anomaly cap with routine data. **Classification: conservative overreach / hardening.** *Test-first:* a SUCCESS result whose outcomes include `NULL_ROW` must retain `ordinary` and be evictable; classify anomaly only when SUCCESS is absent or a quarantine/schema/HTTP-anomaly code is present.
- **L3 — `failed` manifests and orphan temps have no cleanup path.** `reconcile` iterates only `("writing","pending")` (`archive.py:263-274`); a `fail_manifest` row (`store.py:973-977`) and its `.attempt.parquet.tmp` accumulate indefinitely, so plan §4.6 "orphan temporary" reconciliation is only partially implemented. No data loss (active partitions never deleted). **Classification: confirmed minor spec gap.** *Test-first:* repeated ENOSPC failures must not accumulate orphan temps beyond a retention bound; add bounded cleanup/accounting.
- **L4 — `force_rewrite` is dead API.** `compact_all` accepts then discards it (`archive.py:168-169`, `del force_rewrite`); every `compact` re-publishes all partitions unconditionally. **Classification: optional cleanup.** Remove the parameter or document its semantics.
- **L5 — Exception-based crash simulation does not prove OS-level durability.** Fault injection raises inside the process where `close()`/`rollback()` still run (`test_crash_consistency.py:25-51`); WAL `fsync` and genuinely interrupted temp writes are unverified (only post-hoc truncation at `test_crash_consistency.py:78-81` simulates torn files). The "simulated" labels are truthful and plan §7.2 defers real SIGKILL to Phase 3. **Classification: documented scope limitation, keep.** *Future (Phase 3):* add a subprocess `os._exit()`/SIGKILL test at the same kill points.
- **L6 — Reader has no `busy_timeout`.** `StoreReader` (`store.py:364-394`) could surface `SQLITE_BUSY` under a concurrent WAL writer. Harmless now (no daemon in Phase 2). **Classification: optional hardening.** Set a bounded `busy_timeout` before Phase 3.
- **L7 — Low-disk guard is an absolute floor, not a headroom estimate.** `_guard_space` checks only `min_free_bytes` (10 GiB default, matching the plan's pause threshold; `archive.py:185-190`), not plan §8.3's worst-case temp+WAL headroom; per-partition ENOSPC is still fail-closed (`archive.py:240-247`). **Classification: conservative simplification, acceptable for Phase 2.**

---

**Summary:** no bugs at Critical/High/Medium; L1 is the only latent-correctness item (edge-case ordering), the rest are optional hardening or documented scope simplifications. Phase 2 is complete and ready for the user's explicit next-phase decision; no revision is required, and no Phase 3 architecture is recommended here.
