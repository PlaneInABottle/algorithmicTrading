# BIST minute collector decimal recovery review

Verification date: 2026-08-04

Reviewer session: `ses_03834f75effeZZa1No7tD39myV`

Model: `opencode-go/deepseek-v4-flash`

Agent: `plan`

Mode: read-only, one bounded confirmation

## Verbatim final review

# Final Confirmation — decimal/Parquet recovery fix

**Verdict: APPROVE.** No Critical, High, or Medium finding remains. The incident fix is
correctly scoped, test-first, and green (`lastfailed={}`; both new tests collected).

## Focus-area verification (file:line evidence)

- **Losslessness (the ArrowInvalid rescaling incident):** price fields changed to
  `PRICE_TYPE = pa.decimal128(38, 16)` (`archive.py:64,74-77`); the RED/GREEN test
  `test_partition_preserves_full_source_decimal_scale_without_rescaling`
  (`test_archive.py:82-114`) ingests the live 16-fractional-digit source value
  `1.2100000381469727` and asserts an exact Parquet round-trip for open/high/low/close.
  Any value exceeding scale 16 now fails closed via `except pa.ArrowException` →
  `fail_manifest` + `PublicationError("cannot be represented losslessly")`
  (`archive.py:311-315`) — silent rescaling is impossible: values ≤16 dp are exact, >16 dp
  abort.
- **Schema boundary:** metadata bumped to `collector_schema=bist-minute-bars-v2` /
  `transformation_version=decimal-scale-16-v2` (`archive.py:83-84`); the schema test was
  updated to the v2 keys (`test_archive.py:85-88`); `_validate_parquet(...,
  check_metadata=True)` accepts only v2, and `reconcile` revalidates only
  `writing`/`pending` manifests, so existing active files are not force-revalidated.
- **Crash consistency / zero-byte writing manifest:** `_reconcile_manifest`
  (`archive.py:390-400`) marks the original manifest `failed` with the exact reason, then
  rebuilds the partition from committed SQLite bars via `_compact_partition`, and returns.
  `test_zero_byte_writing_manifest_is_rebuilt_from_committed_bars`
  (`test_crash_consistency.py:59-113`) asserts `recovered==1/failed==0`, one active manifest,
  the original failed manifest retained with the correct error, the zero-byte temp retained,
  and the rebuilt partition round-tripping the exact value with `quick_check=ok`.
- **Corrupt non-empty fail-closed behavior:** unchanged and intact —
  `test_corrupt_pending_publication_fails_closed_and_preserves_prior_active` (16-byte
  truncated + wrong-hash) still fails closed with the prior active preserved; a truncated
  non-empty `writing` temp raises `ArrowInvalid` (⊂ `ValueError`), is caught by `reconcile`'s
  `except` (`archive.py:337`), and is *not* auto-rebuilt (only the zero-byte case rebuilds) —
  data remains in SQLite and is retried by the next `compact_through`.
- **Evidence preservation:** the interrupted manifest and both corrupt/zero-byte temps are
  retained as failed evidence; `_cleanup_failed_temps` removes only per the retention/cap
  policy.
- **Scope:** the diff is confined to `archive.py`, `tests/unit/test_archive.py`,
  `tests/integration/test_crash_consistency.py`. No store, CLI, oneshot, network, service, or
  data-root changes.

## Low observations (non-blocking)

- **L1 — v1/v2 archive mix for historical files:** hot sessions are re-published as v2
  (superseding v1) on the next compact, but sessions whose bars have aged out of hot storage
  remain v1 and would not validate against the v2 schema; they are distinguishable via
  `transformation_version`. Document that old v1 archives are historical.
- **L2 — rebuild triggers only for zero-byte `writing` manifests:** a zero-byte `pending` temp
  still fails closed (`ArrowInvalid`) and relies on the next compact; consistent with the
  incident's specific artifact, but worth a documentation note.

Read-only confirmation only — no files edited, no network, no data-root mutation, no
service/install actions. No Critical, High, or Medium finding remains.
