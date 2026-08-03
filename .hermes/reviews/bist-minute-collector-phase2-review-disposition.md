# Phase 2 OpenCode review disposition

- Date: 2026-08-03
- Review: `bist-minute-collector-phase2-opencode-review.md`
- Session: `ses_03834f75effeZZa1No7tD39myV`
- Model/agent: `opencode-go/deepseek-v4-flash` / `plan`
- Verdict: **APPROVE**
- Blocking findings: none; the reviewer explicitly reported no Critical, High, or Medium findings.

## Finding dispositions

| Finding | Disposition | Evidence / boundary |
|---|---|---|
| L1 timestamp string-order hazard | **Accepted and fixed test-first.** | RED tests reproduced both a newer fractional observation being ignored and an exact-second cursor regressing a later fractional cursor. `store.py` now parses stored UTC text before comparing datetimes. The focused tests and full suite pass. |
| L2 anomaly classification over-breadth | **Accepted and fixed test-first.** | A RED test showed an accepted response with a routine `NULL_ROW` issue was protected forever. Accepted responses containing only `NULL_ROW`, `PARTIAL_ROW`, or a provisional-tail notice are now ordinary and evictable; absent success or any other anomaly code remains protected. |
| L3 failed-manifest/orphan-temp cleanup | **Acknowledged; deliberately deferred.** | Failed manifests and corrupt temps remain explicit evidence and do not replace or delete active data. Automatically deleting them needs an effective retention/accounting policy; inventing that policy in Phase 2 would conflict with the conservative no-unexplained-deletion boundary. The manifest error and path remain auditable. Add bounded, separately tested cleanup before unattended operation. |
| L4 dead `force_rewrite` argument | **Acknowledged; deferred cleanup.** | The argument does not change correctness or publication semantics. Removing it is an API cleanup, not a Phase 2 acceptance requirement; no behavior was added around it. |
| L5 exception-based rather than OS-level crash tests | **Accepted as a documented limit.** | Phase 2 proves deterministic reconciliation at all three publication boundaries using injected exceptions plus corrupt/truncated files. Actual `SIGKILL`/power-loss evidence remains a Phase 3 subprocess test as the canonical plan states. |
| L6 reader lacks `busy_timeout` | **Deferred to concurrent operation.** | Phase 2 has no daemon or concurrent writer. Add and test a bounded reader timeout before Phase 3 introduces that concurrency. |
| L7 absolute low-disk floor rather than dynamic headroom | **Accepted as a Phase 2 simplification.** | The 10 GiB preflight and ENOSPC behavior fail closed now. Dynamic temp/WAL/projected-growth headroom belongs to the Phase 3 operating loop and remains an explicit gate there. |

No focused reviewer confirmation was requested: the user's condition requires it only when a Critical, High, or Medium finding required a fix, and none existed. The two accepted Low fixes were rerun through the focused and complete offline gates.
