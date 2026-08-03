# Phase 3 review disposition

Date: 2026-08-03
Review session: `ses_03834f75effeZZa1No7tD39myV`
Initial verdict: **REVISE**
Focused confirmation: **APPROVE; no Critical, High, or Medium findings remain**

## Findings

| ID | Severity | Disposition | Evidence |
|---|---|---|---|
| M1 | Medium | Accepted and fixed test-first | A new RED test proved the persisted schema-drift pause was permanent. Additive schema v3 now stores `paused_until_utc`; a 30-minute fail-closed pause survives reopen, then the reservation transaction clears it and permits one normal bounded probe. The targeted test and v1→v2→v3 migration test pass. |
| L1 | Low | Accepted and fixed test-first | A failed run is now completed/accounted but not successful. `CollectionReport.successful_symbols` is separate and CLI `ok` requires success for every requested symbol. Exhausted 5xx/timeout tests prove the unsuccessful state. |
| L2 | Low | Not accepted as a Phase 3 defect; retained as an explicit operational limitation | A `started` record proves a request was authorized and may have reached the provider before a crash. Counting it conservatively toward the rolling-hour/session cap prevents rate-limit evasion across restart. The rolling-hour effect expires naturally; rewriting ambiguous audit state would claim knowledge the collector does not have. Later recovery may add an explicit `abandoned-unknown` terminal state, but must not erase or stop accounting for the attempt. |
| L3 | Low | Accepted as a documented adapter limitation; no code change | The provider-neutral collector passes its exact planned overlap window to the adapter. The Yahoo personal-research adapter truthfully uses its bounded `range=1d` request and labels the result `PROVIDER_RANGE_1D_APPROXIMATE`; exact windowing is not claimed. Licensed adapters may honor exact windows without storage/scheduler changes. |
| L4 | Low | Deferred outside Phase 3 | The Phase 2 bounded response-evidence ring is implemented and tested, but the current Yahoo adapter exposes a digest and parsed evidence rather than a retained response body to the collector. Adding body transfer/retention is not needed for the Phase 3 scheduler gate and is not being misrepresented as active. |
| L5 | Low | Accepted as a known fidelity limitation; no code change | Attempt timestamps are deterministic, UTC, unique within a cycle, and only microseconds apart. Source retrieval/observation timestamps remain separate. Replacing them with concurrent wall-clock calls would reduce test determinism without materially improving the bounded cap decision. |
| L6 | Low | Not accepted as a demonstrated defect; test gap recorded | `ask()` enqueues an observe/ingest command before awaiting its future, and normal shutdown cancels fetch workers while the single writer actor drains queued commands. The real SIGTERM test proves cancellation during fetch is terminal and cursor-safe. Direct external cancellation at every actor boundary is not claimed as verified and remains a future resilience test, not a reason to broaden Phase 3. |

## Confirmation

Because M1 was Medium and required a correction, the one permitted focused confirmation was run in the exact same session, model, and plan agent. It returned **APPROVE** and explicitly confirmed no Critical, High, or Medium finding remains. No further review loop was run.
