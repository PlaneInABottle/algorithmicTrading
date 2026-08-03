# Review Finding Disposition — BIST Minute Collector Plan

Date: **2026-08-03 (Europe/Istanbul)**
Initial verdict: **REVISE**
Focused confirmation: **APPROVE**
Remaining Critical/High/Medium findings: **none**

Both review turns used exact OpenCode session `ses_03834f75effeZZa1No7tD39myV`, exact model `opencode-go/deepseek-v4-flash`, and `--agent plan`. The first full review is in [`bist-minute-collector-opencode-review.md`](bist-minute-collector-opencode-review.md); the one permitted focused confirmation is in [`bist-minute-collector-opencode-confirmation.md`](bist-minute-collector-opencode-confirmation.md).

| ID | Severity | Disposition | Minimal plan change |
|---|---|---|---|
| M1 | Medium | **Accepted/resolved** | Added Borsa's official current constituent CSV; recorded bounded size/hash/row/date evidence; separated official `.E` codes from versioned Yahoo `.IS` aliases; made the CSV the deterministic current-universe cross-check; retained the explicit current-not-PIT limitation. |
| L2 | Low | **Accepted/resolved** | Clarified that Python 3.12.3 is installed but bare `python3` is 3.11.15; required pinned uv Python 3.12 in every entrypoint and Phase 5 unit test. |
| L3 | Low | **Accepted/resolved** | Labeled 510 minutes as the measured Yahoo grid span used for budget arithmetic, not an official continuous-session duration. |
| L4 | Low | **Accepted/resolved** | Required threshold calibration to be preregistered from the 2/5-symbol canary before the 50-symbol acceptance window; no retroactive loosening. |
| L5 | Low | **Accepted/resolved** | Defined the retained “final” response as the last success immediately before the post-close sweep/compaction. |
| L6 | Low | **Accepted/resolved** | Required atomic-compaction staging inside the collector's writable data root on the same filesystem, never `/tmp`. |

## Preserved conclusions

- Yahoo remains unofficial/personal-research data with no completeness, quota, retention, latency, or SLA claim.
- One minute remains the free bounded granularity; 30 seconds is unsupported by the measured endpoint.
- Null, empty, nonaligned, failed, or beyond-retention data is never turned into a synthetic bar.
- Architecture remains Python + uv + typed code + pytest + one process + two bounded fetch workers + one SQLite WAL writer + daily Parquet. No broader infrastructure was added.
- Universe snapshots and history remain immutable; the daily official CSV improves current membership but does not create point-in-time history.
- systemd installation/enablement, Git initialization/commits, credentials, and continuous collection remain separate future decisions.

## Gate

The plan review is complete and approved. The next gate is the user's explicit implementation decision. No further review or implementation action is authorized by this disposition.
