# Phase 5 RED/GREEN evidence

Date: 2026-08-04
Scope: daily post-close one-shot workflow, additive run-status schema, current-session recovery boundary, closed-session compaction, exact systemd user service/timer definitions, installer isolation, operator status/docs, and no daemon. No unit was installed or enabled during implementation/review.

## RED evidence

1. The first focused Phase 5 test command failed collection because `bist_minute_collector.oneshot` and `.service` did not exist.
2. The first throttle integration expected 50 HTTP calls; it exposed correct persisted provider-wide cooldown behavior after six requests and 44 durable denials. The test was corrected to assert bounded terminal accounting rather than defeat cooldown.
3. The broad suite exposed a test-provider source/result identity mismatch that had hidden cursor state. The fixture was corrected to one source identity; production Yahoo already uses `yahoo_chart` consistently.
4. DeepSeek M1 RED: the consecutive-day test failed after 90.35 seconds with 100 requests, 50 falsely recovered gaps, and 25,900 unresolved current-session gaps before the second sweep.

## GREEN evidence

- Daily/unit/service/archive focused suite before review: 28 passed.
- Consecutive-day M1 test after the recovery-end fix: passed in 1.01 seconds alongside the workflow-order test; exactly 50 second-day sweep requests and zero false recovered gaps.
- Final offline suite before confirmation: 139 passed, 2 opt-in network tests deselected.
- Strict mypy, Ruff format/lint, Python compile, `git diff --check`, and frozen dependency sync: passed.
- `systemd-analyze --user verify` accepted the exact repository service and timer with no diagnostic output.
- `install-service --dry-run` named only the exact two user-unit paths and made no user-unit change.
- Task-owned `run-once --dry-run` planned 50 symbols, made zero requests, created no data root, and truthfully returned `skipped-pre-close` policy context.
- Fresh synthetic all-50 one-shot: 50 terminal requests, 50 bars/provenance/cursors, maximum fetch concurrency two, one writer, one active deterministic partition, no gaps, schema v5, and `quick_check=ok` in an isolated temporary root.
- One-throttle synthetic run: one 429, no retry storm, provider-wide persisted cooldown/denials, failed run evidence, no publication, and no cursor for the throttled/denied symbols.

## Review evidence

The exact same-session DeepSeek review returned **REVISE** only for M1 and no Critical/High finding. The exact one permitted focused confirmation returned **APPROVE**, explicitly with no remaining Critical, High, or Medium finding.

## Truth boundaries

Yahoo remains an unofficial personal-research source with no SLA/completeness guarantee. The daily schedule is historical-research cadence, not near-real-time freshness. Actual timer enablement, a manual user-service invocation, a later scheduled post-close collection, real reboot catch-up, holidays/half-days, natural provider incidents/corrections, and weeks of unattended operation require separate runtime evidence.
