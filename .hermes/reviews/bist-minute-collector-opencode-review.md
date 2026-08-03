# OpenCode Review — BIST Minute Collector Plan

- Date: 2026-08-03 (Europe/Istanbul)
- Session: `ses_03834f75effeZZa1No7tD39myV`
- Model: exactly `opencode-go/deepseek-v4-flash`
- Agent: exactly `plan`
- Mode: read-only review; no file edits or implementation were authorized
- Reviewed file: `.hermes/plans/bist-minute-collector.md`
- Initial verdict: **REVISE**

## Review request

> Perform a focused read-only finance/data-engineering review of .hermes/plans/bist-minute-collector.md in this workspace. This is the exact same BIST research session. Do not edit files, run implementation, create credentials, install/enable services, initialize Git, start collection, contact vendors, or make large downloads.
>
> Review specifically: (1) current BIST 50 source and immutable universe feasibility; (2) whether the measured Yahoo Chart/yfinance one-minute behavior, request/range/rolling-retention limits, lag, null/missing-minute semantics, 429 uncertainty, and personal-research/no-SLA limitations are represented truthfully; (3) crash consistency, single-writer and atomic SQLite/Parquet protocol; (4) idempotency and source-correction rules; (5) gap-state semantics and restart recovery including beyond-retention evidence; (6) BIST calendar, auction, holiday, half-day, timezone, and suspension assumptions; (7) systemd user service boot behavior with Linger=yes and operational safety; (8) request/storage/disk budgets; (9) six TDD phases, failure injection, canary evidence, and untestable claims; (10) security/Git boundaries and overengineering.
>
> Return one unambiguous verdict: APPROVE, REVISE, or BLOCK. Then list every finding with severity Critical/High/Medium/Low, exact plan section, rationale, and a minimal warranted correction. Distinguish factual errors from conservative design choices. Do not request broader architecture or implementation. If no Critical/High/Medium findings exist, say that explicitly and identify only optional Low observations.

## Full reviewer response

All local `[T]` claims verified: systemd 255 user session `running`, `Linger=yes`, `Europe/Istanbul`, `uv 0.5.9`, Python 3.12.3 installed (the default `python3` is 3.11.15), and ~131.7 GiB free (plan's 131.87 GiB — consistent within transient noise). The snapshot's SHA-256, symbol count, periodic-review additions/removals, and the KONTR→AKSEN non-periodic direction are all confirmed correct.

# Review: `.hermes/plans/bist-minute-collector.md`

**Verdict: REVISE (targeted, single-section).** No Critical or High findings. One Medium — a factual error in the universe-source claims — plus optional Low notes. Everything else, including all measured provider behavior, crash-consistency protocol, and the conservative design choices, is accurate and well-calibrated.

## Findings

### Medium — M1: An official downloadable BIST 50 constituent file exists; the plan says it does not

- **Section:** §2.2 "Current BIST 50 universe" (lines 37–41) and blocker #3, §11 line 745 ("an official downloadable constituent file was not established").
- **Rationale:** `https://www.borsaistanbul.com/datum/hisse_endeks_ds.csv` is an official, daily-dated (rows stamped 03/08/2026), machine-readable Borsa İstanbul file referenced from the official BIST Stock Indices page. It contains `XU050` (BIST 50) with exactly the 50 current members, coded `.E` (e.g., `AEFES.E`), and **matches the plan's snapshot symbol-for-symbol**. This directly contradicts the plan's stated blocker, and it means the §2.2 update procedure (§2.2, lines 105–107) relies on manual two-source transcription + per-symbol metadata validation when an authoritative, dated, current-membership source already exists that could drive a deterministic added/removed diff.
- **Smallest correction:** add `hisse_endeks_ds.csv` to §2.2's primary sources and to the update procedure as the machine-readable cross-check authority; record the official `.E` ↔ Yahoo `.IS` suffix mapping in the alias table (§2.2 line 105); and rewrite blocker #3 to say the official file exists but is a *current* snapshot (not point-in-time history) — so the plan's "no historical PIT reconstruction" stance is unaffected.

### Low — L2: Default `python3` is 3.11.15, not 3.12.3

- **Section:** §2.1 line 31 ("system Python 3.12.3"). The claim is accurate (3.12.3 is installed), but the default `python3` is 3.11.15. Ensure every entrypoint (systemd unit absolute executable, `uv run`, CLI shebangs) uses the pinned uv-managed 3.12, never bare `python3`. The plan's "absolute virtual-environment executable" (§7.1) already covers this; make it a Phase-5 test.

### Low — L3: "510-minute session" is the Yahoo grid span, not the trading session

- **Section:** §6.1 line 348 (request-budget arithmetic). Continuous trading is 480 minutes; 510 is the Yahoo grid envelope (09:40–18:10), which is internally consistent with the measured 510-slot/481-row figures. The 2,550–2,600 requests/session estimate is a planning figure under the 3,000 cap, so no correction is needed beyond labeling 510 as the provider grid span, not the official session length.

### Low — L4: Threshold-recalibration wording tension

- **Section:** §7.4 line 463 ("will be recalibrated from canary evidence") vs Phase 6 line 690 ("thresholds are reported, not retroactively loosened"). These are reconcilable, but state explicitly that recalibration happens only *before* the first 50-symbol acceptance measurement, and never after results are seen.

### Low — L5: "First and final successful fetch" raw-body retention is underspecified

- **Section:** §4.5 line 284. "Final" is ambiguous (final before compaction? before session close?). Define it as "the last successful fetch of that session before the post-close sweep/compaction" to make the ≤1.2 GiB budget deterministic and testable.

### Low — L6: Atomic-rename temp path must be inside the writable data root

- **Section:** §4.6 line 296 and §7.1 `PrivateTmp=true`. The plan correctly requires "same filesystem," but with `PrivateTmp` an explicit note that compactor temp files live in `~/.local/share/…` (never `/tmp`) prevents a cross-filesystem `EXDEV` on `os.rename`.

## Factual errors vs conservative design choices

- **Factual error:** the non-existence of an official constituent file (M1). Everything else I could verify (checksum, counts, index changes, KONTR/AKSEN direction, local system facts, Yahoo endpoint/schema/lag/retention semantics, SQLite/systemd claims) checked out.
- **Conservative choices (correct, keep):** manual universe bootstrap with no unattended mutation and stale-source alerts; 28-day recovery budget vs measured ~30-day window; 7-day request limit vs measured 8-day cap; 15-min lag treated as unguaranteed; `synchronous=FULL`; max concurrency two; circuit breaker/cooldown policy; 40 GiB ceiling with pause thresholds; treating null rows, nonaligned tails, and empty responses as evidence rather than prices; deferring to the canary for 429 behavior, boot recovery, and weeks-long behavior; and the honest "untestable claims" list in §9/§11 (real reboot, natural corrections/outages, holidays, long uptime remain observed evidence). No overengineering worth a finding; the provenance/retention machinery is justified by the no-fabrication invariants.

## Recommendation

Apply M1 (add `hisse_endeks_ds.csv` as a primary universe source, add the `.E`/`.IS` mapping, correct blocker #3) and optionally the Low notes. With that single correction, the plan is ready to proceed on its existing approvals: no collector, service, Git, credentials, or downloads are started until the user's explicit implementation decision.
