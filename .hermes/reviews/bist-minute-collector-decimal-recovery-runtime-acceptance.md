# BIST minute collector decimal recovery runtime acceptance

Verification date: 2026-08-04 (`Europe/Istanbul`)

Fix commit under acceptance: `8fd9603` (`Fix lossless Parquet publication`)

## Incident baseline

- Installed timer: enabled, `active (waiting)`, next trigger
  `2026-08-05 19:30:00 +03`.
- Failed service: `Result=exit-code`, `NRestarts=0`.
- Failed durable run: `8797986d63db4b57afa04b224e6a899e`, session `2026-08-04`,
  `ArrowInvalid: Rescaling Decimal value would cause data loss`, `quick_check=ok`.
- SQLite: schema v5, 47,956 hot bars, 50 cursors, 0 unresolved gaps, 100 completed
  successful request attempts, 100 fetch-provenance rows, 0 revisions, 0 active partitions,
  and 1 `writing` manifest.
- Interrupted manifest: `01e1e55a88f747f784f99a3dec1f4a89`, session `2026-08-03`.
- Its retained temp was 0 bytes with SHA-256
  `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`.
- Pre-change SQLite SHA-256 was
  `b14fd32a6e5b52be6a3f984ac5f86e93667b7931e35b278dd0fe28804cdce552`.

## Root-cause reproduction

A read-only SQLite connection and isolated PyArrow conversion loop inspected all 47,956 committed
bars (191,824 OHLC values) without modifying the live root. PyArrow 25.0.0 rejected 132,944 values
against `decimal128(38,12)`. The first deterministic boundary was:

- exchange: `AEFES.E`
- bar open: `2026-08-03T06:55:00Z`
- field/value: `open=22.100000381469727`
- source scale: 15
- error: `Rescaling Decimal value would cause data loss`

The observed maximum source scale was 16 and maximum integer width was 4. Every live value converted
exactly with `decimal128(38,16)`; failures were 0. The production schema now uses that exact measured
boundary and fails closed on any Arrow value that cannot be represented losslessly.

## RED, GREEN, and independent review

- RED: the exact-decimal publication and zero-byte-writing recovery tests both failed before the
  production change (`ArrowInvalid` and `recovered=0/failed=1`).
- GREEN: the two tests passed; the focused archive/crash group passed 21 tests before review.
- Complete offline suite after review: 141 passed, 2 network tests deselected.
- Focused final archive/crash suite: 15 passed.
- Ruff format/check, strict mypy, `uv sync --frozen`, systemd unit verification, Python 3.12
  compile, and diff checks passed.
- Exact read-only OpenCode session `ses_03834f75effeZZa1No7tD39myV` with
  `opencode-go/deepseek-v4-flash` and `--agent plan` returned **APPROVE**, with no Critical, High,
  or Medium finding. Its two Low observations were dispositioned without broadening the fix.

An isolated copy of the complete live root recovered before service execution: it retained the old
manifest as failed evidence, published 23,965 rows for `2026-08-03` and 23,991 rows for
`2026-08-04`, returned `quick_check=ok`, and had zero pending publications. The exact AEFES value
round-tripped equal. Live database/temp checksums were unchanged before and after that rehearsal, and
the task-owned copy was moved to trash.

## Installed service acceptance

Supported boundary:

```text
uv run bist-minute-collector start-now
```

The first repaired invocation succeeded:

- run ID: `7eb71240d7dd428895c25b57d5cb878d`
- attempted/completed UTC: `2026-08-04T17:27:46.132226Z` /
  `2026-08-04T17:28:09.713273Z`
- status/reason: `succeeded` / `daily-post-close-cycle-complete`
- 50 planned, 50 successful, 0 denied, 50 requests
- maximum fetch concurrency 2; maximum writer concurrency 1; 0 cancelled requests
- 0 recovered gaps, 0 unresolved gaps, 2 active partitions, `quick_check=ok`

The failed run remains unchanged and visible. Its interrupted manifest is now `failed` with
`empty writing temp rebuilt from committed SQLite bars`; its zero-byte temp and empty-file checksum
remain preserved. There are no `writing` or `pending` manifests.

Post-run datastore accounting:

| Evidence | Result |
|---|---:|
| SQLite schema / quick check | v5 / `ok` |
| Hot bars | 47,956 |
| Cursors | 50 |
| Cursor rows without bars | 0 |
| Cursor last-bar mismatches | 0 |
| Accounted through / last complete bar | `2026-08-04T15:19:00Z` / `2026-08-04T15:00:00Z` |
| Current/unresolved gaps | 0 / 0 |
| Total completed successful requests | 150 (baseline 100 + accepted run 50) |
| Fetch-provenance rows | 150 |
| Revisions | 0 |
| Manifest states | active 2, superseded 1, failed evidence 1, pending 0 |

Active files:

| Session | Rows | SHA-256 | Schema |
|---|---:|---|---|
| 2026-08-03 | 23,965 | `90b52d62a0ba90f901276e0d2780f74b51150deab8441ebf639bf166f3cbe61b` | `bist-minute-bars-v2`, scale-16 |
| 2026-08-04 | 23,991 | `7ebe788590f267f77a58dd70ee9afc7adf9b79980d271ac1eda9dfa4c1f26be4` | `bist-minute-bars-v2`, scale-16 |

The manifest row total is 47,956, exactly matching SQLite. Each file row count and SHA-256 was
recomputed directly and matched its active manifest. The incident AEFES price read back as
`22.1000003814697270`, numerically equal to the exact committed Decimal
`22.100000381469727`; no rounding or fabrication occurred.

## Service and timer closure

- Service: `ActiveState=inactive`, `SubState=dead`, `Result=success`, `ExecMainStatus=0`,
  `NRestarts=0`.
- No `bist-minute-collector run-once` process remained.
- Timer: enabled, `ActiveState=active`, `SubState=waiting`, next trigger
  `2026-08-05 19:30:00 +03`.
- No unit reinstall was required. Repository and installed SHA-256 values still match:
  - service: `0f07fbada5e80e4144f6b8d58421fb6fc57f24be00fd08bd97994869665dee54`
  - timer: `6c23dbacfe7c5c7b632ee997648dae6837323d9075289e3d5bac60fd7c15619c`

## Not proven by this acceptance

This single successful post-close run does not prove Yahoo completeness or availability, weeks of
unattended operation, a future scheduled trigger, reboot/missed-trigger catch-up, a natural provider
correction/outage, or a real holiday/half-day. It proves the observed decimal failure, retained
manifest recovery, current two-session archive accounting, and installed one-shot boundary on this
machine only.
