# Phase 5 runtime acceptance

Date: 2026-08-04, approximately 09:08 Europe/Istanbul
Data root: `/home/mirza/.local/share/bist-minute-collector/data`
Scope: exact user-unit install, timer enablement, one manual pre-close invocation, and read-only accounting checks. No broker/trading process, credential, remote, push, PM2 process, container, or system service was created.

## Installation and systemd evidence

- `install-service` wrote exactly the repository service and timer to `~/.config/systemd/user/` and did not enable/start them itself.
- `systemd-analyze --user verify` accepted both installed units with no diagnostic output.
- Repository and installed service SHA-256: `ca5a6e32277554ecdf73070744b069c2724205a879363f3171b5186bcba1b78e`.
- Repository and installed timer SHA-256: `b2e5c88e103bf019e74f6cbbe94e5824a9d82415e5213257a818f32bf4973c13`.
- The explicitly authorized `systemctl --user enable --now bist-minute-collector.timer` created the expected timer enablement symlink.
- Timer: `enabled`, `ActiveState=active`, `SubState=waiting`; next trigger `Tue 2026-08-04 19:30:00 +03`.
- Service after manual invocation: `MainPID=0`, `ActiveState=inactive`, `SubState=dead`, `Result=success`, `ExecMainStatus=0`, `NRestarts=0`.
- `Linger=yes` remains verified. A real reboot/missed-trigger catch-up is not yet observed.

## Manual one-shot evidence

The authorized manual invocation occurred before 19:30. It correctly acquired the writer, reconciled startup/archive state, activated the immutable universe, recorded `skipped-pre-close`, made **zero Yahoo requests**, passed `quick_check`, and exited zero. Journal output contains the bounded structured result and systemd start/finish records.

- Session date: `2026-08-04`.
- Reason: `before-19:30-Europe/Istanbul`.
- Planned symbols: 50; requests: 0; successful/denied symbols: 0/0.
- Run health: `provider_calls=0`; no recovery/partition action occurred.
- Status correctly treats the inactive successful service plus waiting enabled timer as healthy.

## Preserved production-data accounting

Before Phase 5 runtime: schema v3, 23,965 bars, 50 provenance records/requests, 50 cursors, zero started requests, `quick_check=ok`.

After the additive v3→v4→v5 migration and pre-close invocation:

| Evidence | Count/state |
|---|---:|
| Schema/migrations | v5 / 5 |
| SQLite quick check | `ok` |
| Active hot bars | 23,965 |
| Fetch provenance | 50 |
| Request attempts | 50 |
| Started requests | 0 |
| Cursors | 50 |
| Current gaps | 0 |
| Active partitions | 0 |
| Collector runs | 1 (`skipped-pre-close`) |
| Universe activations / active members | 1 / 50 |
| Data-root disk use | 16 MiB |

No data was reset, deleted, or fabricated. No residual `run-once`, `collect-once`, or `backfill` process remained; the service `MainPID` is zero. Source/systemd search found no live order or broker client entrypoint.

## Still unproven

The first scheduled post-close all-50 sweep has not happened yet and must not be claimed. Therefore `last_completed_cycle` is still null and active Parquet partitions remain zero. The 19:30 run, timer `Persistent=true` catch-up after a real missed trigger/reboot, official holiday/half-day behavior, natural Yahoo 429/outage/correction handling, and weeks of daily unattended operation remain observation gates. Yahoo remains an unofficial personal-research source with no SLA or completeness guarantee.
