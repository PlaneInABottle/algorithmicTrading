# BIST minute collector operations

This collector is a daily historical-research process, not a trading system. It stores complete,
aligned one-minute bars returned by the unofficial Yahoo Chart adapter. Yahoo provides no SLA or
completeness guarantee; the collector never fills, invents, or silently closes missing minutes.

## Operating contract

- Timer: `bist-minute-collector.timer`, weekdays at 19:30 `Europe/Istanbul`, `Persistent=true`.
- Process: `bist-minute-collector.service`, `Type=oneshot`; it normally becomes inactive after exit.
- Data: `/home/mirza/.local/share/bist-minute-collector/data`.
- Universe: immutable current 2026 Q3 BIST 100 snapshot (`config/universes/bist100/2026q3.json`),
  effective 2026-07-01..2026-09-30 and retrieved from the official Borsa İstanbul constituent CSV on
  2026-08-08. It superseded the BIST 50 snapshot on 2026-08-08. The database records which snapshot
  was active (`universe_activations`).
- Limits: maximum two Yahoo requests concurrently, one SQLite writer, persisted request caps and
  provider cooldowns, and a 28-day conservative recovery boundary.
- Calendar: the repository's sourced calendar is authoritative. Verified closed dates exit zero with
  no Yahoo requests. A manual invocation before 19:30 also exits zero without a premature sweep.

The checked-in calendar is effective only through 2026-12-31. A sourced replacement snapshot must be
reviewed before that date; after expiry the one-shot deliberately fails closed and makes no Yahoo
request. A 429 also fails that day's cycle and persists a cooldown for the next invocation rather than
waiting in-process or creating a retry loop.

An inactive successful service is expected. The enabled timer must remain `active (waiting)` and show
its next trigger. Last-attempt, last-success, and last-completed-cycle evidence is stored in SQLite;
failed or interrupted runs remain visible and are reconsidered on the next bounded invocation.
An abrupt SIGTERM/SIGKILL may leave a run marked `started`; the next invocation deterministically
marks it `interrupted` before recovery. Cursor advancement still follows committed data only.

## Commands

Run these from `/home/mirza/Documents/Projects/algorithmicTrading`:

```bash
uv run bist-minute-collector install-service --dry-run
uv run bist-minute-collector install-service
uv run bist-minute-collector enable-timer
uv run bist-minute-collector start-now
uv run bist-minute-collector status --json
uv run bist-minute-collector logs
uv run bist-minute-collector doctor --data-root /home/mirza/.local/share/bist-minute-collector/data
uv run bist-minute-collector gaps --data-root /home/mirza/.local/share/bist-minute-collector/data --json
uv run bist-minute-collector run-once --data-root /home/mirza/.local/share/bist-minute-collector/data --dry-run
```

Backfill and compaction (used 2026-08-08 to extend the archive from BIST 50 to BIST 100):

```bash
uv run bist-minute-collector backfill --data-root /home/mirza/.local/share/bist-minute-collector/data \
  --symbols AKSA.IS,ALTNY.IS,... --from 2026-08-02T21:00:00Z --to 2026-08-07T15:30:00Z --dry-run
uv run bist-minute-collector backfill --data-root /home/mirza/.local/share/bist-minute-collector/data \
  --symbols ... --from 2026-08-02T21:00:00Z --to 2026-08-07T15:30:00Z --execute
uv run bist-minute-collector compact --data-root /home/mirza/.local/share/bist-minute-collector/data
```

The `backfill` command validates symbols against the active universe snapshot (max 50 symbols per
invocation — split into batches), plans bounded historical recovery with a `--dry-run` first, and
only fetches when `--execute` is given. It also records the universe activation. `compact`
reconciles and publishes pending writes; the backtest loader reads only partitions the collector
DB marks `active`, so superseded partitions from a previous publication are ignored.

Backtest subsystem (separate from the collector):

```bash
uv run bist-backtest run --data-root ~/.local/share/bist-minute-collector/data --strategy sma-crossover --all-symbols
uv run bist-backtest run --data-root ~/.local/share/bist-minute-collector/data --strategy rsi-pullback \
  --param rsi_below=25 --param exit_above=60 --force
uv run bist-backtest attempts
```

Results and per-run manifests go to `backtest-results/`; each run also appends an entry to
`backtest-results/attempts.jsonl` recording strategy, parameters, symbols, session dates, metrics,
and the manifest path. The dedupe gate skips identical (strategy, params, symbols, sessions)
combinations unless `--force` is passed.

Disable future scheduled invocations without deleting data:

```bash
uv run bist-minute-collector disable-timer
```

Direct read-only systemd checks:

```bash
systemctl --user show bist-minute-collector.service -p ActiveState -p SubState -p Result -p NRestarts
systemctl --user show bist-minute-collector.timer -p ActiveState -p SubState -p NextElapseUSecRealtime
systemctl --user is-enabled bist-minute-collector.timer
journalctl --user-unit=bist-minute-collector.service --since=-7days --no-pager
```

`install-service` verifies and writes only the exact user `.service` and `.timer`; it never enables or
starts them. No credential is required or read by the Yahoo adapter. Never delete or reset the data
root to hide gaps or recover from an operational error: disable the timer, run `doctor` and `gaps`, and
preserve the journal/database evidence for diagnosis.

## Evidence still requiring time

Unit and integration tests cannot prove an actual missed-trigger boot catch-up, a naturally occurring
holiday/half-day, long provider outage, provider correction, or weeks of unattended operation. Those
remain observation gates. Personal-research collection does not authorize redistribution of Yahoo
data.
