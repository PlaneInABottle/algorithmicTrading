# Durable BIST 50 One-Minute Collector — Implementation Plan

Verification date: **2026-08-03 (Europe/Istanbul)**
Status: **Phases 1–3 implemented and reviewed; Phase 4 and later phases are not authorized**
Scope: current BIST 50 constituents, Yahoo one-minute data as a bounded free research source, and a provider-neutral path to licensed data later.

This is the canonical implementation plan for the minute-data collector. It complements the broader [BIST backfill/backtest research](../../BIST_BACKTEST_SYSTEM_RESEARCH.md); it does not change that report's conclusions about data rights, historical depth, or evidentiary limits.

## 1. Evidence labels and decision

- **[V] Verified fact:** supported by a linked first-party source or a read-only local-system check on the verification date.
- **[T] Tested observation:** measured in a small bounded experiment on the verification date; it is not a guarantee or service-level agreement.
- **[U] Unknown/volatile:** not established, liable to change, or requiring written provider terms or longer observation.
- **[A] Assumption/proposal:** an implementation decision to test, not a claim about Borsa İstanbul or Yahoo.

### Decision

**[A] Build the smallest possible single-machine collector:** Python 3.12 managed by `uv`, typed domain models, `pytest`, an isolated Yahoo Chart provider adapter, one process, at most two concurrent fetches, one SQLite WAL writer, and daily Zstandard-compressed Parquet archives. Use a task-specific systemd **user service**, not a timer or PM2, for the long-running daemon.

**[V/T] This design can continuously preserve one-minute observations that Yahoo currently returns, survive process and machine restarts, recover recent gaps while the provider still retains them, and record older gaps truthfully. It cannot guarantee complete exchange data, infer trades from empty provider rows, or recover data that has rolled out of Yahoo's window.** Yahoo is an unofficial personal-research source with no completeness or uptime guarantee.

Phase 1, Phase 2, and Phase 3 implementation were separately authorized and completed as recorded in §13. Phase 4 and every later implementation action remain behind an explicit user decision; no persistent collector, credentials, service, or continuous process exists.

## 2. Measured environment and provider facts

### 2.1 Local machine

| Item | Evidence on 2026-08-03 | Consequence |
|---|---|---|
| Workspace | **[T]** The directory contained only `BIST_BACKTEST_SYSTEM_RESEARCH.md` before this planning work and was not a Git repository. | Phase 1 authorization later satisfied this prerequisite locally on `main`; the verified Phase 1–2 foundation is local commit `d098be5`, and Phase 3 has its own authorized local phase-boundary commit. No remote or push exists. |
| Python tooling | **[T]** `uv 0.5.9` and `/usr/bin/python3.12` (Python 3.12.3) are available; the current bare `python3` resolves to Python 3.11.15. | Pin Python 3.12 in `.python-version` and `pyproject.toml`; every entrypoint uses the uv virtual environment, never bare `python3`. |
| User systemd | **[T]** `systemctl --user is-system-running` returned `running`; systemd is 255. | A user service can supervise the collector without root. |
| Boot persistence | **[T]** `loginctl show-user mirza` returned `Linger=yes`. | An enabled user unit can start at boot without an interactive login. This still requires an actual reboot acceptance test. |
| Timezone | **[T]** Local timezone is `Europe/Istanbul`. | Canonical storage remains UTC; session metadata uses the IANA timezone. |
| Disk | **[T]** 141,594,230,784 bytes free, about **131.87 GiB**, on a 502 GB filesystem. | A conservative 40 GiB collector budget fits now, subject to low-disk guards. |

### 2.2 Current BIST 50 universe

**[V]** Borsa İstanbul identifies BIST 50 as code `XU050` with 50 constituent companies. Its periodic review announced four additions (`ECILC`, `EFOR`, `GLRMK`, `KTLEV`) and four removals (`ARCLK`, `DOAS`, `MAVI`, `TSKB`) for **2026-07-01 through 2026-09-30**. Borsa's machine-readable [current stock-index constituent CSV](https://www.borsaistanbul.com/datum/hisse_endeks_ds.csv) and the current KAP index page independently displayed the same 50 members on the retrieval date. Sources: [Borsa İstanbul BIST 50](https://borsaistanbul.com/en/index/xu050), [2026 Q3 periodic review](https://borsaistanbul.com/en/announcement/15483/bist-stock-indices-periodic-review), and [KAP BIST 50 constituents](https://kap.org.tr/en/Endeksler?indice=QklTVCA1MA).

**[T]** The bounded official CSV retrieval on 2026-08-03 was 427,515 bytes with SHA-256 `acf963b223b6d3f861e050f0458cd4b4d1ad6b3e6ecd6f229cd06ff342927346`. Filtering `ENDEKS KODU=XU050` produced exactly 50 rows, all dated `03/08/2026`. The exchange codes end in `.E`; their canonical sorted checksum is `785e877d116b699e71bbd23a4af353ac6c82d00b77b67973cfa0082928bb059e`. Replacing the terminal `.E` with the explicitly provider-scoped Yahoo `.IS` alias produced the snapshot checksum below. This suffix transformation is a versioned alias rule and must still pass provider-symbol validation; it is not a general exchange identity rule.

**[V]** Non-periodic constituent changes can also occur; for example, KAP published a 2026-06-18 change involving `KONTR` and `AKSEN`. The update procedure therefore cannot rely only on quarterly review dates. Source: [KAP disclosure 1618229](https://www.kap.org.tr/tr/Bildirim/1618229).

#### Proposed immutable bootstrap snapshot

- Snapshot ID: `bist50-2026q3-retrieved-2026-08-03`
- Effective interval: `2026-07-01` through `2026-09-30`, unless superseded by a later official non-periodic announcement
- Retrieval date: `2026-08-03`
- Primary sources: the official constituent CSV, Q3 review announcement, KAP current table, and BIST 50 index page above
- Canonicalization: ASCII-sort the Yahoo research symbols, one uppercase symbol plus `.IS` per line, final newline
- SHA-256 of that canonical byte sequence: `3b3b37c63273ae81129fd93ad20ea7edcf39e6dc94b1dfcdc2aa2934991b70f5`

```text
AEFES.IS
AKBNK.IS
AKSEN.IS
ALARK.IS
ASELS.IS
ASTOR.IS
BIMAS.IS
BRSAN.IS
BTCIM.IS
CANTE.IS
CCOLA.IS
CIMSA.IS
DSTKF.IS
ECILC.IS
EFOR.IS
EKGYO.IS
ENKAI.IS
EREGL.IS
FROTO.IS
GARAN.IS
GLRMK.IS
GUBRF.IS
HALKB.IS
HEKTS.IS
ISCTR.IS
KCHOL.IS
KRDMD.IS
KTLEV.IS
KUYAS.IS
MGROS.IS
MIATK.IS
OYAKC.IS
PASEU.IS
PETKM.IS
PGSUS.IS
SAHOL.IS
SASA.IS
SISE.IS
TAVHL.IS
TCELL.IS
THYAO.IS
TOASO.IS
TRALT.IS
TRMET.IS
TTKOM.IS
TUPRS.IS
TURSG.IS
ULKER.IS
VAKBN.IS
YKBNK.IS
```

**[A]** The implementation will store the official `.E` exchange constituent code separately from provider aliases. `.IS` is a Yahoo mapping, not part of the Borsa instrument identity. Every snapshot will contain `exchange_code`, `provider_symbols`, company name, effective dates, retrieval time, source URLs and source-file checksum, canonicalization/alias-rule versions, and snapshot checksum.

**[A] Update procedure:** before each announced quarterly boundary and after any non-periodic index notice, an operator retrieves and checksums the official Borsa constituent CSV, filters `XU050`, compares it with KAP and the matching Borsa announcement, creates a new candidate snapshot, reviews a deterministic added/removed diff, validates the versioned `.E`→`.IS` aliases with metadata-only requests, and explicitly activates the snapshot. The CSV is authoritative for the dated **current** machine-readable cross-check; it does not supply point-in-time membership history. Historical snapshots are never rewritten. If the CSV is unavailable or official sources disagree, the current snapshot remains active with a stale-source alert and no symbol is guessed. Manual bootstrap is only the fallback and requires saved source references, a two-source comparison where available, reviewer/date, and checksum.

### 2.3 Bounded Yahoo one-minute experiments

These tests used only `ASELS.IS`, `AKBNK.IS`, `THYAO.IS`, `TUPRS.IS`, and `DSTKF.IS`; no UI was scraped and no rate limit was evaded.

| Question | Result |
|---|---|
| Basic one-minute response | **[T]** Sequential `range=1d&interval=1m` Chart requests returned HTTP 200 for all five symbols in 0.56–1.01 seconds, with 19,033–34,257 response bytes each. |
| Response shape | **[T]** `chart.result[0]` contained `meta`, epoch-second `timestamp`, and `indicators.quote[0]` arrays for `open`, `high`, `low`, `close`, and `volume`. The metadata reported `currency=TRY`, `exchangeTimezoneName=Europe/Istanbul`, `gmtoffset=10800`, and `dataGranularity=1m`. Raw one-minute responses had no adjusted-close series. |
| Timestamp and lag | **[T]** Source timestamps decoded to Europe/Istanbul. Latest valid observations were roughly **15 minutes behind** the local clock during the test. This is measured lag, not a stable delay contract. |
| Missing-minute behavior | **[T]** Each one-day response had 417 timestamp slots but only 412 complete OHLC rows. Five timestamped rows had null prices. In five-day samples Yahoo often exposed a 510-slot daily grid while typical complete rows were 481; some dates had materially fewer complete rows. A continuous timestamp grid therefore does not prove continuous trades or data completeness. |
| Trailing observation | **[T]** One raw response ended with a non-minute-aligned valid timestamp after a minute-aligned null placeholder. It must be quarantined as provisional/noncanonical, not floored into a fabricated minute. |
| Five-day bounded concurrency | **[T]** Five `range=5d&interval=1m` requests with maximum concurrency two all returned HTTP 200. Wall time was 3.15 seconds; individual requests took 0.90–1.30 seconds and returned 105,449–190,155 bytes. No 429 or timeout occurred in this bounded run. |
| Per-request date limit | **[T]** A recent seven-calendar-day period returned HTTP 200. `range=1mo` returned HTTP 422 saying only eight days of one-minute granularity may be fetched per request. The implementation safety limit is therefore seven calendar days per request. |
| Rolling retention | **[T]** A period within roughly the last 30 days returned data, while requests whose start was outside that boundary returned HTTP 422 saying the range must be within the last 30 days. The operational recovery budget will be **28 days**, not a promise that 30 days will remain available. |
| Thirty-second data | **[T]** `interval=30s` returned HTTP 400 and the valid-interval list began at `1m`. One minute—not 30 seconds—is the free MVP granularity. |
| yfinance comparison | **[T]** `yfinance 1.5.2` downloaded all five symbols for five days with two threads. It returned a Europe/Istanbul index and `Open/High/Low/Close/Adj Close/Volume`; complete rows ranged from 2,277 to 2,294 per symbol. It filters/normalizes the raw response differently. An old request returned an empty frame with a misleading “possibly delisted” message despite the underlying retention error. Empty yfinance output cannot establish delisting. |
| Throttling | **[T/U]** No 429 occurred in the bounded max-two run. An earlier bounded workspace experiment received HTTP 429 from the Chart host. Yahoo publishes no quota or SLA for this use. Concurrency and request budgets remain conservative and adaptive. |
| Batch support | **[U]** The tested Chart URL handled one symbol per request. No documented batch contract or stable public API was found. yfinance batching is implementation behavior, not a provider guarantee. |

Representative research endpoint, recorded only as an isolated provider detail:

```text
https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range=1d&interval=1m&includePrePost=false&events=div,splits
```

**[V]** yfinance describes itself as an open-source tool intended for research/education and states that Yahoo Finance data is intended for personal use only. Yahoo's API terms govern permitted use. Sources: [yfinance documentation](https://ranaroussi.github.io/yfinance/index.html) and [Yahoo API Terms of Use](https://legal.yahoo.com/us/en/yahoo/terms/product-atos/apitnc/index.html).

**[A] Provider choice:** the MVP will use a direct, narrowly isolated Yahoo Chart adapter because it exposes HTTP status, raw schema, null slots, request windows, and provider timestamps needed for provenance. yfinance remains a bounded smoke-test oracle, not a production dependency. The endpoint is undocumented and replaceable; schema failure pauses affected ingestion rather than guessing.

### 2.4 Measured row/storage sample

**[T]** A five-symbol, five-day yfinance sample contained 11,469 complete bars. Encoding the proposed 17-column normalized schema in memory with Arrow and Zstandard Parquet produced:

- Arrow table: 1,651,536 bytes, **144.0 bytes/row**;
- Parquet: 281,652 bytes, **24.6 bytes/row**.

This favorable compression measurement is not a budget promise. The capacity plan uses multiples of it for file overhead, revisions, metadata, schema growth, and nonideal partitions.

## 3. Scope, non-goals, and invariants

### 3.1 In scope

- continuously retrieve available one-minute OHLCV observations for the active, versioned BIST 50 snapshot;
- durably retain canonical UTC bars, source observations, corrections, request provenance, cursors, and gap evidence;
- resume after process crashes and reboots;
- reconcile recoverable recent windows in bounded chunks;
- publish auditable daily Parquet partitions atomically;
- expose operator commands and health evidence;
- isolate provider, calendar, universe, and storage contracts so a licensed minute/trade source can replace Yahoo later.

### 3.2 Explicit non-goals

- no live trading, broker integration, order logic, news or AI decisions;
- no tick, quote, depth, or 30-second collection in the free path;
- no completeness, timeliness, exchange-official, or redistribution claim for Yahoo;
- no historical point-in-time BIST 50 reconstruction before the snapshot's effective date;
- no synthetic bars, forward-filling, interpolation, or silent gap closure;
- no Postgres, Kafka, Airflow, containers, distributed workers, web UI, PM2, LEAN, or NautilusTrader;
- no unattended universe mutation;
- no promise that a later licensed source has identical field semantics.

### 3.3 Hard invariants

1. **No fabrication:** only a complete provider observation becomes a bar. Null, partial, non-minute-aligned, malformed, or absent observations are evidence records, not prices.
2. **UTC identity:** uniqueness is `(source, exchange_code, interval_seconds, bar_open_utc)`. `bar_open_utc` is an exact minute boundary in UTC. Raw source timestamps are also retained.
3. **Single writer:** fetches may overlap concurrently, but only one writer serializes state transitions and bar upserts.
4. **Cursor follows data:** no cursor advances beyond a transactionally committed observation/gap accounting boundary.
5. **Idempotency:** replaying an identical fetch changes no price data and creates no duplicate key.
6. **Corrections are visible:** a changed complete provider observation creates a revision record before the active value changes.
7. **Unknown stays unknown:** source-empty is not automatically “no trade,” “suspension,” or “outside session.”
8. **Immutable history:** universe and calendar snapshots are versioned; published Parquet is content-addressed and replaced only through a manifest-controlled correction workflow.
9. **Bounded operation:** concurrency, HTTP attempts, temporary files, logs, response retention, SQLite/WAL size, and disk use have explicit caps.
10. **Fail closed:** schema drift, invalid time conversion, overlapping processes, and low disk pause ingestion safely.

## 4. Minimal architecture

```text
versioned universe + official calendar snapshots
                    |
             scheduler/recovery planner
                    |
     bounded provider adapter (max concurrency 2)
                    |
        validation + single-writer queue
                    |
    SQLite WAL: hot bars/state/gaps/revisions/provenance
                    |
       daily atomic Parquet compactor + manifest
                    |
       health/status/export (read-only snapshots)
```

### 4.1 Concrete store comparison

| Option | Advantages | Failure/cost concerns | Decision |
|---|---|---|---|
| SQLite WAL only | ACID, unique constraints, simple recovery, one file | Multi-year analytical table and backup grow; archive publication is less portable | Use for hot state, not the sole archive. |
| Parquet only | Compact, immutable, easy analytics | Unsafe for fine-grained cursors/upserts; correction and crash state need a catalog | Use for finalized archive, not live state. |
| SQLite WAL + Parquet | ACID ingestion plus compact immutable archive; both local and inspectable | Requires a small two-phase compaction protocol | **Selected.** |
| DuckDB as writer/catalog | Excellent analytics and Parquet support | Adds another write engine and lock/lifecycle surface | Defer; it can query exported Parquet later without being a collector dependency. |
| Postgres or queue stack | Multi-client/distributed features | Operational complexity without a measured requirement | Reject. |

**[V]** SQLite WAL permits readers and one writer to proceed concurrently, while writers remain serialized. SQLite documents WAL and atomic commit behavior; the plan still tests power-loss boundaries on this filesystem. Sources: [SQLite WAL](https://sqlite.org/wal.html) and [SQLite atomic commit](https://sqlite.org/atomiccommit.html).

### 4.2 Proposed implementation tree

These are planned files, not files created in this turn:

```text
pyproject.toml
uv.lock
.python-version
README.md
config/
  universes/bist50/2026q3.json
  calendars/bist/2026.json
src/bist_minute_collector/
  __init__.py
  cli.py
  config.py
  models.py
  provider.py
  calendar.py
  store.py
  archive.py
  gaps.py
  runner.py
tests/
  fixtures/yahoo_chart/
  unit/
  integration/
  network/
systemd/bist-minute-collector.service
```

Runtime dependencies should initially be only `httpx` and `pyarrow`; use the standard library for SQLite, dataclasses, logging, hashing, locking, TOML reads, and `zoneinfo`. Development dependencies: `pytest`, `pytest-cov`, `mypy`, and `ruff`. `httpx.MockTransport` is sufficient for HTTP failure tests. Do not add a framework until a failing requirement proves it necessary. Source: [uv project documentation](https://docs.astral.sh/uv/) and [PyArrow Parquet documentation](https://arrow.apache.org/docs/python/parquet.html).

### 4.3 Provider contract

The provider boundary accepts an exchange instrument/provider alias, UTC half-open request window, and interval. It returns a `FetchEnvelope` containing:

- source name and adapter/schema versions;
- request ID and requested `[start_utc, end_utc)`;
- started/completed/retrieved UTC timestamps;
- sanitized endpoint identity and parameters;
- HTTP status, latency, byte count, retry count, and selected response headers such as `Retry-After`;
- SHA-256 of the exact response bytes;
- source metadata (`currency`, exchange timezone/name, granularity);
- zero or more raw observations and validation issues.

The normalized `ProviderBar` contains source, exchange code, provider symbol, interval, exact UTC bar-open, raw epoch, session date/timezone/segment, OHLCV, first/last observation times, revision number, quality flags, and fetch ID.

A licensed trade/tick adapter may later emit trade-derived one-minute bars through the same normalized contract, but its `source` and derivation version remain distinct. Yahoo bars and licensed bars are never silently merged under one identity.

### 4.4 SQLite state

SQLite opens with WAL mode, `foreign_keys=ON`, a bounded busy timeout, and `synchronous=FULL` unless the crash tests prove an equally safe machine-specific alternative. Main tables:

- `universe_snapshot`, `universe_member` — immutable membership/version metadata;
- `calendar_snapshot`, `calendar_session`, `session_segment` — effective-dated source and trading segments;
- `fetch_run` — every attempt and outcome, with response checksum and retention classification;
- `bars_hot` — active normalized rows for at least the 45-day repair window;
- `bar_revision` — before/after values and causing fetch for changed observations;
- `cursor` — per source/symbol/interval last accounted window and last complete bar;
- `gap` and `gap_event` — current status plus append-only transition history/evidence;
- `partition_manifest` — pending/active/superseded archive files and hashes;
- `daily_metric`, `daemon_state` — bounded operational counters and heartbeat.

All ingestion for one envelope occurs in one `BEGIN IMMEDIATE` transaction: insert fetch outcome, validate/upsert bars and revisions, add/update gap evidence, then advance the accounted cursor. A killed process leaves the entire transaction committed or rolled back.

### 4.5 Raw/provenance retention

Full duplicate responses are not retained every ten minutes forever. The auditable compromise is:

- retain request/response metadata and SHA-256 for **every** attempt;
- retain compressed exact response bodies for the first successful fetch and the last successful fetch immediately before that session's post-close sweep/compaction per symbol/session for 400 days;
- retain exact bodies that caused schema errors, HTTP anomalies with bodies, quarantines, or price corrections for five years, subject to a separately reported 5 GiB anomaly cap;
- keep ordinary intermediate successful response bodies in a 48-hour/2 GiB ring, oldest-success-first eviction only after their checksums and normalized transaction commit;
- never evict the normalized bars, revision chain, request metadata, manifest, or unresolved-gap evidence under that ring policy;
- record an explicit `body_retained=false` reason when only checksum/metadata remains.

Logs never contain response bodies or secrets. A source correction remains reproducible from before/after normalized values, response hash, retained triggering body when applicable, adapter version, and fetch metadata.

### 4.6 Daily Parquet compaction and correction

- Partition by `source`, `interval`, and BIST `session_date`; store all active-universe symbols for a day together unless measurements show a file-size problem.
- Sort by exchange code and UTC bar-open. Use an explicit Arrow schema, Zstandard compression, dictionary encoding for low-cardinality strings, and file metadata containing dataset/schema/transformation versions.
- Write to a temporary staging path inside the writable collector data root on the same filesystem—never `/tmp`, especially with `PrivateTmp=true`—flush/fsync, compute SHA-256 and row statistics, mark a `PENDING` manifest entry, atomically rename, fsync the parent directory, then mark `ACTIVE` in SQLite.
- Compact only after the session has closed and the normal correction sweep has run. Keep hot rows for at least 45 days so the full conservative 28-day provider recovery window remains mutable.
- If a correction changes an archived day, create a new content-addressed partition, verify it, atomically switch the manifest, and mark the old file `SUPERSEDED`. Keep superseded files for 45 days; the SQLite revision chain persists.
- At startup, reconcile `PENDING`, missing, orphan temporary, and hash-mismatched files deterministically. Never delete a valid active partition merely because a retry failed.

## 5. Time, sessions, and bar interpretation

### 5.1 Canonical time

- Persist all event and retrieval instants in UTC.
- Preserve provider timezone metadata and the raw epoch.
- Derive `session_date` and `session_segment` using a versioned `Europe/Istanbul` calendar snapshot, not the workstation's local settings.
- Require exact-minute UTC alignment. A nonaligned source observation is quarantined; it is never rounded or floored.
- Use half-open windows `[start, end)` throughout.
- Tests include dates around Turkey's historical DST transitions even though current Turkey time is UTC+3, plus a UTC/Istanbul round-trip property test.

### 5.2 Authoritative session source and fallback

**[V]** Borsa İstanbul publishes the Equity Market trading schedule and official holiday/half-day tables. The Equity Market Procedure is the controlling detailed source for session and auction behavior. Sources: [Equity Market trading hours](https://borsaistanbul.com/en/markets/equity-market/trading-hours), [official holidays](https://borsaistanbul.com/en/official-holidays), and [Equity Market Procedure PDF](https://www.borsaistanbul.com/files/equity-market-procedure.pdf).

**[A]** Phase 1 manually transcribes the current effective session segments and yearly holiday/half-day table into a checked, versioned JSON snapshot with source URL, retrieval date, effective date, checksum, and reviewer. The implementation does not infer official sessions from Yahoo metadata.

Fallback policy if the official source cannot be refreshed:

1. continue using the last verified calendar only within its recorded validity;
2. fetch conservatively on weekdays if necessary, but mark the calendar confidence `unverified`;
3. never classify absent data as `outside-session` under an unverified calendar;
4. alert and require manual calendar refresh before making completeness statements.

### 5.3 Expected slot is not expected trade

The calendar creates **provider query slots**, not synthetic price rows. A missing minute might mean no trade, an auction without a minute bar, late publication, a provider omission, suspension, holiday/half-day mismatch, or fetch failure. Opening and closing auctions are modeled as session segments, not assumed to generate a bar at every minute. Symbol-specific suspension or market-state evidence is attached when an authoritative notice is available; otherwise the status remains provider-empty/unverifiable.

Daily `open` or the first Yahoo minute is not labeled an opening-auction price unless an official instrument/session mapping proves it. Collection preserves what the provider labeled and postpones stronger microstructure interpretation.

### 5.4 Complete, provisional, and corrected bars

- A canonical candidate requires all OHLC values, nonnegative integer volume, valid price invariants (`low <= open/close <= high`), correct interval, exact minute alignment, and compatible TRY/Istanbul metadata.
- Timestamped null arrays become source-empty observations/gap evidence, not bars.
- The non-minute-aligned trailing point observed in the test is quarantined.
- A complete aligned current-session bar may be stored as `provisional`; it becomes `finalized` after a later overlapping fetch or the post-session sweep confirms it. Parquet publication waits until the sweep.
- Re-fetching identical values updates `last_observed_at` only.
- Re-fetching changed complete values appends `bar_revision(before, after, observed_at, fetch_id, reason=source_correction)` and replaces the active hot row. Correction order is deterministic by retrieval timestamp then request ID.
- A later empty response never deletes a previously valid bar. It records contradictory provider evidence for review.

## 6. Polling, recovery, and gap semantics

### 6.1 Normal schedule and request budget

**[A] Initial schedule:** during verified session/query periods, start one cycle every ten minutes. Each symbol request overlaps the previous **120 minutes**. Fetch at most two symbols concurrently, feed results to one writer, and apply a small launch jitter/pacing delay. Run one post-close seven-day reconciliation per symbol after a 45-minute source-delay grace period. Outside scheduled sessions, update heartbeat but make no routine Yahoo calls.

This collects one-minute granularity without claiming one-minute latency. Using the measured 510-slot **Yahoo provider grid span** for conservative request arithmetic—not asserting a 510-minute official continuous-trading session:

- about 51 live cycles × 50 symbols = 2,550 requests/session;
- plus about 50 post-close reconciliation calls = **2,600 requests/session**;
- about 655,200 requests per 252-session year before retries;
- a two-worker cycle should take roughly 25–35 seconds at the measured latency, well below ten minutes.

These are planning figures, not Yahoo quotas. A hard initial cap of 3,000 requests per exchange session and 360 per rolling hour prevents runaway loops. The canary may lengthen polling to 15 minutes if 429s or latency show the initial budget is inappropriate. It must never shorten or add concurrency merely to chase lower lag.

### 6.2 Retry and circuit-breaker policy

- Timeout/connection/HTTP 5xx: at most three attempts for that window using approximately 2, 10, and 30 seconds plus bounded jitter.
- HTTP 429: honor `Retry-After`; otherwise use persisted cooldowns of 60 seconds, 2, 4, 8, then 15 minutes across later cycles—never a tight same-cycle loop.
- Three 429 responses in two minutes open a provider-wide 30-minute circuit and stop new calls while preserving recovery tasks.
- HTTP 4xx other than known retention responses: no blind retry; classify and alert.
- Malformed/schema-incompatible response: retain anomaly evidence, do not write bars, and pause the affected adapter after two symbols show the same drift.
- Per-cycle retry and wall-time budgets prevent one symbol from blocking all others. Fair round-robin scheduling avoids starvation.

### 6.3 Durable cursors

Each source/symbol/interval cursor records:

- last requested/accounted half-open window;
- last complete and last finalized bar times;
- latest successful fetch and latest attempted fetch;
- active universe/calendar versions;
- current provider recovery cutoff calculation;
- last error/cooldown.

The cursor represents accounted work, not “all minutes existed.” It advances only in the same transaction that records returned bars and missing-window evidence.

### 6.4 Gap states and transitions

| State | Meaning | Allowed transition |
|---|---|---|
| `pending` | A verified session/query slot is not yet accounted by a complete observation; retry remains due. | `recovered`, `provider-empty/unverifiable`, `outside-session`, or `unrecoverable-beyond-retention`. |
| `recovered` | A later complete aligned observation filled the interval. | Terminal for that gap; later price change is a bar revision, not reopening it. |
| `provider-empty/unverifiable` | At least three successful overlapping responses, including a post-session sweep, supplied no complete bar. This may be no trade, provider omission, suspension, or another unknown. | `recovered` while still retained, then `unrecoverable-beyond-retention` if unresolved. |
| `outside-session` | A versioned authoritative calendar proves the interval was outside an applicable session. | Terminal unless a calendar correction creates a new version and explicit reclassification event. |
| `unrecoverable-beyond-retention` | The gap is older than the conservative provider window or the provider explicitly rejected it as beyond retention. | Terminal for that source; a later licensed source creates separate-source bars and may link explanatory evidence, but does not rewrite this history. |

Every transition appends timestamp, cause, fetch ID or calendar version, attempt count, first/last seen, and recovery cutoff. No status is deleted. Contiguous minutes with identical evidence can be range-compressed while preserving endpoints and counts.

### 6.5 Restart/reboot recovery algorithm

1. Acquire the task lock before opening a write connection. If another writer owns it, exit with a distinct nonzero code and do not let systemd create a restart storm.
2. Run SQLite recovery/`quick_check`, reconcile incomplete fetch transactions and the Parquet manifest, and fail closed on corruption.
3. Load the active immutable universe and verified calendar snapshots.
4. For each symbol, calculate uncovered intervals from durable bars, gap ledger, and cursor—not from wall-clock memory.
5. Intersect those intervals with calendar query segments. Mark only officially proven closed intervals `outside-session`.
6. Calculate `retrieval_time - 28 days` as the conservative Yahoo recovery cutoff. Record older unresolved intervals as `unrecoverable-beyond-retention` with the offline interval and evidence; do not issue futile requests or close them silently.
7. Split recoverable work into windows of at most seven calendar days, process oldest-first with max concurrency two and global budgets, and make every replay idempotent.
8. Run a two-hour overlap for the latest session, then enter the normal schedule.
9. If recovery exceeds a cycle budget, interleave bounded recovery and live work fairly; persist the queue after each window.

A shutdown lasting three days should recover automatically if Yahoo still exposes the interval. A shutdown exceeding 28 days must produce an explicit irrecoverable ledger interval even if a current request returns normally.

## 7. Unattended operation with systemd user service

### 7.1 Service choice

**[T]** User systemd is running and lingering is enabled on this machine. **[A]** Use one long-running `bist-minute-collector.service`; do not add a timer. The daemon already owns market-aware scheduling and persisted recovery. A timer would add overlapping-run and catch-up ambiguity. PM2 and Docker add no measured benefit.

Planned unit location and runtime paths:

```text
~/.config/systemd/user/bist-minute-collector.service
~/.config/bist-minute-collector/config.toml
~/.config/bist-minute-collector/env          # optional, mode 0600, never committed
~/.local/share/bist-minute-collector/         # SQLite, Parquet, retained response evidence
~/.local/state/bist-minute-collector/         # lock, JSONL logs, health snapshot
```

Planned unit responsibilities:

- `After=` and `Wants=network-online.target`; startup never assumes the network is actually usable, so the daemon applies bounded retries;
- `Type=simple`, an absolute virtual-environment executable, explicit working directory and data/config paths;
- `EnvironmentFile=-%h/.config/bist-minute-collector/env`; optional absence is allowed, and the free Yahoo adapter needs no secret;
- `Restart=on-failure`, `RestartSec=30s`, `StartLimitIntervalSec=10min`, `StartLimitBurst=5`;
- `TimeoutStopSec=45s`, `KillSignal=SIGTERM`, and no privilege escalation;
- `UMask=0077`, `NoNewPrivileges=true`, `PrivateTmp=true`, `ProtectSystem=strict`, with only the two task data/state roots writable; validate these directives with local systemd 255 before enablement;
- `WantedBy=default.target`, which works with the verified `Linger=yes` after explicit enablement.

Single-instance protection is layered: systemd has one unit instance; the process also takes a nonblocking advisory lock in its own state directory; SQLite permits one writer. Manual mutating commands refuse to run while the daemon owns the lock, except a controlled local RPC is explicitly deferred.

Source references: [systemd.service](https://www.freedesktop.org/software/systemd/man/latest/systemd.service.html), [systemd.exec](https://www.freedesktop.org/software/systemd/man/latest/systemd.exec.html), and [loginctl/linger](https://www.freedesktop.org/software/systemd/man/latest/loginctl.html).

### 7.2 Graceful and abrupt stop

On `SIGTERM`, stop scheduling new HTTP requests, cancel or finish in-flight requests within their 15-second timeout, drain the single-writer queue, commit/rollback its current transaction, checkpoint health as `stopped`, and exit within 45 seconds. `SIGKILL` is tested separately: WAL recovery must restore the last committed state and reschedule any incomplete fetch without advancing its cursor.

### 7.3 Operator CLI

All commands support `--json` for automation and redact secret-bearing values.

```text
bist-minute-collector install-service   # copy/verify only this user unit; never enable/start
bist-minute-collector start             # systemctl --user start only the named unit
bist-minute-collector stop              # graceful stop only the named unit
bist-minute-collector status            # unit + heartbeat + lag/gap/disk summary
bist-minute-collector doctor            # config, lock, DB, schema, calendar, universe, disk, clock
bist-minute-collector collect-once      # one bounded overlap cycle, no daemon
bist-minute-collector backfill          # explicit bounded source/symbol/time window; dry-run default
bist-minute-collector gaps              # list/summarize ledger; never auto-hide
bist-minute-collector compact           # idempotent eligible-day compaction
bist-minute-collector export            # read-only UTC/session-filtered Parquet or CSV export
```

During implementation via `uv`, equivalent development commands are `uv run bist-minute-collector ...`. `install-service` runs `systemd-analyze --user verify`, writes only the task unit atomically, and runs `systemctl --user daemon-reload`; enabling and starting remain separate explicit actions.

### 7.4 Logs, heartbeat, and alerts

- JSONL application logs rotate at 20 MiB × 10 files (200 MiB cap); journal output contains only concise lifecycle/warning/error records.
- Heartbeat updates every 60 seconds with process start ID, version, universe/calendar versions, last successful cycle, writer queue depth, and lifecycle state.
- Persist per-symbol latest complete/finalized bar age, latest source timestamp, and last fetch outcome.
- Persist counters for HTTP success/429/4xx/5xx/timeout/schema error, retries/circuit opens, inserted/unchanged/revised/quarantined bars, gap transitions, compactions, and bytes transferred/written.
- `status` warns if heartbeat is older than 3 minutes during expected operation and is critical at 15 minutes; during a normal session it warns if per-symbol finalized-bar age exceeds 30 minutes and is critical over 45 minutes. These thresholds include the measured ~15-minute Yahoo lag. Any calibration must be preregistered from the two-/five-symbol canary **before** the first 50-symbol acceptance window; thresholds are never loosened after seeing that window's results.
- Summarize unresolved gaps by state, age, symbol, and recoverability cutoff. “Zero gaps” is never manufactured by excluding unknowns.
- Alert on request-cap exhaustion, circuit open, schema drift, stale calendar/universe, retained-response cap, failed checksum, corrupt DB/partition, and disk thresholds.
- This local plan initially exposes health through `status --json`, logs, and systemd state. Email, ntfy, Prometheus, or a web UI are deferred.

## 8. Storage and retention budget

### 8.1 Row and request calculation

**[T]** Yahoo commonly exposed 481 complete rows within a 510-slot full-day grid in the sample, with lower counts on some dates. The capacity baseline is therefore:

```text
50 symbols × 481 complete rows × 252 sessions = 6,060,600 rows/year
6,060,600 × measured 24.6 Parquet bytes/row ≈ 149 MB/year base payload
```

The design budgets **0.75 GB/year** for normalized Parquet and revisions—about five times the measured compressed payload—to cover partition/schema overhead, corrections, and variation. It does not treat 481 as a completeness target.

### 8.2 Budget by class

| Class | Policy | Conservative bound |
|---|---|---:|
| Normalized active/revision Parquet | Keep indefinitely; 0.75 GB/year budget | 7.5 GB/10 years |
| SQLite hot/state | 45 days of bars; cursor/gap/current metadata; routine vacuum/checkpoint only when safe | 2.0 GiB including WAL headroom |
| Request provenance archive | Every request metadata compacted to Parquet; retain indefinitely at 0.5 GB/year | 5.0 GB/10 years |
| Normal raw evidence | First/final response per symbol/session, 400-day retention | ≤1.2 GiB steady state |
| Anomaly/correction bodies | Five-year retention, explicit cap; never silently evict unresolved evidence | 5.0 GiB |
| Intermediate response ring | 48 hours or 2 GiB, whichever comes first | 2.0 GiB |
| Structured logs | 20 MiB × 10 | 0.2 GiB |
| Environment/source/venv | Project and dependencies | 1.0 GiB |
| Compaction/temp/backup headroom | Same-filesystem atomic publication and one DB safety copy | 4.0 GiB |
| Unallocated schema/provider growth | Guard band | about 12 GiB |
| **Configured collector ceiling** | All classes | **40 GiB** |

The ten-year planned usage before the guard band is roughly 23 GiB. Against the currently measured 131.87 GiB free, the 40 GiB ceiling is feasible, but unrelated disk use can change that conclusion.

### 8.3 Low-disk fail-safe

- Warn when collector-owned bytes exceed 30 GiB, filesystem free space falls below 20 GiB, or projected 30-day growth crosses either threshold.
- Before each network cycle and compaction, calculate free bytes and required worst-case transaction/temp/WAL headroom.
- Pause new fetches safely when collector-owned bytes reach 38 GiB, free space is below 10 GiB, or free space is below twice the measured worst compaction plus WAL requirement—whichever is most conservative.
- Finish or roll back the current transaction, set heartbeat to `paused_low_disk`, retain all existing normalized/gap evidence, and exit nonzero only after state is durable.
- Automated cleanup may remove only expired ordinary response-ring files and rotated logs that already satisfy retention. It never deletes canonical bars, revision history, active manifests, unresolved gaps, or anomaly evidence to keep running.
- Resume only after `doctor` verifies space and an operator explicitly starts the service.

## 9. Test-driven implementation phases

No mocked unit suite is described as comprehensive. Each phase has deterministic tests, failure injection, a bounded real check where appropriate, rollback, and a stop gate. All commands below are future implementation commands, not run in this planning turn.

### Phase 1 — Provider/universe spike and frozen contracts

**RED tests**

- canonical 50-symbol snapshot produces exactly the expected checksum and rejects duplicate/unknown aliases;
- fixture parser accepts complete aligned OHLCV, rejects partial/null/nonaligned values, and retains raw timestamps;
- fixtures reproduce HTTP 200, 422 retention, 429 with/without `Retry-After`, 5xx, timeout, malformed JSON, array-length mismatch, missing metadata, and unexpected granularity;
- request splitter never emits more than seven calendar days;
- source-empty does not become a zero-volume bar;
- calendar snapshot rejects missing source/effective/checksum metadata.

**GREEN scope:** typed domain/provider interfaces, frozen BIST50 snapshot, frozen sanitized Yahoo fixtures from the bounded sample, current official calendar snapshot, parser/validator, and a network-smoke marker. No persistence daemon.

**Failure injection:** corrupted fixture bytes, null arrays, non-minute tail, provider timezone mismatch, simulated retention rollover, and source-list disagreement.

**Verification commands**

```bash
uv sync --frozen
uv run ruff check .
uv run mypy src tests
uv run pytest tests/unit/test_universe.py tests/unit/test_provider_contract.py -q
uv run pytest -m network tests/network/test_yahoo_smoke.py -q
```

The network smoke requests exactly the five named symbols, maximum concurrency two, a one-day window, and no retry storm. It records status/schema/lag without asserting availability as a contract.

**Rollback:** delete only Phase 1 uncommitted implementation files or revert a separately authorized phase commit; never alter the research report or historical snapshot in place.

**Stop gate:** stop if the official 50 cannot be reconciled, provider terms are unacceptable, one-minute granularity disappears, response identity cannot be validated, or the bounded source contract cannot distinguish empty/error/schema states.

### Phase 2 — Crash-safe normalized store, provenance, and compaction

**RED tests**

- duplicate envelope replay leaves one key and stable revision count;
- out-of-order late observations converge to the same state;
- changed values create exactly one auditable revision;
- cursor never advances after injected failure before commit;
- a crash after temp Parquet write, after pending manifest, and after rename is recovered deterministically;
- SQLite uniqueness, transaction rollback, WAL reopen, and `quick_check` pass;
- disk-full/ENOSPC and low-disk preflight preserve existing data;
- retained-response ring cannot evict required anomaly/correction evidence.

**GREEN scope:** schema/migrations, one-writer transaction path, idempotent upsert, revisions, provenance, hot retention, Parquet compactor, manifests, checksums, and disk guard. No scheduler.

**Failure injection:** exception/`SIGKILL` at every transaction and publication boundary; truncated temp file; wrong hash; read-only directory; mocked ENOSPC; duplicated fetch IDs.

**Verification commands**

```bash
uv run pytest tests/unit/test_store.py tests/unit/test_archive.py -q
uv run pytest tests/integration/test_crash_consistency.py -q
uv run bist-minute-collector doctor --data-root "$TEST_DATA_ROOT"
uv run bist-minute-collector compact --data-root "$TEST_DATA_ROOT"
```

**Rollback:** stop writers, preserve a checksumed state copy, revert only the additive migration/code, and reopen the last verified schema. Never roll back by deleting unexplained rows.

**Stop gate:** no phase passes until all transaction kill points recover without duplicate/lost committed rows and a byte-for-byte rerun produces the same active partition hashes.

**Implementation evidence (2026-08-03):** Phase 2 completed strict RED → GREEN → REFACTOR. The final offline suite passed 75 tests with only the opt-in network test deselected; the focused store/archive and crash-consistency suites passed 17 and 5 tests respectively. A task-owned CLI root passed `doctor`, `compact`, a second `doctor`, SQLite `quick_check`, and manifest/file SHA-256 reconciliation before safe removal. The exact same-session implementation review returned **APPROVE** with no Critical, High, or Medium findings. Full evidence and Low-finding dispositions are linked in §13.

### Phase 3 — Scheduler, bounded concurrency, overlap, and backoff

**RED tests**

- fake-clock schedule emits one due cycle and never overlaps itself;
- global fetch concurrency never exceeds two and writer concurrency remains one;
- two-hour overlapping responses are idempotent and late/corrected bars follow revision rules;
- 429 honors `Retry-After`, opens the persisted circuit, and never causes rate-limit evasion;
- 5xx/timeouts exhaust bounded retry budgets; one slow symbol cannot starve others;
- malformed common schema pauses the adapter; per-symbol bad payload isolates that symbol;
- shutdown stops scheduling and drains/rolls back within the deadline;
- request/hour and request/session caps survive process restart.

**GREEN scope:** monotonic-clock scheduler plus UTC wall-clock planning, two fetch workers, one writer queue, overlap planner, persisted cooldown/circuit, fair per-symbol ordering, and `collect-once`.

**Failure injection:** delayed responses, connection reset, timeout, 429 bursts, 500/503, clock jump, SIGTERM, SIGKILL, full writer queue, and two concurrent process starts.

**Verification commands**

```bash
uv run pytest tests/unit/test_scheduler.py tests/unit/test_backoff.py -q
uv run pytest tests/integration/test_collector_once.py tests/integration/test_process_lock.py -q
uv run bist-minute-collector collect-once --symbols ASELS.IS,THYAO.IS --dry-run
uv run bist-minute-collector collect-once --symbols ASELS.IS,THYAO.IS
```

**Rollback:** stop the foreground process; preserve store contents; revert scheduling code/config to the last phase. There is still no enabled service.

**Stop gate:** fail if concurrency/caps can be bypassed, duplicate processes can write, a cancellation advances cursors, or real bounded requests cannot be fully accounted.

**Implementation evidence (2026-08-03):** Phase 3 completed strict RED → GREEN → REFACTOR. The exact scheduler/backoff suite passed 11 tests; the collector/process suite passed 12; the final offline suite passed 102 tests with one opt-in network test deselected. Real subprocess tests cover writer exclusion, SIGTERM cancellation, and SIGKILL transaction rollback. A fresh bounded Yahoo foreground check made exactly two requests (`ASELS.IS`, `THYAO.IS`), recorded two terminal successes, 964 bars, two cursors, zero nonterminal attempts, maximum fetch concurrency two, and maximum writer concurrency one before its task-owned root was removed from `/tmp`. The implementation review initially returned **REVISE** for a permanent schema-drift pause; additive schema v3 introduced a durable 30-minute expiry, and the one permitted focused same-session confirmation returned **APPROVE** with no remaining Critical, High, or Medium findings. Full evidence and dispositions are linked in §13.

### Phase 4 — Gap detection, calendar, and restart/offline recovery

**RED tests**

- all five gap states and permitted transitions preserve append-only evidence;
- three-day simulated offline recovery splits bounded windows oldest-first and repairs available bars;
- 35-day simulated offline recovery emits `unrecoverable-beyond-retention` without a fabricated bar or futile request;
- provider-empty remains uncertain, can recover later, and never deletes an existing bar;
- weekends, official holidays, half-days, opening/closing segments, and unverified-calendar fallback classify correctly;
- historical DST/UTC conversions round-trip; current Istanbul time remains unambiguous;
- a symbol addition/removal changes future scheduling only and preserves old snapshot membership;
- suspensions and unknown instrument state do not become zero bars;
- recovery interruption resumes idempotently from committed state.

**GREEN scope:** versioned calendar engine, coverage/gap planner, transition ledger, backfill command with dry-run default, startup recovery, universe-version switching, and gap reporting.

**Failure injection:** calendar source stale/disagreement, malformed holiday date, missing segment, process kill mid-backfill, 422 retention response, 429 during recovery, provider-empty then late data, and snapshot switch during a stopped process.

**Verification commands**

```bash
uv run pytest tests/unit/test_calendar.py tests/unit/test_gaps.py -q
uv run pytest tests/integration/test_offline_recovery.py -q
uv run bist-minute-collector backfill --symbols ASELS.IS,THYAO.IS --from 2026-08-01T00:00:00Z --to 2026-08-03T00:00:00Z --dry-run
uv run bist-minute-collector gaps --state pending,provider-empty/unverifiable,unrecoverable-beyond-retention --json
```

**Rollback:** deactivate only the candidate calendar/universe snapshot and return to the last verified active versions. Preserve gap transition history.

**Stop gate:** fail if any missing interval disappears without a terminal/evidenced state, authoritative and fallback calendar confidence are conflated, or offline recovery exceeds provider/request bounds.

### Phase 5 — systemd user service and boot recovery

This phase requires a separate explicit authorization to install and enable the user unit.

**RED tests**

- rendered unit uses only task paths, absolute executable, user privileges, validated hardening, and no embedded secret;
- the rendered unit and every subprocess identify the pinned uv-managed Python 3.12 environment and never invoke bare `python3`;
- `install-service` neither enables nor starts the unit;
- second instance fails cleanly without a restart storm;
- `SIGTERM`, `systemctl --user stop`, `kill -9`, and simulated abnormal exit preserve invariants;
- unavailable network at start produces bounded backoff, not permanent failure or tight restarts;
- stale heartbeat and failed restart limit appear in `status`.

**GREEN scope:** unit renderer/installer, start/stop/status wrappers restricted to the exact unit, structured log rotation, heartbeat, health checks, and boot-resume logic.

**Failure injection:** invalid environment-file permissions, absent network, process kill, repeated exits hitting start limit, read-only data root, stale lock owner, and restart with pending compaction.

**Verification commands**

```bash
uv run bist-minute-collector install-service --dry-run
systemd-analyze --user verify systemd/bist-minute-collector.service
uv run bist-minute-collector install-service
systemctl --user daemon-reload
systemctl --user start bist-minute-collector.service
systemctl --user status bist-minute-collector.service --no-pager
journalctl --user-unit=bist-minute-collector.service --since=-10min --no-pager
uv run bist-minute-collector status --json
systemctl --user stop bist-minute-collector.service
```

Actual enablement, if separately authorized:

```bash
systemctl --user enable --now bist-minute-collector.service
```

**Rollback:** `systemctl --user disable --now bist-minute-collector.service`, remove only `~/.config/systemd/user/bist-minute-collector.service`, then `systemctl --user daemon-reload`. Keep data unless the user separately authorizes deletion.

**Stop gate:** unit verification, foreground tests, kill/restart tests, and user-session tests must pass before enablement. Boot/login-independent recovery remains unverified until the user authorizes and observes an actual reboot.

### Phase 6 — Real canary and continuous operation

Roll out without changing test thresholds after seeing results:

1. `ASELS.IS` and `THYAO.IS`: two complete live trading sessions, one graceful restart, and one forced crash/recovery.
2. Add `AKBNK.IS`, `TUPRS.IS`, and `DSTKF.IS`: three complete live sessions plus a weekend restart simulation.
3. All 50: at least **five consecutive trading sessions**, one weekend, a process crash, and a machine reboot if authorized.
4. Continue for weeks only after review of the five-session report and an explicit start/enable decision.

**Acceptance thresholds**

- 100% of normalized rows satisfy schema/OHLC/time invariants; zero fabricated or forward-filled bars;
- zero duplicate primary keys and deterministic identical replay results;
- every request has a terminal fetch record; every absent expected query slot has a current gap state/evidence;
- every changed price row has exactly one ordered correction chain; no later empty response deletes it;
- during ordinary open sessions, p95 finalized-bar age ≤30 minutes and p99 ≤45 minutes, excluding explicitly recorded provider-wide incidents; thresholds are reported, not retroactively loosened;
- p95 50-symbol collection cycle ≤2 minutes, maximum concurrency two, request caps never exceeded;
- HTTP 429 under 1% of attempts during the observation window, with 100% honoring cooldown; at or above 1% pauses scale-up and triggers a longer poll experiment;
- recoverable gaps receive the scheduled overlap and post-close attempts; zero gaps are silently reclassified or aged out;
- a three-day offline simulation recovers all provider-returned bars; a 35-day simulation records the entire irrecoverable interval truthfully;
- SQLite `quick_check` passes, every active Parquet checksum/row count reconciles, and no orphan active manifest exists after crashes;
- average collector CPU below 5% of one core, RSS below 250 MiB, persistent growth below 250 MiB per normal session, and no low-disk threshold crossed;
- calendar/universe checksums match active manifests, and source staleness is visible;
- logs and exports contain no credentials or raw secret-bearing environment values.

**Real verification commands**

```bash
uv run pytest -q
uv run pytest -m network tests/network/test_yahoo_smoke.py -q
uv run bist-minute-collector doctor --deep
uv run bist-minute-collector status --json
uv run bist-minute-collector gaps --json
uv run bist-minute-collector compact --check
uv run bist-minute-collector export --symbols ASELS.IS,THYAO.IS --from <UTC> --to <UTC> --format parquet
systemctl --user show bist-minute-collector.service -p ActiveState -p SubState -p NRestarts
systemctl --user is-enabled bist-minute-collector.service
```

**Rollback:** stop/disable only the task unit, preserve and checksum the dataset, return to foreground `collect-once`, and review canary evidence. Do not discard gaps to make metrics pass.

**Stop gate:** continuous unattended collection begins only after all 50-symbol thresholds pass for the minimum window and the user explicitly authorizes enablement. A real reboot, real market sessions, naturally occurring source corrections/outages, an actual holiday/half-day, and survival for weeks cannot be proven by unit tests; they remain separately observed evidence.

## 10. Git, security, and operating boundaries

- **[T]** Phase 1 authorization initialized this directory as a local Git repository on `main`; it still has no commits, staged files, or remotes.
- **[A]** A secret-safe `.gitignore` now exists. Staging, committing, adding a remote, and pushing remain separate future approvals.
- Source contains no credentials. Free Yahoo needs none. A later licensed provider uses a mode-0600 environment file or OS credential facility, never CLI arguments or logs.
- The service runs as the current unprivileged user. No `sudo`, system service, open port, broker connection, or network listener is required.
- Exports are explicit and read-only; they include source, universe/calendar, schema/transformation, and partition-manifest versions.
- Vendor/storage/redistribution rights remain a legal acceptance gate. Personal-research access does not authorize redistribution.

## 11. Decisions, blockers, and next command

### Decisions frozen by this plan

- one-minute is bar granularity; ten minutes is initial polling cadence;
- Yahoo Chart is an isolated, unofficial free adapter; licensed sources are future adapters;
- SQLite WAL plus daily Parquet, single writer, two bounded fetch workers;
- UTC canonical timestamps plus versioned Europe/Istanbul session metadata;
- immutable universe/calendar snapshots and visible correction/gap histories;
- 28-day conservative Yahoo recovery horizon and seven-day maximum request window;
- systemd user service with no timer, PM2, container, or privilege escalation;
- 40 GiB total cap with safe pause thresholds;
- no synthetic bars under any circumstance.

### Remaining decisions/blockers

1. **[U] Yahoo behavior:** no quota, completeness guarantee, stable schema, or SLA; the canary can reject this source operationally.
2. **[U] Rights:** confirm that intended personal research storage remains within Yahoo terms before continuous use; no redistribution is planned.
3. **[U] Universe history:** the official dated constituent CSV exists and supports deterministic current-universe updates, but it is a current snapshot rather than a documented point-in-time membership archive. Historical membership before retained snapshots remains unavailable through this source.
4. **[U] Calendar/suspensions:** Phase 1 froze the sourced current 2026 schedule and holiday transcription, but symbol-level suspensions and historical calendar/rule states still require later official-source work.
5. **[U] Boot evidence:** `Linger=yes` and unit semantics support boot resume, but only an actual reboot proves this machine's end-to-end behavior.
6. **[U] Long observation:** weeks-long uptime, natural 429/outage/correction behavior, and holiday/half-day behavior cannot be established in this planning turn.
7. **[A] Git and service actions:** local initialization and the authorized phase-boundary commits are complete. Unit installation, enablement, continuous start, remotes, merge, and push remain distinct future approvals.

### Next implementation gate

Phases 1–3 are complete. There is no authorized next implementation command. The next gate is an explicit user decision authorizing **Phase 4 RED tests** for calendar/session classification, append-only gap states, bounded recent recovery, and truthful beyond-retention evidence. The first planned command is `uv run pytest tests/unit/test_calendar.py tests/unit/test_gaps.py -q`; it has not been run. Gap recovery, service work, persistent collection, remotes, merge, and push remain stopped until that decision.

## 12. Source register

Verified/referenced on 2026-08-03:

- Borsa İstanbul, BIST 50 index: <https://borsaistanbul.com/en/index/xu050>
- Borsa İstanbul, current stock-index constituent CSV: <https://www.borsaistanbul.com/datum/hisse_endeks_ds.csv>
- Borsa İstanbul, 2026 Q3 periodic review: <https://borsaistanbul.com/en/announcement/15483/bist-stock-indices-periodic-review>
- KAP, current BIST 50 constituents: <https://kap.org.tr/en/Endeksler?indice=QklTVCA1MA>
- KAP, example non-periodic index change: <https://www.kap.org.tr/tr/Bildirim/1618229>
- Borsa İstanbul, Equity Market trading hours: <https://borsaistanbul.com/en/markets/equity-market/trading-hours>
- Borsa İstanbul, official holidays: <https://borsaistanbul.com/en/official-holidays>
- Borsa İstanbul, Equity Market Procedure: <https://www.borsaistanbul.com/files/equity-market-procedure.pdf>
- yfinance documentation: <https://ranaroussi.github.io/yfinance/index.html>
- Yahoo API Terms of Use: <https://legal.yahoo.com/us/en/yahoo/terms/product-atos/apitnc/index.html>
- SQLite WAL: <https://sqlite.org/wal.html>
- SQLite atomic commit: <https://sqlite.org/atomiccommit.html>
- Apache Arrow Parquet: <https://arrow.apache.org/docs/python/parquet.html>
- uv: <https://docs.astral.sh/uv/>
- systemd service: <https://www.freedesktop.org/software/systemd/man/latest/systemd.service.html>
- systemd execution environment: <https://www.freedesktop.org/software/systemd/man/latest/systemd.exec.html>
- loginctl/linger: <https://www.freedesktop.org/software/systemd/man/latest/loginctl.html>

## 13. Review status

The required read-only OpenCode review was completed in exact session `ses_03834f75effeZZa1No7tD39myV` with exact model `opencode-go/deepseek-v4-flash` and `--agent plan`. Its initial verdict was **REVISE**, with no Critical or High findings and one Medium finding about the official machine-readable constituent CSV. The warranted correction and the five useful Low clarifications were applied. The single permitted focused same-session confirmation returned **APPROVE** with no remaining Critical, High, or Medium findings.

The [full review](../reviews/bist-minute-collector-opencode-review.md), [focused confirmation](../reviews/bist-minute-collector-opencode-confirmation.md), and [finding disposition](../reviews/bist-minute-collector-review-disposition.md) are retained as planning artifacts. That planning-review gate was later satisfied for Phase 1 only; no persistent collector, service, credentials, or continuous process has been created.

**Implementation status (2026-08-03):** the user separately authorized and completed Phases 1–3. A local Git repository was initialized on `main`. The verified Phase 1–2 foundation is commit `d098be5`; Phase 3 is kept in its own authorized local phase-boundary commit. Phase 1 provider/universe contracts and Phase 2 crash-safe storage/provenance/revision/Parquet contracts each completed strict RED → GREEN → REFACTOR and received a same-session DeepSeek Flash implementation verdict of **APPROVE**, with no Critical, High, or Medium findings.

The [full Phase 1 review](../reviews/bist-minute-collector-phase1-opencode-review.md), [Phase 1 disposition](../reviews/bist-minute-collector-phase1-review-disposition.md), [full Phase 2 review](../reviews/bist-minute-collector-phase2-opencode-review.md), [Phase 2 disposition](../reviews/bist-minute-collector-phase2-review-disposition.md), and [Phase 2 RED/GREEN evidence](../reviews/bist-minute-collector-phase2-red-green-evidence.md) are retained. No focused Phase 2 confirmation was requested because only Low findings were fixed, while the user's confirmation condition applies only to Critical, High, or Medium fixes.

The [full Phase 3 review](../reviews/bist-minute-collector-phase3-opencode-review.md) returned **REVISE** with one Medium and six Low findings. M1 and L1 were fixed test-first; all findings are reconciled in the [Phase 3 disposition](../reviews/bist-minute-collector-phase3-review-disposition.md). Because M1 required a fix, the single permitted [focused Phase 3 confirmation](../reviews/bist-minute-collector-phase3-opencode-confirmation.md) ran in the same session/model/agent and returned **APPROVE**, explicitly with no remaining Critical, High, or Medium finding. The [Phase 3 RED/GREEN evidence](../reviews/bist-minute-collector-phase3-red-green-evidence.md) records the exact gates and bounded live request accounting. Phase 4 and every later phase remain unauthorized; no service, credentials, persistent collector, broker connection, remote, merge, or push exists.
