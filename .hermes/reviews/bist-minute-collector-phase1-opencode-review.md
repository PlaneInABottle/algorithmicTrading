# OpenCode Phase 1 Implementation Review

- Date: 2026-08-03 (Europe/Istanbul)
- Session: `ses_03834f75effeZZa1No7tD39myV`
- Model: exactly `opencode-go/deepseek-v4-flash`
- Agent: exactly `plan`
- Mode: read-only; the reviewer did not edit files, install anything, or make network calls
- Verdict: **APPROVE**

## Review request

> Perform the required focused read-only Phase 1 implementation review in this exact BIST finance session. Review the workspace implementation against Phase 1 of .hermes/plans/bist-minute-collector.md and the latest user scope. Do not edit files, initialize/stage/commit Git, install dependencies or services, start processes, make network requests, create credentials, or implement anything. You may inspect files and existing test/config evidence read-only.
>
> Focus on: (1) provider/error outcome truthfulness for success, empty/null/partial, retention 422, 429 with/without Retry-After, 5xx, timeout, malformed/schema errors, timezone/granularity, and nonaligned provisional tails; (2) exact aligned complete 1-minute OHLCV acceptance with raw timestamps/metadata/issues retained and no synthetic bars; (3) official .E identity versus Yahoo .IS alias, exact 50/checksums, source disagreement, and immutable snapshot semantics; (4) sourced current calendar metadata/checksum and explicit limits around history, suspensions, and expected bars; (5) seven-calendar-day splitter; (6) the direct unofficial Yahoo adapter's one-day/no-retry/max-two behavior, isolation, terms/retention uncertainty, and absence of yfinance runtime; (7) tests, fixture sanitization/secret hygiene, typing/lint, and whether the network test avoids SLA assertions; (8) strict Phase 1 scope—no storage, scheduler, gap recovery, systemd, persistent collector, trading, or broker code; (9) Git boundary—initialized main but nothing staged/committed/remoted.
>
> Return exactly one verdict: APPROVE, REVISE, or BLOCK. List every finding with Critical/High/Medium/Low severity, exact file/section, rationale, and the smallest test-first correction. Distinguish bugs from optional hardening and conservative decisions. If there are no Critical/High/Medium findings, say so explicitly. Do not recommend broader architecture or Phase 2 work.

## Full reviewer response

Phase 1 implementation review complete. I inspected all source, config, fixtures, tests, lockfile, Git state, and prior review disposition read-only (I did not run pytest or any network call; test evidence is from the pytest cache, which shows 52 collected tests and an empty `lastfailed`).

# Phase 1 Review — `.hermes/plans/bist-minute-collector.md` implementation

**Verdict: APPROVE.** There are **no Critical, High, or Medium findings**. Every one of the nine focus areas is correctly implemented and bounded. The remaining items are Low observations and optional hardening, none of which requires another review.

## Verified compliance (all nine areas)

1. **Provider/error truthfulness** — `src/bist_minute_collector/provider.py` emits typed outcomes for every required case: success, `SOURCE_EMPTY`, `NULL_ROW`, `PARTIAL_ROW`, retention 422 (`RETENTION_LIMIT`, never misreported as empty/delisted), 429 with/without `Retry-After`, 5xx, timeout, `MALFORMED_JSON`, `ARRAY_LENGTH_MISMATCH`, `MISSING_METADATA`, `UNEXPECTED_GRANULARITY/TIMEZONE/CURRENCY`, `SYMBOL_MISMATCH`, `INVALID_ROW`, and `NON_MINUTE_ALIGNED_PROVISIONAL_TAIL`. Each carries `raw_source_timestamp`/`row_index`/`retry_after` evidence.
2. **Aligned 1-minute OHLCV** — only complete, minute-aligned rows become `MinuteBar` with raw timestamp + UTC `bar_open_utc` + `observed_at`; null/partial/nonaligned rows are evidence, never bars; a complete provider row with volume 0 is accepted as a bar (correct — not synthetic).
3. **`.E` vs `.IS`; exact 50/checksums; disagreement; immutability** — `snapshots.py` enforces the `.E`→`.IS` suffix rule at load, validates both canonical hashes (`785e877d…59e`/`3b3b37c6…f5`), enforces ASCII order and distinct codes/aliases, freezes models, and exposes `compare_source_members`. `config/universes/bist50/2026q3.json` carries source hashes, canonicalization version, and the current-not-PIT limitation.
4. **Sourced calendar with explicit limits** — `config/calendars/bist/2026.json` is checksummed (`transcription_sha256` validated at load), timezone-validated, and its `limitations` explicitly state no historical sessions, no suspensions/VBTS/circuit-breakers, transitions ≠ expected bars, and separate instrument eligibility.
5. **Seven-day splitter** — `windows.py` caps every window at 7 days, preserves adjacency, and rejects naive/reverse windows (tested).
6. **Unofficial adapter** — one-day `range=1d`, zero retry (always `attempts=1`), `1 <= max_concurrency <= 2` enforced, personal-research UA, `follow_redirects=False`, isolated; **no yfinance** (absent from lockfile and venv; only mentioned in the research doc).
7. **Tests/hygiene/typing** — 52 collected tests across universe/calendar/windows/provider-contract/fixture-hygiene/network; `test_fixture_hygiene.py` forbids `/home/`, username, bearer/crumb, and secret keys and verifies `fixture_capture` documents the raw hash + removed transport headers; mypy `strict` + `warn_unreachable`; ruff `[B,E,F,I,RUF,UP]`; the network smoke asserts only bounded behavior and typed invariants, never SLA.
8. **Strict Phase 1 scope** — only `models.py`, `provider.py`, `snapshots.py`, `windows.py`; no storage, scheduler, gap recovery, systemd unit, CLI, trading, or broker code; lockfile has only httpx + dev deps (no pyarrow/duckdb/etc.).
9. **Git boundary** — `main` initialized, **zero commits, nothing staged, no remote**; `.venv` and caches correctly ignored.

## Low observations (optional; none trigger another review)

- **L1 — provenance window label (genuine minor issue).** `provider.py:445` records the one-day request window as `[now−24h, now)`, but Yahoo `range=1d` is a calendar-day semantic; on weekends/holidays returned bars can fall outside the recorded window. No Phase 1 persistence, so impact is limited to the in-memory envelope. Smallest fix: derive the window from the Istanbul session day (`[session_day_start_utc, retrieved_at]`) or label it as an approximation of `range=1d`, and assert in the smoke test that all returned bars fall inside it.
- **L2 — calendar half-day/full-day end-time consistency.** `config/calendars/bist/2026.json` labels holidays `half_day_until_13:00` (lines 55/62/69) while `half_day_transitions` end continuous trading at 12:30 and `end_of_day` at 12:49; the full-day `end_of_day` is 18:19, ~9 min after the closing-price session the research report cites (through 18:10). Unverifiable transcription details in a snapshot Phase 1 never consumes for logic, but reconcile them to one source (current Equity Market Procedure PDF) before Phase 4 uses the calendar for classification.
- **L3 — 3xx handling (hardening).** `follow_redirects=False` (`provider.py:430`) would record a query1→query2 load-balancing redirect as a `CLIENT_ERROR`. Not observed in bounded tests. Smallest fix: enable `follow_redirects=True` or classify 3xx explicitly if the canary ever sees one.
- **L4 — untested 8-day retention branch.** `provider.py:107` handles the "8 days" 422 message but only the "last 30 days" fixture exists. Add one sanitized 422 fixture asserting `RETENTION_LIMIT` for the 8-day message.
- **L5 — non-finite hardening.** `_decimal`/invariant checks reject NaN but would accept a JSON-literal `Infinity` in a non-anchored field. Yahoo never emits it. Smallest fix: require `value.is_finite()`.
- **L6 — pyarrow deferral annotation (scope note).** `pyproject.toml` lists only httpx even though plan §4.2 names pyarrow; this is correct YAGNI for Phase 1 (no Parquet code exists). Optionally annotate the plan that pyarrow is deferred to Phase 2.

## Classification

- **Bugs:** none confirmed. L1 and L2 are genuine-but-minor issues; the rest are hardening/coverage.
- **Optional hardening:** L3, L5.
- **Conservative decisions (correct, keep):** strict timezone/currency/granularity rejection, fail-closed schema drift, single-attempt no-retry provider, `max_concurrency ∈ [1,2]`, sanitized rather than raw fixtures, and the current-not-PIT universe framing.

The implementation faithfully matches Phase 1 GREEN scope and the approved disposition; the Git boundary is clean; and no storage, scheduler, service, trading, or broker code exists. Ready for the next gate (the user's explicit decision to proceed or commit).
