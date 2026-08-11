# BIST Data Backfill and Backtest System Research

Verification date: **2026-08-03 (Europe/Istanbul)**

Scope: research and system planning only. This report does not place trades, connect to a broker, create credentials, or claim that a strategy is profitable. It is not investment, tax, or legal advice.

Collector planning: the canonical unattended one-minute BIST 50 collection plan is maintained separately in [`.hermes/plans/bist-minute-collector.md`](.hermes/plans/bist-minute-collector.md).

## Evidence labels

- **[V] Verified fact:** supported by the linked first-party documentation available on the verification date.
- **[T] Tested observation:** directly observed with a small, non-sensitive request on the verification date. It is evidence about that request, not a service-level guarantee.
- **[U] Unknown:** not established by the public documentation or small test. A written vendor answer, sample file, or contract is required.
- **[A] Assumption/proposal:** a design or research choice, not a fact about a provider or the market.

## Executive conclusion

**[V/T] The project is technically feasible, but there are two different milestones.**

1. We can build a ten-company, daily-bar research prototype now, starting no earlier than 2006. Yahoo Finance currently resolves common BIST symbols ending in .IS and supplies long daily histories, dividends, and splits. That path is suitable for plumbing, deterministic price strategies, and exploratory event alignment. It is not an authoritative or contractually reliable production data source, and its pre-2005 scaling must not be trusted without reconciliation.
2. A credible claim about KAP-event performance or intraday execution requires paid or contract-backed data. The missing pieces are historical intraday or tick data, authoritative corporate actions including rights issues, delisted securities, point-in-time universe membership, historical trading restrictions, bulk KAP access with stable timestamps, and licensed point-in-time fundamentals with original publication timestamps and revision history.

Ten current companies are enough to validate ingestion, timestamp alignment, accounting, and simulation. They are probably not enough to establish an event-driven edge. A fixed list selected today also creates survivorship and selection bias. The economically meaningful study should eventually use a point-in-time BIST 30 or BIST 100 universe and several hundred eligible events.

The recommended first system is deliberately small: Python, immutable source files, Parquet, DuckDB, and a tested daily event simulator. A database server, queue, distributed scheduler, live broker adapter, and a high-frequency engine are not justified at this stage.

## Review reconciliation

This revision incorporates the user-supplied mandatory read-only review from OpenCode session ses_03834f75effeZZa1No7tD39myV, run with exactly opencode-go/deepseek-v4-flash and --agent plan. Its initial verdict was **REVISE**, not BLOCK. The same session then completed its focused read-only confirmation with the same agent mode and exact model and returned **APPROVE**. The requested free-versus-paid-track and point-in-time-fundamentals clarifications are reflected below. Confirmation is complete; no additional review was run or remains pending.

**Current status:** the planning report is approved by that supplied review record. Implementation is not authorized; the next gate is an explicit implementation decision from Hermes/the user.

**What changed**

- The previously stated BIST 100 TRY/USD return-index codes are downgraded to **[U]** pending the official BIST index-code workbook; no code may be hard-coded before code, currency, return/price type, and effective history are confirmed.
- A mandatory 2005-01-01 Turkish lira redenomination gate was added, and the engineering MVP now starts no earlier than 2006.
- Strategy A's 5% contract/revenue rule now has an explicit point-in-time fundamentals dependency and TTM formula. The free run is limited to timestamp, session, fill, and price/volume-confirmation plumbing using a manually adjudicated KAP sample.
- Event confirmation now compares like with like: point-in-time total-return or appropriately action-adjusted stock return against an equivalent verified return-index benchmark.
- Current prose about sessions, ticks, and price limits is descriptive only. Exact effective-dated tables, pre-2022 mappings, instrument auction eligibility, and worked current/historical S/S+1 fixtures are hard gates.
- Multiple-testing control and total strategy-attempt count are promoted to evidence-valid acceptance requirements.
- Benchmark/universe controls now use the same participation, costs, settlement, rebalance, and realistic fill rules as the candidate strategies.
- The smallest acceptance package now targets a known corporate-action/rule case or roughly three months, and includes index-code and redenomination checks.

**What remains unknown**

- the exact BIST 100 TRY and USD return-index machine codes, code history, base/rebasing history, and downloadable coverage;
- complete historical session/auction, tick/price-limit, suspension, VBTS, short-sale, and gross-settlement states;
- authoritative historical rights issues, delisted names, point-in-time index membership, and issuer/security mappings;
- a licensed point-in-time fundamental feed with original KAP publication timestamps and restatement history;
- broker/account-specific commissions, taxes, settlement availability, and any historical borrow terms.

**Questions that require a sample and written quote**

For Borsa/DataStore, KAP, Matriks, ForInvest/Foreks, Finnet, iDeal, ICE, and LSEG, request exact fields and retention for raw/adjusted bars, dividends/splits/rights, fundamentals with publication timestamps, delistings, point-in-time membership, index-code history, session/restriction history, revisions, API/export, quotas, authentication, local storage, non-display/derived-data use, redistribution, and current total price. No vendor is credited with authoritative coverage of those fields until its written terms and sample files demonstrate it.

## 1. Backfill is not backtest

### Backfill

A **backfill** reconstructs the historical inputs the system would have had:

- securities and symbol changes;
- raw daily, minute, trade, or order-book observations;
- exchange sessions, holidays, auctions, suspensions, tick sizes, price limits, and rule changes;
- dividends, splits, capital increases, rights issues, and delistings;
- point-in-time index membership;
- KAP disclosures, original publication times, corrections, attachments, and the time our collector first saw each item;
- benchmark, interest-rate, FX, and inflation series.
- currency denomination, unit, scale, and index-base histories, including the 2005-01-01 Turkish lira conversion.

A backfill is a data-engineering and provenance job. Its output is a frozen, versioned dataset with source, retrieval time, license, checksums, and transformation history.

### Backtest

A **backtest** consumes one frozen dataset and replays a strategy through time. It decides what information was available at each decision time, creates orders, determines whether and when they could fill, applies corporate actions and costs, maintains cash and settlement ledgers, and produces an auditable result.

A backtest cannot repair a weak backfill. For example:

- using today's BIST 100 constituents in 2018 introduces survivorship bias;
- using a corrected KAP disclosure as though the correction were known at the original time introduces look-ahead bias;
- using today's fully backward-adjusted prices without the underlying action history can leak future capital actions into historical features;
- treating a daily close signal as filled at that same close uses information that was not available before the fill;
- assuming a fill during a suspension, circuit breaker, locked limit, or auction creates fictitious liquidity.
- concatenating pre-2005 old-lira values with post-2005 new-lira values without a verified 1,000,000:1 unit reconciliation can create a fictitious price, revenue, market-cap, FX, or inflation jump.

## 2. What was actually tested

### Yahoo Finance / yfinance

**[T]** On 2026-08-03, an isolated yfinance test requested recent daily data for:

ASELS.IS, THYAO.IS, GARAN.IS, AKBNK.IS, KCHOL.IS, SISE.IS, EREGL.IS, TUPRS.IS, BIMAS.IS, SAHOL.IS, and the XU100.IS index. Every symbol returned recent raw and adjusted OHLCV fields.

**[T]** A maximum-period ASELS.IS daily request returned 6,743 rows from 2000-05-10 through 2026-08-03. The last row was still the current trading day and therefore potentially incomplete.

**[T]** A recent ASELS.IS one-minute request returned 3,168 rows from 2026-07-24 09:55 +03:00 through 2026-08-03 15:45 +03:00. A one-minute request beginning 2026-06-15 failed with a provider message that the requested range must be within the last 30 days. This is a **rolling retention window**, not “expired data.” It means old minute bars disappear from the retrievable window unless collected in time.

**[T]** ASELS.IS returned 39 corporate-action rows, consisting of 32 dividends and 7 splits, from 2005 through 2025. This does not establish completeness, correct treatment of rights issues, or point-in-time correctness.

**[T/U]** Yahoo returned ASELS.IS observations before 2005, but this test did not establish how Yahoo reconciles the old Turkish lira/new Turkish lira boundary. Those observations are excluded from the first MVP and must not be scaled or joined to later data without an authoritative cross-source check.

**[T]** A direct unauthenticated batch against Yahoo's chart surface was rate-limited with HTTP 429 during earlier checking, while yfinance requests later succeeded. Yahoo does not publish a stable public market-data API contract for this use.

Yahoo labels Istanbul quotes as delayed and in TRY. The yfinance project states that it is intended for research and educational purposes and that Yahoo data is for personal use. See the [yfinance documentation](https://ranaroussi.github.io/yfinance/index.html) and [Yahoo API terms](https://legal.yahoo.com/us/en/yahoo/terms/product-atos/apitnc/index.html).

### Twelve Data

**[V]** Twelve Data's exchange catalog explicitly lists Borsa İstanbul, MIC XIST, timezone Europe/Istanbul, with **end-of-day coverage only** and Grow access for individuals or Venture access for business users: [Twelve Data exchange coverage](https://twelvedata.com/exchanges?level=pro).

**[T]** The unauthenticated exchanges endpoint identified BIST/XIST, and the stocks endpoint returned 646 current rows associated with BIST/XIST and TRY. This proves current symbol discovery, not historical or delisted coverage.

**[T]** A time-series request using the public demo key returned HTTP 401. No authenticated BIST price series was tested. The current pricing page displayed Grow at USD 79 per month or USD 790 per year; business use is a separate tier. See [Twelve Data pricing](https://twelvedata.com/pricing), [time-series documentation](https://twelvedata.com/docs#time-series), and [terms](https://twelvedata.com/terms).

### Borsa İstanbul DataStore catalog

**[V/T]** The public, unauthenticated DataStore catalog was inspected without purchasing or downloading data. It listed:

- equity end-of-day price and volume data;
- adjusted end-of-day equity prices beginning in 1993;
- intraday trade book, order book, all-index, and ITCH products;
- company yearbooks for 1998–2008 and historical financial statements for 2000–2009;
- current index constituents and companies in the BIST 30, 50, and 100 since 2000;
- price and return index histories and index divisor, free-float, and weight data.

The catalog establishes product existence. It does not establish current price, exact delivery schema, all retention boundaries, amendment policy, or rights to retain and redistribute each purchased file. Those must be confirmed during purchase. See [Borsa İstanbul historical data sales](https://www.borsaistanbul.com/en/data/historical-data-sales) and [DataStore](https://datastore.borsaistanbul.com/).

## 3. Official BIST and KAP facts that affect simulation

### Market data and licensing

**[V]** Borsa İstanbul distributes real-time, delayed, and end-of-day data through licensed vendors and offers multiple order-book levels. Current products include Level 1, Level 1+, Level 2 with ten price levels, and Level 2+ with deeper data and order references. See [market-data products](https://www.borsaistanbul.com/en/data/data-dissemination/market-data-products), [equity-market data](https://www.borsaistanbul.com/en/data/equity-market-data), and the [licensed vendor directory](https://www.borsaistanbul.com/en/data/data-dissemination/data-vendors-directory).

**[V]** Storage, onward distribution, display, and non-display use are licensed activities. A vendor subscription does not automatically grant redistribution rights. See the [Borsa İstanbul Data Distribution Agreement page](https://www.borsaistanbul.com/en/data/data-dissemination/borsa-istanbul-data-distribution-agreement).

### KAP

**[V]** KAP is the official public-disclosure platform. Its public archive exposes disclosure publication date/time, disclosure types, issuers, attachments, and structured financial-table queries. Disclosures can be amended or corrected. See [KAP general information](https://www.kap.org.tr/tr/about/genel-bilgi) and the [public disclosure search](https://www.kap.org.tr/tr/bildirim-sorgu).

**[V]** KAP documents a REST service for disclosures, details, attachments, latest disclosure ID, members, securities, funds, blocked disclosures, and corporate-action status. Contracted API use requires a Borsa İstanbul distribution agreement, MKK authentication, an API key, and whitelisted IP addresses. High-volume use is directed to the API rather than repeated public-page requests. See the [KAP REST API documentation](https://www.kap.org.tr/tr/api/about/content-file/8a019492945fbe080194b26d8bed4873).

**[U]** The current API price, exact historical retention per endpoint, request quotas, attachment-size rules, and storage/redistribution rights require a quote and contract review. A separate bulk API that returns every financial statement as normalized point-in-time facts was not verified. Some financial data may need to be preserved from structured pages, Excel/XML, or issuer attachments.

For research, every disclosure must preserve at least:

- KAP disclosure ID and source URL;
- issuer and security mapping valid at that date;
- published_at from KAP;
- first_seen_at from our collector;
- fetched_at;
- original body and attachment hashes;
- correction, cancellation, postponement, and predecessor/successor links;
- the exact extracted facts and evidence spans used by a strategy.

The tradable time is not automatically the displayed publication time. It is the later of publication and first-seen time, plus a predeclared processing/latency buffer, mapped to the next eligible exchange session.

**[V/A]** A historical KAP backfill can recover the platform's publication timestamp, but it cannot recreate when a collector that did not yet exist would actually have received and parsed the item. Historical tests must therefore add and stress an explicit latency assumption. Only the forward collector can create genuine first_seen_at evidence.

### Point-in-time fundamentals dependency

Strategy A's materiality ratio cannot be calculated from a current financial portal or today's restated history. It requires a financial statement version that had actually been published before the event, mapped to the issuer/security valid at that time.

**[A] Required formula and provenance**

- For an event at time τ, select the latest KAP financial statement whose publication timestamp is earlier than the event's actionable time. Preserve whether it is consolidated or standalone and never switch scope silently.
- When the latest statement is annual, TTM revenue equals that published annual revenue.
- When the latest statement is interim, TTM revenue equals current-year YTD revenue plus the previously published prior full-year revenue minus the comparable prior-year YTD revenue.
- All three components must have been public before τ and use a reconciled currency, unit, accounting scope, and line-item definition. Use the restatement version known at τ, not a correction published later.
- If inflation accounting, a reporting-currency change, issuer mapping, missing comparable period, or unit conflict prevents like-for-like calculation, the rule abstains.
- A non-TRY contract amount needs a preregistered point-in-time FX conversion based on a rate that was available at τ; otherwise the rule abstains.

**Named source path**

- The free engineering sample uses manually transcribed and adjudicated KAP financial statements/attachments whose publication timestamps and source documents are retained.
- A scaled study uses contracted KAP disclosures plus KAP financial tables/files if the contract confirms delivery, or a licensed ForInvest/Finnet/Matriks fundamental product only after a sample demonstrates historical publication timestamps, issuer mapping, statement scope, revisions, and retention.

Until that dependency passes, the automated 5% materiality gate is disabled. A manual materiality label may exercise the pipeline, but it cannot support an automated or evidence-valid Strategy A result.

### Current market mechanics

The following are current rules, not timeless constants. They are descriptive context only. The simulator needs exact, effective-dated tables transcribed from the applicable official procedure and announcements.

**[V]** For ordinary full-day equity trading, the current procedure describes an opening auction beginning at 09:40, continuous trading beginning at 10:00, a closing auction around 18:00–18:07, and closing-price trading through 18:10. Random matching intervals, half days, instrument-specific schedules, and exceptions apply. See [BIST trading hours](https://www.borsaistanbul.com/en/markets/equity-market/trading-hours) and the [Equity Market Procedure PDF](https://www.borsaistanbul.com/files/equity-market-procedure.pdf).

**[V]** Descriptively, the current equity tick table ranges from TRY 0.010 for prices below TRY 20 to TRY 2.500 for prices at or above TRY 2,500, with intermediate bands. General daily price-margin parameters in the current procedure differ by market segment: Stars, Main, Sub/Watchlist/PMTP, exchange-traded funds, and rights coupons do not all share one limit. The current prose figures include ±20%, ±15%, ±10%, and ±50% depending on segment/instrument. These percentages are not simulator constants. Phase 1 must transcribe the complete official table, categories, exceptions, rounding, reference-price rule, effective_from/effective_to, and source document into data; current prose must never be hard-coded backward.

**[V]** A Borsa announcement effective 2025-09-01 changed several controls, including the BIST 100 decline threshold for applying the uptick rule from 3% to 2%, a single market-wide circuit-breaker trigger at a 6% decline, and standardized or shortened interruption periods. See [Borsa announcement 38860](https://www.borsaistanbul.com/files/duyuru-38860-TR.pdf).

**[V]** Official history demonstrates that session and order mechanics change. Borsa publishes a [4 November 2019 session-hours schedule](https://www.borsaistanbul.com/files/pay-piyasasi-islem-yontemleri-ve-seans-saatleri-cizelgeleri-4-Kasim-2019-tarihi-itibariyle.pdf), and its [2021/4 opening-session announcement](https://borsaistanbul.com/en/announcement/14137/change-rules-equity-market-opening-session-20214) states that market and market-to-limit orders were prohibited in opening auctions from 2021-03-01. A current session map cannot be applied to 2019, and an old order type cannot be assumed available today.

**[U/A]** Complete effective-dated pre-2022 mappings have not yet been assembled. Before any historical fill is accepted, the session table must identify for that date and instrument: auction inclusion, order-collection and match times, continuous/single-price segments, permitted order types, half-day status, random windows, reference price, tick table, price-limit band, and applicable restriction state.

**[V]** The Volatility Based Measure System, VBTS, can impose security-specific, effective-dated measures such as short-selling/margin bans, gross settlement, order restrictions, single-price trading, and internet-order restrictions. See the current [BIST market functioning and VBTS page](https://www.borsaistanbul.com/piyasalar/pay-piyasasi/piyasa-isleyisi).

**[V]** Equity settlement is generally T+2 with delivery versus payment and netting, while gross-settlement measures alter availability. See [BIST settlement principles](https://www.borsaistanbul.com/en/markets/equity-market/settlement-principles).

**[U]** A universally applicable current “one lot” assumption was not established for every security and historical period. The simulator must obtain trading unit, tick size, market segment, and order-size limits from point-in-time reference data rather than hard-code one value.

**[U]** Broker commission, exchange/clearing charges, tax treatment, minimum commission, custody cost, and the ability to reuse unsettled proceeds are account- and time-dependent. They must be explicit scenario inputs, not guessed constants.

## 4. Three honest data paths

### Path 1 — Free or low-cost daily-data MVP

**Sources**

- Yahoo .IS daily raw OHLCV, Adj Close, dividends, and splits for personal research, beginning no earlier than 2006 in the first MVP.
- Yahoo XU100.IS as a temporary price-index comparison.
- A small, manually adjudicated KAP archive sample for publication/first-seen, session alignment, correction handling, and price/volume-confirmation plumbing; contracted KAP access is required before bulk operation.
- Borsa public reference files where licensing permits.
- Optional Twelve Data Grow for a documented daily API if its license fits the user type.

**Can answer**

- Does the ingestion, normalization, dataset versioning, and accounting pipeline work?
- Are deterministic daily signals causal and reproducible?
- How do price-only rules compare with cash and a price-index baseline?
- Can KAP timestamps be mapped conservatively to the next full trading session?
- Can a manually supplied event/materiality label flow through S, confirmation, S+1, restriction, and fill accounting without look-ahead?
- How sensitive are daily results to plausible commissions and daily-bar slippage?

**Cannot answer credibly**

- The first minutes or hours after an announcement.
- Queue position, bid/ask spread, depth, market impact, auction allocation, or exact circuit-breaker fills.
- A survivor-free BIST-wide result without point-in-time membership and delisted names.
- Rights-issue accounting without authoritative actions.
- Automated application of Strategy A's 5% materiality rule without point-in-time revenue facts and publication timestamps.
- Production data availability or legal reuse under a stable SLA.
- BIST 100 total-return outperformance unless the official return index is obtained.

This path validates engineering and produces hypotheses. It is not sufficient to establish a tradable edge.

### Path 2 — Forward intraday collector

**Sources**

- For personal prototyping, periodically retrieve every currently available Yahoo one-minute bar and persist it before the rolling window disappears.
- For durable research, replace that source with a licensed Matriks, ForInvest/Foreks, iDeal, or other Borsa-authorized feed whose contract permits local historical retention.
- Poll contracted KAP and record both published_at and first_seen_at.

**Collection rules**

- Append raw responses; never overwrite them.
- Use Europe/Istanbul exchange time and UTC storage.
- Record requested range, response time, source sequence/identifier, and checksum.
- Re-fetch recent bars into a new revision because vendors may correct them.
- Maintain a coverage ledger so missing sessions are visible.
- Do not infer a bar for a suspension or outage.

**Can answer after enough forward time**

- Collector latency and missing-data behavior.
- Minute-level response patterns from the collector start date.
- Whether a daily/event simulator agrees with a forward paper simulation.
- Coarse spread/slippage only if the licensed feed includes bid/ask or trades.

**Cannot answer**

- Events before collection began.
- A five- or ten-year event edge until enough years accumulate.
- Queue position and full market impact from one-minute OHLCV alone.
- Historic VBTS, suspension, constituent, or action state unless separately backfilled.

The observed Yahoo one-minute window was 30 days for ASELS.IS on the test date. The collector therefore needs overlap and monitoring; it must not assume the window will remain 30 days.

### Path 3 — Paid historical intraday or tick research

**Candidate composition**

- Borsa DataStore for official daily, adjusted daily, index membership, trade/order/ITCH products, and reference datasets.
- Matriks historical CSV for a smaller, retail-accessible BIST minute/trade/best-bid-offer acquisition.
- Contracted KAP REST access for bulk disclosures and attachments, plus a contracted/manual point-in-time financial-statement source for materiality facts.
- ICE or LSEG for enterprise historical ticks/depth and delivery APIs where the quote confirms BIST coverage.
- A licensed current feed for forward validation.

**Can answer, if the purchased fields are sufficient**

- Event returns at one-minute, trade, or quote horizons.
- Whether apparently profitable daily entries were actually blocked by spread, depth, price limits, auction state, or interruptions.
- Capacity estimates and participation-rate limits.
- Survivor-free results when delisted securities and point-in-time membership are included.
- Total-return performance when corporate actions and return indices are authoritative.

**Still cannot answer automatically**

- Whether an observed reaction was caused by the KAP event rather than concurrent news or macro moves.
- The fill of an order whose queue priority and broker routing are unknown.
- Whether a historical statistical relationship will persist.
- Any field absent from the purchased license. “Tick data” does not necessarily include full depth or order identifiers.

Before purchase, require a sample file and written data dictionary covering timestamps, timezone, revisions, raw versus adjusted fields, auction flags, cancellation/correction records, corporate actions, delisted securities, retention, API/export, rate limits, local storage, derived-data rights, and redistribution.

## 5. Minimal backtest-system design

### Tool comparison

Maintenance was checked on 2026-08-03 through the projects' documentation, repositories, and package indexes.

| Tool | Current evidence | Fit | Limitation |
|---|---|---|---|
| [backtesting.py](https://pypi.org/project/backtesting/) | **[T]** PyPI 0.6.6, uploaded 2026-07-22; active documentation | Very small OHLC strategy experiments; commissions and spread are supported | Daily-bar assumptions and default next-bar behavior are not a BIST microstructure model; AGPL-3.0 must be reviewed |
| [vectorbt](https://vectorbt.dev/) | **[T]** PyPI 1.1.0, uploaded 2026-07-05 | Fast price-only screening and parameter-sensitivity surfaces | Vectorized fills can hide event order, liquidity, and settlement constraints; easiest tool with which to overfit |
| [NautilusTrader](https://nautilustrader.io/docs/latest/concepts/backtesting/) | **[T]** PyPI 1.231.0, uploaded 2026-08-02 | Event-driven L1/L2/L3 simulation, partial fills, and a route toward detailed paid-data research | Considerably more setup and custom BIST instrument/rule modeling than the daily MVP needs |
| [LEAN](https://www.quantconnect.com/docs/v2/lean-engine) | **[V]** Maintained, open-source multi-asset engine | Mature event engine and useful independent comparison | BIST data, exchange hours, fees, corporate actions, and brokerage behavior would all be custom; not the smallest first stack |

**[A] Recommendation:** use a small explicit daily/event simulation core for the MVP, with pandas or Polars for transformations, PyArrow/Parquet for files, DuckDB for queries, and pytest for invariants. Use vectorbt only as a price-only cross-check. Explicitly defer LEAN, Postgres, Kafka, Airflow, NautilusTrader/tick-depth simulation, a live-trading framework, and broker integration until purchased sample evidence proves that the smaller daily system cannot answer the question.

### Data flow

    provider responses and source documents
                    |
                    v
        immutable raw objects + checksums
                    |
                    v
       validated normalized Parquet tables
                    |
                    v
        sealed DuckDB dataset version
                    |
                    v
    deterministic signals -> order simulator
                    |
                    v
      fills, ledger, metrics, audit artifacts

DuckDB is the query layer, not the sole source of truth. Raw objects and normalized Parquet remain independently readable.

### Minimum normalized tables

1. **instrument**
   - internal instrument ID, symbol, ISIN, market, currency;
   - symbol/issuer valid_from and valid_to;
   - first/last trading date, delisting reason, segment, trading unit.
2. **session**
   - session date, timezone, effective-dated opening/continuous/single-price/closing segments, half-day/holiday;
   - rule-set ID, source document, instrument-category auction eligibility, permitted order types.
3. **bar**
   - instrument ID, interval, start/end timestamps;
   - raw open/high/low/close/volume/turnover, source, revision ID.
4. **trade or quote**, only when licensed
   - exchange timestamp, receive timestamp, sequence, side/price/size, auction/session state.
5. **corporate_action**
   - action type, declaration/publication/ex/effective/pay dates;
   - cash, split ratio, rights ratio/subscription price, source, revision.
6. **index_membership**
   - index ID, instrument ID, effective_from/to, announced_at, weight/free float where available.
7. **kap_event**
   - KAP ID, issuer/instrument, published_at, first_seen_at, fetched_at;
   - category, raw document references, correction chain, extracted-fact version.
8. **fundamental_fact**
   - issuer ID, statement scope, period start/end, publication timestamp, filing/version ID;
   - line-item code, value, currency, scale/unit, YTD/annual flag, restatement lineage;
   - TTM formula inputs and the as-of time at which the fact became eligible.
9. **market_rule and restriction**
   - effective-dated tick/price-limit/circuit/session rules;
   - exact category rows including rights coupons and other special instruments;
   - suspensions, VBTS, short-sale, margin, gross-settlement, and order restrictions.
10. **benchmark**
   - BIST price/return indices, TL cash/rate alternative, USD/TRY, gold, and CPI as separate series with publication metadata;
   - official code, code valid_from/to, currency, price/return type, base/rebasing events, source workbook/version.

### Raw, adjusted, and total-return treatment

- Store raw exchange prices and volumes unchanged.
- Store every action separately; never replace the raw series with a vendor-adjusted series.
- Build adjustment factors with a documented formula and compare them with the vendor's Adj Close.
- Use split-adjusted or point-in-time total-return prices for features only when the action information available at that historical time is respected.
- Credit dividends and rights proceeds/subscriptions in the portfolio ledger according to the selected policy. A rights issue is not equivalent to a stock split.
- Report both price return and total return.
- Preserve revisions; a corrected vendor file creates a new dataset version.

### Turkish lira redenomination gate

**[V]** TCMB records that six zeros were removed and New Turkish lira entered circulation on 2005-01-01. The exact conversion was 1 YTL = 1,000,000 old TL. See the [TCMB currency history](https://www.tcmb.gov.tr/wps/wcm/connect/EN/TCMB%2BEN/Main%2BMenu/About%2Bthe%2BBank/History/Our%2BHistory/) and [TCMB New Turkish Lira conversion material](https://www3.tcmb.gov.tr/ytlkampanya/ytlsitedokuman/tcmb/YTL-BoEJointFXC1.pdf).

Every price, turnover, market-cap, dividend, financial-statement, contract-value, FX, cash, benchmark, and rate series that crosses 2005-01-01 must pass a denomination gate:

- record currency name/code and numerical scale on both sides of the boundary;
- reconcile representative observations against an authoritative source and verify the 1,000,000:1 conversion where the field is monetary;
- distinguish a currency conversion from a split, index rebasing, CPI base-year change, reporting-unit change, or vendor back-adjustment;
- verify CPI linking factors and base definitions from official vintages rather than dividing a CPI index mechanically;
- reject the combined series if a discontinuity cannot be explained field by field;
- preserve both source values and the explicit conversion transformation.

**[A]** The first engineering MVP starts no earlier than 2006, avoiding the transition year. The proposed economic study can retain its later 2013 start. Yahoo observations before 2005 remain excluded until authoritative reconciliation; a visually smooth chart is not sufficient evidence.

### Data-quality gates

Every sealed dataset should pass:

- unique instrument/interval/timestamp keys;
- monotonic timestamps and explicit timezone;
- open and close inside high/low, non-negative volume, valid tick increments;
- complete expected-session coverage or an explained suspension/outage;
- no bars during a known closed segment;
- duplicate, gap, and stale-price reports;
- extreme-return reconciliation against actions and KAP disclosures;
- adjustment-factor continuity and cash/action reconciliation;
- constituent intervals that do not overlap incorrectly;
- KAP correction chains and no event with first_seen_at earlier than published_at;
- a random cross-source sample with defined price/volume tolerances;
- a coverage summary by instrument, field, date, and source.
- a redenomination report for every series crossing 2005-01-01, with authoritative comparison rows and an explicit failure if scale/base is unresolved;
- exact benchmark-code/type/history validation against the official BIST index-code workbook; candidate identifiers are never accepted by string resemblance;
- effective-dated session/rule coverage, including at least one worked current and one worked pre-2022 S/S+1 fixture before historical fills are enabled.

### Reproducibility

A dataset ID should hash:

- raw object checksums;
- provider and query parameters;
- retrieval times and source revision;
- transformation code/version and package lock;
- calendar/rule/corporate-action versions;
- known coverage exceptions.

Every experiment manifest should record dataset ID, universe definition, decision rules, date ranges, benchmark, cost/slippage/fill models, model/prompt version for any AI extraction, random seed, and environment versions.

Result artifacts should include the manifest, data-quality report, signal ledger, order ledger, rejected/expired orders, fill ledger, daily cash/settlement/position ledger, corporate-action ledger, equity curve, benchmark curve, per-event attribution, metrics, and warnings. A headline return without these artifacts is not an acceptable result.

## 6. BIST-realistic order simulation

### Decision and fill timing

- A rule evaluated using a session close can first submit for the next eligible session; it cannot receive the same close.
- A KAP event is actionable only after max(published_at, first_seen_at) plus the frozen processing latency.
- Opening and closing auctions are different market events from continuous trading.
- A daily open is an opening-auction price only when the effective-dated record says that instrument participated in that auction and an opening price was formed. Otherwise it may be the first continuous or single-price print.
- “Market on open” is not a universal BIST order assumption. If the historical rule prohibited that order type, the instrument had no opening auction, no opening price formed, or the submitted price falls outside the effective limit band, reject or defer the order according to the frozen policy.
- If an instrument is suspended, in a circuit breaker, in single-price mode, or locked at a price limit, an order may be delayed or never fill.
- A daily bar cannot reveal queue position. A daily backtest must use conservative fill rules and label that uncertainty.

### Price, spread, liquidity, and capacity

For the daily MVP:

- next-session orders use the recorded daily open only after auction eligibility, price formation, order-type permission, and effective price-limit checks pass; otherwise use the predeclared first-eligible-continuous-print proxy or reject/defer;
- add configurable half-spread and adverse slippage;
- reject or defer a fill when daily volume is zero or a restriction makes it impossible;
- cap participation, for example at **[A] 1% of reported daily volume** in the base case and stress it lower;
- round to the point-in-time tick and trading unit;
- reject a simulated fill outside the effective reference-price limit band, including the correct special-instrument category;
- do not create an intraday stop fill from daily high/low because the order of high and low is unknown.

With quotes or ticks, upgrade the model to spread crossing, displayed-size consumption, partial fills, latency, auction flags, and a conservative queue model. Even full depth does not reveal a broker's exact queue priority unless order-level data and routing are available.

### Costs and settlement

Use a configuration matrix rather than one “BIST fee”:

- broker commission rate and minimum;
- exchange/clearing/custody charges where applicable;
- taxes as explicitly verified for the account and period;
- half-spread, slippage, and market impact;
- borrow/short costs only if a later research phase includes shorting.

Maintain settled and available cash separately. Model T+2 and effective-dated gross settlement. The MVP should be long-only, unlevered, and should sell only owned positions; this avoids pretending that historical stock borrow and changing short-sale rules are available.

### Worked S/S+1 timing fixtures

These are **[A]** acceptance examples, not performance observations.

1. **Current-rule example:** a disclosure becomes actionable on a normal Monday at 18:20, after the current ordinary-equity session has ended. S is Tuesday's next full eligible session. Tuesday's action-adjusted stock return and equivalent return-index return plus volume are observed through the close. The order becomes eligible on Wednesday, S+1. Under the post-2021 rule, the simulator must not invent a market order in the opening auction; it uses a permitted, predeclared limit-order/first-eligible-print rule and rejects or defers if no valid price forms, the instrument is restricted, or the price breaches Wednesday's effective band.
2. **Older-rule example:** for a 2019 ordinary share, suppose an event becomes actionable at 09:30 on a full day and the imported 4 November 2019-era map confirms that its opening collection starts at 09:40. S is that day; confirmation is known only after its close; S+1 is the following eligible session. The simulator applies the 2019 session/order/limit table, not the 2021 opening-order prohibition or the current price-limit prose. If the actual instrument was not in that opening mechanism, the daily open is treated as its first eligible print instead.

Each fixture must cite the exact calendar date, instrument category, procedure/announcement revision, and restriction record in the implemented dataset. A symbolic example in this report does not satisfy that future evidence gate.

### Regimes and benchmarks

Report:

- nominal TRY return;
- CPI-deflated real return;
- USD return;
- BIST 100 **return-index** excess return;
- an investable TL cash/rate alternative and, where useful, USD/TRY and gold alternatives.

**[V]** Borsa distinguishes price and return indices, and a return index incorporates dividend reinvestment rather than representing only price change. See [BIST stock indices](https://www.borsaistanbul.com/en/indices/bist-stock-indices) and the [BIST indices FAQ](https://www.borsaistanbul.com/en/faq/indices).

**[U]** The strings XU100_CFNNTLTL and XU100_CFNNTLUS are candidate identifiers from prior research, not verified codes in this report. Their exact index names, currencies, return/price type, start dates, base/rebasing history, and historical code changes must be confirmed from the official BIST index-code workbook and a sample series. They must not be hard-coded, used in a query, or named as acceptance evidence before confirmation. Macro series can be sourced from [TCMB EVDS](https://evds2.tcmb.gov.tr/) and [TÜİK data](https://data.tuik.gov.tr/), with their publication calendars preserved.

At minimum, break results into 2018 currency stress, the 2020 pandemic/short-sale environment, 2021–2023 high-inflation/currency conditions, and later periods, but define regime dates before examining strategy results. The purpose is robustness, not finding a favorable slice.

## 7. Research protocol designed to resist self-deception

### Universe and chronology

1. Use ten liquid current companies only for engineering validation.
2. For economic inference, use point-in-time BIST 30 or BIST 100 membership including entries, exits, name changes, and delistings.
3. Freeze an investability rule based only on lagged information, such as prior-month median traded value and available history.
4. Split chronologically. For a 2013–2026 dataset, an initial proposal is:
   - development: 2013–2019;
   - validation: 2020–2022;
   - untouched final test: 2023–2026.
5. Do not view final-test performance while changing labels, thresholds, holding periods, or costs. If it is viewed and the strategy changes, it is no longer the final test.
6. Inside development/validation, run rolling walk-forward windows; never randomly shuffle financial time.

The dates above are **[A]** and must be adapted to the verified common coverage date. The separation principle is mandatory; the particular dates are not.

### Hypotheses and multiple comparisons

- Write a short preregistration for each strategy: economic rationale, universe, fields, rules, parameters, costs, and primary metric.
- Keep a complete strategy-attempt ledger, including failures and informal variants. Ten discarded variants plus one successful-looking variant count as eleven tests; the experiment manifest must state the total attempt count.
- Choose parameters on development data, freeze them on validation, and evaluate once on the untouched test.
- Show a parameter neighborhood rather than only the best point. A real effect should not vanish when a 20-day window becomes 19 or 21.
- Require an economic effect after costs, not merely a small p-value.

**[A] Evidence-valid hard gate:** before the first result is viewed, define the hypothesis family as every registered strategy/parameter variant's primary endpoint, obtain time-block/bootstrap p-values that preserve date and company dependence, and apply Holm's step-down family-wise-error correction at α = 0.05 across the full family. Report raw and adjusted values and the total attempt count. A bootstrap reality check or deflated Sharpe ratio may be additional diagnostics, but cannot replace the preregistered Holm gate for the first study. If the procedure, family, primary endpoint, or trial count is missing or changed after inspection, the result is not evidence-valid.

### Metrics

At minimum report:

- total and annualized return in nominal TRY, real TRY, and USD;
- excess return versus BIST 100 total return and TL cash alternative;
- annualized volatility, Sharpe/Sortino with stated rate, maximum drawdown, and recovery time;
- turnover, commission, spread/slippage, exposure, cash utilization, and number of positions;
- hit rate, average/median win and loss, tail loss, and profit concentration;
- trade count, event count, company concentration, average daily-volume participation, and rejected/delayed fills;
- capacity under lower participation limits;
- results by year, company, event type, liquidity bucket, market direction, inflation/FX regime, and announcement timing.

Use block bootstrap confidence intervals by date or month and cluster event estimates by company and announcement date. Ordinary independent-trade standard errors are too optimistic when several securities react to the same macro event.

Stress at least:

- base, 2×, and 3× transaction-cost/slippage assumptions;
- one-session decision delay;
- missing or corrected events;
- alternate reasonable liquidity caps;
- nearby parameter values;
- removal of the best company, best year, and five best trades.

## 8. First deterministic strategies to test

These are hypotheses and control experiments, not recommendations or promises.

### Strategy A — Material KAP event plus next-full-session confirmation

This is the previously discussed daily KAP strategy in a causal form.

**Universe**

- Engineering run: the ten named companies in the local inventory section, using a small manually adjudicated KAP sample only to validate event timestamp, S/S+1 mapping, price/volume confirmation, rejection, fill, and ledger plumbing. It does not automate the 5% materiality gate and cannot support a strategy-performance conclusion.
- Research run: point-in-time BIST 30 or a predeclared liquid subset of point-in-time BIST 100.
- Long-only, no leverage, no short selling.

**Eligible positive event**

For a later materiality-enabled research run, an event passes only when all deterministic fields and point-in-time fundamentals are present:

1. category is signed contract/order, quantified guidance increase, or quantified capacity/production increase;
2. it is not merely an intention, non-binding memorandum, rumor, tender participation, or framework with no guaranteed amount;
3. for a contract/order, disclosed attributable value divided by point-in-time TTM revenue, calculated with the formula and publication rules in the point-in-time fundamentals section, is at least 5%;
4. for guidance or capacity, the disclosed midpoint/increase is at least 10%;
5. no cancellation or correction known before the decision negates it.

These thresholds are **[A] preregistered starting values**, not optimized truths. Separate event categories should be reported separately; combining them can hide opposite effects.

The data record must name the KAP filing IDs used for every TTM component, issuer mapping, statement scope, publication timestamps, currencies/units, restatement versions, and formula output. Missing provenance means no signal.

**Timing and confirmation**

1. Let S be the first **full eligible** session whose first applicable order-acceptance/trading segment begins after the event became actionable, using the effective-dated instrument/session map. For an ordinary current share included in the opening auction, that segment is its opening-auction collection; it is not assumed for every instrument or historical regime.
2. Observe S completely.
3. Confirmation passes if:
   - the stock's point-in-time total-return or appropriately action-adjusted close-to-previous-close return minus the equivalent verified BIST 100 return-index return for S is greater than zero; and
   - S volume is at least 1.5 times the median volume of the 20 preceding eligible sessions.
4. If S crosses a dividend, split, or rights effective date and authoritative like-for-like adjustment is unavailable, exclude the event rather than mix a raw stock return with a return index.
5. The order becomes eligible at the first permitted execution event of S+1. Use the effective-dated auction/order table: a permitted opening limit order when supportable, otherwise the predeclared first-eligible-continuous-print proxy. Reject or defer impossible market-on-open fills and enforce the effective price-limit band.
6. Allocate 10% of pre-trade net asset value, rounded down to the valid unit, with no more than five open event positions and no averaging down.
7. Hold ten full sessions and submit the exit for the next eligible execution event. A cancellation/correction that negates the event may submit an earlier exit at the next eligible execution event.

The ten-session hold and thresholds must be frozen before the holdout. Five- and twenty-session variants may be sensitivity checks in development, not three opportunities to select the best holdout answer.

**Negative disclosures**

The MVP does not short. A deterministic negative/cancellation rule may exit an already owned position at the next eligible execution event and block new entries for ten sessions. It should be evaluated as a separate rule, not silently mixed into the long-entry result.

**AI's allowed role**

AI may convert Turkish disclosure text and attachments into a fixed schema: category, amount, currency, binding status, effective date, related-party flag, prior/corrected disclosure, and verbatim evidence spans. It must:

- receive only the document version available at that historical time;
- use a frozen model and prompt version;
- be evaluated against a manually adjudicated Turkish gold set;
- abstain when required fields conflict or are missing.

The deterministic rule and independent risk/fill engine remain the order authority. AI sentiment alone must never create a trade.

### Strategy B — Price-only 12-minus-1 monthly momentum

**[A] Exact initial rule**

1. At each month-end close, calculate total-return momentum as close at t−21 sessions divided by close at t−252 sessions minus one.
2. Require at least 252 sessions of history, positive momentum, and observations on at least 90% of the prior 60 expected sessions.
3. Require the officially verified BIST 100 return index to be above its 200-session moving average.
4. Rank the point-in-time eligible universe, select the top three, and hold them at equal target weights.
5. Submit rebalance orders at the next eligible execution event under the same effective-dated auction/order, price-limit, liquidity, participation, cost, settlement, rejection, and defer rules as Strategy A; long-only and unlevered.
6. If the market filter fails or fewer securities qualify, keep the remainder in the chosen TL cash benchmark.
7. Rebalance monthly; apply the same BIST restrictions, participation cap, costs, and settlement ledger as Strategy A.

This provides a deterministic price-only comparison. It must be run with and without the market filter so that the filter is counted as a tested variant.

### Strategy C — Do-nothing and benchmark controls

Run all of these without selecting the best after the fact:

1. **Do nothing:** non-interest-bearing TRY cash.
2. **Cash alternative:** the selected investable TL rate proxy with documented lag, attainable return, and implementation costs.
3. **Market:** buy and hold an investable BIST 100 TRY total-return proxy, including its actual rebalance, spread, participation, fee, settlement, and fill assumptions. The official return index may be shown as an analytical benchmark but cannot be treated as a frictionless tradable asset.
4. **Universe control:** equal-weight buy and hold of the same eligible universe, reconstituted point in time and subject to the same rebalance schedule, participation cap, effective-dated restrictions, price limits, spread/slippage, commissions, settlement, and realistic fill/rejection rules as the candidate strategy.

If a complex event strategy does not improve on these after costs and risk, its complexity has not earned a place.

## 9. Minimum evidence horizon

**[A]** The first ten-company MVP backfill starts no earlier than 2006 and should use at least eight to ten years of daily data so it crosses several inflation, currency, market, and restriction regimes. The proposed economic study can retain its cleaner 2013 start. Price-only monthly strategies need roughly 2,000–2,500 sessions before their stability can be discussed.

For event research, target at least 200 independently useful eligible events across five to ten years and many issuers. This is a planning floor, not a guarantee. As an illustration, detecting a mean 1% excess event return with a 5% standard deviation at 80% power and a two-sided 5% threshold needs roughly 196 independent observations before company/date clustering, selection, and multiple testing. Real dependence raises the requirement.

A recent 30-day one-minute sample contains only about 20–23 sessions, one market regime, and probably very few qualifying announcements. Intraday bars from the same day are also highly correlated. Such a sample can prove that timestamp alignment and order simulation work; it cannot establish an edge, estimate tail losses, validate capacity, or support a profitability claim.

## 10. Source register

All links were checked or used in the research on 2026-08-03 unless a row says otherwise.

### Official Borsa İstanbul and KAP

- [Borsa İstanbul historical data sales](https://www.borsaistanbul.com/en/data/historical-data-sales)
- [Borsa İstanbul DataStore](https://datastore.borsaistanbul.com/)
- [Market-data products](https://www.borsaistanbul.com/en/data/data-dissemination/market-data-products)
- [Equity-market data](https://www.borsaistanbul.com/en/data/equity-market-data)
- [Licensed data-vendor directory](https://www.borsaistanbul.com/en/data/data-dissemination/data-vendors-directory)
- [Data-distribution agreement information](https://www.borsaistanbul.com/en/data/data-dissemination/borsa-istanbul-data-distribution-agreement)
- [Equity-market functioning](https://www.borsaistanbul.com/en/markets/equity-market/market-functioning)
- [Trading hours](https://www.borsaistanbul.com/en/markets/equity-market/trading-hours)
- [Settlement principles](https://www.borsaistanbul.com/en/markets/equity-market/settlement-principles)
- [Equity Market Procedure PDF](https://www.borsaistanbul.com/files/equity-market-procedure.pdf)
- [4 November 2019 Equity Market session-hours schedule PDF](https://www.borsaistanbul.com/files/pay-piyasasi-islem-yontemleri-ve-seans-saatleri-cizelgeleri-4-Kasim-2019-tarihi-itibariyle.pdf)
- [2021/4 opening-session rule change](https://borsaistanbul.com/en/announcement/14137/change-rules-equity-market-opening-session-20214)
- [2025 rule-change announcement PDF](https://www.borsaistanbul.com/files/duyuru-38860-TR.pdf)
- [KAP general information](https://www.kap.org.tr/tr/about/genel-bilgi)
- [KAP REST API documentation](https://www.kap.org.tr/tr/api/about/content-file/8a019492945fbe080194b26d8bed4873)
- [TCMB currency history](https://www.tcmb.gov.tr/wps/wcm/connect/EN/TCMB%2BEN/Main%2BMenu/About%2Bthe%2BBank/History/Our%2BHistory/)
- [TCMB New Turkish Lira conversion material PDF](https://www3.tcmb.gov.tr/ytlkampanya/ytlsitedokuman/tcmb/YTL-BoEJointFXC1.pdf)

### Turkish and global vendors

- [Matriks historical data store](https://betastore.matriksdata.com/Product/Default.aspx?ProductId=19&do=ProductDetails)
- [MatriksIQ](https://www.matriksdata.com/website/urunlerimiz/kullanici-platformlari/matriksiq-veri-terminali)
- [MatriksIQ BIST data licenses](https://iqyardim.matriksdata.com/docs/non-knowledgebase/sikca-sorulan-sorular/borsa-istanbul-bist-veri-lisanslari/)
- [ForInvest developer/MCP documentation](https://mcp.forinvest.com/docs)
- [ForInvest business data services](https://www.forinvest.com/business?lang=en)
- [ForInvest live-data terms](https://www.forinvest.com/canli-veri)
- [Finnet AnalizExpert](https://www.finnet.com.tr/FinnetStore/Tr/Urun/AnalizExpert)
- [iDeal product description PDF](https://idealdata.com.tr/downloads/iDeal_tanitim.pdf)
- [iDeal posted prices](https://estore.idealdata.com.tr/prices)
- [Twelve Data Borsa İstanbul coverage](https://twelvedata.com/exchanges?level=pro)
- [ICE Borsa İstanbul catalog](https://developer.ice.com/fixed-income-data-services/catalog/borsa-istanbul)
- [LSEG Tick History REST API](https://developers.lseg.com/en/api-catalog/refinitiv-tick-history/refinitiv-tick-history-rth-rest-api)

## 11. Decision package

### Current local-data inventory

**[T] Workspace inspected:** /home/mirza/Documents/Projects/algorithmicTrading

The inspection was limited to that project root. Before this report was created, the directory was empty apart from the normal . and .. entries. It contained:

- no source code;
- no downloaded price or KAP data;
- no Parquet, CSV, database, notebook, or experiment artifact;
- no prior report or manifest;
- no project-local agent-instruction file;
- no credentials or environment file.

No unrelated private directories or secret values were scanned. Earlier Yahoo/Twelve checks were transient and did not produce retained project data. Therefore the current durable local research asset is this report only; we do not yet possess a backfill.

### Provider comparison table

In this table, “unknown” means not established from current public material and must be resolved with a sample and written terms.

| Source | Symbols / universe | Daily, intraday, tick, retention | Raw/adjusted OHLCV and actions | Delisted names / point-in-time membership | API/export, auth, rates | Storage, redistribution, price, and backfill verdict |
|---|---|---|---|---|---|---|
| **Borsa İstanbul DataStore and official products** | **[V]** Official BIST securities, indices, reference data | **[V/T]** EOD raw; adjusted EOD from 1993; intraday trades, orders, all-index and ITCH; individual product dates vary | **[V]** Separate raw/adjusted EOD products; official corporate-action and index products. Exact dividend, split and rights fields/formulas require sample | **[V]** Public delisting/reference files exist; DataStore lists BIST 30/50/100 membership since 2000 | **[V/U]** Portal/file products and licensed feeds; portal account/purchase is expected, but exact format, per-product API, authentication and rates require checkout/contract | **[V/U]** Technically and legally backfillable under purchased terms. Current price and retention/derived-data/redistribution rights are quote or checkout dependent |
| **KAP** | **[V]** Listed issuers, funds, securities, disclosures, financial tables | Not a price source; public historical disclosures with publication times and attachments; exact API retention unknown | Corporate decisions/actions and financial statements, but no OHLCV; completeness of normalized dividend/split/rights fields is unknown and corrections must be versioned | Issuer/security metadata available; historical identifier mapping completeness unknown | **[V]** Public search is unauthenticated; REST supplies JSON/document/attachment resources and requires contract, MKK auth, key and IP allowlist; quotas unknown | **[V/U]** Small public research is possible; reliable bulk backfill requires contract. Price, quotas, storage and redistribution terms require quote |
| **Yahoo Finance / yfinance** | **[T]** Common .IS symbols and XU100.IS; all 10 test shares worked | **[T]** ASELS daily 2000–2026; recent 1m worked, older 1m enforced a 30-day window; no tick/depth | **[T]** Raw and Adj Close, volume, dividends and splits. Rights, adjustments and action completeness unverified | **[U]** Current symbols work; delisted coverage and point-in-time index membership not established | No API key in the test; unofficial yfinance/web access, no stable SLA or published quota; HTTP 429 observed | No separate data fee in the tested path, but personal/research terms apply. Technically usable for a daily MVP and rolling personal intraday collection; not a licensed production or redistribution dependency |
| **Matriks historical product / MatriksIQ** | **[V]** BIST equities, indices, VİOP and other product coverage | **[V]** Historical CSV advertises daily from Nov 2010, 60m from Jan 2010, 1m/5m from Jan 2017, historical trades and best bid/offer; delivered data described as 15m delayed | IQ exposes corporate-action information; exact raw/adjusted CSV semantics, dividend/split/rights fields and revision policy unknown | **[U]** Delisted coverage and point-in-time constituent membership not publicly established | CSV purchase and licensed terminal/API surfaces; account/authentication, automation, rate limits and export rights depend on product | **[V/U]** Strongest documented retail-style paid historical candidate. Backfill is technically possible; current total price, local-use rights, and redistribution require checkout/contract |
| **ForInvest / Foreks services** | **[V]** BIST and broad multi-market coverage | **[V/U]** Current docs expose HistoricalData, transactions, market data and depth; business page claims large historical holdings. Exact BIST retention/frequency by plan unknown | Corporate-actions, KAP data and financial-statement tools are documented; raw/adjusted semantics and rights issues unknown | **[U]** Delisted and point-in-time membership coverage not established | **[V]** Current developer surface lists 22 read-only MCP tools with account and OAuth2 PKCE; plan quotas unknown | Personal terms restrict redistribution. Backfill may be technically possible, but price, entitlements, retention, API storage and derived-use rights require a written quote |
| **Finnet AnalizExpert** | **[V]** BIST plus a stated universe over 10,000 instruments | **[V/U]** Daily, weekly, monthly and annual history; no public confirmation of intraday/tick/depth or exact start date | Financial statements, capital and dividend data; raw/adjusted price semantics, splits and rights details unknown | **[U]** Delisted/PIT membership not established | Licensed account; Excel/API advertised, but schema, authentication, rate and bulk limits are unknown | Price and license are quote dependent. Plausible daily/fundamental backfill, not a verified intraday solution |
| **iDeal / iDeAlgo** | **[V]** BIST real-time/depth plus over 10,000 codes stated in product material | **[V/U]** Real-time, depth, historical series, tester, and automatic data completion; exact historical depth and tick retention unknown | **[U]** Raw/adjusted semantics and dividend/split/rights fields not established in the checked public material | **[U]** Delisted/PIT membership not established | Licensed terminal/account; external API/export, formats, authentication and limits unknown | Posted page displayed TRY line items including terminal 1,099, iDeAlgo 1,990, L1+ 235, L2 535, L2+ 625, but cadence, VAT, bundles and historical entitlement need confirmation. Licensed terminal research is possible; automated backfill is unverified |
| **Twelve Data** | **[V/T]** Explicit XIST/BIST EOD; public list returned 646 current rows | **[V]** BIST is EOD only; no BIST intraday/tick. Generic time-series output supports date ranges/JSON/CSV; actual BIST history untested | Generic daily series are split-adjusted and separate dividend/split endpoints exist on paid plans; BIST action completeness and rights issues unknown | **[U]** Current discovery works; delisted/PIT membership not established | API key; BIST requires Grow individual or Venture business; plan credit limits apply, but the selected tier's effective BIST quota was not tested | Grow displayed USD 79/month or 790/year. Technically suitable for documented daily API backfill after an authenticated sample. Personal/business storage and redistribution depend on tier/terms |
| **ICE** | **[V]** Catalog explicitly covers all BIST-listed contracts and asset classes in its BIST service | **[V]** Streaming L1/L2/full depth and settlement; historical/EOD. BIST catalog describes daily history from June 2007; broader ICE history advertises up to 15 years of tick L1/L2/L3 | **[U]** Exact BIST adjustment, dividend/split/rights, and revision fields require data dictionary | **[U]** Delisted/PIT constituent coverage requires confirmation | ICE Connect, Excel, APIs, feeds and bulk; enterprise credentials and entitlement | Strong paid enterprise route if sample confirms fields. Quote and contract required; storage, non-display and redistribution are licensed |
| **LSEG** | **[V]** Borsa lists LSEG/Refinitiv as a licensed vendor; exact entitled BIST instruments require quote | **[V/U]** Tick History REST supports EOD, intraday bars, trades/quotes and market depth generally; exact BIST retention/depth unverified | Corporate-action/reference products exist generally; exact BIST raw/adjusted/rights package unverified | **[U]** Delisted/PIT membership must be specified in quote | REST/bulk delivery with account authentication and entitlements; limits contract-specific | Credible enterprise candidate, but no BIST backfill should be assumed until a sample and entitlement schedule confirm coverage, price, storage and derived-data rights |

Marketstack, EODHD, and other generic APIs were not selected merely because they advertise global coverage. No current first-party page or successful small test in this research established the required BIST history and legal fit. TradingView and undocumented private endpoints are explicitly excluded as production dependencies.

**[U] No provider in this table may yet be claimed to supply authoritative rights issues, complete delistings, point-in-time membership, historical restriction state, or exact current pricing/storage/derived-use/redistribution rights.** Those claims require written terms, an entitlement schedule, a data dictionary, and accepted sample files for the requested dates and instruments.

### Recommended MVP architecture

**[A] Smallest credible stack**

- Python 3.12 or later, pinned.
- pandas for the small daily research set; PyArrow for Parquet.
- DuckDB for local analytical queries and validation.
- A compact, tested event loop with explicit signal, order, fill, corporate-action, cash, and settlement ledgers.
- pytest for causal-timing, accounting, action, restriction, and reproducibility tests.
- Optional vectorbt cross-check for the price-only rule.
- No server process and no broker integration.

**MVP datasets**

1. Ten-company raw and adjusted daily OHLCV plus dividends/splits, beginning no earlier than 2006.
2. XU100.IS price series only for plumbing, plus an official BIST return-index sample. The candidate strings XU100_CFNNTLTL and XU100_CFNNTLUS remain **[U]** until the official workbook confirms code, type, currency, and history; do not hard-code them.
3. Exact effective-dated exchange sessions/rules, including the current table and at least one pre-2022 table, with instrument-specific auction eligibility and price-limit categories.
4. A small manually adjudicated KAP sample for timestamp, correction, S/S+1, price/volume confirmation, and schema validation.
5. Manually sourced point-in-time KAP financial-statement facts for formula plumbing only; the automated 5% gate remains disabled.
6. Corporate-action, membership, restriction, index-code, and redenomination samples from authoritative sources for cross-checking.

The initial ten names are ASELS, THYAO, GARAN, AKBNK, KCHOL, SISE, EREGL, TUPRS, BIMAS, and SAHOL. This is a current engineering sample, not a historically unbiased universe and not an assertion that every name is currently in a particular index.

### Concrete phase plan and rough cost bands

Costs exclude engineering labor, taxes, and broker costs. Vendor numbers are observations from public pages, not quotes.

| Phase | Deliverable | Data decision | Rough data/compute cost |
|---|---|---|---|
| **0. Contract and sample gate** | Written field/rights matrix and a targeted sample from Borsa, Matriks, ForInvest, or ICE/LSEG | Ask for raw/adjusted fields, actions/rights, PIT fundamentals and publication times, delisted/PIT membership, historical sessions/restrictions, revisions, retention, API/export, local/derived use and redistribution. Obtain the official index-code workbook/history and pre/post-2005 redenomination comparison rows; forbid hard-coded benchmark codes before acceptance | **USD 0** to request quotes; vendor purchase unknown |
| **1. Daily engineering MVP** | Ten-company 2006-or-later frozen daily dataset; deterministic price baseline; KAP/session/fill alignment proof; exact effective-dated tick/limit/session table in data | Yahoo personal research initially; optional authenticated Twelve sample; official cross-check subset. Transcribe procedure/announcement tables including rights coupons and pre-2022 mappings rather than coding current prose | **USD 0** using existing local machine and free personal research data. Twelve Grow displayed **USD 79/month** if chosen |
| **2. Forward collector** | Overlapping minute-bar and KAP first-seen archive from start date, with coverage alerts | Yahoo only as a personal proof; licensed Turkish feed for durable use | Local Parquet storage is likely below a few GB/year for ten symbols of minute bars **[A]**. Feed cost is quote/plan dependent. iDeal publicly displayed line items from TRY 235 to TRY 1,990, but billing cadence and historical/API entitlements are **unknown** |
| **3. Historical event dataset** | Five-to-ten-year intraday/trade/quote backfill, authoritative actions, PIT universe and restrictions | Matriks/DataStore for smaller scope; ICE/LSEG for enterprise depth; contracted KAP | **Quote required.** It would be misleading to invent a numeric band before symbols, years, depth, usage rights, and delivery are specified |
| **4. Detailed simulator, only if justified** | Partial-fill/auction/depth research and capacity analysis | Licensed tick/depth sample accepted first | Compute can remain local for a bounded ten-symbol sample; full tick/depth storage and license cost are unknown until sample-size measurement and quote |

**[A] The Phase 0 sample/contract package and Phase 1 daily MVP are parallel tracks.** Phase 0 gates only paid or institutional-grade paths: licensed historical intraday/tick/depth, contracted bulk KAP, licensed point-in-time fundamentals, and claims that require authoritative actions, delistings, membership, restrictions, or benchmark history. It does **not** gate the free Yahoo daily-data MVP. After an explicit implementation decision, Phase 1 may begin immediately at USD 0 for the ten current names using 2006-or-later data and the report's engineering-only limitations.

Phase 3 has a procurement gate: do not buy until a targeted sample can reproduce one known session, one corporate action, one auction, one suspension/restriction case, one KAP event, point-in-time fundamental provenance, the official benchmark-code mapping, and the 2005 redenomination boundary end to end.

### Acceptance criteria for the first valid backtest

There are two separate acceptance levels.

**Engineering-valid**

- A sealed dataset manifest identifies sources, terms, queries, retrieval time, checksums, transformations, and known gaps.
- Every bar is aligned to an exchange session in Europe/Istanbul; current-day partial bars are excluded.
- The first MVP starts no earlier than 2006. Any later dataset crossing 2005-01-01 passes the 1,000,000:1 denomination, CPI/base, FX, financial-unit, and benchmark continuity checks.
- Raw prices remain immutable and corporate actions reconcile to the position/cash ledger.
- Close-derived signals cannot fill before the next eligible market event.
- Suspensions, zero-volume observations, and known restrictions do not create automatic fills.
- Exact effective-dated session, auction eligibility, order type, tick, price-limit, special-instrument, and restriction tables are data, not hard-coded current prose.
- One current and one pre-2022 worked S/S+1 fixture cite their official rule revisions and reproduce expected eligible/rejected fills.
- A daily open is classified as opening-auction price or first eligible print from the historical instrument/session mapping.
- Commission, spread/slippage, participation, settlement, and rejection behavior are explicit and stressable.
- A one-share buy/hold fixture, a dividend, a split, a no-fill day, and a KAP correction have hand-calculated expected results that tests reproduce.
- The KAP engineering fixture is manually adjudicated. Any test invoking the 5% materiality rule names the PIT statement filings, issuer mapping, publication timestamps, TTM inputs/formula, scope, currency/unit, and restatement versions; otherwise the gate is disabled.
- A random sample of at least 50 symbol-dates is reconciled with an authoritative or second independent source; discrepancies are documented rather than silently repaired.
- The BIST benchmark code, price/return type, currency, history, base/rebasing, and sample values match the official index-code workbook; candidate strings are not hard-coded before this passes.
- Re-running the same manifest produces identical signals, fills, ledger, metrics, and dataset hash.
- Results include all audit artifacts and all attempted strategy variants.

**Evidence-valid for a strategy conclusion**

- At least 8–10 years of daily history and several regimes for price strategies.
- At least 200 eligible event observations across many issuers for the event study. With fewer, the result remains underpowered/descriptive and does not pass this evidence-valid gate.
- Point-in-time universe and delisted securities are included.
- Licensed point-in-time fundamentals include original publication timestamps, issuer mapping, statement scope, units/currency, and revision history sufficient to reproduce every Strategy A TTM input as it was known at the event time.
- Officially verified return-index benchmark code/history and appropriately lagged TL/FX/inflation alternatives are present.
- Development, validation, and untouched holdout boundaries were frozen before results were inspected.
- The hypothesis family, primary endpoint, Holm α = 0.05 correction, dependence-preserving p-value method, and total strategy-attempt count were preregistered and completely reported. Missing trials or post-result changes fail this gate.
- Walk-forward, cost stress, parameter sensitivity, concentration, capacity, and block-bootstrap confidence intervals are reported.
- Market and point-in-time equal-weight controls use the same participation cap, rebalance costs, settlement, restriction, price-limit, and realistic fill/rejection rules.
- The conclusion survives realistic costs and is not carried by one company, one year, or a few trades.

Passing these criteria still would not prove future profitability. It would establish that the historical test is reproducible and materially harder to fool.

### Risks and blockers

1. **Data rights:** public visibility is not permission for automated bulk storage or redistribution.
2. **KAP bulk access:** contracted API price, quotas, financial-table delivery, and archive retention remain unknown.
3. **Historical microstructure:** no local minute/tick/depth data exists; Yahoo's tested one-minute window rolls after about 30 days.
4. **Corporate actions:** Yahoo dividends/splits are insufficient evidence for Turkish rights issues and capital increases.
5. **Survivorship:** the current ten companies are not a point-in-time universe.
6. **Point-in-time fundamentals:** the 5% materiality gate cannot run automatically until revenue facts, issuer mapping, publication timestamps, TTM inputs, scope, currency/unit, and restatement versions are licensed or manually established.
7. **Rule history:** complete historical tick, limit, auction, suspension, VBTS, short-sale, and gross-settlement states, especially pre-2022, are not yet assembled.
8. **Benchmark:** Yahoo XU100.IS is a price-index proxy; the official total-return series is required, and the candidate return-index codes remain unverified.
9. **Redenomination:** pre-2005 Yahoo and other vendor series can contain a hidden old-TL/new-TL, CPI-base, FX, or reporting-unit discontinuity.
10. **Costs:** broker- and period-specific commissions, taxes, and settlement availability are not verified.
11. **Event causality:** simultaneous macro, political, sector, and issuer news can confound a KAP reaction.
12. **AI extraction:** Turkish finance language, tables, amendments, and PDFs require a manually labeled accuracy test. Model changes can alter old labels.
13. **Small sample:** ten issuers may not produce 200 independent material events.
14. **Manipulation and liquidity:** suspicious price/volume is not automatically a signal; it may make a fill impossible or an apparent backtest gain untradeable.
15. **Vendor ambiguity:** several commercial pages document capabilities but not exact retention, schemas, authoritative rights/delistings/PIT/restrictions, usage rights, or current total price.
16. **Operational drift:** source schemas, symbols, exchange rules, and rolling windows can change; collectors need coverage and schema alarms.

### Next smallest experiment

Do not implement a trading system yet. The next experiment is a **two-symbol sample/contract acceptance package**, not a performance test.

**[A] This package is a parallel procurement/data-quality track that gates only the paid or institutional-grade paths. It is not a prerequisite for the free Yahoo daily-data MVP.** Following an explicit implementation decision, that MVP may start immediately at USD 0 with ASELS, THYAO, GARAN, AKBNK, KCHOL, SISE, EREGL, TUPRS, BIMAS, and SAHOL using data from 2006 onward. Its output remains engineering/exploratory evidence under the limitations in this report.

The parallel sample/contract package is:

1. Identify a documented ASELS or THYAO corporate action, VBTS restriction, suspension, auction anomaly, or other effective-dated rule case. Request a sample window centered on that case; prefer roughly three months. If a vendor offers only one month, it must be centered on the known case rather than selected arbitrarily.
2. Keep ASELS and THYAO if the window covers the selected cases. Otherwise request a different case before expanding the symbol count.
3. Require daily raw and adjusted bars, one-minute bars or trades, auction/session flags, corporate actions including rights fields, restriction state, instrument reference data, PIT fundamental publication metadata, and the vendor's license/data dictionary.
4. Obtain the matching KAP disclosure/financial statement and preserve published_at, assumed or observed first_seen_at, attachments, corrections, issuer mapping, scope, units, and TTM formula inputs.
5. Obtain the official BIST index-code workbook and a return-index sample; confirm exact code, currency, price/return type, history, and rebasing. Do not hard-code XU100_CFNNTLTL or XU100_CFNNTLUS before this check.
6. Request a small excerpt on both sides of 2005-01-01 for every available monetary/index series needed later. Reconcile the 1,000,000:1 old-TL/new-TL conversion and distinguish CPI/index rebasing, FX, reporting units, and corporate actions. The MVP itself still starts no earlier than 2006.
7. Reconcile timestamps, OHLCV, action adjustment, tick and effective price-limit validity, session/auction classification, restriction behavior, current and pre-2022 S/S+1 fixtures, and one hypothetical first-eligible-event fill.
8. Record exact API/export, retention, quota, authentication, storage, non-display/derived-use, redistribution, and total-price answers in the acceptance package.
9. Measure missing fields and file size, then decide which paid or institutional-grade path and quote, if any, is worth accepting. This decision does not determine whether the free Yahoo daily MVP can proceed.

Success means we can account for every input and explain every mismatch. It does **not** mean the event or strategy made money. Stop at that evidence package and review it before implementing the broader backfill or simulator.

The same-session DeepSeek Flash confirmation is complete with **APPROVE**. No implementation begins unless and until Hermes/the user makes the explicit implementation decision to proceed.

## 12. Implemented local backtest subsystem and BIST-100 extension (2026-08-08)

This section records what was actually built and measured after the planning above, and the honest limits of that evidence. It does **not** claim a tradable edge.

### 12.1 What exists now

A deterministic intraday backtest engine lives in `src/bist_backtest/` with unit tests in
`tests/unit/test_backtest_*`. It is a research tool, not a trading system:

- **Loader** (`loader.py`) reads the collector's published Parquet partitions into per-symbol-day
  frames. On 2026-08-08 it was made compaction-manifest aware: when the collector database exists, it
  reads only the partitions the DB marks `active`, so superseded partitions from a previous
  publication are ignored; synthetic test archives without a DB still work via the glob fallback.
- **Features** (`features.py`) compute intraday primitives: opening-range high/low, first-window
  return, VWAP, SMA fast/slow, Wilder RSI, volume average, plus a `PreviousSession` summary
  (prior day's close/high/low) used by previous-day strategies.
- **Engine** (`engine.py`) is a deterministic single-symbol event loop with next-bar fills,
  configurable commission/slippage/half-spread costs, and forced session-close exits; it emits a
  bar-by-bar ledger and a trades frame per simulation.
- **Strategies** (`strategies.py`) register nine price-only decision rules: opening-range-breakout
  (ORB), vwap-mean-reversion, intraday-momentum, intraday-reversal, sma-crossover, rsi-pullback,
  previous-day-breakout, volume-breakout, and buy-and-hold. Each has adjustable parameters surfaced
  through the CLI (`--param key=value`); feature-affecting parameters are plumbed into feature
  computation (windows, SMA periods, RSI period), while decision-only parameters (thresholds, gaps,
  volume multipliers) stay in the decision function.
- **Runners** (`runners.py`) link each (symbol, session) to its prior session and run the strategy
  plus an equal-weight buy-and-hold control under the same fill/cost model.
- **Metrics** (`metrics.py`) aggregate across symbol-days: total return, profit factor,
  expectancy-per-trade, hit rate, avg win/loss, annualized Sharpe and Sortino (from per-session
  returns, sqrt-252), time exposure, max drawdown, forced closes, skipped entries. Values are plain
  JSON-safe primitives (`NaN` is avoided and "no trade" runs do not crash).
- **Report + ledger** (`report.py`, `ledger.py`) write a timestamped manifest per run recording
  strategy, parameters, symbols, session dates, metrics, and the buy-and-hold benchmark, plus
  trades/ledger CSVs, and append every attempt to `attempts.jsonl` for dedupe and audit.

### 12.2 BIST-100 universe extension and backfill

- The collector previously tracked the BIST 50 (`config/universes/bist50/2026q3.json`). On
  2026-08-08 the official Borsa İstanbul constituent CSV
  (`https://www.borsaistanbul.com/datum/hisse_endeks_ds.csv`, SHA-256
  `6a106401971fecda68be710f51b450998f7763c09b7dccad080ce73a0d476b9d`) was retrieved with exactly 100
  distinct XU100 constituents.
- `config/universes/bist100/2026q3.json` is the new immutable 2026 Q3 snapshot: canonical
  exchange-code SHA-256 `ef98a56775121529280541cee982db55e527204c516c5896868738401e1f0720`,
  canonical Yahoo `.IS` SHA-256 `b12f9b20f3360a322deb6e7dfe97df2fcb6082b5f77b9293e76cba113538d5f6`,
  and the same `.E → .IS` versioned alias rule as BIST 50. The collector's `UNIVERSE_PATH` now points
  at it.
- The 50 companies that are in BIST-100 but were not in BIST-50 had no archive history. They were
  backfilled for the sessions 2026-08-03..2026-08-07 (two bounded batches of 25 symbols, 125 Yahoo
  chart requests each, zero denied) and the archive was compacted. The archive now holds 100 symbols
  × 5 sessions = 500 symbol-days. The remaining unresolved gaps (~14k per-minute slots) are partial
  session-boundary minutes recorded truthfully as pending; they do not change the headline
  coverage, and the loader reports per-symbol coverage for follow-up.

### 12.3 First structured runs

All nine strategies were run over the same five sessions (2026-08-03..2026-08-07) on three universe
breadths — the original 10-symbol engineering list, the full 50, and the full 100 — with a common
cost model (10 bps commission, 5 bps slippage, 1 bps half-spread, 100k starting equity). A small
parameter sweep was run on the 100-universe set. Results live under `backtest-results/set10`,
`set50`, `set100` (manifests + CSVs + `attempts.jsonl`).

Headline 100-universe numbers (5 sessions, default parameters):

| Strategy | Total return | Profit factor | Trades | Hit rate | Sharpe |
|---|---|---|---|---|---|
| buy-and-hold (control) | -0.11% | 0.90 | 500 | 46.6% | -0.63 |
| opening-range-breakout | +0.02% | 1.05 | 283 | 45.2% | 0.19 |
| intraday-momentum | +0.20% | 1.64 | 235 | 55.3% | 1.80 |
| vwap-mean-reversion | -0.13% | 0.84 | 450 | 41.6% | -0.85 |
| intraday-reversal | -0.40% | 0.41 | 230 | 31.7% | -3.57 |
| sma-crossover | -5.38% | 0.15 | 8319 | 10.7% | -29.1 |
| rsi-pullback | -0.72% | 0.28 | 1250 | 28.2% | -8.8 |
| previous-day-breakout | -0.08% | 0.79 | 202 | 42.1% | -0.8 |
| volume-breakout | +0.01% | 1.02 | 270 | 44.1% | 0.1 |

Parameter sensitivity (100-universe; parameters genuinely change behavior — e.g. RSI
`rsi_below=25` improved -0.72% → -0.42%, volume-breakout `volume_mult=3.0` improved +0.01% →
+0.04%). Full sweeps are recorded in the `set100` manifests.

### 12.4 Honest limitations of these results

- **Five sessions only** (a single trading week). The Sharpe/Sortino numbers are annualized from 5
  observations and should not be read as stable.
- **No corporate actions, dividends, or rights adjustments** and **no point-in-time membership**; the
  universe is today's constituents (survivorship bias). The research policy in §7 stands.
- **No market-impact or liquidity model** beyond a fixed half-spread; capacity is unbounded in the
  sim and would not fill realistically at scale.
- **Not statistically significant**: with ≤5 days, differences of a few bp are within noise, and
  multiple-comparison risk applies across 9 strategies × 3 universes × a param sweep.
- **Yahoo is an unofficial source** with no SLA; its one-minute window rolls after ~30 days, so this
  evidence cannot be reproduced or extended further back from the live archive.

These runs are engineering/exploratory evidence, matching the manifest `scope_note`. The next
meaningful step — beyond this week's window, point-in-time membership, or event-based signals —
still requires the data and acceptance work described in §9–§11.

