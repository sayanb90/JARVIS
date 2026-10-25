# JARVIS Go-Live Runbook

**Next test: section 14 (VPS test day).** Living document. Update the **Status** column and tick the gates as each item is
proven *on the VPS*, not on a dev box. Last reviewed: 2026-10-25.

Legend: `[x]` done and verified · `[~]` built, **not yet verified in prod** · `[ ]` not started

---

## 1. Scope and principles

- Scope (ADR-2, amended by ADR-3): **daily OHLC for India, US, UK and EU; everything intraday, live ticks and fundamentals for India (NSE) only.** Universe lives in `data/universe.csv` (single source of truth for every feed); the `market` column is `IN`, `US`, `UK` or `EU`.
- **Ingest ticks and L1 live, store aggregates.** The Upstox feed (section 13) delivers ticks and top-of-book; we fold them into 1-minute bars (with last bid/ask, VWAP, tick count) in-process. Raw tick storage is optional and off by default. Strategies are not sub-second.
- Free sources first (FMP free tier for fundamentals, Yahoo Finance for prices). Paid upgrades are gated on a concrete limitation, listed in section 9.
- Storage: Parquet lake queried with DuckDB (ADR-1, section 12). Partitioned by year (daily) and by day (bars); no server process, no heap ceiling.
- Prod runs on the VPS. Dev/cloud sandboxes can build and run the test suite; collectors run on the VPS.

## 2. Feed inventory

| Feed | Source | Grain | Cadence | History | Store | Status |
|---|---|---|---|---|---|---|
| Fundamentals (income, balance, cash flow, ROIC/shares) | FMP free (250 calls/day) | Annual | Rotating by staleness, ~62 tickers/day | Last 5 fiscal years | `lake/fundamentals/*.parquet` | `[~]` |
| Daily OHLC, 4 markets | Yahoo (`yfinance`) | 1 day | Every trading day after the last close (22:30 UTC) | 5 years backfill | `lake/ohlc/year=YYYY/` | `[~]` not run live (Yahoo blocked in the build sandbox) |
| Intraday bars, interim, **India only** | Yahoo (`yfinance`) | 5 min | Once, after the NSE close | About 59 days via `--backfill`, then forward (see 2.1) | `lake/bars/interval=5m/date=YYYY-MM-DD/` | `[~]` retire once the Upstox feed is live |
| **Live ticks + L1 (new)** | Upstox API **v3** WebSocket (Protobuf) | Tick / L1 snapshot, aggregated to 1 min | During market hours | Forward-only | `lake/bars/interval=1m/`, optional `lake/ticks/` | `[~]` built and tested against a fake server; not run against Upstox (section 13) |
| Index F&O intraday | TBD | TBD | TBD | TBD | TBD | `[ ]` see section 9 |

### 2.1 Constraint that shapes the whole intraday plan

Yahoo serves only a short trailing window of intraday history (about 7 days of 1m, about 60 days of 5m/15m/30m). **There is no deep backfill.** Our intraday history starts the day the collector first runs, so:

- The collector must run daily without gaps. A missed week of 1m data is unrecoverable; a missed week of 5m is recoverable only inside the 60-day window (the job re-fetches the last 5 days every run, so a short outage self-heals).
- Backtests on intraday strategies cannot start until enough history accumulates. **Start the collector first; everything else can wait.**
- If a strategy needs years of intraday history on day one, that is a paid-data decision (section 9), not a code change.

### 2.2 Storage sizing (approximate, uncompressed)

Row is about 56 bytes (timestamp, 2 enumerated syms, 4 floats, long).

| Scenario | Rows/day | Per year |
|---|---|---|
| 250 stocks, 5m (about 77 bars/day) | about 19 k | about 270 MB |
| 250 stocks, 1m | about 96 k | about 1.4 GB |
| 1,000 stocks, 1m | about 385 k | about 5.5 GB |

Fits comfortably (bars are India only, so these are the upper bound). Parquet files are already ZSTD-compressed (typically several times smaller than the figures above). Keep a retention policy for the per-day CSVs (section 7).

## 3. Environments

| | Dev / cloud sandbox | VPS (prod) |
|---|---|---|
| Purpose | Build, unit-test Python | Run cron collectors, host backtester webapp |
| `duckdb` / `pyarrow` (pip) | Yes | Yes |
| Network egress to Yahoo / FMP | Via proxy | Direct |
| Holds real data | No | Yes |

VPS spec, provider and OS are **not yet decided** (open decision O1). Minimum I would plan for: 2 vCPU, 4 GB RAM, 50 GB SSD, Linux, UTC clock.

## 4. Phase plan and go-live gates

### Phase 0 — VPS foundation
- [ ] Provision VPS; set timezone to **UTC** (all cron times in this runbook are UTC)
- [ ] Non-root `jarvis` user; SSH keys only; password login off; firewall allowing only 22 and 443
- [ ] Install Python 3.11+, create venv, `pip install -r requirements.txt`
- [ ] Clone repo; copy `.env.example` to `.env` and add `FMP_API_KEY`
- [ ] Backups configured and a restore tested (section 7)

**Gate 0:** `pytest` passes, `python -m data_layer.yf_client --dry-run` runs, a test backup restores.

### Phase 1 — Data foundation (start collecting immediately)
Order matters: bars first because they cannot be backfilled.

1. [ ] `python scheduler.py --job bars --dry-run` shows the expected ticker counts
2. [ ] Run once for real, with the deepest history Yahoo allows: `python scheduler.py --job bars --market IN --backfill` (about 59 days of 5m). Daily runs after that
3. [ ] Confirm CSVs exist under `data/bars/5m/in/YYYY-MM-DD.csv`
4. [ ] Confirm the load worked: `python -c "from data_layer.query import Lake; print(Lake().sql('SELECT market, count(*) n FROM bars_5m GROUP BY 1'))"`
5. [ ] Daily OHLC 5-year backfill for all four markets: `python -m data_layer.yf_client --mode full --market ALL`, then `python -m data_layer.lake --load ohlc`
6. [ ] Fundamentals first pass: `python scheduler.py --job fundamentals` daily until `fund_meta.csv` covers the universe (about 4 days for 250 stocks)
7. [ ] Install cron (section 5)
8. [ ] Spot-check 3 NSE tickers against a second source (close price and volume, one day each)

**Gate 1 (data is trustworthy):**
- [ ] Bars: every universe ticker has bars on the last trading day; bar count per ticker is about 75–78 for a full session
- [ ] No duplicate `(ts, symbol)` rows after running the same job twice
- [ ] First bar of the day is 03:45 UTC (09:15 IST), last bar is complete (ends 10:00 UTC)
- [ ] 5 consecutive trading days of unattended cron runs with no failures in `logs/scheduler.log`
- [ ] Scoring: `Lake().top_by_score("IN", 10)` returns sensible results against a known-good ticker

### Phase 2 — Backtester webapp on the VPS
`backtester/` and `webapp/` are empty stubs today. Plan:

- [ ] Decide stack (open decision O3). Suggested: Python backend (FastAPI) importing `data_layer.query.Lake` in-process (read-only), thin frontend
- [ ] Data access layer: one module that queries `bars5m` and `ohlc` over IPC and returns frames; backtester never reads CSVs
- [ ] Backtest engine rules baked in from day one: signals use **bar close**, fills at **next bar open**, explicit commission and slippage parameters, no look-ahead into fundamentals (use the date a filing became public, not the fiscal period end)
- [ ] Corporate actions: stored intraday bars are adjusted as of fetch time and are **not re-adjusted** after later splits/dividends. Backtests across a split date must be flagged (see section 8)
- [ ] Strategy registry: strategies as versioned modules; each run stores strategy version, parameters, data range and results
- [ ] Deploy behind a reverse proxy (Caddy or nginx) with TLS and authentication. There is no database port to expose; the app opens the lake files read-only
- [ ] Run the app as a `systemd` service under the `jarvis` user with restart on failure
- [ ] Resource limits: cap concurrent backtests so a heavy run cannot exhaust RAM and kill the collectors

**Gate 2:** a known simple strategy (for example SMA crossover on one ticker) reproduces the same result in the webapp and in an independent calculation, and the run is stored and retrievable.

### Phase 3 — Futures and options (index products)
Planning only. See section 9. **Do not write collector code until the decisions there are made.**

## 5. Daily operations (all times UTC, trading weekdays)

```cron
# Daily OHLC, all four markets (after the last close, NYSE 21:00 UTC in winter)
30 22 * * 1-5  cd /home/jarvis/JARVIS && python scheduler.py --job ohlc-daily   >> logs/scheduler.log 2>&1
# Interim Yahoo bars, after the NSE close (retire when the Upstox feed runs)
20 10 * * 1-5  cd /home/jarvis/JARVIS && python scheduler.py --job bars --market IN >> logs/scheduler.log 2>&1
# Fundamentals rotation
0 12 * * 1-5   cd /home/jarvis/JARVIS && python scheduler.py --job fundamentals  >> logs/scheduler.log 2>&1
```

Notes:
- NSE regular session is 03:45-10:00 UTC (09:15-15:30 IST). India has no daylight saving, so these times never shift. LSE and Euronext close about 16:30 UTC, NYSE at 20:00 or 21:00 UTC; the single 22:30 UTC run is after all of them.
- Different holiday calendars: a market closed that day simply returns no rows.
- The live feed process (section 13) is a long-running service started before 03:45 UTC, not a cron job.
- A job on an NSE holiday should fetch nothing and exit cleanly; confirm this during Gate 1.
- The scheduler holds a lock file (`logs/scheduler.lock`). If a run was killed, delete a stale lock before re-running.

### Daily check (2 minutes)
- [ ] `tail logs/scheduler.log`: all three jobs finished successfully
- [ ] Row counts for yesterday are in the expected range (section 4, Gate 1)

### Failure handling
| Symptom | Likely cause | Action |
|---|---|---|
| Bars job returns 0 rows | Holiday, or Yahoo throttling/outage | Re-run later the same day; the 5-day overlap window repairs gaps |
| Some tickers missing | Delisted, symbol changed, or one failed chunk | Check `logs/yf_client.log`; update `universe.csv` |
| `scheduler.lock` exists | Previous run still running or killed | Check PID in the file; delete only if the process is gone |
| Lake load fails | Disk full or bad CSV | Check disk, inspect the day-file; reload with `python -m data_layer.lake --load bars --since YYYY-MM-DD` |
| FMP returns 429/403 | Daily budget exceeded | Wait for reset; lower `FMP_DAILY_BUDGET` |
| Gap longer than the Yahoo window | Collector down too long | Unrecoverable from Yahoo; record the gap in section 10 |

## 6. Loader commands

```bash
python -m data_layer.lake --load all                        # fundamentals + full-history OHLC
python -m data_layer.lake --load fundamentals               # upsert fundamentals tables
python -m data_layer.lake --load daily                      # upsert daily OHLC increment
python -m data_layer.lake --load bars --interval 5m         # upsert last 7 days of bar files
python -m data_layer.lake --load bars --since 2026-10-01    # rebuild bar partitions from a date
```

Reloads are idempotent: every load is an upsert on the table key, and each bar day-file holds the complete state for that market and day.

## 7. Backup and retention

- Nightly: archive `lake/`, `data/universe.csv`, `data/fund_meta.csv` to off-VPS storage; keep 7 daily and 4 weekly
- **A backup is not real until a restore has been tested** (Gate 0)
- Bar CSVs under `data/bars/` are the rebuild source for `lake/bars/`; compress and archive files older than 90 days, do not delete them
- Treat the intraday history as irreplaceable (section 2.1): it is the one dataset we cannot re-download

## 8. Known risks and limits

| Risk | Impact | Mitigation |
|---|---|---|
| Yahoo is an unofficial, unsupported source; its terms limit use and it can change or break without notice | Collector can fail silently or return bad data | Alerting on row counts; plan a paid-feed swap (section 9); keep personal use |
| Intraday retention window (above) | No deep backfill | Collect forward from day one |
| Bar timestamps are UTC; sessions differ by market and DST | Wrong session alignment in strategies | Convert at the edge only; test the DST week |
| Stored bars not re-adjusted for later splits/dividends | Spurious returns across corporate actions | Maintain a corporate-actions table; flag or re-fetch affected tickers |
| Yahoo data may be delayed or revised, especially NSE | Not suitable for live execution decisions | Research and backtest only until a licensed real-time feed exists |
| Single-writer lake | Two loaders writing the same file at once | Scheduler lock file serialises jobs; writes are atomic renames |
| FMP free tier rate limit | Fundamentals refresh slows as universe grows | Rotation by staleness; upgrade tier if the universe passes about 250 |
| Look-ahead bias in fundamentals | Inflated backtest results | Join on filing/announcement date |
| Loader and query code covered by unit tests on synthetic data only | First real fetch may expose provider quirks | Gate 1 steps 4–5 are the first real execution |

## 9. Planning entry: index futures and options intraday data

**Status: `[ ]` planning. Owner decisions needed before any build. Nothing here is verified; vendor names are candidates to evaluate, and pricing, licensing and data coverage must be checked directly with each.**

### 9.1 Why this is not a copy of the equity feed
- Instrument count explodes: one index has many expiries times many strikes times calls and puts, and weekly expiries multiply it. Storage and request volume are orders of magnitude above equities
- Contracts are **short-lived and numerous**: you need a contract master (underlying, expiry, strike, type, lot size) and a lifecycle, not a ticker list
- Lot sizes, expiry days and strike steps change over time; the backtester must use the values in force on each date
- Useful analytics need more than OHLC: open interest, implied volatility, greeks, bid/ask. These are often a separate, paid feed
- Free equity sources like Yahoo do **not** cover this well enough for systematic work

### 9.2 Decisions to make (in order)
1. [ ] **Underlyings:** for example Nifty 50 and Bank Nifty on NSE. Pick the first one or two only
2. [ ] **Instruments:** futures only, options only, or both; weekly expiries or monthly only
3. [ ] **Data needed by the first strategy:** bars only, or bars plus OI/IV/greeks and chain snapshots? Define the strategy before buying data
4. [ ] **Granularity:** 1-minute bars to start (consistent with "no raw tick"); revisit only if a strategy needs finer
5. [ ] **History needed vs forward collection:** need years of history now (buy it) or accumulate forward (cheap, slow)?
6. [ ] **Source:** evaluate at least two options per market against the criteria below
7. [ ] **Budget:** set a monthly ceiling before evaluating vendors
8. [ ] **Execution intent:** research-only, or eventual live trading (which requires a licensed real-time feed and broker API)?

### 9.3 Source candidates to evaluate (unverified)
| Market | Candidate type | Examples to look at | What to check |
|---|---|---|---|
| India | Exchange end-of-day files | NSE bhavcopy (free, EOD only) | Good for daily F&O and OI history, not intraday |
| India | Broker APIs | **Upstox (already chosen for equities, section 13)**, Zerodha Kite Connect, Angel One SmartAPI | Intraday history depth, rate limits, cost, whether expired contracts are available |
| India | Data vendors | TrueData, Global Datafeeds | Historical F&O depth, licence for personal use, price |

Evaluation criteria: coverage of **expired** contracts, adjusted vs raw history, bar sizes available, OI/IV included, licence terms for storing and processing, API rate limits, total monthly cost, reliability.

### 9.4 Design sketch (to confirm after decisions)
- `contractMaster` table: `instId, underlying, market, kind (FUT/CE/PE), expiry, strike, lotSize, tickSize, listedDate, expiredDate`
- `fnoBars1m` table in its own Parquet tree `lake_fno/` (volume is too high to share `lake/bars/`), keyed by `instId`, with `oi` and optional `iv`, `bid`, `ask`
- Optional `chainSnapshot` table (end-of-bar chain for an underlying) if strategies need the whole surface
- Separate collector module and cron entry; do not extend the equity collector
- Sizing must be estimated from real contract counts before choosing a source (rows per day equal contracts times bars per session)

### 9.5 Exit criteria for this planning item
Decisions 1–8 recorded, one source chosen with a documented cost, a sizing estimate, and a one-week pilot plan on one underlying. Then open a build task.

## 10. Change and incident log

| Date | Event | Notes |
|---|---|---|
| 2026-10-25 | Added `data_layer/upstox/auth.py`: login URL, code exchange, `.env` token storage, and a feed probe that tests access vs extended token. Owner must first create the Upstox developer-console app | Waiting on app credentials |
| 2026-10-25 | Upstox v3 feed adapter built (`data_layer/upstox/`): decode, instrument keys from ISIN, reconnecting WebSocket runner, daily scheduling, 8 tests incl. fake server. Protobuf module vendored from the official SDK (ADR-4) | Not yet run against Upstox |
| 2026-10-25 | Daily OHLC re-extended to India+US+UK+EU (5y, Yahoo); intraday, ticks and fundamentals stay India only; `--backfill` for deepest Yahoo intraday window; `currency` column added to universe (ADR-3). Earlier same day: scope narrowed to NSE only; US removed from universe, fetchers, scheduler, lake and tests. Upstox v2 WebSocket chosen for live tick/L1 (ADR-2); tick model, bar aggregator and segment storage built | Adapter blocked on proto/docs access (section 13) |
| 2026-10-25 | Runbook created; intraday bars collector, `bars5m` schema and loader added | Not yet run on the VPS |
| 2026-10-25 | `yf_client` universe now read from `data/universe.csv` (previously a hardcoded 20-ticker US fallback) | |

## 11. Open decisions for the owner

| # | Question | Blocks |
|---|---|---|
| O1 | VPS provider, OS and size? | Phase 0 |
| O2 | ~~q licence~~ Closed by ADR-1 (no KDB+ dependency) | — |
| O3 | Backtester webapp stack and who uses it (just you, or others)? | Phase 2 |
| O4 | Universe target: stay at about 250 stocks or move toward 1,000? (drives FMP tier and storage) | Phase 1 |
| O5 | First F&O underlying, and the first strategy that needs it | Phase 3 |
| O6 | Monthly data budget ceiling | Phase 3 |
| O7 | ~~Upstox v2 or v3~~ Closed: v3 (ADR-4) | — |
| O8 | Keep raw ticks/L1, or bars only? (sizing in 13.4) | Section 13 |
| O9 | Fundamentals source for NSE: does the FMP free tier cover Indian tickers? Unverified, must be tested before relying on it | Phase 1 |
| O11 | Which UK and EU indices define the universe, and how many names (about 1,200 total was mentioned earlier)? | Phase 1 |
| O10 | Token handling. Plan: test whether the v3 feed accepts the `extended_token` (`auth check`); if yes, adopt it; if no, build the mobile-approval webhook. Stored in `.env` (mode 600) | Section 13 |
| 2026-10-25 | Replaced KDB+ with Parquet + DuckDB (`lake.py`, `query.py`, tests); removed `schema.q`, `kdb_loader.q`, `helpers.q`. Pillar scorer ported to Python with explicit date ordering for the growth/decrease pillars | ADR-1 |

## 12. Architecture decisions

### ADR-1: Parquet lake + DuckDB instead of KDB+ (2026-10-25)
- **Context:** the project had no KDB+ subscription, so only the free 32-bit build was usable. Its 4 GB address-space limit also counts memory-mapped columns, which caps query size, and an in-VPS webapp would need a q process plus an IPC bridge from Python.
- **Decision:** store data as partitioned Parquet (ZSTD) and query it with embedded DuckDB. Collectors still write CSV staging files; `data_layer/lake.py` upserts them, `data_layer/query.py` reads.
- **Consequences:** free, no server, no heap ceiling, SQL as-of joins and window functions available to the backtester, plain Python on the VPS. We give up kdb's native tick-plant and in-memory tables.
- **Hot layer:** with aggregated bars (not ticks) today's data for 1,000 symbols at 5 minutes is a few MB, so the current day stays as a pandas/Polars frame in the collector or webapp process. Revisit if the F&O chain feed (section 9) outgrows one box: ClickHouse is the next step, not a return to KDB+.
- **Reversible:** CSV staging files remain the rebuild source for every table.

### ADR-2: NSE only, Upstox WebSocket for live tick/L1 (2026-10-25)
- **Context:** the product now targets Indian markets only. A live broker feed gives real ticks and top-of-book, which Yahoo cannot (Yahoo is delayed, unofficial and keeps only about 60 days of 5-minute history).
- **Decision:** drop US/UK/EU. Ingest Upstox API market data over WebSocket (Protobuf, binary frames), aggregate to 1-minute bars in-process, persist bars (and optionally raw ticks) as append-only Parquet segments.
- **Consequences:** history starts the day the feed first runs. Upstox's historical-candle API can backfill daily bars; user reports say 1-minute history is limited to a rolling window of a few months (unverified, section 13.3). Fundamentals still need a separate source (Upstox does not provide them).
- **Kept:** Yahoo daily/intraday collectors stay as an interim and cross-check source until the Upstox path is verified.

### ADR-3: Daily OHLC for four markets, intraday for India only (2026-10-25)
- **Decision:** 5 years of daily OHLC from Yahoo for India, US, UK and EU. Intraday bars, live ticks and fundamentals remain India only. Amends ADR-2.
- **Intraday depth:** Yahoo allows about 59 days of 5m and 6 of 1m; 60m bars reach about 729 days. **6 months of 5-minute history cannot come from Yahoo.** Options: take 60m for 2 years from Yahoo, rely on Upstox historical candles (reported as about six months of 1m; unverified, section 13.3), or let the live feed accumulate forward.
- **Not yet handled:** (1) UK `.L` prices are in pence (GBp), not pounds, and EU is quoted in EUR, US in USD, India in INR; no FX table exists, so cross-market return comparisons need one. (2) Yahoo prices are adjusted at fetch time and the daily job refreshes only the last 7 days, so after a split or dividend older rows disagree with new ones. (3) UK and EU universe rows are 10-name placeholders without ISINs; the real index constituents (FTSE 100/250? STOXX 600? Euro Stoxx 50?) are not chosen. (4) Yahoo was unreachable from the build sandbox, so no live fetch has run.

### ADR-4: Upstox API v3 for the live feed (2026-10-25)
- **Decision:** use the v3 market data feed (`wss://api.upstox.com/v3/feed/market-data-feed`), not v2, which community posts report as heading for deprecation. Amends ADR-2.
- **Source of truth for the protocol:** the official `upstox-python-sdk` 2.30.0 (MIT). The Upstox docs site was unreachable from the build sandbox, so endpoint, subscribe message, modes and Protobuf schema were read from the SDK's own source and generated module, which is vendored in `data_layer/upstox/proto/`.

## 13. Upstox live feed (tick + L1)

**Status: `[~]` Built and unit-tested end to end against a local fake server that speaks the SDK's protocol. NOT yet run against the real Upstox service (needs a valid token on a trading day). That run is Gate U1.**

### 13.1 What exists
- `data_layer/upstox/decode.py`: Protobuf `FeedResponse` to `Tick`. Handles stock/derivative full feeds (LTP, last trade time, cumulative volume `vtt`, first level bid/ask and sizes, OI), index feeds (LTP only), `ltpc` mode and `option_greeks` mode; reads market-status frames.
- `data_layer/upstox/instruments.py`: universe symbol to instrument key, `NSE_EQ|<ISIN>`, from the ISIN column. Symbols without a valid ISIN are reported and skipped. Optional validation against a locally saved instrument master.
- `data_layer/upstox/feed.py`: `FeedProcessor` (frame bytes to bars, drops last-trade-yesterday and future-timestamped ticks) and `UpstoxFeed` (WebSocket with bearer token, binary JSON subscribe in batches of 500, periodic flush and write, watchdog that reconnects after 120 s without frames, exponential backoff, fatal error on HTTP 401/403 with a "token expires daily" message). The daemon waits for each weekday session (03:40-10:05 UTC), then compacts the day. `--once` runs a single session now.
- `data_layer/ticks.py`, `data_layer/tick_store.py`: aggregation and storage (section 13.4). Bars land in `lake/bars/interval=1m/` and are queryable as `bars_1m`.
- Tests: `tests/test_upstox.py`.

### 13.2 Get a token, then run it (VPS)
One-time, by the owner: at the Upstox developer console choose **New App**, set the redirect URL (for example `http://localhost:8000`), and put the API key, API secret and redirect URL in `.env` (`UPSTOX_API_KEY`, `UPSTOX_API_SECRET`, `UPSTOX_REDIRECT_URI`). Nothing below can run without this.
```bash
python -m data_layer.upstox.auth login-url      # open the link on your phone, log in, approve
python -m data_layer.upstox.auth exchange "<the URL you were redirected to>"   # stores tokens in .env (mode 600)
python -m data_layer.upstox.auth check          # does the v3 feed accept the access token? the extended token?
python -m data_layer.upstox.auth check --adopt  # if the extended token works: collector uses it (UPSTOX_FEED_TOKEN)
```
`check` is the O10 experiment: if the feed accepts the extended token and it lives long enough, no daily refresh is needed; otherwise the access token must be redone every trading morning (steps 1-2) or the approval-webhook flow (`/v3/login/auth/token/request/{client_id}`, unverified) is built. Tokens are never printed or logged.
```bash
python -m data_layer.upstox.feed --once --limit 20      # smoke test on a trading day
python -m data_layer.upstox.feed                         # daemon
```
`systemd` unit (`/etc/systemd/system/jarvis-feed.service`):
```ini
[Service]
User=jarvis
WorkingDirectory=/home/jarvis/JARVIS
EnvironmentFile=/home/jarvis/JARVIS/.env
ExecStart=/home/jarvis/JARVIS/.venv/bin/python -m data_layer.upstox.feed
Restart=always
RestartSec=10
```

### 13.3 Gate U1: first real run (still to do)
- [ ] **U0:** developer-console app created, `auth exchange` succeeds, `auth check` shows which token the feed accepts and I record the extended token's real lifetime here: ______
- [ ] Token obtained and the feed accepts it (HTTP 101, not 401)
- [ ] One captured real frame saved as a test fixture; confirm `ltt` is epoch **milliseconds** (the processor warns if timestamps look wrong)
- [ ] `full` mode for the whole universe works; find the real per-connection and per-user subscription limits and set `SUBSCRIBE_BATCH` and the connection count accordingly (not documented in the SDK)
- [ ] Bars for a full session match a second source (open, high, low, close; volume differs slightly because bars are built from tick deltas)
- [ ] First bar 03:45 UTC, last bar 09:59 UTC, no duplicate `(symbol, ts)` after `compact_day`
- [ ] Behaviour on an NSE holiday (connection works, no ticks; the watchdog will reconnect every 2 minutes, harmless but noisy)
- [ ] Decide O10: how the daily token is refreshed. Today it is an environment variable set by hand.

### 13.4 Known limits
- The aggregator builds volume from cumulative-volume deltas; the first tick after connect is a baseline, so volume traded before the collector connected is not attributed to any bar. Start before 03:45 UTC.
- If the feed is down, ticks in the gap are unrecoverable from the live path. Upstox's historical-candle API (community reports: about six months of 1-minute data, unverified) can repair 1-minute bars; no code for that exists yet.
- Index instruments carry no volume.

### 13.5 Storage sizing for the optional raw tick/L1 store
At an average of 1 update per second per symbol over the 6.25-hour session, 1,200 symbols produce about 27 million ticks a day, roughly 1-2 GB a day compressed with L1 fields, about 250-500 GB a year. That is why bars are the default and raw ticks are opt-in (O8). 1-minute bars for the same universe are about 450 k rows a day, a few MB.

## 14. VPS test day: 5-year OHLC (US, UK, EU, India) and one day of Upstox intraday (India)

**Goal:** prove on the VPS, with real data, the two things that have only been tested offline: (A) a 5-year daily OHLC backfill for all four markets, and (B) one full NSE session of Upstox v3 ticks turned into 1-minute bars.
**Tested so far:** everything in this section's code path runs end to end against stubbed Yahoo data and a local fake Upstox server (20 tests). **Not tested:** real Yahoo, real Upstox, the real token flow.
**Scope of the test universe:** `data/universe.csv` has 10 stocks per market (UK and EU rows are placeholders, section 11 O11). That is enough to prove the pipeline; widen the universe after it passes.

### 14.0 Timeline (all times UTC; IST = UTC + 5:30)

| UTC | IST | Step |
|---|---|---|
| Any time before market | any | 14.1 setup, 14.2 OHLC test (needs no token, about 15-30 min) |
| before 03:35 | 09:05 | 14.3 get and check the Upstox token |
| 03:40 | 09:10 | 14.4 start the feed (inside `tmux`) |
| 03:45 to 10:00 | 09:15 to 15:30 | session runs unattended; peek at the log |
| 10:05 | 15:35 | feed stops by itself and compacts the day |
| after about 11:00 | after 16:30 | 14.5 verify; Yahoo's daily bar for the day must exist first |

First check the NSE holiday calendar for the test date (likely Monday 2026-10-26). On a holiday the connection works but no ticks arrive, so move 14.3-14.5 to the next trading day.

### 14.1 One-time setup (VPS)
```bash
timedatectl | grep "Time zone"        # must show UTC; fix: sudo timedatectl set-timezone UTC
git clone <repo> JARVIS && cd JARVIS && git checkout claude/us-stock-market-analysis-MZtcm   # until merged to master
python3 -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt
cp .env.example .env && chmod 600 .env     # fill UPSTOX_API_KEY, UPSTOX_API_SECRET, UPSTOX_REDIRECT_URI
python -m pytest -q tests                  # expect: 20 passed
python scheduler.py --job all --dry-run    # expect 10 tickers per market in the log
```
Before this: the owner has created the app in the Upstox developer console (section 13.2). FMP is **not** needed for this test.

### 14.2 Test A: 5-year daily OHLC, four markets (no token needed)
```bash
python -m data_layer.yf_client --mode full --market ALL     # about 1-3 min; logs/yf_client.log
python -m data_layer.lake --load ohlc
python -m data_layer.verify ohlc --years 5
```
Quick look:
```bash
python - <<'PY'
from data_layer.query import Lake
q = Lake()
print(q.sql("SELECT market, count(*) rows, count(DISTINCT symbol) symbols, min(date) first, max(date) last FROM ohlc GROUP BY 1 ORDER BY 1"))
print(q.ohlc("SHEL.L").tail(3))      # UK closes are in PENCE (about 2,500-3,000), not pounds
PY
ls lake/ohlc                          # year=2021 ... year=2026
```
Idempotency (must not change the row count):
```bash
python -m data_layer.yf_client --mode daily --market ALL && python -m data_layer.lake --load daily
python -m data_layer.lake --load ohlc            # re-run the full load
python -m data_layer.verify ohlc | grep -i "duplicate"
```
**Pass when:**
- [ ] `verify ohlc` prints no `FAIL`; every market shows all its universe symbols with data
- [ ] 5-6 year folders under `lake/ohlc/`; about 1,200-1,270 rows per symbol (US/UK/EU more than India because of holidays)
- [ ] duplicates stay 0 after the re-runs
- [ ] 3 spot checks per market (close and volume for one date) agree with a second source such as the exchange site or a broker screen
- [ ] write down every `WARN` and why; unexplained one-day moves over 40% usually mean unadjusted splits

### 14.3 Test B1: Upstox token (before 03:35 UTC)
```bash
python -m data_layer.upstox.auth login-url        # open on your phone, log in, approve
python -m data_layer.upstox.auth exchange "<the full URL you were redirected to>"
python -m data_layer.upstox.auth check            # tells you which token the v3 feed accepts
```
Record the result of `check` in section 13.3 (Gate U0). If it says the extended token is accepted, run `check --adopt`; otherwise the access token is used and must be redone every trading morning. The exchange step must happen within a couple of minutes of the login (the code is single-use).

### 14.4 Test B2: run the feed for one session (India)
```bash
tmux new -s feed
. .venv/bin/activate
python -m data_layer.upstox.feed --once --limit 20 --dump-frames 5 2>&1 | tee logs/feed-test.log
```
`--dump-frames 5` saves the first 5 raw frames to `logs/frames/*.bin`: bring these back, they become the real test fixtures and settle whether `ltt` is in milliseconds. Detach with `Ctrl+b d`. It subscribes to the universe's India rows (10 today, the `--limit` is an upper bound) in `full` mode and stops itself at 10:05 UTC. `Ctrl+C` at any time is safe: it saves the last bars and compacts the day.

What the log should show:
- `subscribed 10 instruments in mode full` (a `N universe symbols have no usable Upstox key` warning means missing ISINs)
- `market status: {...NORMAL_OPEN...}` around 03:45
- every 30 s `frames=... ticks=... bars_written=...` with `ticks` growing
- at the end `session YYYY-MM-DD done: compacted N bars`

Red flags in the log: `ahead of local clock` (timestamp unit or VPS clock wrong), `no frames for 120s` repeatedly during market hours, `rejected the token` (expired; redo 14.3), repeated `connection lost`.

### 14.5 Verify the intraday day (after about 11:00 UTC)
```bash
python -m data_layer.yf_client --mode daily --market IN --days-back 3 && python -m data_layer.lake --load daily   # Yahoo daily bar to cross-check against
python -m data_layer.verify bars --date 2026-10-26 --interval 1m      # use the real date
du -sh lake/bars lake
```
**Pass when:**
- [ ] `verify bars` prints no `FAIL`
- [ ] first bar 03:45 UTC, last bar about 09:59 UTC
- [ ] liquid names (for example RELIANCE, HDFCBANK, INFY) have close to 375 one-minute bars; illiquid names may have fewer (that is a `WARN`, not a failure)
- [ ] last-bar close is within 0.5% of Yahoo's daily close for each symbol
- [ ] median volume ratio (bars / Yahoo daily) is close to 1; far from 1 means a unit or volume-delta problem
- [ ] no `seg-*.parquet` files left in `lake/bars/interval=1m/date=<day>/` (only `bars.parquet`)
- [ ] 3 spot checks of a one-minute bar (open, high, low, close) against a charting screen

### 14.6 Results to bring back
Paste or attach: the output of both `verify` commands; the first and last 15 lines of `logs/feed-test.log`; the `auth check` output (tokens are masked); `ls lake/ohlc`; `du -sh lake`; the `logs/frames/*.bin` files; the lines of the log containing `WARN` or `ahead of local clock`; and answers to: did the extended token work, how many instruments subscribed without error, any disconnects.

### 14.7 If something goes wrong
| Symptom | Likely cause | Action |
|---|---|---|
| `yf_client` returns 0 rows or "possibly delisted" | Yahoo throttling, network, or wrong ticker | Retry later; test one ticker by hand; fix the symbol in `universe.csv` |
| UK prices look 100x too big | `.L` prices are in pence | Expected; divide by 100 for pounds. See ADR-3 |
| `exchange` fails with HTTP 4xx | Redirect URI differs from the console, or the code expired or was reused | Make the redirect URI identical in `.env` and the console; redo `login-url` |
| `check` shows `rejected HTTP 401` for both tokens | Token expired or app not authorised for market data | Redo 14.3; if it persists, report the exact message |
| Connected, no frames all day | NSE holiday, subscribed keys wrong, or the instrument master uses different keys | Check the key warning in the log; confirm the ISINs in `universe.csv`; try `--mode ltpc` |
| Ticks flow but `verify bars` finds no rows | All ticks dropped as last-trade-yesterday or in the future | Look at the `stale=` and `future=` counters in the 30 s log lines; check the VPS clock |
| Feed killed, `seg-*` files remain | Crash or `kill -9` | `python -c "from datetime import date; from data_layer.tick_store import compact_day; print(compact_day('1m', date(2026,10,26)))"` |
| Volume ratio far from 1 | Cumulative-volume baseline or units | Report it; do not tune by hand |
| Disk fills | Raw ticks enabled | Do not use `--raw-ticks` on the test day |

### 14.8 After a pass
Tick the Gate 1 and Gate U1 boxes in sections 4 and 13.3, record the extended-token result, then: widen the universe (real UK and EU constituents, O11), schedule the daily OHLC cron (section 5), and enable the `jarvis-feed` service (section 13.2). Do not enable the daemon before the intraday checks above pass.
