# JARVIS Go-Live Runbook

Living document. Update the **Status** column and tick the gates as each item is
proven *on the VPS*, not on a dev box. Last reviewed: 2026-10-25.

Legend: `[x]` done and verified · `[~]` built, **not yet verified in prod** · `[ ]` not started

---

## 1. Scope and principles

- Markets: US (S&P 500 subset) and India (NSE). Universe lives in `data/universe.csv` (single source of truth for every feed).
- **Aggregated bars, not raw ticks.** Default 5-minute regular-session bars. Strategies are not sub-second; raw tick storage is out of scope until a strategy proves it needs it.
- Free sources first (FMP free tier for fundamentals, Yahoo Finance for prices). Paid upgrades are gated on a concrete limitation, listed in section 9.
- Storage: Parquet lake queried with DuckDB (ADR-1, section 12). Partitioned by year (daily) and by day (bars); no server process, no heap ceiling.
- Prod runs on the VPS. Dev/cloud sandboxes can build and run the test suite; collectors run on the VPS.

## 2. Feed inventory

| Feed | Source | Grain | Cadence | History | Store | Status |
|---|---|---|---|---|---|---|
| Fundamentals (income, balance, cash flow, ROIC/shares) | FMP free (250 calls/day) | Annual | Rotating by staleness, ~62 tickers/day | Last 5 fiscal years | `lake/fundamentals/*.parquet` | `[~]` |
| Daily OHLC | Yahoo (`yfinance`) | 1 day | Every trading day after close | 5 years backfill | `lake/ohlc/year=YYYY/` | `[~]` |
| **Intraday bars (new)** | Yahoo (`yfinance`) | 5 min (1m/15m/30m/60m selectable) | Once per market, after close | **Forward-only** (see 2.1) | `lake/bars/interval=5m/date=YYYY-MM-DD/` | `[~]` |
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

Fits comfortably. Parquet files are already ZSTD-compressed (typically several times smaller than the figures above). Keep a retention policy for the per-day CSVs (section 7).

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

1. [ ] `python scheduler.py --job bars --market ALL --dry-run` shows the expected ticker counts
2. [ ] Run once for real: `python scheduler.py --job bars --market IN` then `--market US`
3. [ ] Confirm CSVs exist under `data/bars/5m/{in,us}/YYYY-MM-DD.csv`
4. [ ] Confirm the load worked: `python -c "from data_layer.query import Lake; print(Lake().sql('SELECT market, count(*) n FROM bars_5m GROUP BY 1'))"`
5. [ ] Daily OHLC 5-year backfill: `python -m data_layer.yf_client --mode full --market ALL`, then `python -m data_layer.lake --load ohlc`
6. [ ] Fundamentals first pass: `python scheduler.py --job fundamentals` daily until `fund_meta.csv` covers the universe (about 4 days for 250 stocks)
7. [ ] Install cron (section 5)
8. [ ] Spot-check 3 US and 3 India tickers against a second source (close price and volume, one day each)

**Gate 1 (data is trustworthy):**
- [ ] Bars: every universe ticker has bars on the last trading day; bar count per ticker is about 75–78 for a full session
- [ ] No duplicate `(ts, symbol)` rows after running the same job twice
- [ ] First bar of the day is 03:45 UTC for NSE and 13:30 or 14:30 UTC for NYSE (DST), last bar is complete
- [ ] 5 consecutive trading days of unattended cron runs with no failures in `logs/scheduler.log`
- [ ] Scoring: `topByScore[`US;10]` and `topByScore[`IN;10]` return sensible results against a known-good ticker

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
# Daily OHLC (both markets, after US close)
30 22 * * 1-5  cd /home/jarvis/JARVIS && python scheduler.py --job ohlc-daily   >> logs/scheduler.log 2>&1
# Intraday bars, once per market after its close
20 10 * * 1-5  cd /home/jarvis/JARVIS && python scheduler.py --job bars --market IN >> logs/scheduler.log 2>&1
20 21 * * 1-5  cd /home/jarvis/JARVIS && python scheduler.py --job bars --market US >> logs/scheduler.log 2>&1
# Fundamentals rotation
0 23 * * 1-5   cd /home/jarvis/JARVIS && python scheduler.py --job fundamentals  >> logs/scheduler.log 2>&1
```

Notes:
- NSE closes 10:00 UTC. NYSE closes 20:00 UTC (summer) or 21:00 UTC (winter); the 21:20 run covers both.
- Exchange holidays differ between the two markets. A job on a holiday should fetch nothing and exit cleanly; confirm this during Gate 1.
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
1. [ ] **Underlyings:** for example Nifty 50 and Bank Nifty on NSE; S&P 500 index options and index futures in the US. Pick the first one or two only
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
| India | Broker APIs | Zerodha Kite Connect, Upstox, Angel One SmartAPI | Intraday history depth, rate limits, cost, whether expired contracts are available |
| India | Data vendors | TrueData, Global Datafeeds | Historical F&O depth, licence for personal use, price |
| US | Data vendors | Databento, Polygon, CBOE DataShop | Index options coverage, history depth, price per GB or per month |
| US | Broker API | Interactive Brokers | Historical data limits, market-data subscriptions required |

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
| 2026-10-25 | Runbook created; intraday bars collector, `bars5m` schema and loader added | Not yet run on the VPS |
| 2026-10-25 | (superseded by ADR-1) Fixed `kdb_loader.q` CLI parsing (`.Q.opt` was missing, so `-mode`/`-load` flags were ignored and every run did a full reload) | Affected scheduler-triggered loads |
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
| 2026-10-25 | Replaced KDB+ with Parquet + DuckDB (`lake.py`, `query.py`, tests); removed `schema.q`, `kdb_loader.q`, `helpers.q`. Pillar scorer ported to Python with explicit date ordering for the growth/decrease pillars | ADR-1 |

## 12. Architecture decisions

### ADR-1: Parquet lake + DuckDB instead of KDB+ (2026-10-25)
- **Context:** the project had no KDB+ subscription, so only the free 32-bit build was usable. Its 4 GB address-space limit also counts memory-mapped columns, which caps query size, and an in-VPS webapp would need a q process plus an IPC bridge from Python.
- **Decision:** store data as partitioned Parquet (ZSTD) and query it with embedded DuckDB. Collectors still write CSV staging files; `data_layer/lake.py` upserts them, `data_layer/query.py` reads.
- **Consequences:** free, no server, no heap ceiling, SQL as-of joins and window functions available to the backtester, plain Python on the VPS. We give up kdb's native tick-plant and in-memory tables.
- **Hot layer:** with aggregated bars (not ticks) today's data for 1,000 symbols at 5 minutes is a few MB, so the current day stays as a pandas/Polars frame in the collector or webapp process. Revisit if the F&O chain feed (section 9) outgrows one box: ClickHouse is the next step, not a return to KDB+.
- **Reversible:** CSV staging files remain the rebuild source for every table.
