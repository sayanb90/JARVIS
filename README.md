# JARVIS

> *Just Another Real-time Valuation & Investment System*

---

> "Sir, I've simulated 14,000,605 market outcomes."
> "How many did we beat the benchmark?"
> "All of them—once you implement this library."

---

JARVIS is a quantitative research platform that applies an 8-pillar fundamental scoring model to a **global universe** (India plus S&P 500, FTSE 100, STOXX 600, Nikkei 225, KOSPI 200, Taiwan 50, STI, CSI 300 and Hang Seng), keeps 5 years of daily prices and collects 1-minute bars for all of those markets, and ingests live tick / L1 data for India from the Upstox API, storing everything in a **Parquet lake queried with DuckDB** (no database server, no memory ceiling, free).

---

## Project Structure

```
JARVIS/
├── data_layer/
│   ├── yf_fundamentals.py  # Yahoo Finance fundamentals (yfinance): statements, profile, dividends, splits; classified failures, versioned values
│   ├── yf_pillars.py     # pillar inputs: ROIC, debt, FCF, shares in the price's currency
│   ├── yf_completeness.py  # four-year completeness of every symbol; drives the daily backfill
│   ├── fx.py             # daily FX rates from Yahoo
│   ├── fmp_client.py     # retired FMP client (unscheduled; still provides read_universe)
│   ├── yf_client.py      # Yahoo Finance client — daily OHLC (free, no key)
│   ├── lake.py           # CSV staging files → Parquet lake (idempotent upserts)
│   └── query.py          # DuckDB query helpers + 8-pillar scoring
├── tests/                # pytest: loader + query + scoring (no network)
├── data/
│   ├── universe.csv      # Stock universe with all identifiers (see below)
│   ├── fund_meta.csv     # Auto-generated: last fundamentals refresh per ticker
│   ├── fundamentals/     # retired FMP staging CSVs (no longer written)
│   └── ohlc/             # yfinance output CSVs (year-partitioned + daily)
├── lake/                 # Parquet lake (created by lake.py, git-ignored)
│   ├── fundamentals/     # one file per table, both markets
│   ├── ohlc/year=YYYY/   # daily bars
│   └── bars/interval=5m/date=YYYY-MM-DD/   # intraday bars (UTC day)
├── scheduler.py          # Cron-invokable orchestrator
├── logs/
├── .env.example
└── requirements.txt
```

---

## Stock Universe & Identifiers

The universe is defined in `data/universe.csv`. Each row carries all common identifier types:

| Column | Description | Example |
|---|---|---|
| `symbol` | **Primary key** used throughout the codebase (Yahoo `.NS` style) | `RELIANCE.NS` |
| `market` | `IN`, `US`, `UK` or `EU` | `IN` |
| `currency` | Quote currency (`INR`, `USD`, `GBp` = pence, `EUR`) | `INR` |
| `exchange` | Listing exchange | `NSE` |
| `isin` | 12-char identifier; also the basis of Upstox equity instrument keys (`NSE_EQ\|<ISIN>`) | `INE002A01018` |
| `ric` | Reuters code | `RELI.NS` |
| `bbg_code` | Bloomberg ticker | `RIL IN Equity` |
| `sector` | Sector | `Energy` |

`cusip` is blank for Indian securities. Symbols use Yahoo's `.NS` suffix, which FMP also accepts.

To expand the universe, add rows to `data/universe.csv`. The scheduler will automatically pick up new stocks and prioritise them (they have no `last_updated` date and sort to the top of the refresh queue).

---

## Data Layer

### Data sources

| Source | Data | Cost | API key |
|---|---|---|---|
| **Yahoo Finance** (`yfinance`, an open-source library that reads Yahoo's own endpoints; not an official API) | Daily and 1-minute prices for every market but India, annual fundamentals, dividends, splits, FX rates | Free | None |
| **Upstox** | India: live ticks, option Greeks, company fundamentals (statements, key ratios) | Plus plan | Required |

FMP was used for fundamentals until 2026-11-11 (decision record SCRUM-138): its free plan covers US exchanges only and its global tier is expensive for the whole universe, so it is retired. Yahoo is unofficial and can change without notice, so every call is classified (ok, empty, throttled, exception, parse) and a completeness report names what to fix.

### What gets fetched

| Dataset | Source | Frequency | Fields |
|---|---|---|---|
| Income statement | Yahoo, last 4 annual statements | A daily slice, each stock about monthly | revenue, net income, EBIT, tax rate, share counts |
| Balance sheet | Yahoo | same | total debt, long-term debt, invested capital, equity, shares, cash |
| Cash flow | Yahoo | same | operating cash flow, capex, free cash flow |
| Profile | Yahoo | same | price currency, reporting currency, market cap, sector |
| Dividends and splits | Yahoo | same | full history |
| FX rates | Yahoo | Daily | 18 currencies against the dollar, 5 years |
| OHLC | yfinance | Daily | open, high, low, close, volume |
| 1-minute bars | yfinance | Daily, built forward (Yahoo keeps 7 days) | every market but India |

### 8 Pillar scoring model

Scores each ticker 0-8 (0-7 for banks and insurers, see below). Inputs come from the last four annual Yahoo statements (`python -m data_layer.yf_pillars build` writes them into the lake tables `Lake.score_ticker` reads). The multi-year pillars need at least 3 statements; with fewer they are not scored. The tables keep every fiscal year ever stored, so the window grows past four years as Yahoo adds one each year.

| # | Pillar | Metric | Threshold | Inputs (Yahoo lines, with fallbacks) |
|---|---|---|---|---|
| 1 | Normalised P/E | Market cap / average net income over the window | < 22.5 | `NetIncome`; market cap = latest close x latest share count |
| 2 | ROIC | NOPAT / invested capital (average of this and last year) | > 9% | NOPAT = `EBIT` (or `OperatingIncome`, or pretax income plus interest) x (1 - `TaxRateForCalcs`); `InvestedCapital` (or debt + equity) |
| 3 | Revenue Growth | Latest revenue vs the oldest year in the window | Positive trend | `TotalRevenue` |
| 4 | Net Income Growth | Latest NI vs the oldest year | Positive trend | `NetIncome` |
| 5 | Buybacks | Share count over the window | Declining | `OrdinarySharesNumber` (or `ShareIssued`, `DilutedAverageShares`) |
| 6 | Debt serviceability | Long-term debt / average FCF | < 5x | `LongTermDebt`, else `LongTermDebtAndCapitalLeaseObligation`, else `TotalDebt - CurrentDebt`, else `TotalDebt` |
| 7 | FCF Growth | Latest FCF vs the oldest year | Positive trend | `FreeCashFlow` (or operating cash flow minus capex) |
| 8 | Normalised P/FCF | Market cap / average FCF | < 20 | price x shares, `FreeCashFlow` |

Currency: statements are in the reporting currency and prices in the listing currency (pence for London). Earnings, cash flow and debt are converted into the price's own unit with the latest stored FX rate before pillars 1, 6 and 8 divide by them; a missing rate leaves them empty (`fx_missing`), never a guess. ROIC is not applicable to banks, insurers and other financial-sector companies: their score is out of 7 (`max_score` in `Lake.score_all`). Each stock's `pillar_metrics` row carries `periods_used`, the currencies, the FX rate used and a `flags` text naming every fallback used.

---

## Quick Start

### 1. Configure

```bash
cp .env.example .env      # Upstox settings for India; Yahoo needs no key
pip install -r requirements.txt
```

### 2. Populate the universe

Edit `data/universe.csv` to add your stocks. The 10 NSE rows shipped with the repo are working examples.

### 3. Initial OHLC backfill (free, ~30 seconds for 250 stocks)

```bash
# Fetch 5 years of OHLC via Yahoo Finance — no API key needed
python -m data_layer.yf_client --mode full --market ALL    # India, US, UK, EU (daily, 5 years)
python -m data_layer.yf_client --mode full --market US --years 20     # deeper history, one market at a time (or --years max)
```

### 4. Initial fundamentals and FX load (Yahoo, free)

```bash
python -m data_layer.index_universe fetch && python -m data_layer.index_universe build --validate   # the global universe
python -m data_layer.fx refresh                      # 5 years of daily FX rates
python -m data_layer.yf_fundamentals run --limit 100 # a first slice; the cron job continues with the rest
python -m data_layer.yf_pillars build                # pillar inputs for the scorer
python -m data_layer.yf_fundamentals check           # four-year completeness of every symbol
```

### 4b. One command for all of the history above

`bash deploy/historical-run.sh` (a dry run that prints every stage) and `bash deploy/historical-run.sh --apply` run the whole historical load in order and can be repeated: global universe, daily candles (20 years), Yahoo 1-minute bars for every market but India (the last 29 days: all Yahoo serves, in windows of 7 days), FX, fundamentals, then from Upstox the 1-minute candles of the 500 stocks and the indices since January 2022 and the expired NIFTY and BANKNIFTY options and futures (`python -m data_layer.upstox.backfill`, needs Upstox Plus for the expired contracts). Start it after 10:30 UTC; it stops long stages at 21:30 UTC, marks them partial and continues the next day. `bash deploy/historical-run.sh status` shows where it is.

### 5. Load into the lake

```bash
python -m data_layer.lake --load all            # fundamentals + full-history OHLC
python -m data_layer.lake --load fundamentals   # upsert fundamentals, pillar metrics, fund_meta
python -m data_layer.lake --load ohlc           # full-history year files
python -m data_layer.lake --load daily          # incremental daily OHLC
python -m data_layer.lake --load bars --since 2026-10-20
```

Loads are upserts keyed on (symbol, date) or (symbol, ts), so re-running is safe and fundamentals accumulate across the rotating refresh.

### 6. Query

```python
from data_layer.query import Lake
lake = Lake()

lake.top_by_score("IN", 10)                    # 8-pillar screen vs latest close
lake.score_all()                               # whole universe
lake.ohlc("INFY.NS", "2024-01-01", "2024-12-31")
lake.bars("RELIANCE.NS", "2026-10-23", "2026-10-24")  # 5m bars, UTC
lake.sql("SELECT market, count(*) FROM ohlc GROUP BY 1")   # ad-hoc SQL
```
---

## Scheduler (daily automation)

`scheduler.py` runs the price jobs; the fundamentals and FX jobs are their own modules. The cron template is `deploy/crontab` (every job in it must be installed: `python -m data_layer.cron_check`).

```bash
# Daily OHLC for every market, after the 22:00 UTC 1-minute bars job (both take the scheduler lock)
0 23 * * 1-5  cd $JARVIS && python scheduler.py --job ohlc-daily >> logs/scheduler.log 2>&1
# Daily FX rates
30 0 * * *    cd $JARVIS && python -m data_layer.fx refresh >> logs/fx.log 2>&1
# Yahoo fundamentals: a daily slice, with a catch-up cap for missed days
0 1 * * *     cd $JARVIS && python -m data_layer.yf_fundamentals run --limit 150 --catch-up 400 >> logs/yf-fundamentals.log 2>&1
# Then the pillar inputs and the four-year completeness check
0 2 * * *     cd $JARVIS && python -m data_layer.yf_pillars build >> logs/yf-pillars.log 2>&1 && python -m data_layer.yf_fundamentals check >> logs/yf-completeness.log 2>&1
```

**How the fundamentals job backfills.** What is due is decided from what is stored, not from the date of the last run, in this order: symbols never fetched; symbols whose last fetch failed (retried after 20 hours); symbols fetched but incomplete (a missing year or item in the latest four years; retried every 7 days, because Yahoo fills gaps slowly and a young company stays short); then the oldest complete fetches past 30 days. A day the job did not run is therefore caught up by the next one: when more symbols are overdue than `--limit`, it takes up to `--catch-up`. The check (`yf_completeness`) gives each symbol a status (complete, short, incomplete, gap, stale, fetch_failed, pending) and writes `lake/yf_fundamentals/completeness.parquet`; the health report shows the totals.

**Interim Yahoo intraday bars** (5-minute, a stopgap cross-check only; the intraday base is the 1-second Upstox feed below):

```bash
python scheduler.py --job bars --market IN --backfill   # first run: deepest Yahoo window (about 59d of 5m)
python scheduler.py --job bars --market IN              # daily; India only, upserts into lake/bars/
```

**Live tick / L1 (Upstox API v3):** `data_layer/upstox/` connects to the v3 WebSocket, decodes the Protobuf frames, aggregates ticks into **1-second bars** (OHLC, volume, VWAP, tick count, last bid/ask) and stores them in `lake/bars/interval=1s/`. Coarser bars are derived on demand: `Lake().rollup(60)` gives 1-minute bars, `rollup(300)` 5-minute, aligned to the 09:15 IST open.

```bash
# one-time: create an Upstox app (developer console), put API key/secret/redirect URI in .env
python -m data_layer.upstox.auth login-url             # log in on your phone, then:
python -m data_layer.upstox.auth exchange "<redirected URL>"
python -m data_layer.upstox.auth check                 # does the feed accept the long-lived extended token?
python -m data_layer.upstox.feed --once --limit 20     # one session now, 20 symbols
python -m data_layer.upstox.feed                       # daemon (waits for each NSE session)
```

Missed the session? Pull a whole past or present day from the candle API: `python -m data_layer.upstox.candles --date 2026-10-26` (fills gaps only).

**Futures and options:** the same pipeline handles NSE_FO contracts, with open interest stored on each bar.

```bash
python -m data_layer.upstox.fo_instruments import --download     # contract table (keeps expired contracts)
python -m data_layer.upstox.fo_instruments select --underlying NIFTY RELIANCE --futures 2 --option-expiries 1 --strikes 5 --spot NIFTY=24500
python -m data_layer.upstox.feed --once --segment fo --limit 20  # live, 1-second bars + open interest
python -m data_layer.verify fo --date 2026-10-30
```

Tested against a local fake server; not yet run against Upstox itself (runbook sections 13 and 17, Gate U1).

**Admin:** set `ADMIN_USER` and `ADMIN_PASSWORD` to enable `/admin`, a password-protected data browser (tables grouped by service, paged), a read-only SQL console and the subscription list with ticker upload (runbook section 18).

Go-live steps, daily ops, backups and the futures/options section are in [the JARVIS Runbook in Notion](https://app.notion.com/p/40a23f85984081c4bf1eff52ac4bfcd5).

**Manual runs:**

```bash
python scheduler.py --job ohlc-daily                # daily OHLC, all markets
python scheduler.py --job fundamentals              # the retired FMP rotation (unscheduled)
python scheduler.py --job all                       # ohlc + fundamentals
python scheduler.py --job all --dry-run             # print plan only
python scheduler.py --job fundamentals --budget 200 # custom call budget
```

**Environment variables:**

| Variable | Default | Description |
|---|---|---|
| `JARVIS_YF_FUND_PAUSE_S` | `1.0` | Seconds between two symbols of the Yahoo fundamentals job |
| `JARVIS_YF_CHUNK_PAUSE_S` | `2` | Seconds between two Yahoo price downloads |
| `JARVIS_LAKE` | `<root>/lake` | Parquet lake directory |

---

## Lake Layout

```
lake/
├── fundamentals/           ← income_statement, balance_sheet, cash_flow,
│                              ratios, pillar_metrics (.parquet; written by yf_pillars)
├── yf_fundamentals/        ← versions, profile, actions, fetch_log, completeness (.parquet), raw/
├── fx/rates.parquet        ← daily FX rates (usd per unit)
├── ohlc/year=YYYY/ohlc.parquet
└── bars/interval=5m/date=YYYY-MM-DD/bars.parquet
```

All tables carry a `market` column (`IN`, `US`, `UK`, `EU`, `JP`, `KR`, `TW`, `SG`, `CN`, `HK`). Keys: fundamentals (symbol, date); pillar_metrics and fund_meta (symbol); ohlc (symbol, date); bars (symbol, ts, UTC). Files are ZSTD-compressed and replaced atomically. DuckDB reads only the columns and partitions a query touches, so memory is bounded by the query, not the dataset. Storage decision record: ADR-1 in the JARVIS Runbook (Notion).

---

## Tests

```bash
pip install -r requirements-dev.txt
python -m pytest --cov            # unit tests + Gherkin scenarios, no network or credentials needed
python -m pytest -m bdd           # only the Gherkin scenarios in tests/features
```

About 220 tests, about 98% coverage; CI fails below 90%. Layout, scenario list and the limits of what they prove are in section 15 of [the JARVIS Runbook in Notion](https://app.notion.com/p/40a23f85984081c4bf1eff52ac4bfcd5).

---

## Setting up the server

Section 0 of [the JARVIS Runbook in Notion](https://app.notion.com/p/40a23f85984081c4bf1eff52ac4bfcd5) takes you from a blank VPS to data flowing: hardening, install, `.env`, building the stock universe (`python -m data_layer.universe import-nse <NSE csv>`), the test day, going live with the files in `deploy/` (systemd services, cron, log rotation), backups (`python -m data_layer.backup`), and the daily routine (the morning token refresh is skippable once the Analytics Token is adopted, section 13.2). `python -m data_layer.health` is the two-minute daily check.

---

## Intraday viewer

A read-only web page over the lake: pick a date and an instrument and see price, volume, the best bid and ask with sizes, the spread and rolling volatility on one linked chart, plus a market-wide overview that ranks instruments.

```bash
python -m webapp.demo                         # synthetic data (clearly marked), to try it before real data exists
python -m webapp --lake lake_demo             # open http://127.0.0.1:8000
python -m webapp                              # on the real lake
```

Set `WEBAPP_USER` and `WEBAPP_PASSWORD` to require a login, and put a TLS reverse proxy in front for phone access. Details, definitions and limits: section 16 of [the JARVIS Runbook in Notion](https://app.notion.com/p/40a23f85984081c4bf1eff52ac4bfcd5).
