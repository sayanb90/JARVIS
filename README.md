# JARVIS

> *Just Another Real-time Valuation & Investment System*

---

> "Sir, I've simulated 14,000,605 market outcomes."
> "How many did we beat the benchmark?"
> "All of them—once you implement this library."

---

JARVIS is a quantitative research platform that applies an 8-pillar fundamental scoring model to **NSE-listed Indian equities**, keeps 5 years of daily prices for **India, US, UK and EU**, and ingests live tick / L1 data for India from the Upstox API, storing everything in a **Parquet lake queried with DuckDB** (no database server, no memory ceiling, free).

---

## Project Structure

```
JARVIS/
├── data_layer/
│   ├── fmp_client.py     # FMP API client — annual fundamentals only
│   ├── yf_client.py      # Yahoo Finance client — daily OHLC (free, no key)
│   ├── lake.py           # CSV staging files → Parquet lake (idempotent upserts)
│   └── query.py          # DuckDB query helpers + 8-pillar scoring
├── tests/                # pytest: loader + query + scoring (no network)
├── data/
│   ├── universe.csv      # Stock universe with all identifiers (see below)
│   ├── fund_meta.csv     # Auto-generated: last fundamentals refresh per ticker
│   ├── fundamentals/     # FMP output CSVs (income, balance, cashflow, ratios)
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

### Two data sources

| Source | Data | Cost | API key |
|---|---|---|---|
| **FMP** (Financial Modeling Prep) | Annual fundamentals | Free tier: 250 calls/day | Required |
| **Yahoo Finance** (`yfinance`) | Daily OHLC | Free, unlimited | None |

### What gets fetched

| Dataset | Source | Frequency | Fields |
|---|---|---|---|
| Income Statement | FMP `/income-statement` | Monthly refresh | revenue, netIncome, ebit |
| Balance Sheet | FMP `/balance-sheet-statement` | Monthly refresh | totalAssets, totalLiabilities, longTermDebt, totalEquity |
| Cash Flow | FMP `/cash-flow-statement` | Monthly refresh | freeCashFlow |
| Ratios | FMP `/ratios` | Monthly refresh | roic (true ROIC), sharesOutstanding |
| OHLC | yfinance | Daily | open, high, low, close, volume |

### 8 Pillar scoring model

Scores each ticker 0–8. All data is sourced and verified against FMP.

| # | Pillar | Metric | Threshold | Data source |
|---|---|---|---|---|
| 1 | Normalised P/E | Market cap / 5-yr avg net income | < 22.5 | `pillarMetrics.avgNetIncome5y` |
| 2 | ROIC | Return on Invested Capital | > 9% | `ratios.roic` (true ROIC via FMP) |
| 3 | Revenue Growth | Latest revenue vs 5 years ago | Positive trend | `incomeStatement.revenue` |
| 4 | Net Income Growth | Latest NI vs 5 years ago | Positive trend | `incomeStatement.netIncome` |
| 5 | Buybacks | Shares outstanding over period | Declining | `ratios.sharesOutstanding` |
| 6 | Debt serviceability | Long-term debt / 5-yr avg FCF | < 5× | `pillarMetrics.latestLongTermDebt / avgFcf5y` |
| 7 | FCF Growth | Latest FCF vs 5 years ago | Positive trend | `cashFlow.freeCashFlow` |
| 8 | Normalised P/FCF | Market cap / 5-yr avg FCF | < 20 | `pillarMetrics.avgFcf5y` |

---

## Quick Start

### 1. Configure

```bash
cp .env.example .env      # add FMP_API_KEY
pip install -r requirements.txt
```

### 2. Populate the universe

Edit `data/universe.csv` to add your stocks. The 10 NSE rows shipped with the repo are working examples.

### 3. Initial OHLC backfill (free, ~30 seconds for 250 stocks)

```bash
# Fetch 5 years of OHLC via Yahoo Finance — no API key needed
python -m data_layer.yf_client --mode full --market ALL    # India, US, UK, EU (daily, 5 years)
```

### 4. Initial fundamentals load (FMP — uses API quota)

```bash
# Dry run first to see the call plan
python -m data_layer.fmp_client --market IN --dry-run

# Full run for all stocks in universe.csv
python -m data_layer.fmp_client --market IN

# Or batch to stay within 250-call free tier (62 tickers × 4 calls = 248)
python -m data_layer.fmp_client --market IN --slice 0 61
python -m data_layer.fmp_client --market IN --slice 62 123
# ... etc.
```

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

`scheduler.py` is designed to be invoked by cron. It reads `data/universe.csv` and `data/fund_meta.csv` to automatically prioritise the stocks with the oldest fundamentals refresh date.

**Add to crontab** (`crontab -e`):

```bash
# Daily OHLC — 6:30 PM weekdays (~10 seconds, 0 API calls)
30 18 * * 1-5 cd /home/user/JARVIS && python scheduler.py --job ohlc-daily >> logs/scheduler.log 2>&1

# Fundamentals rotation — 7:00 PM weekdays (auto-picks 62 stalest stocks)
0 19 * * 1-5 cd /home/user/JARVIS && python scheduler.py --job fundamentals >> logs/scheduler.log 2>&1
```

**How fundamentals rotation works** (250 stocks, free tier):

| Day | Stocks fetched | FMP calls used |
|---|---|---|
| Mon | 62 oldest (never-fetched first) | 248 |
| Tue | next 62 by staleness | 248 |
| Wed | next 62 | 248 |
| Thu | remaining ~64 | 256 |
| Following Mon | back to whoever is oldest | — |

Each stock gets a full refresh roughly once a week.

**Intraday bars** (aggregated 5-minute bars, collected forward after each market close):

```bash
python scheduler.py --job bars --market IN --backfill   # first run: deepest Yahoo window (about 59d of 5m)
python scheduler.py --job bars --market IN              # daily; India only, upserts into lake/bars/
```

**Live tick / L1 (Upstox API v3):** `data_layer/upstox/` connects to the v3 WebSocket, decodes the Protobuf frames, aggregates ticks into 1-minute bars (with last bid/ask, VWAP, tick count) and stores them in `lake/bars/interval=1m/`.

```bash
# one-time: create an Upstox app (developer console), put API key/secret/redirect URI in .env
python -m data_layer.upstox.auth login-url             # log in on your phone, then:
python -m data_layer.upstox.auth exchange "<redirected URL>"
python -m data_layer.upstox.auth check                 # does the feed accept the long-lived extended token?
python -m data_layer.upstox.feed --once --limit 20     # one session now, 20 symbols
python -m data_layer.upstox.feed                       # daemon (waits for each NSE session)
```

Tested against a local fake server; not yet run against Upstox itself (runbook section 13, Gate U1).

Go-live steps, daily ops, backups and the futures/options planning entry are in [`docs/RUNBOOK.md`](docs/RUNBOOK.md).

**Manual runs:**

```bash
python scheduler.py --job ohlc-daily                # daily OHLC, all four markets
python scheduler.py --job fundamentals              # fundamentals only
python scheduler.py --job all                       # ohlc + fundamentals
python scheduler.py --job all --dry-run             # print plan only
python scheduler.py --job fundamentals --budget 200 # custom call budget
```

**Environment variables:**

| Variable | Default | Description |
|---|---|---|
| `FMP_API_KEY` | — | Required for fundamentals |
| `FMP_DAILY_BUDGET` | `248` | FMP calls to spend per scheduler run |
| `FMP_RATE_LIMIT_DELAY` | `0.5` | Seconds between FMP calls (free tier) |
| `JARVIS_LAKE` | `<root>/lake` | Parquet lake directory |

---

## Lake Layout

```
lake/
├── fundamentals/           ← income_statement, balance_sheet, cash_flow,
│                              ratios, pillar_metrics, fund_meta (.parquet)
├── ohlc/year=YYYY/ohlc.parquet
└── bars/interval=5m/date=YYYY-MM-DD/bars.parquet
```

All tables carry a `market` column (`IN`, `US`, `UK`, `EU` for daily OHLC; India only for bars and fundamentals). Keys: fundamentals (symbol, date); pillar_metrics and fund_meta (symbol); ohlc (symbol, date); bars (symbol, ts, UTC). Files are ZSTD-compressed and replaced atomically. DuckDB reads only the columns and partitions a query touches, so memory is bounded by the query, not the dataset. Storage decision record: ADR-1 in `docs/RUNBOOK.md`.

---

## Tests

```bash
pip install -r requirements-dev.txt
python -m pytest --cov            # unit tests + Gherkin scenarios, no network or credentials needed
python -m pytest -m bdd           # only the Gherkin scenarios in tests/features
```

About 220 tests, about 98% coverage; CI fails below 90%. Layout, scenario list and the limits of what they prove are in section 15 of [`docs/RUNBOOK.md`](docs/RUNBOOK.md).

