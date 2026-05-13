# JARVIS

> *Just Another Real-time Valuation & Investment System*

---

> "Sir, I've simulated 14,000,605 market outcomes."
> "How many did we beat the benchmark?"
> "All of them—once you implement this library."

---

JARVIS is a quantitative research platform that applies an 8-pillar fundamental scoring model to both **Indian (Nifty 50 / NSE)** and **US (S&P 500)** equities, storing everything in a **KDB+/q** time-series database optimised for the 32-bit (4 GB) engine.

---

## Project Structure

```
JARVIS/
├── data_layer/
│   ├── fmp_client.py     # FMP API client — annual fundamentals only
│   ├── yf_client.py      # Yahoo Finance client — daily OHLC (free, no key)
│   ├── schema.q          # KDB+ table definitions + 8-pillar scoring
│   ├── kdb_loader.q      # CSV → KDB+ ingestion (splayed + partitioned)
│   └── helpers.q         # KDB+ query helpers for analysis
├── data/
│   ├── universe.csv      # Stock universe with all identifiers (see below)
│   ├── fund_meta.csv     # Auto-generated: last fundamentals refresh per ticker
│   ├── fundamentals/     # FMP output CSVs (income, balance, cashflow, ratios)
│   └── ohlc/             # yfinance output CSVs (year-partitioned + daily)
├── db/                   # KDB+ on-disk tables (created by kdb_loader.q)
│   ├── fundamentals/     # Splayed: incomeStatement, balanceSheet, cashFlow,
│   │   │                 #          ratios, pillarMetrics, fundMeta
│   │   └── ...
│   └── YYYY/ohlc/        # Date-partitioned OHLC (one dir per year)
├── scheduler.py          # Cron-invokable orchestrator
├── logs/
├── .env.example
└── requirements.txt
```

---

## Stock Universe & Identifiers

The universe is defined in `data/universe.csv`. Each row carries all common identifier types:

| Column | Description | US example | India example |
|---|---|---|---|
| `symbol` | **Primary key** used throughout the codebase | `AAPL` | `RELIANCE.NS` |
| `market` | `US` or `IN` | `US` | `IN` |
| `exchange` | Listing exchange | `NASDAQ` | `NSE` |
| `isin` | 12-char global identifier | `US0378331005` | `INE002A01018` |
| `cusip` | 9-char US/Canada only | `037833100` | *(blank — not applicable)* |
| `ric` | Reuters code | `AAPL.O` (NASDAQ) / `JPM.N` (NYSE) | `RELI.NS` |
| `bbg_code` | Bloomberg ticker | `AAPL US Equity` | `RIL IN Equity` |
| `sector` | GICS sector | `Information Technology` | `Energy` |

**How symbols differ between markets:**
- **US stocks** use plain exchange tickers (`AAPL`, `MSFT`, `BRK-B`). These are the same across FMP and yfinance.
- **Indian stocks** use Yahoo Finance's `.NS` suffix for NSE-listed securities (`RELIANCE.NS`, `INFY.NS`). This is also the format FMP uses, and is equivalent to the Reuters RIC convention Yahoo adopted.
- Both are stored in the `symbol` column in KDB+. The `market` column (`\`IN` or `\`US`) distinguishes them.

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

Edit `data/universe.csv` to add your stocks. The 20 rows shipped with the repo (10 US, 10 India) are working examples.

### 3. Initial OHLC backfill (free, ~30 seconds for 250 stocks)

```bash
# Fetch 5 years of OHLC via Yahoo Finance — no API key needed
python -m data_layer.yf_client --mode full --market ALL
```

### 4. Initial fundamentals load (FMP — uses API quota)

```bash
# Dry run first to see the call plan
python -m data_layer.fmp_client --market ALL --dry-run

# Full run for all stocks in universe.csv
python -m data_layer.fmp_client --market ALL

# Or batch to stay within 250-call free tier (62 tickers × 4 calls = 248)
python -m data_layer.fmp_client --market ALL --slice 0 61
python -m data_layer.fmp_client --market ALL --slice 62 123
# ... etc.
```

### 5. Load into KDB+

```bash
q data_layer/kdb_loader.q                     # full load (all tables)
q data_layer/kdb_loader.q -load fundamentals  # fundamentals only
q data_layer/kdb_loader.q -load ohlc          # OHLC only
q data_layer/kdb_loader.q -mode daily         # append daily OHLC increment
```

### 6. Query with helpers

```q
\l data_layer/helpers.q

// Score all US stocks against latest price
topByScore[`US; 10]

// Compare top scorers across both markets
compareMarkets[5]

// Latest fundamentals for a ticker
getLatestFundamentals[`AAPL]
getLatestFundamentals[`RELIANCE.NS]

// Revenue growth history
revenueGrowth[`TCS.NS]

// Market summary
marketSummary[]
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

**Manual runs:**

```bash
python scheduler.py --job ohlc-daily                # OHLC only
python scheduler.py --job fundamentals              # fundamentals only
python scheduler.py --job all                       # both
python scheduler.py --job all --dry-run             # print plan only
python scheduler.py --job fundamentals --budget 200 # custom call budget
```

**Environment variables:**

| Variable | Default | Description |
|---|---|---|
| `FMP_API_KEY` | — | Required for fundamentals |
| `FMP_DAILY_BUDGET` | `248` | FMP calls to spend per scheduler run |
| `FMP_RATE_LIMIT_DELAY` | `0.5` | Seconds between FMP calls (free tier) |
| `Q_BINARY` | `q` | Path to the q executable |

---

## KDB+ Database Layout

```
db/
├── fundamentals/           ← splayed on-disk tables (lazy mmap)
│   ├── incomeStatement/
│   ├── balanceSheet/
│   ├── cashFlow/
│   ├── ratios/
│   ├── pillarMetrics/
│   └── fundMeta/           ← last fundamentals refresh date per ticker
└── YYYY/ohlc/              ← date-partitioned OHLC
    ├── 2021/ohlc/
    ├── 2022/ohlc/
    └── ...
```

All tables carry a `market` column (`\`IN` or `\`US`) so both universes coexist in the same tables. Fundamentals are keyed on `[date; symbol]`; `pillarMetrics` and `fundMeta` are keyed on `[symbol]`.

Only partitions that are actively queried are mmap'd into RAM, keeping heap usage well within the 4 GB 32-bit engine limit. Run `memReport[]` after loading to verify.

---

## FMP API tier guide

| Tier | Cost | Calls | Suitable for |
|---|---|---|---|
| Free | $0 | 250 / day | Up to ~250 stocks with weekly fundamentals rotation |
| Starter | $19 / month | 300 / min | Up to ~1,000 stocks with daily full refresh |

For a passion project at ≤ 250 stocks, the free tier is sufficient: OHLC runs free via yfinance, and fundamentals rotate across the universe over ~4 weekdays.
