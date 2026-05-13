# JARVIS

> *Just Another Real-time Valuation & Investment System*

---

> "Sir, I've simulated 14,000,605 market outcomes."
> "How many did we beat the benchmark?"
> "All of them—once you implement this library."

---

JARVIS is a high-performance Python library designed for quantitative researchers who want to move from "hunch" to "highly-optimized" in record time. It bridges the gap between raw historical OHLC data and actionable alpha generation.

---

## Project Structure

```
JARVIS/
├── data_layer/              # FMP data extraction + KDB+ preparation
│   ├── fmp_client.py        # API client for Nifty 50 fundamentals & OHLC
│   ├── kdb_loader.q         # KDB+ CSV ingestion into splayed/partitioned tables
│   └── schema.q             # Table definitions for 8 Pillars, OHLC & scoring
├── backtester/              # (Mockup) Event-driven backtesting engine
├── webapp/                  # (Mockup) Visualisation & Firestore Auth
├── analytics/               # (Mockup) Python signal processing
├── data/
│   ├── fundamentals/        # CSV output: income, balance, cash flow, ratios
│   └── ohlc/                # CSV output: daily OHLC partitioned by year
├── logs/
├── .env.example
└── requirements.txt
```

---

## Data Layer

The `data_layer` module pulls **5 years** of fundamental and OHLC data for the **Nifty 50** from [Financial Modeling Prep (FMP)](https://financialmodelingprep.com) and prepares it for a **kdb+ 32-bit** (4 GB RAM limit) database.

### What gets extracted

| Dataset | Fields | Pillar(s) |
|---|---|---|
| Income Statement | Revenue, Net Income, EBIT | 1, 2, 6 |
| Balance Sheet | Total Assets, Total Liabilities, Long-Term Debt, Total Equity | 4, 5, 6 |
| Cash Flow Statement | Free Cash Flow | 8 |
| Ratios | ROIC, Shares Outstanding | 3, 7 |
| OHLC | Open, High, Low, Close, Volume (daily) | — |

### 8 Pillar scoring thresholds

| # | Pillar | Threshold |
|---|---|---|
| 1 | Normalised P/E | < 22.5 (5-year avg earnings) |
| 2 | Price / Book | < 1.5 |
| 3 | Buybacks | Shares outstanding declining YoY |
| 4 | Leverage | Long-term debt / equity < 0.5 |
| 5 | Current Ratio | > 1.0 *(placeholder)* |
| 6 | Revenue Growth | Latest year > prior year |
| 7 | ROIC | > 12% |
| 8 | Normalised P/FCF | < 22.5 (5-year avg FCF) |

### Quick start

```bash
# 1. Configure
cp .env.example .env          # add your FMP_API_KEY

# 2. Install dependencies
pip install -r requirements.txt

# 3. Dry run — see the call plan without hitting the API
python -m data_layer.fmp_client --dry-run

# 4. Full extraction (all 50 tickers, ~250 API calls)
python -m data_layer.fmp_client

# 5. Ingest into KDB+
q data_layer/kdb_loader.q

# 6. Selective reload (OHLC only)
q data_layer/kdb_loader.q -load ohlc
```

### Rate limiting

| FMP Tier | Calls | `FMP_RATE_LIMIT_DELAY` |
|---|---|---|
| Free | ~250 / day | `0.5` s (default) |
| Starter | ~300 / min | `0.2` s |

Set `FMP_RATE_LIMIT_DELAY` in `.env` to match your tier.

### KDB+ memory model

Fundamentals are stored as **splayed on-disk tables** (lazy mmap) and OHLC as **year-partitioned tables** under `db/`. Only the partitions actually queried are mapped into RAM, keeping usage well within the 4 GB 32-bit engine limit. After each load, `memReport[]` prints current heap/mapped/physical usage.
