"""JARVIS data extraction layer.

Two sub-clients:
  fmp_client  — FMP API, annual fundamentals (income, balance, cashflow, ratios)
  yf_client   — Yahoo Finance, daily OHLC + intraday bars (free, no API key)
Storage/query: lake (CSV -> Parquet upsert) and query (DuckDB helpers + scoring).
"""

from .fmp_client import (
    NIFTY_50,
    FMPExtractor,
    compute_pillar_metrics,
    write_fundamentals,
    write_pillar_metrics,
    run,
)
from .yf_client import (
    BarRow,
    OHLCRow,
    fetch_bars,
    fetch_ohlc_full,
    fetch_ohlc_daily,
    write_ohlc,
    write_bars,
    write_ohlc_daily,
    run_bars,
    run_ohlc,
    run_ohlc_daily,
)

__all__ = [
    # FMP — fundamentals
    "NIFTY_50",
    "FMPExtractor",
    "compute_pillar_metrics",
    "write_fundamentals",
    "write_pillar_metrics",
    "run",
    # yfinance — OHLC
    "OHLCRow",
    "BarRow",
    "fetch_bars",
    "write_bars",
    "run_bars",
    "fetch_ohlc_full",
    "fetch_ohlc_daily",
    "write_ohlc",
    "write_ohlc_daily",
    "run_ohlc",
    "run_ohlc_daily",
]
