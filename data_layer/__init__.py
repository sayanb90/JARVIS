"""JARVIS data extraction layer.

  yf_client        — Yahoo Finance, daily OHLC + intraday bars (free, no API key)
  yf_fundamentals  — Yahoo Finance statements, profile, dividends and splits; yf_pillars builds the pillar tables
Storage/query: lake (CSV -> Parquet upsert) and query (DuckDB helpers + scoring).
"""

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
