"""JARVIS data extraction layer.

Two sub-clients:
  fmp_client  — FMP API, annual fundamentals (income, balance, cashflow, ratios)
  yf_client   — Yahoo Finance, daily OHLC (free, no API key)
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
    OHLCRow,
    fetch_ohlc_full,
    fetch_ohlc_daily,
    write_ohlc,
    write_ohlc_daily,
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
    "fetch_ohlc_full",
    "fetch_ohlc_daily",
    "write_ohlc",
    "write_ohlc_daily",
    "run_ohlc",
    "run_ohlc_daily",
]
