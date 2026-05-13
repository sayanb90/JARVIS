"""
Yahoo Finance OHLC data layer for JARVIS.

Fetches daily price data for both US (S&P 500) and Indian (Nifty 50) stocks
using the yfinance library — completely free, no API key required.

Ticker formats (same as the rest of the codebase):
  US    : plain exchange ticker   (e.g. AAPL, MSFT)
  India : Yahoo .NS suffix        (e.g. RELIANCE.NS, INFY.NS)

Two modes:
  full   — 5-year historical OHLC, written as year-partitioned CSVs
            (data/ohlc/ohlc_YYYY_in.csv, data/ohlc/ohlc_YYYY_us.csv)
            Run once for initial setup, then weekly/monthly for backfill.

  daily  — Last N calendar days, written as a flat incremental CSV
            (data/ohlc/ohlc_daily_in.csv, data/ohlc/ohlc_daily_us.csv)
            Run every trading day; kdb_loader.q -mode daily appends to DB.

Usage:
    python -m data_layer.yf_client --mode full  --market ALL
    python -m data_layer.yf_client --mode daily --market ALL
    python -m data_layer.yf_client --mode daily --market US --days-back 3
    python -m data_layer.yf_client --dry-run
"""

from __future__ import annotations

import csv
import logging
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Iterator, Optional

import pandas as pd
import yfinance as yf

from .fmp_client import NIFTY_50, _SP500_FALLBACK, log as _fmp_log

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
YEARS_BACK       = 5
DAILY_OHLC_DAYS  = 7          # calendar days back for incremental pulls
                               # (7 covers Mon after a long weekend + any gaps)
CHUNK_SIZE       = 100         # tickers per yfinance batch call

DATA_DIR = Path("data")
OHLC_DIR = DATA_DIR / "ohlc"

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Domain dataclass (mirrors schema.q ohlc table)
# ---------------------------------------------------------------------------
@dataclass
class OHLCRow:
    date:   str
    symbol: str
    market: str
    open:   Optional[float]
    high:   Optional[float]
    low:    Optional[float]
    close:  Optional[float]
    volume: Optional[int]


# ---------------------------------------------------------------------------
# Core download helpers
# ---------------------------------------------------------------------------

def _batch_download(tickers: list[str], **kwargs) -> dict[str, pd.DataFrame]:
    """
    Download OHLC for a list of tickers via yfinance, returning {ticker: df}.

    Handles the yfinance quirk where a single-ticker download returns a plain
    DataFrame but a multi-ticker download returns a MultiIndex DataFrame.
    Tickers with no data are omitted from the result.
    """
    if not tickers:
        return {}

    raw = yf.download(
        tickers=tickers,
        group_by="ticker",
        auto_adjust=True,   # Close = split+dividend adjusted; no Adj Close column
        progress=False,
        threads=True,
        **kwargs,
    )

    if len(tickers) == 1:
        # Single ticker: raw is a plain DataFrame (no MultiIndex)
        ticker = tickers[0]
        return {ticker: raw.dropna(how="all")} if not raw.empty else {}

    result: dict[str, pd.DataFrame] = {}
    for ticker in tickers:
        try:
            df = raw[ticker].dropna(how="all")
            if not df.empty:
                result[ticker] = df
            else:
                log.warning("No data returned for %s", ticker)
        except KeyError:
            log.warning("Ticker not found in download response: %s", ticker)
    return result


def _df_to_rows(df: pd.DataFrame, ticker: str, market: str) -> list[OHLCRow]:
    """Convert a yfinance OHLC DataFrame into a list of OHLCRow."""
    rows = []
    for ts, row in df.iterrows():
        if pd.isna(row.get("Close")):
            continue
        rows.append(OHLCRow(
            date   = ts.strftime("%Y.%m.%d"),
            symbol = ticker,
            market = market,
            open   = _fv(row.get("Open")),
            high   = _fv(row.get("High")),
            low    = _fv(row.get("Low")),
            close  = _fv(row.get("Close")),
            volume = _iv(row.get("Volume")),
        ))
    return rows


def _fv(v) -> Optional[float]:
    try:
        f = float(v)
        return None if pd.isna(f) else f
    except (TypeError, ValueError):
        return None


def _iv(v) -> Optional[int]:
    f = _fv(v)
    return int(f) if f is not None else None


# ---------------------------------------------------------------------------
# Public fetch functions
# ---------------------------------------------------------------------------

def fetch_ohlc_full(tickers: list[str], market: str) -> list[OHLCRow]:
    """
    Fetch YEARS_BACK years of daily OHLC for all tickers in one market.
    Downloads in chunks of CHUNK_SIZE to stay within yfinance's sweet spot.
    """
    all_rows: list[OHLCRow] = []
    total_chunks = (len(tickers) + CHUNK_SIZE - 1) // CHUNK_SIZE

    for i in range(0, len(tickers), CHUNK_SIZE):
        chunk = tickers[i : i + CHUNK_SIZE]
        chunk_num = i // CHUNK_SIZE + 1
        log.info("[%s] OHLC full chunk %d/%d (%d tickers) …",
                 market, chunk_num, total_chunks, len(chunk))
        try:
            batch = _batch_download(chunk, period=f"{YEARS_BACK}y")
        except Exception as exc:
            log.error("Chunk %d failed: %s — skipping", chunk_num, exc)
            continue

        for ticker, df in batch.items():
            rows = _df_to_rows(df, ticker, market)
            all_rows.extend(rows)
            log.debug("  %s → %d rows", ticker, len(rows))

    log.info("[%s] OHLC full: %d rows from %d tickers", market, len(all_rows), len(tickers))
    return all_rows


def fetch_ohlc_daily(tickers: list[str], market: str,
                     days_back: int = DAILY_OHLC_DAYS) -> list[OHLCRow]:
    """
    Fetch the last `days_back` calendar days of OHLC for incremental updates.
    Typically returns 1-5 trading-day rows per ticker depending on weekends/holidays.
    """
    start_date = (date.today() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    all_rows: list[OHLCRow] = []
    total_chunks = (len(tickers) + CHUNK_SIZE - 1) // CHUNK_SIZE

    for i in range(0, len(tickers), CHUNK_SIZE):
        chunk = tickers[i : i + CHUNK_SIZE]
        chunk_num = i // CHUNK_SIZE + 1
        log.info("[%s] OHLC daily chunk %d/%d (%d tickers) …",
                 market, chunk_num, total_chunks, len(chunk))
        try:
            batch = _batch_download(chunk, start=start_date)
        except Exception as exc:
            log.error("Chunk %d failed: %s — skipping", chunk_num, exc)
            continue

        for ticker, df in batch.items():
            all_rows.extend(_df_to_rows(df, ticker, market))

    log.info("[%s] OHLC daily: %d rows from %d tickers (last %d calendar days)",
             market, len(all_rows), len(tickers), days_back)
    return all_rows


# ---------------------------------------------------------------------------
# CSV writers  (kdb+-compatible format — same as fmp_client conventions)
# ---------------------------------------------------------------------------

_OHLC_HEADERS = ["date", "symbol", "market", "open", "high", "low", "close", "volume"]


def write_ohlc(rows: list[OHLCRow], market: str) -> None:
    """
    Write year-partitioned OHLC CSVs for a full historical load.
      data/ohlc/ohlc_YYYY_in.csv
      data/ohlc/ohlc_YYYY_us.csv
    """
    OHLC_DIR.mkdir(parents=True, exist_ok=True)
    suffix = market.lower()

    by_year: dict[str, list[OHLCRow]] = {}
    for row in rows:
        year = row.date[:4]
        by_year.setdefault(year, []).append(row)

    for year, yr_rows in sorted(by_year.items()):
        path = OHLC_DIR / f"ohlc_{year}_{suffix}.csv"
        _write_csv(path, _OHLC_HEADERS,
                   ([r.date, r.symbol, r.market,
                     r.open, r.high, r.low, r.close, r.volume]
                    for r in yr_rows))
    log.info("OHLC CSVs written: %d year-partitions for market=%s", len(by_year), market)


def write_ohlc_daily(rows: list[OHLCRow], market: str) -> None:
    """
    Write a flat incremental OHLC CSV for the daily append path.
      data/ohlc/ohlc_daily_in.csv
      data/ohlc/ohlc_daily_us.csv
    The KDB+ loader reads these and upserts into the correct year partitions.
    """
    OHLC_DIR.mkdir(parents=True, exist_ok=True)
    path = OHLC_DIR / f"ohlc_daily_{market.lower()}.csv"
    _write_csv(path, _OHLC_HEADERS,
               ([r.date, r.symbol, r.market,
                 r.open, r.high, r.low, r.close, r.volume]
                for r in rows))
    log.info("Daily OHLC written: %s (%d rows)", path, len(rows))


def _write_csv(path: Path, headers: list[str], rows: Iterator) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for row in rows:
            writer.writerow(["" if v is None else v for v in row])


# ---------------------------------------------------------------------------
# Universe resolver (mirrors fmp_client logic, no API call needed for IN)
# ---------------------------------------------------------------------------

def _resolve_universe(market: str, tickers: list[str] | None) -> list[str]:
    if tickers:
        return tickers
    if market == "IN":
        return NIFTY_50
    # US: try to reuse any already-fetched constituent list, else use fallback
    # (full S&P 500 list is fetched by fmp_client.py — run that first)
    return list(_SP500_FALLBACK)


# ---------------------------------------------------------------------------
# Entry-point run functions
# ---------------------------------------------------------------------------

def run_ohlc(
    market: str = "ALL",
    tickers: list[str] | None = None,
    dry_run: bool = False,
) -> None:
    """
    Full historical OHLC refresh (5 years).  No API key needed.
    Run once for initial setup, then monthly to catch any backfill gaps.
    """
    if market == "ALL":
        run_ohlc("IN", tickers=tickers, dry_run=dry_run)
        run_ohlc("US", tickers=tickers, dry_run=dry_run)
        return

    universe = _resolve_universe(market, tickers)

    if dry_run:
        chunks = (len(universe) + CHUNK_SIZE - 1) // CHUNK_SIZE
        log.info("DRY RUN full [%s] — %d tickers in %d batch(es), 0 API keys needed",
                 market, len(universe), chunks)
        return

    rows = fetch_ohlc_full(universe, market)
    write_ohlc(rows, market)


def run_ohlc_daily(
    market: str = "ALL",
    tickers: list[str] | None = None,
    days_back: int = DAILY_OHLC_DAYS,
    dry_run: bool = False,
) -> None:
    """
    Daily incremental OHLC (last N calendar days).  No API key needed.
    Run every trading day, then:  q data_layer/kdb_loader.q -mode daily
    """
    if market == "ALL":
        run_ohlc_daily("IN", tickers=tickers, days_back=days_back, dry_run=dry_run)
        run_ohlc_daily("US", tickers=tickers, days_back=days_back, dry_run=dry_run)
        return

    universe = _resolve_universe(market, tickers)

    if dry_run:
        log.info("DRY RUN daily [%s] — %d tickers, last %d calendar days, 0 API keys needed",
                 market, len(universe), days_back)
        return

    rows = fetch_ohlc_daily(universe, market, days_back)
    write_ohlc_daily(rows, market)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse
    import logging as _logging
    from pathlib import Path as _Path

    _logging.basicConfig(
        level=_logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        handlers=[
            _logging.StreamHandler(sys.stdout),
            _logging.FileHandler(_Path("logs/yf_client.log"), mode="a"),
        ],
    )

    parser = argparse.ArgumentParser(description="JARVIS yfinance OHLC fetcher")
    parser.add_argument("--mode", default="daily", choices=["full", "daily"],
                        help=(
                            "full  = 5-year OHLC for initial/monthly backfill; "
                            "daily = last --days-back calendar days (run every trading day)"
                        ))
    parser.add_argument("--market", default="ALL", choices=["IN", "US", "ALL"],
                        help="IN = Nifty 50, US = S&P 500, ALL = both")
    parser.add_argument("--days-back", type=int, default=DAILY_OHLC_DAYS,
                        help=f"Calendar days to look back in daily mode (default: {DAILY_OHLC_DAYS})")
    parser.add_argument("--tickers", nargs="*", default=None,
                        help="Override ticker list")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print plan without downloading")
    args = parser.parse_args()

    _Path("logs").mkdir(exist_ok=True)

    if args.mode == "full":
        run_ohlc(market=args.market, tickers=args.tickers, dry_run=args.dry_run)
    else:
        run_ohlc_daily(market=args.market, tickers=args.tickers,
                       days_back=args.days_back, dry_run=args.dry_run)
