"""
Yahoo Finance OHLC data layer for JARVIS.

Fetches daily price data for India, US, UK and EU stocks, and intraday bars for India only,
using the yfinance library — completely free, no API key required.

Ticker formats (same as the rest of the codebase):
  India : Yahoo .NS suffix        (e.g. RELIANCE.NS, INFY.NS)

Two modes:
  full   — 5-year historical OHLC, written as year-partitioned CSVs
            (data/ohlc/ohlc_YYYY_in.csv)
            Run once for initial setup, then weekly/monthly for backfill.

  daily  — Last N calendar days, written as a flat incremental CSV
            (data/ohlc/ohlc_daily_in.csv)
            Run every trading day; data_layer.lake --load daily upserts into the Parquet lake.

Usage:
    python -m data_layer.yf_client --mode full  --market ALL
    python -m data_layer.yf_client --mode daily --market ALL
    python -m data_layer.yf_client --mode daily --market UK --days-back 3
    python -m data_layer.yf_client --mode intraday --market IN --interval 5m --backfill
    python -m data_layer.yf_client --dry-run

  intraday — Aggregated bars (default 5m), regular session only, forward
            collection (Yahoo keeps only ~60d of 5m history). One CSV per
            market per UTC day under data/bars/<interval>/<market>/.
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

from .fmp_client import read_universe
from .lake import BARS_MARKETS, MARKETS

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
YEARS_BACK       = 5
DAILY_OHLC_DAYS  = 7          # calendar days back for incremental pulls
                               # (7 covers Mon after a long weekend + any gaps)
CHUNK_SIZE       = 100         # tickers per yfinance batch call

from .lake import ROOT

DATA_DIR = ROOT / "data"
OHLC_DIR = DATA_DIR / "ohlc"

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Domain dataclass (mirrors the lake ohlc table)
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
# CSV writers  (staging format; dates as YYYY.MM.DD, accepted by lake.py)
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
    lake.py upserts these into the correct year partitions.
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
# Intraday aggregated bars  (1m / 5m / 15m / ... — NOT raw ticks)
#
# Yahoo only serves a short trailing window of intraday history, so this feed
# can only be collected FORWARD (run daily after each market closes). There is
# no deep backfill: history starts the day the job first runs.
# ---------------------------------------------------------------------------
BARS_DIR = DATA_DIR / "bars"

# Max look-back Yahoo allows per interval (calendar days)
INTRADAY_MAX_DAYS = {"1m": 7, "2m": 60, "5m": 60, "15m": 60, "30m": 60, "60m": 730}
INTRADAY_INTERVAL = "5m"
INTRADAY_DAYS_BACK = 5     # overlap window; re-fetched bars overwrite (idempotent)

_INTERVAL_MINUTES = {"1m": 1, "2m": 2, "5m": 5, "15m": 15, "30m": 30, "60m": 60}
_BAR_HEADERS = ["ts", "symbol", "market", "open", "high", "low", "close", "volume"]


@dataclass
class BarRow:
    ts:     str          # UTC, 2026.10.24D13:30:00.000000000 (lake.py also accepts ISO)
    symbol: str
    market: str
    open:   Optional[float]
    high:   Optional[float]
    low:    Optional[float]
    close:  Optional[float]
    volume: Optional[int]


def _bars_to_rows(df: pd.DataFrame, ticker: str, market: str,
                  interval: str, now_utc: pd.Timestamp) -> list[BarRow]:
    """DataFrame -> BarRows in UTC, dropping bars that have not yet closed."""
    width = pd.Timedelta(minutes=_INTERVAL_MINUTES[interval])
    rows: list[BarRow] = []
    for ts, row in df.iterrows():
        if pd.isna(row.get("Close")):
            continue
        ts = pd.Timestamp(ts)
        ts = ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")
        if ts + width > now_utc:          # still-forming bar: skip
            continue
        rows.append(BarRow(
            ts     = ts.strftime("%Y.%m.%dD%H:%M:%S.000000000"),
            symbol = ticker,
            market = market,
            open   = _fv(row.get("Open")),
            high   = _fv(row.get("High")),
            low    = _fv(row.get("Low")),
            close  = _fv(row.get("Close")),
            volume = _iv(row.get("Volume")),
        ))
    return rows


def fetch_bars(tickers: list[str], market: str,
               interval: str = INTRADAY_INTERVAL,
               days_back: int = INTRADAY_DAYS_BACK) -> list[BarRow]:
    """Fetch regular-session intraday bars for the last `days_back` days."""
    if interval not in INTRADAY_MAX_DAYS:
        raise ValueError(f"Unsupported interval {interval!r}; use one of {sorted(INTRADAY_MAX_DAYS)}")
    days_back = min(days_back, INTRADAY_MAX_DAYS[interval])
    now_utc = pd.Timestamp.now(tz="UTC")

    all_rows: list[BarRow] = []
    total_chunks = (len(tickers) + CHUNK_SIZE - 1) // CHUNK_SIZE
    for i in range(0, len(tickers), CHUNK_SIZE):
        chunk = tickers[i : i + CHUNK_SIZE]
        chunk_num = i // CHUNK_SIZE + 1
        log.info("[%s] bars %s chunk %d/%d (%d tickers) …",
                 market, interval, chunk_num, total_chunks, len(chunk))
        try:
            batch = _batch_download(chunk, period=f"{days_back}d",
                                    interval=interval, prepost=False)
        except Exception as exc:
            log.error("Chunk %d failed: %s — skipping", chunk_num, exc)
            continue
        for ticker, df in batch.items():
            all_rows.extend(_bars_to_rows(df, ticker, market, interval, now_utc))

    log.info("[%s] bars %s: %d rows from %d tickers", market, interval,
             len(all_rows), len(tickers))
    return all_rows


def write_bars(rows: list[BarRow], market: str, interval: str) -> int:
    """
    Merge rows into one CSV per UTC session day:
      data/bars/<interval>/<market>/YYYY-MM-DD.csv
    Each file always holds the COMPLETE state for that market/day (new rows
    overwrite on (ts, symbol)), so lake.py can rebuild a day partition
    from the files alone. Returns the number of day-files written.
    """
    folder = BARS_DIR / interval / market.lower()
    folder.mkdir(parents=True, exist_ok=True)

    by_day: dict[str, dict[tuple[str, str], BarRow]] = {}
    for r in rows:
        day = r.ts[:10].replace(".", "-")
        by_day.setdefault(day, {})[(r.ts, r.symbol)] = r

    for day, fresh in sorted(by_day.items()):
        path = folder / f"{day}.csv"
        merged: dict[tuple[str, str], tuple] = {}
        if path.exists():
            with path.open(newline="", encoding="utf-8") as f:
                for rec in csv.DictReader(f):
                    merged[(rec["ts"], rec["symbol"])] = tuple(rec[h] for h in _BAR_HEADERS)
        for key, r in fresh.items():
            merged[key] = (r.ts, r.symbol, r.market, r.open, r.high, r.low, r.close, r.volume)
        _write_csv(path, _BAR_HEADERS,
                   (merged[k] for k in sorted(merged, key=lambda k: (k[1], k[0]))))
    log.info("Bars written: %d day-file(s) under %s", len(by_day), folder)
    return len(by_day)


def run_bars(
    market: str = "IN",
    tickers: list[str] | None = None,
    interval: str = INTRADAY_INTERVAL,
    days_back: int = INTRADAY_DAYS_BACK,
    dry_run: bool = False,
    backfill: bool = False,
) -> None:
    """
    Intraday bar collection (India only).  backfill=True pulls the deepest window
    Yahoo allows (about 59 days of 5m, 6 of 1m, 729 of 60m); there is no more.  Run AFTER each market closes, then:
      python -m data_layer.lake --load bars --interval 5m
    """
    if market == "ALL":
        market = BARS_MARKETS[0]
    if market not in BARS_MARKETS:
        raise ValueError(f"intraday bars are collected for {BARS_MARKETS} only, not {market!r}")
    if backfill:
        days_back = INTRADAY_MAX_DAYS[interval] - 1   # deepest window Yahoo allows
    universe = _resolve_universe(market, tickers)
    if dry_run:
        log.info("DRY RUN bars [%s] — %d tickers, interval=%s, last %d days",
                 market, len(universe), interval, days_back)
        return

    rows = fetch_bars(universe, market, interval, days_back)
    write_bars(rows, market, interval)


# ---------------------------------------------------------------------------
# Universe resolver (mirrors fmp_client logic, no API call needed for IN)
# ---------------------------------------------------------------------------

def _resolve_universe(market: str, tickers: list[str] | None) -> list[str]:
    """Explicit tickers win; otherwise data/universe.csv is the single source of truth."""
    if tickers:
        return tickers
    return [r["symbol"] for r in read_universe(market)]


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
        for m in MARKETS:
            run_ohlc(m, tickers=tickers, dry_run=dry_run)
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
    Run every trading day, then:  python -m data_layer.lake --load daily
    """
    if market == "ALL":
        for m in MARKETS:
            run_ohlc_daily(m, tickers=tickers, days_back=days_back, dry_run=dry_run)
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
            _logging.FileHandler(ROOT / "logs" / "yf_client.log", mode="a"),
        ],
    )

    parser = argparse.ArgumentParser(description="JARVIS yfinance OHLC fetcher")
    parser.add_argument("--mode", default="daily", choices=["full", "daily", "intraday"],
                        help=(
                            "full     = 5-year OHLC for initial/monthly backfill; "
                            "daily    = last --days-back calendar days (run every trading day); "
                            "intraday = aggregated intraday bars (run after each market close)"
                        ))
    parser.add_argument("--interval", default=INTRADAY_INTERVAL,
                        choices=sorted(INTRADAY_MAX_DAYS),
                        help=f"Bar size for intraday mode (default: {INTRADAY_INTERVAL})")
    parser.add_argument("--market", default="ALL", choices=[*MARKETS, "ALL"],
                        help="Daily OHLC: IN, US, UK, EU or ALL. Intraday: IN only")
    parser.add_argument("--days-back", type=int, default=DAILY_OHLC_DAYS,
                        help=f"Calendar days to look back in daily mode (default: {DAILY_OHLC_DAYS})")
    parser.add_argument("--backfill", action="store_true",
                        help="intraday: fetch the deepest window Yahoo allows for --interval")
    parser.add_argument("--tickers", nargs="*", default=None,
                        help="Override ticker list")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print plan without downloading")
    args = parser.parse_args()

    (ROOT / "logs").mkdir(parents=True, exist_ok=True)

    if args.mode == "full":
        run_ohlc(market=args.market, tickers=args.tickers, dry_run=args.dry_run)
    elif args.mode == "intraday":
        run_bars(market=args.market, tickers=args.tickers, interval=args.interval,
                 days_back=args.days_back if args.days_back != DAILY_OHLC_DAYS else INTRADAY_DAYS_BACK,
                 dry_run=args.dry_run, backfill=args.backfill)
    else:
        run_ohlc_daily(market=args.market, tickers=args.tickers,
                       days_back=args.days_back, dry_run=args.dry_run)
