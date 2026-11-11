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
import os
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator, Optional

import pandas as pd
import warnings

# yfinance 1.7 on pandas 3 prints a deprecation warning for every call (an `inplace` keyword it uses); with two thousand symbols that would
# bury the real lines of the cron logs
warnings.filterwarnings("ignore", message=r".*inplace keyword.*")
import yfinance as yf

from .fmp_client import read_universe
from .lake import BARS_MARKETS, BARS_YF_DEFAULT, MARKETS

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

_LAST_DOWNLOAD = 0.0


def _pace() -> None:
    """Leave `JARVIS_YF_CHUNK_PAUSE_S` (default 2 s) between two downloads: with a universe of about two thousand tickers Yahoo
    throttles a client that sends the chunks back to back."""
    global _LAST_DOWNLOAD
    gap = float(os.getenv("JARVIS_YF_CHUNK_PAUSE_S", "2")) - (time.monotonic() - _LAST_DOWNLOAD)
    if gap > 0 and _LAST_DOWNLOAD:
        time.sleep(gap)
    _LAST_DOWNLOAD = time.monotonic()


def _batch_download(tickers: list[str], **kwargs) -> dict[str, pd.DataFrame]:
    """
    Download OHLC for a list of tickers via yfinance, returning {ticker: df}.  One chunk that raises (a throttled request) is
    tried once more after a longer pause before the caller gives up on it.

    Handles the yfinance quirk where a single-ticker download returns a plain
    DataFrame but a multi-ticker download returns a MultiIndex DataFrame.
    Tickers with no data are omitted from the result.
    """
    if not tickers:
        return {}

    for attempt in (1, 2):
        _pace()
        try:
            raw = yf.download(
                tickers=tickers,
                group_by="ticker",
                auto_adjust=True,   # Close = split+dividend adjusted; no Adj Close column
                progress=False,
                threads=True,
                **kwargs,
            )
            break
        except Exception as exc:
            if attempt == 2:
                raise
            wait = 5 * float(os.getenv("JARVIS_YF_CHUNK_PAUSE_S", "2"))
            log.warning("download of %d tickers failed (%s); trying once more in %.0f s", len(tickers), exc, wait)
            time.sleep(wait)

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

def history_period(years: int | None = YEARS_BACK) -> str:
    """The Yahoo `period` for a full download: "<N>y", or "max" (everything Yahoo has) when `years` is None or 0."""
    if years is not None and years < 0:
        raise ValueError("years must be positive, or 0 / None for everything Yahoo has")
    return f"{years}y" if years else "max"


def parse_years(text: str) -> int | None:
    """Command-line value of --years: a whole number of years, or "max" (returned as None)."""
    if text.strip().lower() == "max":
        return None
    n = int(text)
    if n <= 0:
        raise ValueError("--years must be a positive number or 'max'")
    return n


def fetch_ohlc_full(tickers: list[str], market: str, years: int | None = YEARS_BACK) -> list[OHLCRow]:
    """
    Fetch `years` years of daily OHLC (default YEARS_BACK; None or 0 = everything Yahoo has) for all tickers in one market.
    Downloads in chunks of CHUNK_SIZE to stay within yfinance's sweet spot.

    A full download replaces every stored day of a symbol with prices adjusted on one basis (today's), which is also the way to
    repair a series whose older rows were adjusted before a later split or dividend.  Rows are held in memory for the whole
    market before they are written: twenty years of a 600-stock market is about three million rows, so run one market at a time.
    """
    all_rows: list[OHLCRow] = []
    total_chunks = (len(tickers) + CHUNK_SIZE - 1) // CHUNK_SIZE

    for i in range(0, len(tickers), CHUNK_SIZE):
        chunk = tickers[i : i + CHUNK_SIZE]
        chunk_num = i // CHUNK_SIZE + 1
        log.info("[%s] OHLC full chunk %d/%d (%d tickers) …",
                 market, chunk_num, total_chunks, len(chunk))
        try:
            batch = _batch_download(chunk, period=history_period(years))
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
# Tested 2026-11-11: Yahoo refuses a 1-minute request that reaches back 30 days or more ("must be within the last 30 days"), serves at most
# about 8 days per request, and 5-minute bars only within 60 days.  So 1-minute history can be fetched 29 days back, in windows of 7 days.
INTRADAY_MAX_DAYS = {"1m": 30, "2m": 60, "5m": 60, "15m": 60, "30m": 60, "60m": 730}
WINDOW_DAYS = {"1m": 7}                 # days per request for the intervals that Yahoo serves only a few days at a time
INTRADAY_INTERVAL = "5m"
INTRADAY_DAYS_BACK = 5     # overlap window; re-fetched bars overwrite (idempotent).  A daily run with a 5-day overlap survives a missed
                           # day or two.

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


def bar_windows(interval: str, days_back: int, today: Optional[date] = None) -> list[tuple[date, date]]:
    """[(first day, last day)] to request when `days_back` is more than Yahoo serves in one request for `interval` (1-minute: 7 days),
    oldest first; empty when one `period=` request is enough."""
    size = WINDOW_DAYS.get(interval)
    days_back = min(days_back, INTRADAY_MAX_DAYS[interval] - 1)
    if not size or days_back <= size:
        return []
    today = today or datetime.now(timezone.utc).date()
    out, first = [], today - timedelta(days=days_back)
    while first <= today:
        out.append((first, min(first + timedelta(days=size - 1), today)))
        first += timedelta(days=size)
    return out


def fetch_bars(tickers: list[str], market: str,
               interval: str = INTRADAY_INTERVAL,
               days_back: int = INTRADAY_DAYS_BACK,
               window: Optional[tuple[date, date]] = None) -> list[BarRow]:
    """Fetch regular-session intraday bars for the last `days_back` days, or for the days of `window` (first, last) when it is given."""
    if interval not in INTRADAY_MAX_DAYS:
        raise ValueError(f"Unsupported interval {interval!r}; use one of {sorted(INTRADAY_MAX_DAYS)}")
    days_back = min(days_back, INTRADAY_MAX_DAYS[interval] - 1 if interval == "1m" else INTRADAY_MAX_DAYS[interval])
    now_utc = pd.Timestamp.now(tz="UTC")
    span = ({"start": window[0].isoformat(), "end": (window[1] + timedelta(days=1)).isoformat()} if window
            else {"period": f"{days_back}d"})

    all_rows: list[BarRow] = []
    total_chunks = (len(tickers) + CHUNK_SIZE - 1) // CHUNK_SIZE
    for i in range(0, len(tickers), CHUNK_SIZE):
        chunk = tickers[i : i + CHUNK_SIZE]
        chunk_num = i // CHUNK_SIZE + 1
        log.info("[%s] bars %s chunk %d/%d (%d tickers) …",
                 market, interval, chunk_num, total_chunks, len(chunk))
        try:
            batch = _batch_download(chunk, interval=interval, prepost=False, **span)
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


def _all_markets(fn, label: str, **kw) -> None:
    """Run `fn(market, **kw)` for every market; keep going after a failure, then raise once."""
    failed = []
    for m in MARKETS:
        try:
            fn(m, **kw)
        except Exception as exc:
            log.error("%s [%s] failed: %s", label, m, exc)
            failed.append(m)
    if failed:
        raise RuntimeError(f"{label} failed for market(s): {', '.join(failed)}")


def _require_rows(rows: list, universe: list[str], market: str, what: str) -> None:
    """Zero rows for a non-empty universe means Yahoo is down or blocking us: fail loudly so cron alerts."""
    if universe and not rows:
        raise RuntimeError(f"{what} [{market}]: Yahoo returned no data for any of {len(universe)} tickers")


def run_bars(
    market: str = "IN",
    tickers: list[str] | None = None,
    interval: str = INTRADAY_INTERVAL,
    days_back: int = INTRADAY_DAYS_BACK,
    dry_run: bool = False,
    backfill: bool = False,
) -> None:
    """
    Intraday bar collection from Yahoo: US, UK and EU by default (`ALL`), India on request (the Upstox feed records India tick by
    tick, so a Yahoo copy of it adds nothing).  backfill=True pulls the deepest window Yahoo allows (59 days of 5m, 29 of 1m in windows
    of 7 days, 729 of 60m); there is no more, so longer 1-minute history can only be built forward.  Each window is written as soon as
    it is fetched, so a 29-day backfill of a market holds one week of bars in memory, not a month.  Run AFTER each market closes (the cron
    job runs once at 22:00 UTC, after the US close in winter and summer), then:
      python -m data_layer.lake --load bars --interval 1m
    """
    if market == "ALL":
        failed = []
        for m in BARS_YF_DEFAULT:                                  # one market failing must not stop the others
            try:
                run_bars(m, tickers, interval, days_back, dry_run, backfill)
            except Exception as exc:
                log.error("bars %s [%s] failed: %s", interval, m, exc)
                failed.append(m)
        if failed:
            raise RuntimeError(f"bars {interval} failed for market(s): {', '.join(failed)}")
        return
    if market not in BARS_MARKETS:
        raise ValueError(f"intraday bars are collected for {BARS_MARKETS} only, not {market!r}")
    if backfill:
        days_back = INTRADAY_MAX_DAYS[interval] - 1   # deepest window Yahoo allows
    universe = _resolve_universe(market, tickers)
    windows = bar_windows(interval, days_back)
    if dry_run:
        log.info("DRY RUN bars [%s] — %d tickers, interval=%s, last %d days%s",
                 market, len(universe), interval, days_back, f" in {len(windows)} windows of {WINDOW_DAYS[interval]} days" if windows else "")
        return

    if not windows:
        rows = fetch_bars(universe, market, interval, days_back)
        _require_rows(rows, universe, market, f"{interval} bars")
        write_bars(rows, market, interval)
        return
    total = 0
    for first, last in windows:                       # oldest window first; each is written before the next is fetched
        log.info("[%s] bars %s window %s .. %s", market, interval, first, last)
        rows = fetch_bars(universe, market, interval, days_back, window=(first, last))
        total += len(rows)
        if rows:
            write_bars(rows, market, interval)
    if universe and not total:                        # Yahoo is down or blocking us: fail loudly so cron alerts
        raise RuntimeError(f"{interval} bars [{market}]: Yahoo returned no data for any of {len(universe)} tickers")


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
    years: int | None = YEARS_BACK,
) -> None:
    """
    Full historical OHLC refresh (`years` years, default 5; None or 0 = everything Yahoo has).  No API key needed.
    Run once for initial setup, then monthly to catch any backfill gaps.
    """
    if market == "ALL":
        return _all_markets(run_ohlc, "full OHLC", tickers=tickers, dry_run=dry_run, years=years)

    universe = _resolve_universe(market, tickers)

    if dry_run:
        chunks = (len(universe) + CHUNK_SIZE - 1) // CHUNK_SIZE
        log.info("DRY RUN full [%s] — %d tickers in %d batch(es), period %s, 0 API keys needed",
                 market, len(universe), chunks, history_period(years))
        return

    rows = fetch_ohlc_full(universe, market, years)
    _require_rows(rows, universe, market, "full OHLC")
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
        return _all_markets(run_ohlc_daily, "daily OHLC", tickers=tickers, days_back=days_back, dry_run=dry_run)

    universe = _resolve_universe(market, tickers)

    if dry_run:
        log.info("DRY RUN daily [%s] — %d tickers, last %d calendar days, 0 API keys needed",
                 market, len(universe), days_back)
        return

    rows = fetch_ohlc_daily(universe, market, days_back)
    _require_rows(rows, universe, market, "daily OHLC")
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
                            "full     = --years of daily OHLC (default 5) for initial/monthly backfill; "
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
    parser.add_argument("--years", default=str(YEARS_BACK), metavar="N|max",
                        help=f"full mode: years of daily history to download (default {YEARS_BACK}); 'max' = everything Yahoo has")
    parser.add_argument("--backfill", action="store_true",
                        help="intraday: fetch the deepest window Yahoo allows for --interval")
    parser.add_argument("--tickers", nargs="*", default=None,
                        help="Override ticker list")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print plan without downloading")
    args = parser.parse_args()

    (ROOT / "logs").mkdir(parents=True, exist_ok=True)

    if args.mode == "full":
        run_ohlc(market=args.market, tickers=args.tickers, dry_run=args.dry_run, years=parse_years(args.years))
    elif args.mode == "intraday":
        run_bars(market=args.market, tickers=args.tickers, interval=args.interval,
                 days_back=args.days_back if args.days_back != DAILY_OHLC_DAYS else INTRADAY_DAYS_BACK,
                 dry_run=args.dry_run, backfill=args.backfill)
    else:
        run_ohlc_daily(market=args.market, tickers=args.tickers,
                       days_back=args.days_back, dry_run=args.dry_run)
