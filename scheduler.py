"""
JARVIS Scheduler — cron-invokable data refresh orchestrator.

Jobs
----
ohlc-daily      Fetch last N days of OHLC via yfinance (free, no API key).
                Writes data/ohlc/ohlc_daily_*.csv then calls:
                  data_layer.lake.load_ohlc(daily=True)  -> lake/ohlc/year=YYYY/

fundamentals    Read data/universe.csv + data/fund_meta.csv, pick the
                stalest tickers that fit within today's FMP free-tier budget
                (default 248 calls = 62 tickers × 4 endpoints), fetch from FMP,
                update fund_meta.csv, then upsert into the Parquet lake:
                  data_layer.lake.load_fundamentals()  -> lake/fundamentals/

bars            Collect aggregated intraday bars (default 5m) via yfinance and
                upsert them into per-day Parquet files:
                  data_layer.lake.load_bars()  -> lake/bars/interval=5m/date=YYYY-MM-DD/
                Run once per market AFTER its close (see crontab below).
                Not part of `all` because the two markets close at different times.

all             Run ohlc-daily then fundamentals.

Crontab examples (crontab -e)
------------------------------
# Daily OHLC — 6:30 PM every weekday
30 18 * * 1-5 cd /home/user/JARVIS && python scheduler.py --job ohlc-daily >> logs/scheduler.log 2>&1

# Fundamentals rotation — 7:00 PM every weekday (auto-picks stalest stocks)
0 19 * * 1-5 cd /home/user/JARVIS && python scheduler.py --job fundamentals >> logs/scheduler.log 2>&1

# Intraday bars — after each close. Server clock must be UTC.
#   NSE closes 15:30 IST = 10:00 UTC  -> run 10:20 UTC
#   NYSE closes 16:00 ET = 20:00 UTC (EDT) / 21:00 UTC (EST) -> run 21:20 UTC covers both
20 10 * * 1-5 cd /home/user/JARVIS && python scheduler.py --job bars --market IN >> logs/scheduler.log 2>&1
20 21 * * 1-5 cd /home/user/JARVIS && python scheduler.py --job bars --market US >> logs/scheduler.log 2>&1

# Both together (if runtime is acceptable)
0 18 * * 1-5 cd /home/user/JARVIS && python scheduler.py --job all >> logs/scheduler.log 2>&1

Environment
-----------
FMP_API_KEY        Required for fundamentals job.
FMP_DAILY_BUDGET   FMP calls to spend today (default 248).
FMP_RATE_LIMIT_DELAY  Seconds between FMP calls (default 0.5 for free tier).
JARVIS_LAKE        Parquet lake directory (default: <root>/lake).
JARVIS_ROOT        Project root; defaults to the directory of this file.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date
from pathlib import Path

# Ensure project root is on the path so data_layer imports work
ROOT = Path(os.getenv("JARVIS_ROOT", Path(__file__).parent)).resolve()
sys.path.insert(0, str(ROOT))

from data_layer.fmp_client import (
    FMP_DAILY_BUDGET,
    get_stale_tickers,
    run as fmp_run,
)
from data_layer import lake
from data_layer.yf_client import INTRADAY_INTERVAL, run_bars, run_ohlc_daily

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_DIR = ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  [scheduler]  %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_DIR / "scheduler.log", mode="a"),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
LOCK_FILE = ROOT / "logs" / "scheduler.lock"


# ---------------------------------------------------------------------------
# Lock — prevents two scheduler instances running concurrently
# ---------------------------------------------------------------------------
class SchedulerLock:
    def __enter__(self):
        if LOCK_FILE.exists():
            pid = LOCK_FILE.read_text().strip()
            log.error("Lock file exists (PID %s). Another scheduler may be running. "
                      "Delete %s if this is stale.", pid, LOCK_FILE)
            sys.exit(2)
        LOCK_FILE.write_text(str(os.getpid()))
        return self

    def __exit__(self, *_):
        LOCK_FILE.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Lake loader helper
# ---------------------------------------------------------------------------
def markets_of(market: str) -> tuple[str, ...]:
    return lake.MARKETS if market == "ALL" else (market,)


def _load_lake(description: str, fn, *args, **kwargs) -> bool:
    """Run a data_layer.lake loader in-process.  Returns True on success."""
    log.info("Lake load: %s", description)
    try:
        fn(*args, **kwargs)
        log.info("Lake load complete: %s", description)
        return True
    except Exception as exc:
        log.error("Lake load %s failed: %s", description, exc)
        return False


# ---------------------------------------------------------------------------
# Jobs
# ---------------------------------------------------------------------------

def job_ohlc_daily(market: str = "ALL", days_back: int = 7,
                   dry_run: bool = False) -> bool:
    """
    Fetch incremental OHLC via yfinance → upsert into the Parquet lake.

    Zero FMP API calls.  Safe to run every trading day.
    """
    log.info("=== JOB: ohlc-daily [market=%s, days_back=%d] ===", market, days_back)
    try:
        run_ohlc_daily(market=market, days_back=days_back, dry_run=dry_run)
    except Exception as exc:
        log.error("OHLC fetch failed: %s", exc)
        return False

    if dry_run:
        log.info("DRY RUN — skipping lake load")
        return True

    return _load_lake("daily OHLC append", lake.load_ohlc, markets_of(market), daily=True)


def job_fundamentals(market: str = "ALL", budget: int = FMP_DAILY_BUDGET,
                     dry_run: bool = False) -> bool:
    """
    Read universe + fund_meta, fetch fundamentals for the stalest tickers
    within today's FMP call budget, update fund_meta, upsert into the lake.

    Automatic rotation: over a 4-day cycle every stock in a 250-stock
    universe gets refreshed (62 tickers/day × 4 days = 248 calls/day).
    """
    log.info("=== JOB: fundamentals [market=%s, budget=%d calls] ===", market, budget)

    # Determine which tickers to refresh today
    mkt_filter = None if market == "ALL" else market
    stale = get_stale_tickers(market=mkt_filter, budget=budget)

    if not stale:
        log.info("No tickers to refresh (universe.csv may be empty)")
        return True

    # Split by market and run FMP for each
    in_tickers = [r["symbol"] for r in stale if r["market"] == "IN"]
    us_tickers = [r["symbol"] for r in stale if r["market"] == "US"]

    ok = True
    if in_tickers:
        log.info("Refreshing %d Indian tickers: %s …", len(in_tickers),
                 ", ".join(in_tickers[:5]) + ("…" if len(in_tickers) > 5 else ""))
        try:
            fmp_run(tickers=in_tickers, market="IN", dry_run=dry_run)
        except Exception as exc:
            log.error("FMP IN fundamentals failed: %s", exc)
            ok = False

    if us_tickers:
        log.info("Refreshing %d US tickers: %s …", len(us_tickers),
                 ", ".join(us_tickers[:5]) + ("…" if len(us_tickers) > 5 else ""))
        try:
            fmp_run(tickers=us_tickers, market="US", dry_run=dry_run)
        except Exception as exc:
            log.error("FMP US fundamentals failed: %s", exc)
            ok = False

    if dry_run:
        log.info("DRY RUN — skipping lake load")
        return ok

    # Upsert fundamentals, pillar metrics and fund_meta into the Parquet lake
    lake_ok = _load_lake("fundamentals upsert", lake.load_fundamentals, markets_of(market))
    return ok and lake_ok


def job_bars(market: str = "ALL", interval: str = INTRADAY_INTERVAL,
             dry_run: bool = False) -> bool:
    """Fetch aggregated intraday bars -> data/bars/ -> upsert recent day files."""
    log.info("=== JOB: bars [market=%s, interval=%s] ===", market, interval)
    try:
        run_bars(market=market, interval=interval, dry_run=dry_run)
    except Exception as exc:
        log.error("Bars fetch failed: %s", exc)
        return False

    if dry_run:
        log.info("DRY RUN — skipping lake load")
        return True

    return _load_lake("intraday bars", lake.load_bars, interval, markets=markets_of(market))


def job_all(market: str = "ALL", budget: int = FMP_DAILY_BUDGET,
            days_back: int = 7, dry_run: bool = False) -> bool:
    """Run ohlc-daily then fundamentals."""
    ohlc_ok = job_ohlc_daily(market=market, days_back=days_back, dry_run=dry_run)
    fund_ok = job_fundamentals(market=market, budget=budget, dry_run=dry_run)
    return ohlc_ok and fund_ok


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(
        description="JARVIS Scheduler — invoke via cron or manually",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--job", required=True,
                        choices=["ohlc-daily", "fundamentals", "bars", "all"],
                        help="Which job to run")
    parser.add_argument("--market", default="ALL", choices=["IN", "US", "ALL"],
                        help="Market universe (default: ALL)")
    parser.add_argument("--budget", type=int, default=FMP_DAILY_BUDGET,
                        help=f"FMP daily call budget for fundamentals (default: {FMP_DAILY_BUDGET})")
    parser.add_argument("--days-back", type=int, default=7,
                        help="Calendar days to look back for OHLC (default: 7)")
    parser.add_argument("--interval", default=INTRADAY_INTERVAL,
                        help=f"Bar size for the bars job (default: {INTRADAY_INTERVAL})")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print plan without making any API calls or DB writes")

    args = parser.parse_args()

    log.info("JARVIS Scheduler starting — job=%s market=%s dry_run=%s date=%s",
             args.job, args.market, args.dry_run, date.today())

    with SchedulerLock():
        if args.job == "ohlc-daily":
            success = job_ohlc_daily(
                market=args.market, days_back=args.days_back, dry_run=args.dry_run)
        elif args.job == "fundamentals":
            success = job_fundamentals(
                market=args.market, budget=args.budget, dry_run=args.dry_run)
        elif args.job == "bars":
            success = job_bars(
                market=args.market, interval=args.interval, dry_run=args.dry_run)
        else:
            success = job_all(
                market=args.market, budget=args.budget,
                days_back=args.days_back, dry_run=args.dry_run)

    if success:
        log.info("Scheduler finished successfully")
        return 0
    else:
        log.error("Scheduler finished with errors — check logs/scheduler.log")
        return 1


if __name__ == "__main__":
    sys.exit(main())
