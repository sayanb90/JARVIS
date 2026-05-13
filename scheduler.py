"""
JARVIS Scheduler — cron-invokable data refresh orchestrator.

Jobs
----
ohlc-daily      Fetch last N days of OHLC via yfinance (free, no API key).
                Writes data/ohlc/ohlc_daily_*.csv then calls:
                  q data_layer/kdb_loader.q -mode daily

fundamentals    Read data/universe.csv + data/fund_meta.csv, pick the
                stalest tickers that fit within today's FMP free-tier budget
                (default 248 calls = 62 tickers × 4 endpoints), fetch from FMP,
                update fund_meta.csv, then reload fundamentals into KDB+:
                  q data_layer/kdb_loader.q -load fundamentals

all             Run ohlc-daily then fundamentals.

Crontab examples (crontab -e)
------------------------------
# Daily OHLC — 6:30 PM every weekday
30 18 * * 1-5 cd /home/user/JARVIS && python scheduler.py --job ohlc-daily >> logs/scheduler.log 2>&1

# Fundamentals rotation — 7:00 PM every weekday (auto-picks stalest stocks)
0 19 * * 1-5 cd /home/user/JARVIS && python scheduler.py --job fundamentals >> logs/scheduler.log 2>&1

# Both together (if runtime is acceptable)
0 18 * * 1-5 cd /home/user/JARVIS && python scheduler.py --job all >> logs/scheduler.log 2>&1

Environment
-----------
FMP_API_KEY        Required for fundamentals job.
FMP_DAILY_BUDGET   FMP calls to spend today (default 248).
FMP_RATE_LIMIT_DELAY  Seconds between FMP calls (default 0.5 for free tier).
Q_BINARY           Path to the q executable (default: q).
JARVIS_ROOT        Project root; defaults to the directory of this file.
"""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
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
from data_layer.yf_client import run_ohlc_daily

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
Q_BINARY = os.getenv("Q_BINARY", "q")
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
# KDB+ helper
# ---------------------------------------------------------------------------
def _run_kdb(args: list[str], description: str) -> bool:
    """
    Invoke q with the given arguments.  Returns True on success.
    Requires q to be installed and on PATH (or Q_BINARY env var set).
    """
    cmd = [Q_BINARY, "data_layer/kdb_loader.q"] + args
    log.info("KDB+: %s", " ".join(cmd))
    try:
        result = subprocess.run(
            cmd,
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            timeout=300,
        )
        if result.stdout:
            for line in result.stdout.strip().splitlines():
                log.info("  q> %s", line)
        if result.returncode != 0:
            log.error("KDB+ %s failed (exit %d): %s",
                      description, result.returncode, result.stderr.strip())
            return False
        log.info("KDB+ %s complete", description)
        return True
    except FileNotFoundError:
        log.warning("q binary not found (%s) — skipping KDB+ load. "
                    "Run manually: q data_layer/kdb_loader.q %s",
                    Q_BINARY, " ".join(args))
        return False
    except subprocess.TimeoutExpired:
        log.error("KDB+ %s timed out after 300 s", description)
        return False


# ---------------------------------------------------------------------------
# Jobs
# ---------------------------------------------------------------------------

def job_ohlc_daily(market: str = "ALL", days_back: int = 7,
                   dry_run: bool = False) -> bool:
    """
    Fetch incremental OHLC via yfinance → append to KDB+ year partitions.

    Zero FMP API calls.  Safe to run every trading day.
    """
    log.info("=== JOB: ohlc-daily [market=%s, days_back=%d] ===", market, days_back)
    try:
        run_ohlc_daily(market=market, days_back=days_back, dry_run=dry_run)
    except Exception as exc:
        log.error("OHLC fetch failed: %s", exc)
        return False

    if dry_run:
        log.info("DRY RUN — skipping KDB+ load")
        return True

    return _run_kdb(["-mode", "daily"], "daily OHLC append")


def job_fundamentals(market: str = "ALL", budget: int = FMP_DAILY_BUDGET,
                     dry_run: bool = False) -> bool:
    """
    Read universe + fund_meta, fetch fundamentals for the stalest tickers
    within today's FMP call budget, update fund_meta, reload KDB+.

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
        log.info("DRY RUN — skipping KDB+ fundamentals reload")
        return ok

    # Reload all fundamental tables (income, balance, cashflow, ratios, pillars, fundmeta)
    kdb_ok = _run_kdb(["-load", "fundamentals"], "fundamentals reload")
    return ok and kdb_ok


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
                        choices=["ohlc-daily", "fundamentals", "all"],
                        help="Which job to run")
    parser.add_argument("--market", default="ALL", choices=["IN", "US", "ALL"],
                        help="Market universe (default: ALL)")
    parser.add_argument("--budget", type=int, default=FMP_DAILY_BUDGET,
                        help=f"FMP daily call budget for fundamentals (default: {FMP_DAILY_BUDGET})")
    parser.add_argument("--days-back", type=int, default=7,
                        help="Calendar days to look back for OHLC (default: 7)")
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
