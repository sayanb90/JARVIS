"""
JARVIS Scheduler — cron-invokable data refresh orchestrator.

Jobs
----
ohlc-daily      Fetch last N days of daily OHLC via yfinance for India, US, UK, EU, JP, KR, TW, SG, CN and HK
                (free, no API key).
                Writes data/ohlc/ohlc_daily_*.csv then calls:
                  data_layer.lake.load_ohlc(daily=True)  -> lake/ohlc/year=YYYY/

bars            Collect aggregated intraday bars (default 5m, use --interval 1m) via yfinance for
                every market but India (--market ALL) or one market, and upsert them into per-day Parquet files:
                  data_layer.lake.load_bars()  -> lake/bars/interval=1m/date=YYYY-MM-DD/
                Yahoo keeps only 7 days of 1m and 60 days of 5m, so history is built forward: run it every
                weekday after the US close (22:00 UTC).  India is recorded by the Upstox live feed instead.

Fundamentals and FX rates are not scheduler jobs: they have their own cron entries (see deploy/crontab):
  python -m data_layer.yf_fundamentals run | check,  python -m data_layer.yf_pillars build,  python -m data_layer.fx refresh

Crontab examples (crontab -e)
------------------------------
# Daily OHLC, every market — after the last close (UTC server)
0 23 * * 1-5 cd /home/user/JARVIS && python scheduler.py --job ohlc-daily >> logs/scheduler.log 2>&1

# Intraday bars for every market but India — after the last of those closes (NYSE closes 21:00 UTC in winter). Server clock must be UTC.
0 22 * * 1-5 cd /home/user/JARVIS && python scheduler.py --job bars --market ALL --interval 1m >> logs/scheduler.log 2>&1

Environment
-----------
JARVIS_LAKE        Parquet lake directory (default: <root>/lake).
JARVIS_ROOT        Project root; defaults to the directory of this file.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date, timedelta
from pathlib import Path

# Ensure project root is on the path so data_layer imports work
ROOT = Path(os.getenv("JARVIS_ROOT", Path(__file__).parent)).resolve()
sys.path.insert(0, str(ROOT))

from data_layer import lake
from data_layer.yf_client import INTRADAY_INTERVAL, INTRADAY_MAX_DAYS, run_bars, run_ohlc_daily

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_DIR = ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  [scheduler]  %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],             # cron appends stdout to logs/scheduler.log: a file handler would write every line twice
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

    Safe to run every trading day.
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


def job_bars(market: str = "ALL", interval: str = INTRADAY_INTERVAL,
             dry_run: bool = False, backfill: bool = False) -> bool:
    """Fetch aggregated intraday bars -> data/bars/ -> upsert recent day files."""
    log.info("=== JOB: bars [market=%s, interval=%s] ===", market, interval)
    fetched = True
    try:
        run_bars(market=market, interval=interval, dry_run=dry_run, backfill=backfill)
    except Exception as exc:
        log.error("Bars fetch failed: %s", exc)
        fetched = False                                   # the markets that did work wrote day files: still load them

    if dry_run:
        log.info("DRY RUN — skipping lake load")
        return True

    markets = lake.BARS_YF_DEFAULT if market == "ALL" else (market,)
    since = None                                         # default: the last 7 days of day files
    if backfill:                                         # a backfill wrote up to 59 days of files: load all of them
        since = date.today() - timedelta(days=INTRADAY_MAX_DAYS.get(interval, 7) + 1)
    return _load_lake("intraday bars", lake.load_bars, interval, since, markets=markets) and fetched


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
                        choices=["ohlc-daily", "bars"],
                        help="Which job to run")
    parser.add_argument("--market", default="ALL", choices=[*lake.MARKETS, "ALL"],
                        help="Markets (default ALL; for bars, ALL means every market but India)")
    parser.add_argument("--days-back", type=int, default=7,
                        help="Calendar days to look back for OHLC (default: 7)")
    parser.add_argument("--backfill", action="store_true",
                        help="bars job: fetch the deepest intraday window Yahoo allows")
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
        else:
            success = job_bars(
                market=args.market, interval=args.interval, dry_run=args.dry_run, backfill=args.backfill)

    if success:
        log.info("Scheduler finished successfully")
        return 0
    else:
        log.error("Scheduler finished with errors — check logs/scheduler.log")
        return 1


if __name__ == "__main__":
    sys.exit(main())
