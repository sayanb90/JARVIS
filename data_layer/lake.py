"""
JARVIS Parquet lake — CSV staging files -> partitioned Parquet.

The collectors (fmp_client.py, yf_client.py) keep writing CSVs under data/.
This module upserts them into an on-disk Parquet lake that DuckDB queries
(see query.py).  Nothing here needs a server process.

Lake layout (LAKE_DIR, default ./lake):
  lake/fundamentals/<table>.parquet          income_statement, balance_sheet,
                                             cash_flow, ratios, pillar_metrics,
                                             fund_meta   (both markets, one file)
  lake/ohlc/year=YYYY/ohlc.parquet           daily bars, both markets
  lake/bars/interval=5m/date=YYYY-MM-DD/bars.parquet
                                             intraday bars, UTC session day

Every write is an upsert on the table's key (new rows win), so re-running a
load is idempotent.  Files are written to a temp name and renamed, so a reader
never sees a half-written file.

CLI:
  python -m data_layer.lake --load fundamentals [--market US]
  python -m data_layer.lake --load ohlc          # full-history year files
  python -m data_layer.lake --load daily         # ohlc_daily_*.csv
  python -m data_layer.lake --load bars [--interval 5m] [--since 2026-10-20]
  python -m data_layer.lake --load all
"""

from __future__ import annotations

import argparse
import logging
import os
import re
from datetime import date, timedelta
from pathlib import Path

import duckdb

log = logging.getLogger(__name__)

ROOT = Path(os.getenv("JARVIS_ROOT", Path(__file__).resolve().parent.parent))
DATA_DIR = ROOT / "data"
LAKE_DIR = Path(os.getenv("JARVIS_LAKE", ROOT / "lake"))

MARKETS = ("IN", "US")

# table -> (csv stem, key columns, {column: sql type})
# Types are applied to the CSV on read; dates in the CSVs may be YYYY.MM.DD.
FUNDAMENTALS: dict[str, tuple[str, list[str], dict[str, str]]] = {
    "income_statement": ("income_statement", ["symbol", "date"], {
        "date": "DATE", "symbol": "VARCHAR", "market": "VARCHAR",
        "revenue": "DOUBLE", "net_income": "DOUBLE", "ebit": "DOUBLE"}),
    "balance_sheet": ("balance_sheet", ["symbol", "date"], {
        "date": "DATE", "symbol": "VARCHAR", "market": "VARCHAR",
        "total_assets": "DOUBLE", "total_liabilities": "DOUBLE",
        "long_term_debt": "DOUBLE", "total_equity": "DOUBLE"}),
    "cash_flow": ("cash_flow", ["symbol", "date"], {
        "date": "DATE", "symbol": "VARCHAR", "market": "VARCHAR",
        "free_cash_flow": "DOUBLE"}),
    "ratios": ("ratios", ["symbol", "date"], {
        "date": "DATE", "symbol": "VARCHAR", "market": "VARCHAR",
        "roic": "DOUBLE", "shares_outstanding": "DOUBLE"}),
    "pillar_metrics": ("pillar_metrics", ["symbol"], {
        "symbol": "VARCHAR", "market": "VARCHAR",
        "avg_net_income_5y": "DOUBLE", "avg_fcf_5y": "DOUBLE",
        "latest_revenue": "DOUBLE", "latest_ebit": "DOUBLE",
        "latest_total_assets": "DOUBLE", "latest_total_liabilities": "DOUBLE",
        "latest_long_term_debt": "DOUBLE", "latest_total_equity": "DOUBLE",
        "latest_roic": "DOUBLE", "latest_shares_outstanding": "DOUBLE"}),
}
FUND_META_TYPES = {"symbol": "VARCHAR", "market": "VARCHAR", "last_updated": "DATE"}
OHLC_TYPES = {"date": "DATE", "symbol": "VARCHAR", "market": "VARCHAR",
              "open": "DOUBLE", "high": "DOUBLE", "low": "DOUBLE",
              "close": "DOUBLE", "volume": "BIGINT"}
BAR_TYPES = {"ts": "TIMESTAMP", "symbol": "VARCHAR", "market": "VARCHAR",
             "open": "DOUBLE", "high": "DOUBLE", "low": "DOUBLE",
             "close": "DOUBLE", "volume": "BIGINT"}


# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------
def _q(path: Path) -> str:
    """SQL string literal for a path."""
    return "'" + str(path).replace("'", "''") + "'"


def _read_csv_sql(path: Path, types: dict[str, str]) -> str:
    """
    SELECT over one staging CSV with explicit column types.  Dates and
    timestamps are read as text first so the legacy dotted formats
    (2026.10.23, 2026.10.23D13:30:00.000000000) are accepted alongside ISO.
    """
    raw = {c: "VARCHAR" if t in ("DATE", "TIMESTAMP") else t for c, t in types.items()}
    cols = ", ".join(f"'{c}': '{t}'" for c, t in raw.items())
    out = []
    for c, t in types.items():
        if t == "DATE":
            out.append(f"CAST(replace(\"{c}\", '.', '-') AS DATE) AS \"{c}\"")
        elif t == "TIMESTAMP":
            out.append(f"CAST(replace(substr(\"{c}\", 1, 10), '.', '-') || ' ' || substr(\"{c}\", 12) "
                       f"AS TIMESTAMP) AS \"{c}\"")
        else:
            out.append(f'"{c}"')
    return (f"SELECT {', '.join(out)} FROM read_csv({_q(path)}, header=true, "
            f"columns={{{cols}}}, nullstr='')")


def _upsert(con: duckdb.DuckDBPyConnection, target: Path, new_sql: str,
            keys: list[str], order: list[str]) -> int:
    """
    Merge `new_sql` into the Parquet file at `target` on `keys` (new rows win)
    and rewrite it atomically, sorted by `order`.  Returns the row count.
    """
    target.parent.mkdir(parents=True, exist_ok=True)
    key_list = ", ".join(f'"{k}"' for k in keys)
    ord_list = ", ".join(f'"{o}"' for o in order)
    parts = [f"SELECT *, 1 AS _prio FROM ({new_sql})"]
    if target.exists():
        parts.append(f"SELECT *, 0 AS _prio FROM read_parquet({_q(target)})")
    merged = (
        f"SELECT * EXCLUDE (_prio, _rn) FROM ("
        f"SELECT *, row_number() OVER (PARTITION BY {key_list} ORDER BY _prio DESC) AS _rn "
        f"FROM ({' UNION ALL BY NAME '.join(parts)})) WHERE _rn = 1 ORDER BY {ord_list}"
    )
    tmp = target.with_name(target.name + ".tmp")
    con.execute(f"COPY ({merged}) TO {_q(tmp)} (FORMAT PARQUET, COMPRESSION ZSTD)")
    os.replace(tmp, target)
    return con.execute(f"SELECT count(*) FROM read_parquet({_q(target)})").fetchone()[0]


def _csv_paths(pattern: str, markets: tuple[str, ...]) -> list[Path]:
    paths = [DATA_DIR / pattern.format(m=m.lower()) for m in markets]
    return [p for p in paths if p.exists()]


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------
def load_fundamentals(markets: tuple[str, ...] = MARKETS) -> None:
    con = duckdb.connect()
    for name, (stem, keys, types) in FUNDAMENTALS.items():
        for csv_path in _csv_paths(f"fundamentals/{stem}_{{m}}.csv", markets):
            n = _upsert(con, LAKE_DIR / "fundamentals" / f"{name}.parquet",
                        _read_csv_sql(csv_path, types), keys, keys)
            log.info("%s <- %s (table now %d rows)", name, csv_path.name, n)
    meta = DATA_DIR / "fund_meta.csv"
    if meta.exists():
        meta_types = {**FUND_META_TYPES}
        # Header is symbol, market, last_updated; comment lines start with '#'
        n = _upsert(con, LAKE_DIR / "fundamentals" / "fund_meta.parquet",
                    _read_csv_sql(meta, meta_types), ["symbol"], ["symbol"])
        log.info("fund_meta <- %s (%d rows)", meta.name, n)


def load_ohlc(markets: tuple[str, ...] = MARKETS, daily: bool = False) -> None:
    """Full-history year files (ohlc_YYYY_<m>.csv) or the incremental ohlc_daily_<m>.csv."""
    con = duckdb.connect()
    pattern = "ohlc_daily_{m}.csv" if daily else "ohlc_????_{m}.csv"
    files: list[Path] = []
    for m in markets:
        files += sorted((DATA_DIR / "ohlc").glob(pattern.format(m=m.lower())))
    if not files:
        log.warning("no OHLC CSVs found in %s", DATA_DIR / "ohlc")
        return
    for f in files:
        src = _read_csv_sql(f, OHLC_TYPES)
        years = [r[0] for r in con.execute(
            f"SELECT DISTINCT year(date) FROM ({src}) ORDER BY 1").fetchall()]
        for yr in years:
            n = _upsert(con, LAKE_DIR / "ohlc" / f"year={yr}" / "ohlc.parquet",
                        f"SELECT * FROM ({src}) WHERE year(date) = {yr}",
                        ["symbol", "date"], ["date", "symbol"])
            log.info("ohlc %d <- %s (partition now %d rows)", yr, f.name, n)


def load_bars(interval: str = "5m", since: date | None = None,
              markets: tuple[str, ...] = MARKETS) -> int:
    """Upsert data/bars/<interval>/<m>/YYYY-MM-DD.csv into per-day Parquet files."""
    since = since or date.today() - timedelta(days=7)
    con = duckdb.connect()
    days: dict[str, list[Path]] = {}
    for m in markets:
        for p in sorted((DATA_DIR / "bars" / interval / m.lower()).glob("*.csv")):
            if re.fullmatch(r"\d{4}-\d{2}-\d{2}", p.stem) and date.fromisoformat(p.stem) >= since:
                days.setdefault(p.stem, []).append(p)
    for day, paths in sorted(days.items()):
        target = LAKE_DIR / "bars" / f"interval={interval}" / f"date={day}" / "bars.parquet"
        for p in paths:
            n = _upsert(con, target, _read_csv_sql(p, BAR_TYPES),
                        ["symbol", "ts"], ["symbol", "ts"])
        log.info("bars %s %s -> %d rows", interval, day, n)
    if not days:
        log.info("no bar CSVs on/after %s — nothing to load", since)
    return len(days)


def load_all(markets: tuple[str, ...] = MARKETS) -> None:
    load_fundamentals(markets)
    load_ohlc(markets)


# ---------------------------------------------------------------------------
def main() -> None:
    ap = argparse.ArgumentParser(description="Load staging CSVs into the Parquet lake")
    ap.add_argument("--load", required=True,
                    choices=["fundamentals", "ohlc", "daily", "bars", "all"])
    ap.add_argument("--market", choices=["IN", "US", "ALL"], default="ALL")
    ap.add_argument("--interval", default="5m")
    ap.add_argument("--since", type=date.fromisoformat, default=None,
                    help="bars only: first day to (re)load, YYYY-MM-DD; default last 7 days")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    mk = MARKETS if a.market == "ALL" else (a.market,)
    {"fundamentals": lambda: load_fundamentals(mk),
     "ohlc": lambda: load_ohlc(mk),
     "daily": lambda: load_ohlc(mk, daily=True),
     "bars": lambda: load_bars(a.interval, a.since, mk),
     "all": lambda: load_all(mk)}[a.load]()


if __name__ == "__main__":
    main()
