"""
Half-spreads measured from the tick era's quotes (SCRUM-218), for the backtester's market fills.

Before 3 Nov 2026 the history is 1-minute candles: traded prices only, no bid or ask, so a backtest has to assume what crossing the spread costs.  This
measures it from the live feed's quotes (best bid and ask on every F&O tick) and keeps a table the backtest looks up per fill:

  half-spread = (ask - bid) / (ask + bid), a fraction of the mid (what a market order pays against the mid, each way)

by bucket (BANDS below): kind (future | option); for options the premium band, the moneyness (strike against the front future, out of the money > 0)
and the days to expiry; and the time of day (IST).  Coarser levels are kept as well (GROUPING SETS: a bucket with too few quotes falls back to the
next coarser one).  Median and 75th percentile.

  python -m backtester spreads [--underlying NIFTY] [--from 2026-11-03] [--to D] [--lake L] [--store S]   ->  <store>/reports/spreads-<U>.parquet

Quotes are read by DuckDB straight from lake/ticks/segment=fo (bounded memory, approximate quantiles); nothing is written but the table.
"""

from __future__ import annotations

import math
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd

MIN_QUOTES = 200                                        # a bucket with fewer quotes is not used: the lookup falls back to a coarser one
# (upper edge, label); the last edge is +inf
PREMIUM = [(20, "<20"), (50, "20-50"), (150, "50-150"), (400, "150-400"), (math.inf, "400+")]
MONEYNESS = [(-0.02, "itm>2%"), (-0.005, "itm"), (0.005, "atm"), (0.02, "otm"), (math.inf, "otm>2%")]
DTE = [(0.5, "0"), (1.5, "1"), (3.5, "2-3"), (7.5, "4-7"), (math.inf, "8+")]
TOD = [(15, "open"), (135, "morning"), (285, "midday"), (345, "afternoon"), (math.inf, "close")]      # minutes after 09:15 IST
LEVELS = [("kind", "premium", "moneyness", "dte", "tod"), ("kind", "premium", "moneyness", "dte"), ("kind", "premium", "moneyness"),
          ("kind", "premium"), ("kind", "tod"), ("kind",)]


def band(bands: list, x: float) -> Optional[str]:
    if x is None or x != x:
        return None
    for edge, label in bands:
        if x < edge:
            return label
    return bands[-1][1]


def _case(expr: str, bands: list) -> str:
    whens = " ".join(f"WHEN {expr} < {edge} THEN '{label}'" for edge, label in bands if edge != math.inf)
    return f"CASE WHEN {expr} IS NULL THEN NULL {whens} ELSE '{bands[-1][1]}' END"


def tick_files(lake, d0: date, d1: date) -> list:
    base = Path(lake) / "ticks"
    out = []
    for root in (base / "segment=fo", base):
        for folder in sorted(root.glob("date=*")) if root.is_dir() else []:
            try:
                d = date.fromisoformat(folder.name.split("=", 1)[1])
            except ValueError:
                continue
            if d0 <= d <= d1:
                out += [str(f) for f in sorted(folder.glob("*.parquet")) if not f.name.endswith(".tmp")]
    return out


def measure(lake, underlying: str, d0: date, d1: date, memory_mb: int = 1000) -> pd.DataFrame:
    """The spread table: one row per bucket at every level of LEVELS (columns not in the level are None), with n, p50 and p75."""
    files = tick_files(lake, d0, d1)
    if not files:
        return pd.DataFrame(columns=["kind", "premium", "moneyness", "dte", "tod", "n", "p50", "p75"])
    q = "[" + ", ".join("'" + f.replace("'", "''") + "'" for f in files) + "]"
    u = underlying.replace("'", "")
    tod_min = "(hour(ts) * 60 + minute(ts) + 330 - 555)"
    sets = ", ".join("(" + ", ".join(lv) + ")" for lv in LEVELS)
    sql = f"""
    WITH t AS (SELECT ts, symbol, bid, ask, ltp FROM read_parquet({q}, union_by_name=true)
               WHERE regexp_matches(symbol, '^{u}_') AND bid > 0 AND ask >= bid AND (ask - bid) / (ask + bid) < 0.25),
    fut AS (SELECT date_trunc('minute', ts) AS m, median(ltp) AS spot FROM t WHERE contains(symbol, '_FUT_') GROUP BY 1),
    o AS (SELECT t.ts, (t.ask - t.bid) / (t.ask + t.bid) AS hs, (t.ask + t.bid) / 2 AS mid,
                 CASE WHEN contains(t.symbol, '_FUT_') THEN 'future' ELSE 'option' END AS kind,
                 TRY_CAST(regexp_extract(t.symbol, '^{u}_([0-9.]+)_(CE|PE)_', 1) AS DOUBLE) AS strike,
                 regexp_extract(t.symbol, '_(CE|PE)_', 1) AS cp,
                 TRY_CAST(regexp_extract(t.symbol, '([0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}})$', 1) AS DATE) AS expiry,
                 f.spot
          FROM t LEFT JOIN fut f ON date_trunc('minute', t.ts) = f.m),
    b AS (SELECT hs, kind,
                 CASE WHEN kind = 'option' THEN {_case('mid', PREMIUM)} END AS premium,
                 CASE WHEN kind = 'option' THEN {_case("(CASE WHEN cp = 'CE' THEN strike / spot - 1 ELSE 1 - strike / spot END)", MONEYNESS)} END AS moneyness,
                 CASE WHEN kind = 'option' THEN {_case("date_diff('day', CAST(ts AS DATE), expiry)", DTE)} END AS dte,
                 {_case(tod_min, TOD)} AS tod
          FROM o)
    SELECT kind, premium, moneyness, dte, tod, count(*) AS n, approx_quantile(hs, 0.5) AS p50, approx_quantile(hs, 0.75) AS p75
    FROM b GROUP BY GROUPING SETS ({sets})
    """
    con = duckdb.connect()
    try:
        con.execute("SET threads=2")
        con.execute(f"SET memory_limit='{int(memory_mb)}MB'")
        df = con.execute(sql).df()
    finally:
        con.close()
    return df


def table_path(store, underlying: str) -> Path:
    return Path(store) / "reports" / f"spreads-{underlying}.parquet"


class SpreadTable:
    """Lookup of a measured half-spread (a fraction of the price) for one fill; None when no bucket at any level has MIN_QUOTES quotes."""

    def __init__(self, df: pd.DataFrame, quantile: float = 0.5):
        self.col = "p75" if quantile >= 0.75 else "p50"
        self.rows: dict = {}
        for r in df.itertuples(index=False):
            if r.n >= MIN_QUOTES:
                key = tuple(None if (v is None or v != v) else v for v in (r.kind, r.premium, r.moneyness, r.dte, r.tod))
                self.rows[key] = float(getattr(r, self.col))

    @classmethod
    def load(cls, path, quantile: float = 0.5) -> "SpreadTable":
        return cls(pd.read_parquet(path), quantile)

    def lookup(self, kind: str, price: float, t_utc: pd.Timestamp, strike: Optional[float] = None, right: Optional[str] = None,
               spot: Optional[float] = None, expiry: Optional[date] = None) -> Optional[float]:
        tod = band(TOD, (t_utc.hour * 60 + t_utc.minute) + 330 - 555)
        if kind == "future":
            full = {"kind": "future", "premium": None, "moneyness": None, "dte": None, "tod": tod}
        else:
            m = None
            if strike and spot:
                m = band(MONEYNESS, strike / spot - 1 if right == "CE" else 1 - strike / spot)
            d = band(DTE, (expiry - t_utc.date()).days) if expiry else None
            full = {"kind": "option", "premium": band(PREMIUM, price), "moneyness": m, "dte": d, "tod": tod}
        for level in LEVELS:
            key = tuple(full[c] if c in level else None for c in ("kind", "premium", "moneyness", "dte", "tod"))
            if any(full[c] is None for c in level):
                continue
            if key in self.rows:
                return self.rows[key]
        return None
