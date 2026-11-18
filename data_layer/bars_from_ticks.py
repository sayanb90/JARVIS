"""
1-minute bars of the indices, India VIX and the futures from our own ticks (17 Nov 2026).

Upstox serves the 1-minute candles of a day only after that day (the 10:30 UTC `backfill recent` job asks for 16 Nov and gets nothing; the next
day's run gets it), so at the next open the engines' history, and with it the daily anchors (HAR forecast, time-of-day profile, VIX percentile),
ended one session early.  The ticks of the day are in the lake from the close, so the bars are built from them.

  python -m data_layer.bars_from_ticks build --date 2026-11-16            one day (the nightly job does this for the newest closed tick day)
  python -m data_layer.bars_from_ticks backfill --from 2026-11-03 --to 2026-11-16

Where they go: lake/bars_ticks/interval=1m/date=D/bars.parquet (market 'IN', src 'ticks'), NOT lake/bars/: the candle job's gap detection
(data_layer/gaps.py) counts a day as present when lake/bars has rows for it, so rows written there would stop the vendor candles of the whole
universe from ever being fetched for that day.  signals/lakeio.read_bars reads both and takes the vendor row where both exist.

The bar rules are those of the engine (signals/live_runner.ticks_to_bars): price = the last trade, else the quote mid; open, high, low and close
of that price in the minute; volume = the change of the day's cumulative counter since the symbol's previous minute (the first minute counts from
its first tick); open interest = the last value in the minute.  Minutes are those of the session, 03:45 to 10:00 UTC, in event time (fts, else ts):
auction ticks before the open are left out, as in the candles.  Indices carry no traded volume (the feed sends zero).
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd

SESSION_UTC = ("03:45:00", "10:00:00")
FUT_REGEX = r"^(NIFTY|BANKNIFTY|FINNIFTY|MIDCPNIFTY|SENSEX)_FUT_"
OUT_COLUMNS = ["ts", "symbol", "open", "high", "low", "close", "volume", "oi", "market", "src"]


def out_dir(lake: Path, day: date) -> Path:
    return Path(lake) / "bars_ticks" / "interval=1m" / f"date={day}"


def _files(lake: Path, day: date) -> list[str]:
    from data_layer.tick_store import day_files
    return [str(f) for f in day_files(Path(lake), day) if not f.name.endswith(".tmp")]


def build_day(lake: Path, day: date, log=print, memory_gb: float = 2.0, threads: int = 2) -> int:
    """Write the day's index, VIX and futures minute bars; returns the rows written (0 when the day has no ticks)."""
    files = _files(lake, day)
    if not files:
        return 0
    q = "[" + ", ".join("'" + f.replace("'", "''") + "'" for f in files) + "]"
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{memory_gb}GB'")
    con.execute(f"SET threads={int(threads)}")
    cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet({q}, union_by_name=true)").fetchall()}
    tcol = "fts" if "fts" in cols else "ts"
    ltp = "ltp" if "ltp" in cols else "CAST(NULL AS DOUBLE)"
    quote = "(bid + ask) / 2" if {"bid", "ask"} <= cols else "CAST(NULL AS DOUBLE)"
    vol = "vol_today" if "vol_today" in cols else "CAST(0 AS DOUBLE)"
    oi = "oi" if "oi" in cols else "CAST(NULL AS DOUBLE)"
    lo, hi = SESSION_UTC
    sql = f"""
    WITH t AS (
        SELECT symbol, CAST({tcol} AS TIMESTAMP) AS t, COALESCE({ltp}, {quote}) AS px, {vol} AS vol, {oi} AS oi
        FROM read_parquet({q}, union_by_name=true)
        WHERE (symbol LIKE 'NSE_INDEX:%' OR symbol LIKE 'BSE_INDEX:%' OR regexp_matches(symbol, '{FUT_REGEX}'))
          AND CAST({tcol} AS TIMESTAMP) >= TIMESTAMP '{day} {lo}' AND CAST({tcol} AS TIMESTAMP) < TIMESTAMP '{day} {hi}'
    ), m AS (
        SELECT symbol, date_trunc('minute', t) AS ts,
               arg_min(px, t) FILTER (WHERE px IS NOT NULL) AS open, max(px) AS high, min(px) AS low,
               arg_max(px, t) FILTER (WHERE px IS NOT NULL) AS close,
               arg_max(vol, t) FILTER (WHERE vol IS NOT NULL) AS vol_last, arg_min(vol, t) FILTER (WHERE vol IS NOT NULL) AS vol_first,
               arg_max(oi, t) FILTER (WHERE oi IS NOT NULL) AS oi
        FROM t GROUP BY symbol, date_trunc('minute', t)
    )
    SELECT ts, symbol, open, high, low, close,
           CAST(GREATEST(COALESCE(vol_last - COALESCE(lag(vol_last) OVER (PARTITION BY symbol ORDER BY ts), vol_first), 0), 0) AS BIGINT) AS volume,
           oi, 'IN' AS market, 'ticks' AS src
    FROM m WHERE close IS NOT NULL ORDER BY symbol, ts
    """
    df = con.execute(sql).df()
    con.close()
    if not len(df):
        return 0
    folder = out_dir(lake, day)
    folder.mkdir(parents=True, exist_ok=True)
    tmp = folder / "bars.parquet.tmp"
    df[OUT_COLUMNS].to_parquet(tmp, index=False)
    tmp.replace(folder / "bars.parquet")
    log(f"bars from ticks {day}: {len(df):,} minute bars, {df['symbol'].nunique()} symbols -> {folder / 'bars.parquet'}")
    return len(df)


def main(argv=None) -> int:
    from data_layer.lake import LAKE_DIR
    ap = argparse.ArgumentParser(prog="python -m data_layer.bars_from_ticks")
    sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build")
    b.add_argument("--date", type=date.fromisoformat, required=True)
    k = sub.add_parser("backfill")
    k.add_argument("--from", dest="d0", type=date.fromisoformat, required=True)
    k.add_argument("--to", dest="d1", type=date.fromisoformat, required=True)
    for p in (b, k):
        p.add_argument("--lake", type=Path, default=Path(LAKE_DIR))
    a = ap.parse_args(argv)
    if a.cmd == "build":
        build_day(a.lake, a.date)
    else:
        d = a.d0
        while d <= a.d1:
            build_day(a.lake, d)
            d += timedelta(days=1)
    return 0


if __name__ == "__main__":
    sys.exit(main())
