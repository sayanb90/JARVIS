"""
Append-only persistence for live bars and raw ticks/L1 (Parquet segments).

  lake/bars/interval=<iv>/date=YYYY-MM-DD/seg-<ms>.parquet   written every flush
  lake/bars/interval=<iv>/date=YYYY-MM-DD/bars.parquet       after compact_day()
  lake/ticks/date=YYYY-MM-DD/seg-<ms>.parquet                optional raw ticks + L1

Segments are tiny files written once and never modified, so a crash loses at most
the current flush.  `compact_day` merges a day's segments (and any existing
bars.parquet) into one deduplicated file; run it after the close.  Newest segment
wins on (symbol, ts).  The Lake query views read segments and compacted files alike.
"""

from __future__ import annotations

import os
import time as _time
from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import Iterable

import duckdb
import pandas as pd

from .lake import LAKE_DIR
from .ticks import Bar, Tick


# extended stream fields (see ticks.Tick): written only when a batch carries them, so output is unchanged while off
EXT_COLUMNS = {"ltq": "Int64", "atp": "float64", "tbq": "Int64", "tsq": "Int64", "cp": "float64", "iep": "float64",
               "ieq": "Int64", "iiq_total": "Int64", "iiq_market": "Int64", "ref_price": "float64", "cas_eligible": "boolean"}


def _write_segment(folder: Path, df: pd.DataFrame) -> Path:
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"seg-{_time.time_ns() // 1_000_000:015d}.parquet"
    tmp = path.with_suffix(".tmp")
    con = duckdb.connect()
    con.register("t", df)
    con.execute(f"COPY (SELECT * FROM t) TO '{tmp}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    con.close()
    os.replace(tmp, path)
    return path


def write_bars(bars: Iterable[Bar], interval: str, lake_dir: Path | None = None) -> int:
    """Write bars as segments, one per UTC day.  Returns the number of rows written."""
    df = pd.DataFrame([asdict(b) for b in bars])
    if df.empty:
        return 0
    df["volume"] = df["volume"].astype("int64")
    for c in ("vwap", "bid", "ask"):                    # stable column types so live and REST segments union cleanly
        df[c] = df[c].astype("float64")
    for c in ("n_ticks", "bid_qty", "ask_qty", "oi"):
        df[c] = df[c].astype("Int64")
    for day, part in df.groupby(df["ts"].dt.date):
        _write_segment((lake_dir or LAKE_DIR) / "bars" / f"interval={interval}" / f"date={day}", part)
    return len(df)


def write_ticks(ticks: Iterable[Tick], lake_dir: Path | None = None) -> int:
    """Write raw ticks (with L1 fields) as segments, one per UTC day."""
    df = pd.DataFrame([{s: getattr(t, s) for s in Tick.__slots__} for t in ticks])
    if df.empty:
        return 0
    for c in ("iv", "delta", "theta", "gamma", "vega", "rho"):   # stable type so equity-only and option segments union
        df[c] = df[c].astype("float32")
    for c, dtype in EXT_COLUMNS.items():                         # extended stream fields: only when present in this batch
        if df[c].notna().any():
            df[c] = df[c].astype(dtype)
        else:
            df = df.drop(columns=c)
    for day, part in df.groupby(df["ts"].dt.date):
        _write_segment((lake_dir or LAKE_DIR) / "ticks" / f"date={day}", part)
    return len(df)


def write_events(events: Iterable[dict], lake_dir: Path | None = None) -> int:
    """Session events (market_info changes: ts, kind, segment, status, updated) as segments, one folder per UTC day."""
    df = pd.DataFrame(list(events))
    if df.empty:
        return 0
    df["ts"] = pd.to_datetime(df["ts"])
    df["updated"] = pd.to_datetime(df["updated"]) if "updated" in df else pd.NaT
    for day, part in df.groupby(df["ts"].dt.date):
        _write_segment((lake_dir or LAKE_DIR) / "events" / f"date={day}", part)
    return len(df)


def compact_ticks_day(day: date, lake_dir: Path | None = None) -> int:
    """Merge a day's raw-tick segments into one ticks.parquet.  Returns row count.

    Unlike bars, a tick is a point event written exactly once (no repeated flushes of the same
    bucket to dedupe), so this only concatenates and sorts; it does not drop any row.
    """
    folder = (lake_dir or LAKE_DIR) / "ticks" / f"date={day}"
    files = sorted(folder.glob("*.parquet"))
    if not files:
        return 0
    con = duckdb.connect()
    tmp = folder / "ticks.parquet.tmp"
    # hive_partitioning=false: the date=YYYY-MM-DD folder must not become a stored column; files compacted before
    # this fix carry one, so it is dropped here too
    src = f"read_parquet('{folder}/*.parquet', union_by_name=true, hive_partitioning=false)"
    cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {src}").fetchall()}
    sel = "* EXCLUDE (date)" if "date" in cols else "*"
    con.execute(f"COPY (SELECT {sel} FROM {src} ORDER BY symbol, ts) TO '{tmp}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    os.replace(tmp, folder / "ticks.parquet")
    for f in files:
        if f.name != "ticks.parquet":
            f.unlink()
    return con.execute(f"SELECT count(*) FROM read_parquet('{folder}/ticks.parquet')").fetchone()[0]


def compact_day(interval: str, day: date, lake_dir: Path | None = None) -> int:
    """Merge segments + bars.parquet for one day into bars.parquet.  Returns row count."""
    folder = (lake_dir or LAKE_DIR) / "bars" / f"interval={interval}" / f"date={day}"
    files = sorted(folder.glob("*.parquet"))
    if not files:
        return 0
    con = duckdb.connect()
    tmp = folder / "bars.parquet.tmp"
    con.execute(f"""
        COPY (
          SELECT * EXCLUDE (filename, _rn) FROM (
            SELECT *, row_number() OVER (PARTITION BY symbol, ts ORDER BY filename DESC) _rn
            FROM read_parquet('{folder}/*.parquet', union_by_name=true, filename=true)
          ) WHERE _rn = 1 ORDER BY symbol, ts
        ) TO '{tmp}' (FORMAT PARQUET, COMPRESSION ZSTD)""")
    os.replace(tmp, folder / "bars.parquet")
    for f in files:
        if f.name != "bars.parquet":
            f.unlink()
    return con.execute(f"SELECT count(*) FROM read_parquet('{folder}/bars.parquet')").fetchone()[0]
