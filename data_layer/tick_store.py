"""
Append-only persistence for live bars and raw ticks/L1 (Parquet segments).

  lake/bars/interval=<iv>/date=YYYY-MM-DD/seg-<ms>.parquet   written every flush
  lake/bars/interval=<iv>/date=YYYY-MM-DD/bars.parquet       after compact_day()
  lake/ticks/segment=eq|fo/date=YYYY-MM-DD/seg-<ms>.parquet   raw ticks + L1, one folder per segment (SCRUM-133): the equity
                                                              service owns segment=eq, the F&O job segment=fo
  lake/ticks/segment=.../date=.../ticks.parquet               after compact_ticks_day()
  lake/ticks/date=YYYY-MM-DD/                                 the older layout (both segments in one folder): still read, no
                                                              longer written; `python -m data_layer.tick_store split` converts it

Segments are tiny files written once and never modified, so a crash loses at most
the current flush.  `compact_day` merges a day's segments (and any existing
bars.parquet) into one deduplicated file; run it after the close.  Newest segment
wins on (symbol, ts).  The Lake query views read segments and compacted files alike.
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import sys
import time as _time
from contextlib import contextmanager
from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import Iterable

import duckdb
import pandas as pd

try:                                                                    # POSIX only; see _folder_lock
    import fcntl
except ImportError:                                                     # pragma: no cover
    fcntl = None

from .lake import LAKE_DIR
from .ticks import Bar, Tick


# extended stream fields (see ticks.Tick): written only when a batch carries them, so output is unchanged while off
EXT_COLUMNS = {"ltq": "Int64", "atp": "float64", "tbq": "Int64", "tsq": "Int64", "cp": "float64", "iep": "float64",
               "ieq": "Int64", "iiq_total": "Int64", "iiq_market": "Int64", "ref_price": "float64", "cas_eligible": "boolean"}


SEGMENTS = ("eq", "fo")                         # equities, and futures and options
FO_REGEX = "_(?:CE|PE|FUT)_"                  # lake symbols of futures and options, e.g. NIFTY_25000_CE_09_NOV_26, NIFTY_FUT_23_NOV_26
_FO_RE = re.compile(FO_REGEX)
OPTION_ONLY = ("iv", "delta", "theta", "gamma", "vega", "rho", "oi")      # carried by futures and options, empty or zero on a stock
ZERO_IS_EMPTY = ("delta", "gamma", "oi")        # the feed sends 0.0 for these on stocks: constant zeros carry nothing


INDEX_PREFIX = "NSE_INDEX:"                     # lake symbols of indices (India VIX included): stored with the stocks, shown apart from them


def is_index(symbol: str) -> bool:
    return symbol.startswith(INDEX_PREFIX)


def segment_of(symbol: str) -> str:
    return "fo" if _FO_RE.search(symbol) else "eq"


def segment_folder(lake_dir: Path, day, segment: str) -> Path:
    return lake_dir / "ticks" / f"segment={segment}" / f"date={day}"


def legacy_folder(lake_dir: Path, day) -> Path:
    return lake_dir / "ticks" / f"date={day}"


def day_folders(lake_dir: Path | None, day, segment: str | None = None) -> list[Path]:
    """Existing folders that hold the ticks of `day`: the older single folder (both segments in it) and the segment folders (all, or one)."""
    lake = lake_dir or LAKE_DIR
    wanted = SEGMENTS if segment is None else (segment,)
    return [f for f in [legacy_folder(lake, day), *[segment_folder(lake, day, sg) for sg in wanted]] if f.is_dir()]


def day_files(lake_dir: Path | None, day, segment: str | None = None) -> list[Path]:
    """Every parquet file of `day`'s ticks: segments and the compacted file, in the older layout and the new one."""
    return [f for folder in day_folders(lake_dir, day, segment) for f in sorted(folder.glob("*.parquet"))]


def tick_days(lake_dir: Path | None) -> set[str]:
    """Days (YYYY-MM-DD) that have tick files in any layout."""
    base = (lake_dir or LAKE_DIR) / "ticks"
    folders = [*base.glob("date=*"), *base.glob("segment=*/date=*")]
    return {f.name.split("=", 1)[1] for f in folders if any(f.glob("*.parquet"))}


def _trim_for_segment(df: pd.DataFrame, segment: str) -> pd.DataFrame:
    """A stock has no Greeks and no open interest: leave those columns out when they are empty (or, for delta, gamma and oi, constant
    zero as the feed sends them) instead of storing a column of nothing.  Futures and options keep everything."""
    if segment != "eq":
        return df
    drop = []
    for c in OPTION_ONLY:
        if c not in df.columns:
            continue
        col = df[c].dropna()
        if col.empty or (c in ZERO_IS_EMPTY and (col == 0).all()):
            drop.append(c)
    return df.drop(columns=drop)


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
    """Write raw ticks (with L1 fields) as files: one per segment (equity or futures and options, by symbol) and UTC day."""
    df = pd.DataFrame([{s: getattr(t, s) for s in Tick.__slots__} for t in ticks])
    if df.empty:
        return 0
    for c in ("iv", "delta", "theta", "gamma", "vega", "rho"):   # stable type so equity-only and option segments union
        df[c] = df[c].astype("float32")
    df["ltp"] = df["ltp"].astype("float64")                      # None for a quote-only row; keep the type stable
    for c in ("fts", "rts"):                                     # event time and arrival time: only when this batch has them
        if df[c].notna().any():
            df[c] = pd.to_datetime(df[c])
        else:
            df = df.drop(columns=c)
    for c, dtype in EXT_COLUMNS.items():                         # extended stream fields: only when present in this batch
        if df[c].notna().any():
            df[c] = df[c].astype(dtype)
        else:
            df = df.drop(columns=c)
    lake = lake_dir or LAKE_DIR
    df["_segment"] = df["symbol"].str.contains(FO_REGEX, regex=True).map({True: "fo", False: "eq"})      # each tick goes to its segment's folder
    for (segment, day), part in df.groupby(["_segment", df["ts"].dt.date]):
        _write_segment(segment_folder(lake, day, segment), _trim_for_segment(part.drop(columns="_segment"), segment))
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


def order_expr(columns: list[str]) -> str:
    """SQL ordering key from `columns`, most significant first: arrival time, then message time, then last-trade time, whichever the files
    have.  A row tuple, not a coalesce: where arrival times are equal (a coarse or fixed clock) the next column decides, so ordering
    never falls apart into ties."""
    return columns[0] if len(columns) == 1 else "ROW(" + ", ".join(columns) + ")"


@contextmanager
def _folder_lock(folder: Path):
    """An exclusive lock on one day's tick folder, held across processes (flock on a hidden file beside the folder, so the
    day's folder holds nothing but data).  The
    equity service and the F&O job both compact the same folder when they stop at 10:05 UTC; without this two
    compactions could fail on each other's temporary file or merge the same rows twice (SCRUM-114)."""
    if fcntl is None:                                                   # not Linux: no cross-process lock available
        yield
        return
    with open(folder.parent / f".{folder.name}.compact.lock", "a+") as fh:
        fcntl.flock(fh, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(fh, fcntl.LOCK_UN)


COMPACT_MEMORY_GB = 1.5            # DuckDB memory the compaction may use; the sort spills to disk beyond it (SCRUM-127)


def _compact_memory_limit() -> str:
    """The compaction's memory limit, e.g. '2.5GB': JARVIS_COMPACT_MEMORY_GB, else COMPACT_MEMORY_GB.  Without a limit DuckDB took
    about 360 bytes per row (5.5 GB for the 15 million rows of 2026-11-04) and a day twice as large would not fit the box."""
    try:
        gb = float(os.environ.get("JARVIS_COMPACT_MEMORY_GB") or COMPACT_MEMORY_GB)
    except ValueError:
        gb = COMPACT_MEMORY_GB
    return f"{max(gb, 0.1):g}GB"


def compact_ticks_day(day: date, lake_dir: Path | None = None, segment: str | None = None) -> int:
    """Merge a day's raw-tick segment files into one ticks.parquet per folder.  Returns the row count of the folder(s) compacted.

    `segment` ("eq" or "fo") compacts only that segment's folder, which is what each feed does when it stops: the equity service and
    the F&O job no longer share a folder (SCRUM-133).  Without it every folder of the day is compacted (the older layout's single
    folder and each segment's).

    Unlike bars, a tick is a point event written exactly once (no repeated flushes of the same
    bucket to dedupe), so this only concatenates and sorts; it does not drop any row.

    Safe to call from several processes at once: compactions of one folder take turns under a lock, each reads and deletes exactly
    the files it listed under the lock (a segment written meanwhile is left for the next compaction, never merged
    twice) and writes through its own temporary file.

    Memory is capped (see _compact_memory_limit): the sort over symbol and time spills to a temporary directory inside the folder,
    which is removed afterwards.  The result is the same file content as without the cap.
    """
    lake = lake_dir or LAKE_DIR
    folders = [segment_folder(lake, day, segment)] if segment else day_folders(lake, day)
    return sum(_compact_folder(f) for f in folders if f.is_dir())


def _compact_folder(folder: Path) -> int:
    with _folder_lock(folder):
        files = sorted(folder.glob("*.parquet"))
        if not files:
            return 0
        con = duckdb.connect()
        tmp = folder / f"ticks.parquet.{os.getpid()}.tmp"
        spill = folder / f".compact-spill.{os.getpid()}"                 # not *.parquet, so no reader or lister sees it
        try:
            spill.mkdir(exist_ok=True)
            con.execute(f"SET memory_limit='{_compact_memory_limit()}'")
            con.execute(f"SET temp_directory='{spill}'")
            con.execute("SET enable_progress_bar=false")                      # it would write a screenful of bars into the feed's log
            # hive_partitioning=false: the date=YYYY-MM-DD folder must not become a stored column; files compacted before
            # this fix carry one, so it is dropped here too.  An explicit file list, not a glob, so a segment that appears
            # after the listing is not read without also being deleted.
            names = ", ".join("'" + str(f).replace("'", "''") + "'" for f in files)
            src = f"read_parquet([{names}], union_by_name=true, hive_partitioning=false)"
            cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {src}").fetchall()}
            sel = "* EXCLUDE (date)" if "date" in cols else "*"
            order = order_expr([c for c in ("rts", "fts", "ts") if c in cols])         # arrival order when it is known
            con.execute(f"COPY (SELECT {sel} FROM {src} ORDER BY symbol, {order}) TO '{tmp}' (FORMAT PARQUET, COMPRESSION ZSTD)")
            os.replace(tmp, folder / "ticks.parquet")
        finally:
            con.close()
            tmp.unlink(missing_ok=True)
            shutil.rmtree(spill, ignore_errors=True)
        for f in files:
            if f.name != "ticks.parquet":
                f.unlink(missing_ok=True)
        return duckdb.connect().execute(f"SELECT count(*) FROM read_parquet('{folder}/ticks.parquet')").fetchone()[0]


def dedupe_ticks_day(day: date, lake_dir: Path | None = None, keep_backup: bool = True) -> tuple[int, int]:
    """
    Drop the rows that are identical in every column from a day's compacted ticks.parquet (each folder of the day).  Returns (rows
    before, rows after).  A maintenance tool, not part of any scheduled job: use it only for a day shown to hold rows doubled by
    overlapping compactions (row counts above what the feeds logged).  Identical rows also occur naturally when the feed
    repeats an unchanged snapshot, and this removes those as well, so do not run it as routine tidying.  The original stays
    beside the file as ticks.pre-dedupe.bak (not a *.parquet, so no reader sees it) unless keep_backup is False.  Refuses
    while segment files are present: compact first.
    """
    before = after = 0
    for folder in day_folders(lake_dir, day):
        b, a = _dedupe_folder(folder, keep_backup)
        before, after = before + b, after + a
    return before, after


def _dedupe_folder(folder: Path, keep_backup: bool) -> tuple[int, int]:
    f = folder / "ticks.parquet"
    if not f.exists():
        return 0, 0
    with _folder_lock(folder):
        extra = [x.name for x in folder.glob("*.parquet") if x.name != "ticks.parquet"]
        if extra:
            raise RuntimeError(f"{len(extra)} segment files are still in {folder}; run compact first")
        tmp = folder / f"ticks.parquet.dedupe.{os.getpid()}.tmp"
        con = duckdb.connect()
        try:
            before = con.execute(f"SELECT count(*) FROM read_parquet('{f}')").fetchone()[0]
            con.execute(f"COPY (SELECT DISTINCT * FROM read_parquet('{f}') ORDER BY symbol, ts) TO '{tmp}' (FORMAT PARQUET, COMPRESSION ZSTD)")
            after = con.execute(f"SELECT count(*) FROM read_parquet('{tmp}')").fetchone()[0]
            if keep_backup:
                shutil.copy2(f, folder / "ticks.pre-dedupe.bak")
            os.replace(tmp, f)
        finally:
            con.close()
            tmp.unlink(missing_ok=True)
        return before, after


def split_ticks_day(day: date, lake_dir: Path | None = None) -> dict:
    """Convert one day from the older layout (ticks/date=DAY: stocks and options in one file) to the segment layout.

    Reads the compacted ticks/date=DAY/ticks.parquet and writes ticks/segment=eq/date=DAY/ticks.parquet and
    ticks/segment=fo/date=DAY/ticks.parquet, each sorted as compaction sorts.  The equity file leaves out the option-only columns that
    are empty or constant zero (the same rule as for new ticks).  Before anything is moved it checks that the row counts add up and that
    every row of the original, restricted to the kept columns, is in the new files and the other way round.  The original folder is then
    MOVED to lake/_pre_split/date=DAY (outside ticks/, so no reader sees it); nothing is deleted.  Run it when the feeds are not writing
    that day.  Returns the numbers; raises if the day is not compacted, is already split, or a check fails (then nothing was moved)."""
    lake = lake_dir or LAKE_DIR
    old = legacy_folder(lake, day)
    if not old.is_dir() or not any(old.glob("*.parquet")):
        return {"day": str(day), "skipped": "no ticks in the older layout"}
    done = [segment_folder(lake, day, sg) for sg in SEGMENTS if segment_folder(lake, day, sg).is_dir() and any(segment_folder(lake, day, sg).glob("*.parquet"))]
    if done:
        raise RuntimeError(f"{day} already has segment folders ({[str(d) for d in done]}); refusing to mix them with the older layout")
    with _folder_lock(old):
        extra = [x.name for x in old.glob("*.parquet") if x.name != "ticks.parquet"]
        if extra or not (old / "ticks.parquet").exists():
            raise RuntimeError(f"{old} holds uncompacted files ({len(extra)}); run compact first")
        src_file = old / "ticks.parquet"
        src = f"read_parquet('{src_file}', hive_partitioning=false)"
        spill = lake / "ticks" / f".split-spill.{os.getpid()}"
        spill.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect()
        out: dict = {"day": str(day), "bytes_before": src_file.stat().st_size, "segments": {}}
        written: list[Path] = []
        try:
            con.execute(f"SET memory_limit='{_compact_memory_limit()}'")
            con.execute(f"SET temp_directory='{spill}'")
            con.execute("SET enable_progress_bar=false")
            cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {src}").fetchall()]
            cols = [c for c in cols if c != "date"]
            order = order_expr([c for c in ("rts", "fts", "ts") if c in cols])
            total = con.execute(f"SELECT count(*) FROM {src}").fetchone()[0]
            for sg in SEGMENTS:
                cond = f"{'' if sg == 'fo' else 'NOT '}regexp_matches(symbol, '{FO_REGEX}')"
                keep = list(cols)
                if sg == "eq":                                           # leave out what a stock never has
                    for c in OPTION_ONLY:
                        if c in keep:
                            n, nz = con.execute(f"SELECT count({c}), count({c}) FILTER (WHERE {c} <> 0) FROM {src} WHERE {cond}").fetchone()
                            if n == 0 or (c in ZERO_IS_EMPTY and nz == 0):
                                keep.remove(c)
                folder = segment_folder(lake, day, sg)
                folder.mkdir(parents=True, exist_ok=True)
                tmp = folder / "ticks.parquet.tmp"
                sel = ", ".join(keep)
                con.execute(f"COPY (SELECT {sel} FROM {src} WHERE {cond} ORDER BY symbol, {order}) TO '{tmp}' (FORMAT PARQUET, COMPRESSION ZSTD)")
                final = folder / "ticks.parquet"
                os.replace(tmp, final)
                written.append(final)
                new = f"read_parquet('{final}', hive_partitioning=false)"
                rows = con.execute(f"SELECT count(*) FROM {new}").fetchone()[0]
                lost = con.execute(f"SELECT count(*) FROM (SELECT {sel} FROM {src} WHERE {cond} EXCEPT ALL SELECT {sel} FROM {new})").fetchone()[0]
                extra_rows = con.execute(f"SELECT count(*) FROM (SELECT {sel} FROM {new} EXCEPT ALL SELECT {sel} FROM {src} WHERE {cond})").fetchone()[0]
                if lost or extra_rows:
                    raise RuntimeError(f"{day} {sg}: the new file differs from the original ({lost} rows missing, {extra_rows} rows extra)")
                out["segments"][sg] = {"rows": rows, "columns": len(keep), "dropped": [c for c in cols if c not in keep], "bytes": final.stat().st_size}
            if sum(v["rows"] for v in out["segments"].values()) != total:
                raise RuntimeError(f"{day}: {total} rows in the original but {sum(v['rows'] for v in out['segments'].values())} in the segment files")
            out["rows"] = total
        except BaseException:
            for f in written:                                           # a check failed: take the new files back, leave the original alone
                f.unlink(missing_ok=True)
            for sg in SEGMENTS:
                try:
                    segment_folder(lake, day, sg).rmdir()
                except OSError:
                    pass
            raise
        finally:
            con.close()
            shutil.rmtree(spill, ignore_errors=True)
        keep_dir = lake / "_pre_split" / f"date={day}"
        keep_dir.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(old), str(keep_dir))
        out["original_moved_to"] = str(keep_dir)
        out["bytes_after"] = sum(v["bytes"] for v in out["segments"].values())
        return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Maintenance for a day's raw ticks")
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name, text in (("compact", "merge the day's segment files into ticks.parquet (every folder of the day)"),
                       ("dedupe", "remove rows identical in every column from the compacted day, only for a day shown to hold doubled rows (keeps a .bak copy)")):
        c = sub.add_parser(name, help=text)
        c.add_argument("day", type=date.fromisoformat)
        c.add_argument("--lake", type=Path, default=None)
        if name == "dedupe":
            c.add_argument("--no-backup", action="store_true")
    sp = sub.add_parser("split", help="convert days from the older layout (stocks and options in one file) to lake/ticks/segment=eq|fo; "
                                      "checks the result row for row and moves the original to lake/_pre_split (nothing is deleted)")
    sp.add_argument("days", nargs="*", type=date.fromisoformat, help="days to convert (default: every day still in the older layout)")
    sp.add_argument("--lake", type=Path, default=None)
    a = ap.parse_args(argv)
    if a.cmd == "compact":
        print(f"{a.day}: {compact_ticks_day(a.day, a.lake)} rows")
    elif a.cmd == "split":
        lake = a.lake or LAKE_DIR
        days = a.days or sorted(date.fromisoformat(p.name.split("=", 1)[1]) for p in (lake / "ticks").glob("date=*") if any(p.glob("*.parquet")))
        if not days:
            print("nothing to split: no day is in the older layout")
        for d in days:
            r = split_ticks_day(d, lake)
            if "skipped" in r:
                print(f"{d}: {r['skipped']}")
                continue
            print(f"{d}: {r['rows']:,} rows, {r['bytes_before'] / 1e6:.1f} MB -> {r['bytes_after'] / 1e6:.1f} MB "
                  f"({(r['bytes_after'] / r['bytes_before'] - 1) * 100:+.1f} %)")
            for sg, v in r["segments"].items():
                print(f"    {sg}: {v['rows']:,} rows, {v['bytes'] / 1e6:.1f} MB, {v['columns']} columns, left out: {', '.join(v['dropped']) or 'nothing'}")
            print(f"    checked row for row; original moved to {r['original_moved_to']}")
    else:
        before, after = dedupe_ticks_day(a.day, a.lake, keep_backup=not a.no_backup)
        print(f"{a.day}: {before} rows before, {after} after, {before - after} duplicates removed")
    return 0



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


if __name__ == "__main__":
    sys.exit(main())
