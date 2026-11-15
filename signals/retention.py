"""
The live signal table after the close (owner decision 15 Nov 2026): python -m signals nightly runs this after the day's replay.

During the session every final signal row is written to <store>/live/date=D/ (a new small file every 15 s, for the viewer and the
strategy); after the close the nightly replay writes the same rows to the permanent history
(<store>/signals/<id>/v<n>/intraday/bucket=<1s|10s|1m>/underlying=<U>/date=D/).  So each row is stored twice, and nothing removed the
live copy.  This module, for each night:

  1. compare     row counts per (underlying, signal, bucket): live against history, logged (the two can differ where live dropped a
                 late tick or the engine restarted: the comparison is a check, not a gate)
  2. compact     the day's live files into one file (written to a temporary name, row count checked against the parts, then renamed;
                 only then are the parts removed)
  3. remove      live folders older than KEEP_DAYS days, only when every (underlying, signal) in that folder has its history for that
                 day on disk; a folder that fails the check is kept and reported

Nothing else is touched.  The history and the lake are never modified here.
"""

from __future__ import annotations

import shutil
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd

from signals import catalogue  # noqa: F401  (registers every signal)
from signals.framework import REGISTRY

KEEP_DAYS = 5
COMPACT_NAME = "live-compacted.parquet"
LAYER = {"1s": "1s", "10s": "10s", "1min": "1m", "1m": "1m"}


def _q(files) -> str:
    return "[" + ", ".join("'" + str(f).replace("'", "''") + "'" for f in files) + "]"


def live_folder(store: Path, day: date) -> Path:
    return Path(store) / "live" / f"date={day}"


def live_counts(store: Path, day: date) -> pd.DataFrame:
    """Rows of the day's live table per underlying, signal and bucket."""
    files = sorted(live_folder(store, day).glob("*.parquet"))
    if not files:
        return pd.DataFrame(columns=["underlying", "signal", "bucket", "rows"])
    con = duckdb.connect()
    cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet({_q(files)}, union_by_name=true)").fetchall()}
    u = "underlying" if "underlying" in cols else "NULL"
    b = "bucket" if "bucket" in cols else "NULL"
    df = con.execute(f"SELECT {u} AS underlying, signal, {b} AS bucket, count(*) AS rows FROM read_parquet({_q(files)}, union_by_name=true) "
                     "GROUP BY 1, 2, 3").df()
    con.close()
    return df


def history_dirs(store: Path, sid: str, underlying: str, day: date) -> list[Path]:
    """The day's history folders of a signal (any intraday bucket layer, and the batch layer), that hold a part file."""
    if sid not in REGISTRY:
        return []
    base = Path(store) / "signals" / sid / f"v{REGISTRY[sid][0].version}"
    cands = [base / f"underlying={underlying}" / f"date={day}"]
    cands += sorted((base / "intraday").glob(f"bucket=*/underlying={underlying}/date={day}"))
    return [c for c in cands if (c / "part.parquet").exists()]


def history_rows(store: Path, sid: str, underlying: str, day: date, bucket: Optional[str]) -> Optional[int]:
    if sid not in REGISTRY or bucket is None:
        return None
    f = (Path(store) / "signals" / sid / f"v{REGISTRY[sid][0].version}" / "intraday" / f"bucket={LAYER.get(bucket, bucket)}"
         / f"underlying={underlying}" / f"date={day}" / "part.parquet")
    if not f.exists():
        return None
    con = duckdb.connect()
    n = con.execute(f"SELECT count(*) FROM read_parquet('{f}')").fetchone()[0]
    con.close()
    return int(n)


def compare(store: Path, day: date) -> pd.DataFrame:
    """Live rows against history rows per (underlying, signal, bucket)."""
    lc = live_counts(store, day)
    if not len(lc):
        return lc
    lc["history_rows"] = [history_rows(store, s, u, day, b) for u, s, b in zip(lc["underlying"], lc["signal"], lc["bucket"])]
    return lc.sort_values(["underlying", "signal", "bucket"]).reset_index(drop=True)


def compact(store: Path, day: date) -> dict:
    """Merge the day's live files into one.  The parts are removed only after the merged file holds exactly their rows."""
    folder = live_folder(store, day)
    parts = sorted(f for f in folder.glob("*.parquet") if f.name != COMPACT_NAME) if folder.is_dir() else []
    if len(parts) <= 1:
        return {"day": str(day), "files": len(parts), "compacted": False}
    files = parts + ([folder / COMPACT_NAME] if (folder / COMPACT_NAME).exists() else [])
    tmp = folder / (COMPACT_NAME + ".tmp")
    con = duckdb.connect()
    con.execute("SET memory_limit='1GB'")
    want = con.execute(f"SELECT count(*) FROM read_parquet({_q(files)}, union_by_name=true)").fetchone()[0]
    con.execute(f"COPY (SELECT * FROM read_parquet({_q(files)}, union_by_name=true)) TO '{tmp}' (FORMAT PARQUET)")
    got = con.execute(f"SELECT count(*) FROM read_parquet('{tmp}')").fetchone()[0]
    con.close()
    if got != want:
        tmp.unlink(missing_ok=True)
        return {"day": str(day), "files": len(parts), "compacted": False, "error": f"merged {got} rows, parts hold {want}"}
    tmp.replace(folder / COMPACT_NAME)
    for p in parts:
        p.unlink()
    return {"day": str(day), "files": len(parts), "compacted": True, "rows": int(got)}


def remove_old(store: Path, today: date, keep_days: int = KEEP_DAYS) -> list[dict]:
    """Remove live folders older than keep_days whose every (underlying, signal) has its history for that day."""
    out = []
    root = Path(store) / "live"
    for folder in sorted(root.glob("date=*")) if root.is_dir() else []:
        try:
            d = date.fromisoformat(folder.name.split("=", 1)[1])
        except ValueError:
            continue
        if d >= today - timedelta(days=keep_days):
            continue
        lc = live_counts(store, d)
        missing = sorted({f"{u}/{s}" for u, s in zip(lc["underlying"], lc["signal"]) if not history_dirs(store, s, u, d)})
        if missing:
            out.append({"day": str(d), "removed": False, "missing_history": missing[:10], "missing_count": len(missing)})
            continue
        shutil.rmtree(folder)
        out.append({"day": str(d), "removed": True, "rows": int(lc["rows"].sum()) if len(lc) else 0})
    return out


def nightly(store: Path, day: date, keep_days: int = KEEP_DAYS, log=print) -> dict:
    """Compare, compact the day, remove old folders.  Never raises (the nightly signal history is already written)."""
    res: dict = {"day": str(day)}
    try:
        cmp_ = compare(store, day)
        if len(cmp_):
            diff = cmp_[cmp_["history_rows"].notna() & (cmp_["rows"] != cmp_["history_rows"])]
            nohist = cmp_[cmp_["history_rows"].isna()]
            res["compared"] = len(cmp_)
            res["differ"] = diff[["underlying", "signal", "bucket", "rows", "history_rows"]].to_dict("records")
            res["no_history_at_that_bucket"] = len(nohist)
            for r in res["differ"][:20]:
                log(f"live vs history {day} {r['underlying']} {r['signal']} {r['bucket']}: live {r['rows']}, history {r['history_rows']}")
        res["compact"] = compact(store, day)
        res["removed"] = remove_old(store, day, keep_days)
        for r in res["removed"]:
            log(f"live {r['day']}: " + ("removed" if r["removed"] else f"kept, history missing for {r['missing_count']} signals"))
    except Exception as exc:                                       # noqa: BLE001 -- retention must never fail the nightly job
        res["error"] = repr(exc)
        log(f"live retention {day} failed: {exc!r}")
    return res
