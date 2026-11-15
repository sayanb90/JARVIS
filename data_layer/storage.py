"""
Where the disk goes, and how long it lasts: python -m data_layer.storage report [--json] [--days 10]

Walks the lake, the signal store, the research snapshots and the logs, and reports per dataset: size on disk, files, and for day-
partitioned data the size per day over the last sessions.  Hard links are counted once (the research snapshots and the backup share
files with the lake): each file is charged to the first place it is found, in the order lake, signal store, snapshots, logs, so the
snapshot line shows only what the snapshots alone hold.

Then the projection: free space on the disk, growth per session (tick days and the signal store's per-session layers), and sessions of
runway at that rate (250 sessions a year).  Each dataset is tagged with where it should live:
  keep    needed every day for research, signals, the viewer (aggregates, signal history, reference tables)
  offload raw tick days older than OFFLOAD_AFTER_DAYS: replayable, rarely read, the bulk of the growth (Backblaze B2 or similar)
A line of the report is appended to logs/storage.jsonl on each run (cron, after the close), so growth is tracked over time.
Read only.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sys
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

OFFLOAD_AFTER_DAYS = 30
SESSIONS_PER_YEAR = 250
DATE_RE = re.compile(r"date=(\d{4}-\d{2}-\d{2})")


def _walk(root: Path, seen: set):
    """(path, size) of every regular file under root not already charged (by device and inode)."""
    if not root.exists():
        return
    for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
        for f in filenames:
            p = Path(dirpath) / f
            try:
                st = p.lstat()
            except OSError:
                continue
            if not os.path.isfile(p) or os.path.islink(p):
                continue
            key = (st.st_dev, st.st_ino)
            if key in seen:
                continue
            seen.add(key)
            yield p, st.st_blocks * 512 if hasattr(st, "st_blocks") else st.st_size


def dataset_of(rel: Path, area: str) -> tuple[str, str]:
    """(dataset name, where it should live) of a file, from its path relative to its area root."""
    parts = rel.parts
    if area == "lake":
        if parts[0] == "ticks":
            seg = next((p for p in parts if p.startswith("segment=")), "segment=?")
            return f"lake/ticks/{seg}", "offload"
        if parts[0] in ("bars", "ohlc") and len(parts) > 1:
            return f"lake/{parts[0]}/{parts[1]}", "keep"
        return f"lake/{parts[0]}", "keep"
    if area == "store":
        if parts[0] == "signals" and len(parts) > 2:
            layer = "intraday" if "intraday" in parts else "batch"
            if "source=candles" in parts:
                layer = "intraday-candles"
            b = next((p for p in parts if p.startswith("bucket=")), "")
            return f"store/signals ({layer}{' ' + b if b else ''})", "keep"
        return f"store/{parts[0]}", "keep"
    return area, "keep"


def scan(lake: Path, store: Optional[Path], snapshots: Optional[Path], logs: Optional[Path]) -> dict:
    seen: set = set()
    sizes: dict = defaultdict(lambda: {"bytes": 0, "files": 0, "where": "keep"})
    per_day: dict = defaultdict(lambda: defaultdict(int))              # dataset -> day -> bytes
    for area, root in (("lake", lake), ("store", store), ("snapshots", snapshots), ("logs", logs)):
        if root is None:
            continue
        for p, sz in _walk(Path(root), seen):
            try:
                rel = p.relative_to(root)
            except ValueError:
                continue
            name, where = dataset_of(rel, area) if area in ("lake", "store") else (area, "keep")
            d = sizes[name]
            d["bytes"] += sz
            d["files"] += 1
            d["where"] = where
            m = DATE_RE.search(str(rel))
            if m:
                per_day[name][m.group(1)] += sz
    return {"sizes": dict(sizes), "per_day": {k: dict(v) for k, v in per_day.items()}}


def report(lake: Path, store: Optional[Path] = None, snapshots: Optional[Path] = None, logs: Optional[Path] = None,
           days: int = 10, today: Optional[date] = None) -> dict:
    today = today or datetime.now(timezone.utc).date()
    s = scan(lake, store, snapshots, logs)
    du = shutil.disk_usage(lake if Path(lake).exists() else "/")
    growth, rows = 0.0, []
    for name, d in sorted(s["sizes"].items(), key=lambda x: -x[1]["bytes"]):
        pd_ = s["per_day"].get(name, {})
        recent = sorted(k for k in pd_ if k < str(today))[-days:]
        per_session = sum(pd_[k] for k in recent) / len(recent) if recent else 0.0
        old = sum(v for k, v in pd_.items() if (today - date.fromisoformat(k)).days > OFFLOAD_AFTER_DAYS)
        if name.startswith("lake/ticks") or name.startswith("store/signals (intraday") or name.startswith("store/live"):
            growth += per_session
        rows.append({"dataset": name, "gb": d["bytes"] / 1e9, "files": d["files"], "where": d["where"],
                     "mb_per_session": per_session / 1e6 if recent else None, "sessions_seen": len(pd_),
                     "offloadable_gb": old / 1e9 if d["where"] == "offload" else 0.0})
    runway = du.free / growth if growth else None
    return {"at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "disk_total_gb": du.total / 1e9, "disk_free_gb": du.free / 1e9,
            "growth_mb_per_session": growth / 1e6, "runway_sessions": runway,
            "runway_years": runway / SESSIONS_PER_YEAR if runway else None, "datasets": rows}


def to_text(r: dict) -> str:
    lines = [f"disk: {r['disk_free_gb']:.1f} GB free of {r['disk_total_gb']:.1f} GB; growth {r['growth_mb_per_session']:.0f} MB a session"
             + (f"; runway {r['runway_sessions']:.0f} sessions ({r['runway_years']:.1f} years)" if r["runway_sessions"] else ""),
             f"{'dataset':<48} {'GB':>8} {'files':>8} {'MB/session':>11} {'sessions':>8} {'where':>8} {'offloadable GB':>15}"]
    for d in r["datasets"]:
        mps = f"{d['mb_per_session']:.1f}" if d["mb_per_session"] is not None else "-"
        lines.append(f"{d['dataset']:<48} {d['gb']:>8.2f} {d['files']:>8} {mps:>11} {d['sessions_seen']:>8} {d['where']:>8} "
                     f"{d['offloadable_gb']:>15.2f}")
    return "\n".join(lines)


def main(argv=None) -> int:
    from data_layer.lake import LAKE_DIR
    ap = argparse.ArgumentParser(prog="python -m data_layer.storage")
    sub = ap.add_subparsers(dest="cmd", required=True)
    rp = sub.add_parser("report")
    rp.add_argument("--lake", type=Path, default=Path(LAKE_DIR))
    rp.add_argument("--store", type=Path, default=Path(os.environ.get("JARVIS_SIGNAL_STORE", "/srv/jarvis-research/store")))
    rp.add_argument("--snapshots", type=Path, default=Path(os.environ.get("JARVIS_RESEARCH_ROOT", "/srv/jarvis-research")) / "snapshots")
    rp.add_argument("--logs", type=Path, default=Path("logs"))
    rp.add_argument("--days", type=int, default=10)
    rp.add_argument("--json", action="store_true")
    a = ap.parse_args(argv)
    r = report(a.lake, a.store, a.snapshots, a.logs, a.days)
    print(json.dumps(r) if a.json else to_text(r))
    if a.logs:
        a.logs.mkdir(parents=True, exist_ok=True)
        with (a.logs / "storage.jsonl").open("a") as f:
            f.write(json.dumps(r) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
