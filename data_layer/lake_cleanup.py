"""
Tidy the data lake after the FMP retirement (SCRUM-138): find empty tables and FMP leftovers, and set them aside.

  python -m data_layer.lake_cleanup            # DRY RUN: lists what it would move, changes nothing
  python -m data_layer.lake_cleanup --apply    # moves them into logs/lake-cleanup-<timestamp>/ (same relative paths)

What it looks at:
  * every Parquet table under lake/ except lake/ticks/ (the raw tick days are never touched);
  * the FMP leftovers that nothing reads any more: lake/fundamentals/fund_meta.parquet, data/fund_meta.csv and
    data/fundamentals/*.csv (the CSVs the FMP extractor wrote).

What it moves: a table with 0 rows (a table with rows is never moved, whoever wrote it) and the FMP leftovers above.
What it only reports: a Parquet file that cannot be read (it may be mid-write or damaged: look at it yourself).

Nothing is deleted: --apply moves files, so undoing it is `mv` back from logs/lake-cleanup-<timestamp>/ (the run prints the
folder).  Empty folders left behind are removed.  Run it while no job is writing (any time outside 00:30-03:30 and 22:00-23:30 UTC).
"""

from __future__ import annotations

import argparse
import shutil
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from . import lake as lake_mod

SKIP_TOP_LEVEL = ("ticks",)                                   # raw tick days: large, hive-partitioned, never part of this
FMP_LEFTOVER_FILES = ("lake/fundamentals/fund_meta.parquet", "data/fund_meta.csv")
FMP_LEFTOVER_GLOBS = ("data/fundamentals/*.csv",)


@dataclass
class Item:
    path: Path
    reason: str                                             # "empty table", "FMP leftover", "unreadable"
    detail: str = ""
    move: bool = True


def _rows(path: Path) -> int:
    con = duckdb.connect()
    try:
        return int(con.execute("SELECT count(*) FROM read_parquet(?)", [str(path)]).fetchone()[0])
    finally:
        con.close()


def scan(root: Path | None = None, lake_dir: Path | None = None) -> list[Item]:
    root = root or lake_mod.ROOT
    lake_dir = lake_dir or lake_mod.LAKE_DIR
    items: dict[Path, Item] = {}
    for rel in FMP_LEFTOVER_FILES:
        p = root / rel
        if p.exists():
            items[p] = Item(p, "FMP leftover", "written by the retired FMP extractor")
    for pattern in FMP_LEFTOVER_GLOBS:
        for p in sorted(root.glob(pattern)):
            if p.is_file():
                items[p] = Item(p, "FMP leftover", "CSV written by the retired FMP extractor")
    if lake_dir.exists():
        for p in sorted(lake_dir.rglob("*.parquet")):
            rel = p.relative_to(lake_dir)
            if rel.parts[0] in SKIP_TOP_LEVEL or p in items:
                continue
            try:
                n = _rows(p)
            except Exception as exc:                                            # noqa: BLE001 -- reported, not moved
                items[p] = Item(p, "unreadable", f"{type(exc).__name__}: {str(exc)[:80]}", move=False)
                continue
            if n == 0:
                items[p] = Item(p, "empty table", "0 rows")
    return list(items.values())


def apply(items: list[Item], root: Path | None = None, now: datetime | None = None) -> Path | None:
    """Move the `move` items under logs/lake-cleanup-<timestamp>/; returns that folder (None when nothing moved)."""
    root = root or lake_mod.ROOT
    todo = [i for i in items if i.move]
    if not todo:
        return None
    stamp = (now or datetime.now(timezone.utc)).strftime("%Y%m%d-%H%M%S")
    dest = root / "logs" / f"lake-cleanup-{stamp}"
    for i in todo:
        target = dest / i.path.relative_to(root) if i.path.is_relative_to(root) else dest / i.path.name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(i.path), str(target))
    for d in sorted({i.path.parent for i in todo}, key=lambda x: len(x.parts), reverse=True):         # empty folders left behind
        try:
            if d != root and d.exists() and not any(d.iterdir()):
                d.rmdir()
        except OSError:
            pass
    return dest


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Set aside empty lake tables and FMP leftovers (dry run unless --apply)")
    ap.add_argument("--apply", action="store_true", help="move the files (default: only list them)")
    ap.add_argument("--root", type=Path, default=None, help="project root (default: this checkout)")
    ap.add_argument("--lake", type=Path, default=None)
    a = ap.parse_args(argv)
    root = a.root or lake_mod.ROOT
    items = scan(root, a.lake or (root / "lake" if a.root else None))
    mode = "APPLY" if a.apply else "DRY RUN (nothing is changed; add --apply to move the files)"
    print(f"lake cleanup: {mode}")
    if not items:
        print("nothing to clean: no empty tables, no FMP leftovers")
        return 0
    for i in items:
        shown = i.path.relative_to(root) if i.path.is_relative_to(root) else i.path
        print(f"  {'move  ' if i.move else 'report'}  {shown}  [{i.reason}{': ' + i.detail if i.detail else ''}]")
    movable = sum(1 for i in items if i.move)
    if a.apply:
        dest = apply(items, root)
        print(f"moved {movable} item(s) into {dest}" if dest else "nothing to move")
    else:
        print(f"{movable} item(s) would move, {len(items) - movable} only reported")
    return 1 if any(i.reason == "unreadable" for i in items) else 0


if __name__ == "__main__":
    sys.exit(main())
