"""
Dated, hard-linked snapshot backups of everything that cannot be re-downloaded.

  python -m data_layer.backup run [--dest DIR]                    # one snapshot named YYYY-MM-DD
  python -m data_layer.backup list [--dest DIR]
  python -m data_layer.backup verify [--dest DIR] [--snapshot DATE]     # every file present, every Parquet file readable
  python -m data_layer.backup restore --snapshot DATE --to DIR [--force]

DIR defaults to $JARVIS_BACKUP_DIR.  Point it at a different disk or a mounted bucket, never the same disk as the data.
Unchanged files are hard-linked to the previous snapshot, so a daily snapshot costs only what changed (a compacted day
file never changes again).  Keeps the newest 7 daily snapshots plus the newest 4 Sunday snapshots.
Not included on purpose: .env (it holds secrets; keep those in a password manager) and logs.
For off-site copies, sync the destination directory with rclone or rsync (hard links are preserved with `rsync -aH`).
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb

from . import lake as lake_mod
from .lake import ROOT

INCLUDE = ["lake", "data/universe.csv", "data/fund_meta.csv", "data/fundamentals", "data/ohlc", "data/bars"]
SKIP_SUFFIXES = (".tmp",)


def backup_dir(arg: str | Path | None = None) -> Path:
    d = arg or os.getenv("JARVIS_BACKUP_DIR")
    if not d:
        raise SystemExit("no backup destination: pass --dest or set JARVIS_BACKUP_DIR")
    return Path(d)


def list_snapshots(dest: Path) -> list[str]:
    if not dest.exists():
        return []
    out = []
    for p in dest.iterdir():
        try:
            date.fromisoformat(p.name)
        except ValueError:
            continue
        if p.is_dir() and (p / "MANIFEST.json").exists():
            out.append(p.name)
    return sorted(out)


def _files(root: Path, include: list[str]):
    for rel in include:
        p = root / rel
        if p.is_file():
            yield Path(rel)
        elif p.is_dir():
            for f in sorted(p.rglob("*")):
                if f.is_file() and not f.name.endswith(SKIP_SUFFIXES):
                    yield f.relative_to(root)


def _same(a: Path, b: Path) -> bool:
    sa, sb = a.stat(), b.stat()
    return sa.st_size == sb.st_size and int(sa.st_mtime) == int(sb.st_mtime)


def run_backup(dest: Path, root: Path | None = None, today: date | None = None, include: list[str] | None = None,
               keep_daily: int = 7, keep_weekly: int = 4) -> dict:
    root, today, include = root or ROOT, today or date.today(), include or INCLUDE
    name = today.isoformat()
    dest.mkdir(parents=True, exist_ok=True)
    previous = [s for s in list_snapshots(dest) if s != name]
    ref = dest / previous[-1] if previous else None
    tmp = dest / f".tmp-{name}"
    if tmp.exists():
        shutil.rmtree(tmp)
    stats = {"snapshot": name, "files": 0, "bytes": 0, "copied": 0, "linked": 0, "copied_bytes": 0}
    manifest = []
    for rel in _files(root, include):
        src, out = root / rel, tmp / rel
        out.parent.mkdir(parents=True, exist_ok=True)
        old = ref / rel if ref else None
        if old is not None and old.exists() and _same(src, old):
            os.link(old, out)
            stats["linked"] += 1
        else:
            shutil.copy2(src, out)
            stats["copied"] += 1
            stats["copied_bytes"] += out.stat().st_size
        size = out.stat().st_size
        stats["files"] += 1
        stats["bytes"] += size
        manifest.append({"path": rel.as_posix(), "size": size})
    (tmp / "MANIFEST.json").write_text(json.dumps({
        "created_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"), "root": str(root), "files": manifest}))
    final = dest / name
    if final.exists():                                    # running twice on the same day replaces that day's snapshot
        old_final = dest / f".old-{name}"
        final.rename(old_final)
        tmp.rename(final)
        shutil.rmtree(old_final)
    else:
        tmp.rename(final)
    stats["pruned"] = _prune(dest, keep_daily, keep_weekly)
    return stats


def _prune(dest: Path, keep_daily: int, keep_weekly: int) -> list[str]:
    snaps = list_snapshots(dest)
    keep = set(snaps[-keep_daily:])
    sundays = [s for s in snaps if date.fromisoformat(s).weekday() == 6]
    keep |= set(sundays[-keep_weekly:])
    gone = [s for s in snaps if s not in keep]
    for s in gone:
        shutil.rmtree(dest / s)
    return gone


def verify_snapshot(snap: Path) -> list[str]:
    """Return a list of problems (empty = fine): missing files, wrong sizes, unreadable Parquet files."""
    problems: list[str] = []
    try:
        manifest = json.loads((snap / "MANIFEST.json").read_text())
    except (OSError, ValueError) as exc:
        return [f"manifest unreadable: {exc}"]
    con = duckdb.connect()
    for f in manifest["files"]:
        p = snap / f["path"]
        if not p.exists():
            problems.append(f"missing: {f['path']}")
        elif p.stat().st_size != f["size"]:
            problems.append(f"size changed: {f['path']}")
        elif p.suffix == ".parquet":
            try:
                con.execute(f"SELECT count(*) FROM read_parquet('{p}')").fetchone()
            except Exception as exc:
                problems.append(f"unreadable parquet {f['path']}: {exc}")
    return problems


def restore(snap: Path, to: Path, force: bool = False) -> int:
    """Copy a snapshot's files under `to`.  Refuses to overwrite existing files unless force=True."""
    manifest = json.loads((snap / "MANIFEST.json").read_text())
    clashes = [f["path"] for f in manifest["files"] if (to / f["path"]).exists()]
    if clashes and not force:
        raise SystemExit(f"{len(clashes)} files already exist under {to} (e.g. {clashes[0]}); "
                         "restore into an empty directory, or pass --force to overwrite")
    for f in manifest["files"]:
        out = to / f["path"]
        out.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(snap / f["path"], out)
    return len(manifest["files"])


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("run", "list", "verify", "restore"):
        sp = sub.add_parser(name)
        sp.add_argument("--dest")
        if name in ("verify", "restore"):
            sp.add_argument("--snapshot")
        if name == "restore":
            sp.add_argument("--to", required=True)
            sp.add_argument("--force", action="store_true")
    a = ap.parse_args(argv)
    from dotenv import load_dotenv
    load_dotenv(lake_mod.ENV_FILE)                       # so cron needs no shell tricks to find JARVIS_BACKUP_DIR
    dest = backup_dir(a.dest)
    if a.cmd == "run":
        s = run_backup(dest)
        print(f"snapshot {s['snapshot']}: {s['files']} files, {s['bytes'] / 1e6:.1f} MB; copied {s['copied']} "
              f"({s['copied_bytes'] / 1e6:.1f} MB), hard-linked {s['linked']}; pruned {s['pruned'] or 'none'}")
        return 0
    snaps = list_snapshots(dest)
    if a.cmd == "list":
        print("\n".join(snaps) or "no snapshots")
        return 0
    if not snaps:
        print("no snapshots", file=sys.stderr)
        return 1
    name = a.snapshot or snaps[-1]
    if name not in snaps:
        print(f"no snapshot {name}; have {snaps}", file=sys.stderr)
        return 1
    if a.cmd == "verify":
        problems = verify_snapshot(dest / name)
        print(f"snapshot {name}: " + ("OK" if not problems else f"{len(problems)} problem(s)"))
        for p in problems[:20]:
            print("  " + p)
        return 1 if problems else 0
    n = restore(dest / name, Path(a.to), a.force)
    print(f"restored {n} files from {name} into {a.to}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
