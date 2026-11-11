"""
Frozen, read-only snapshots of the production lake for research (workstream 1 of the research build).

  python -m research.snapshot create  [--root /srv/jarvis-research] [--lake lake] [--copy] [--keep N]
  python -m research.snapshot status  [--max-age-days 2]       # latest snapshot, its age and coverage; exit 1 when stale or missing
  python -m research.snapshot verify  [SNAPSHOT] [--deep]       # sizes (and with --deep every checksum) against the manifest
  python -m research.snapshot list

Why: the lake is Parquet files that the collector writes all day, and research must never read a file the collector can still change,
slow the collector down, or be able to change anything in it.  So research reads a *snapshot*: a directory tree that mirrors the lake's
layout (so `data_layer.query.Lake(snapshot_dir)` works on it unchanged), made of hard links to the lake's files that are already closed.

Why a hard link is a true frozen copy: every writer in data_layer replaces a file by writing a temporary file and renaming it over the
old one (`os.replace`), never by editing it in place.  A hard link keeps the OLD file alive after a rename, so a snapshot keeps showing
exactly what it saw, at no extra disk space.  The tool never opens a lake file for writing and never changes a lake file's mode (a
hard link shares its inode with the lake file).  It writes only under the snapshot root.

What is closed (and so included):
  * day partitions (ticks, bars, events): a folder `date=D` with no `seg-*` segment files and no `.tmp` file in it, where D is before
    today (UTC), or is today and it is past CLOSED_AFTER_UTC (the feeds have compacted at 10:05 UTC).  An open day is skipped and listed.
  * single-file tables (daily OHLC years, fundamentals, instruments, FX, reference, Yahoo and Upstox fundamentals): any file whose size
    and modification time do not change while it is hashed and that has not been modified in the last QUIET_SECONDS.
  * not included: anything with a name ending `.tmp` or starting with `.`; `raw/` payload folders (large, replayable); `_pre_split/`;
    `backfill/` (the backfill's own progress state).  `--include-raw` adds the raw payloads.

Result: <root>/snapshots/<id>/ with the lake layout and a `manifest.json` (every file: size, mtime, sha256; the coverage by table and
date; anything skipped; warnings), and <root>/snapshots/latest -> <id>.  The id is the UTC date plus a short hash of the manifest's
files, so two snapshots of an unchanged lake have the same id and every backtest can record exactly what it ran on.

Limits it enforces: the snapshot root must be on the same filesystem as the lake (otherwise a link is impossible; `--copy` copies and
spends disk, and refuses below MIN_FREE_GB free); the root must not be inside the lake or contain it.  Files that are not readable by
"other" users are listed in the warnings, because the read-only research user can then not read them (fixing that means changing the
production files' mode, which this tool never does).
"""

from __future__ import annotations

import argparse
import errno
import hashlib
import json
import os
import shutil
import sys
import time
from dataclasses import dataclass, field
from datetime import date, datetime, time as dtime, timedelta, timezone
from pathlib import Path
from typing import Optional

DEFAULT_ROOT = Path(os.environ.get("JARVIS_RESEARCH_ROOT", "/srv/jarvis-research"))
CLOSED_AFTER_UTC = dtime(11, 0)                 # both feeds have compacted their day by 10:05 UTC
QUIET_SECONDS = 120                             # a single-file table modified more recently than this is skipped this time
MIN_FREE_GB = 20.0                              # --copy refuses below this much free disk after the copy
MANIFEST = "manifest.json"
SNAPSHOT_DIRS = ("ticks", "bars", "events", "ohlc", "fundamentals", "instruments", "fx", "reference", "yf_fundamentals",
                 "upstox_fundamentals")
DAY_PARTITIONED = ("ticks", "bars", "events")
SKIP_DIRS = {"raw", "_pre_split", "backfill"}
ID_FORMAT = "%Y%m%d"


class SnapshotError(Exception):
    """The snapshot could not be made (or verified); the message says what to change."""


@dataclass
class Plan:
    """What one run found in the lake: files to include and partitions or files skipped, with reasons."""
    files: list[Path] = field(default_factory=list)                  # paths relative to the lake
    skipped: list[dict] = field(default_factory=list)                # {"path", "reason"}


# ---------------------------------------------------------------------------------------------- what is closed
def _is_temp(name: str) -> bool:
    return name.startswith(".") or name.endswith(".tmp") or name.endswith(".lock")


def partition_day(folder: Path) -> Optional[date]:
    name = folder.name
    if not name.startswith("date="):
        return None
    try:
        return date.fromisoformat(name[5:])
    except ValueError:
        return None


def day_is_closed(folder: Path, day: date, now: datetime) -> tuple[bool, str]:
    """Is a `date=D` folder safe to link?  No open segments or temporary files, and the day is over (or its compaction has run)."""
    for f in folder.iterdir():
        if f.name.startswith("seg-") or f.name.endswith(".tmp"):
            return False, f"open: {f.name} present (the day has not been compacted)"
    if day < now.date():
        return True, ""
    if day == now.date() and now.time() >= CLOSED_AFTER_UTC:
        return True, ""
    return False, "the day is not over yet" if day >= now.date() else ""


def plan_lake(lake: Path, now: Optional[datetime] = None, include_raw: bool = False) -> Plan:
    now = now or datetime.now(timezone.utc)
    plan = Plan()
    for top in SNAPSHOT_DIRS:
        base = lake / top
        if not base.is_dir():
            continue
        _walk(base, lake, top in DAY_PARTITIONED, now, plan, include_raw)
    plan.files.sort()
    return plan


def _walk(folder: Path, lake: Path, day_tree: bool, now: datetime, plan: Plan, include_raw: bool) -> None:
    day = partition_day(folder)
    if day is not None and day_tree:
        closed, why = day_is_closed(folder, day, now)
        rel = folder.relative_to(lake).as_posix()
        if not closed:
            plan.skipped.append({"path": rel, "reason": why})
            return
        for f in sorted(folder.iterdir()):
            if f.is_file() and not _is_temp(f.name):
                plan.files.append(f.relative_to(lake))
        return
    for entry in sorted(folder.iterdir()):
        if _is_temp(entry.name):
            continue
        if entry.is_dir():
            if entry.name in SKIP_DIRS and not (include_raw and entry.name == "raw"):
                continue
            _walk(entry, lake, day_tree, now, plan, include_raw)
        elif entry.is_file():
            plan.files.append(entry.relative_to(lake))


# ---------------------------------------------------------------------------------------------- hashing and linking
def sha256_of(path: Path) -> str:
    with open(path, "rb") as fh:
        return hashlib.file_digest(fh, "sha256").hexdigest()


def stat_key(st: os.stat_result) -> tuple[int, int, int]:
    """What identifies one version of a file: a replaced file (written elsewhere, renamed over the old one) has a new inode even when
    its size and time happen to match, so the stored checksum is reused only when all three are unchanged."""
    return st.st_size, st.st_mtime_ns, st.st_ino


def _load_manifest(snapshot: Path) -> dict:
    p = snapshot / MANIFEST
    if not p.is_file():
        raise SnapshotError(f"{snapshot} has no {MANIFEST}: not a snapshot made by this tool")
    return json.loads(p.read_text())


def _previous_hashes(root: Path) -> dict[str, tuple[tuple, str]]:
    """{relative path: ((size, mtime_ns, inode), sha256)} from the latest snapshot, so unchanged files are not hashed again."""
    latest = root / "snapshots" / "latest"
    try:
        m = _load_manifest(latest)
    except (SnapshotError, OSError, ValueError):
        return {}
    return {f["path"]: ((f["size"], f["mtime_ns"], f.get("ino")), f["sha256"]) for f in m.get("files", [])}


def _link_or_copy(src: Path, dst: Path, copy: bool) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if copy:
        shutil.copy2(src, dst)
        return
    try:
        os.link(src, dst)
    except OSError as exc:
        if exc.errno == errno.EXDEV:
            raise SnapshotError("the snapshot root is on a different filesystem from the lake, so files cannot be hard-linked.  Put the root "
                                "on the lake's filesystem (recommended), or pass --copy to copy them (this spends disk space)") from exc
        raise


def _check_roots(lake: Path, root: Path) -> None:
    lr, rr = lake.resolve(), root.resolve()
    if rr == lr or lr in rr.parents:
        raise SnapshotError(f"the snapshot root {root} is inside the lake {lake}: refusing, snapshots must live outside it")
    if rr in lr.parents:
        raise SnapshotError(f"the snapshot root {root} contains the lake {lake}: refusing")


def _snapshot_id(day: str, files: list[dict]) -> str:
    h = hashlib.sha256()
    for f in sorted(files, key=lambda f: f["path"]):
        h.update(f"{f['path']}\0{f['size']}\0{f['sha256']}\n".encode())
    return f"{day}-{h.hexdigest()[:8]}"


def _coverage(files: list[dict]) -> dict:
    """{table: {"first", "last", "days", "files"}} from the file paths: the manifest's record of what each table covers, which the
    signal availability manifest is checked against."""
    cov: dict[str, dict] = {}
    seen: dict[str, set] = {}
    for f in files:
        parts = Path(f["path"]).parts
        d = next((p[5:] for p in parts if p.startswith("date=")), None)
        y = next((p[5:] for p in parts if p.startswith("year=")), None)
        key = "/".join(p for p in parts[:-1] if not p.startswith(("date=", "year="))) or parts[0]
        c = cov.setdefault(key, {"first": None, "last": None, "days": 0, "files": 0})
        c["files"] += 1
        v = d or (f"{y}-01-01" if y else None)
        if v:
            c["first"] = v if c["first"] is None or v < c["first"] else c["first"]
            c["last"] = v if c["last"] is None or v > c["last"] else c["last"]
            if d:
                seen.setdefault(key, set()).add(d)
    for key, days in seen.items():
        cov[key]["days"] = len(days)
    return cov


# ---------------------------------------------------------------------------------------------- create
def create(lake: Path, root: Path, copy: bool = False, include_raw: bool = False, now: Optional[datetime] = None,
           keep: int = 0, quiet_seconds: float = QUIET_SECONDS) -> dict:
    """Make a snapshot of the lake's closed files under root/snapshots.  Returns the manifest (also written to disk)."""
    now = now or datetime.now(timezone.utc)
    lake, root = Path(lake), Path(root)
    if not lake.is_dir():
        raise SnapshotError(f"no lake at {lake}")
    _check_roots(lake, root)
    snaps = root / "snapshots"
    snaps.mkdir(parents=True, exist_ok=True)
    if not copy and lake.stat().st_dev != snaps.stat().st_dev:
        raise SnapshotError("the snapshot root is on a different filesystem from the lake, so files cannot be hard-linked.  Put the root "
                            "on the lake's filesystem (recommended), or pass --copy to copy them (this spends disk space)")
    plan = plan_lake(lake, now, include_raw)
    prev = _previous_hashes(root)
    entries: list[dict] = []
    warnings: list[str] = []
    skipped = list(plan.skipped)
    total = 0
    for rel in plan.files:
        src = lake / rel
        try:
            st1 = src.stat()
            if now.timestamp() - st1.st_mtime < quiet_seconds and not partition_day(src.parent):
                skipped.append({"path": rel.as_posix(), "reason": f"modified in the last {quiet_seconds:g} s"})
                continue
            old = prev.get(rel.as_posix())
            digest = old[1] if old and old[0] == stat_key(st1) else sha256_of(src)
            st2 = src.stat()
        except FileNotFoundError:
            skipped.append({"path": rel.as_posix(), "reason": "disappeared while the snapshot was made"})
            continue
        if stat_key(st1) != stat_key(st2):
            skipped.append({"path": rel.as_posix(), "reason": "changed while it was hashed"})
            continue
        if not st1.st_mode & 0o004:
            warnings.append(f"{rel.as_posix()} is not readable by other users: the research user cannot read it")
        entries.append({"path": rel.as_posix(), "size": st1.st_size, "mtime_ns": st1.st_mtime_ns, "ino": st1.st_ino, "sha256": digest})
        total += st1.st_size
    if copy:
        free = shutil.disk_usage(snaps).free / 1e9
        if free - total / 1e9 < MIN_FREE_GB:
            raise SnapshotError(f"copying {total / 1e9:.1f} GB would leave {free - total / 1e9:.1f} GB free (minimum {MIN_FREE_GB:g}): not copied")
    sid = _snapshot_id(now.strftime(ID_FORMAT), entries)
    final = snaps / sid
    manifest = {"snapshot_id": sid, "created_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"), "lake_dir": str(lake.resolve()),
                "mode": "copy" if copy else "hard-link", "files": entries, "bytes": total, "coverage": _coverage(entries),
                "skipped": skipped, "warnings": warnings, "include_raw": include_raw}
    if final.exists():                                              # same date, same content: nothing to do
        _publish_latest(snaps, sid)
        return _load_manifest(final)
    building = snaps / f".building-{sid}"
    if building.exists():
        shutil.rmtree(building)
    try:
        for e in entries:
            _link_or_copy(lake / e["path"], building / e["path"], copy)
        (building / MANIFEST).write_text(json.dumps(manifest, indent=1, sort_keys=True))
        os.replace(building, final)
    except BaseException:
        shutil.rmtree(building, ignore_errors=True)
        raise
    _publish_latest(snaps, sid)
    if keep:
        prune(root, keep)
    return manifest


def _publish_latest(snaps: Path, sid: str) -> None:
    tmp = snaps / ".latest.tmp"
    if tmp.is_symlink() or tmp.exists():
        tmp.unlink()
    tmp.symlink_to(sid)
    os.replace(tmp, snaps / "latest")


# ---------------------------------------------------------------------------------------------- list, verify, prune, status
def list_snapshots(root: Path) -> list[str]:
    snaps = Path(root) / "snapshots"
    if not snaps.is_dir():
        return []
    return sorted(p.name for p in snaps.iterdir() if p.is_dir() and not p.is_symlink() and not p.name.startswith(".") and (p / MANIFEST).is_file())


def resolve(root: Path, snapshot: str = "latest") -> Path:
    p = Path(root) / "snapshots" / snapshot
    if not p.exists():
        raise SnapshotError(f"no snapshot {snapshot!r} under {Path(root) / 'snapshots'}" + (" (run `python -m research.snapshot create`)" if snapshot == "latest" else ""))
    return p.resolve()


def verify(snapshot: Path, deep: bool = False) -> list[str]:
    """Problems found (empty = the snapshot matches its manifest): a missing file, a different size, and with `deep` a different checksum."""
    snapshot = Path(snapshot)
    m = _load_manifest(snapshot)
    problems = []
    for f in m["files"]:
        p = snapshot / f["path"]
        if not p.is_file():
            problems.append(f"missing: {f['path']}")
        elif p.stat().st_size != f["size"]:
            problems.append(f"size differs: {f['path']}")
        elif deep and sha256_of(p) != f["sha256"]:
            problems.append(f"checksum differs: {f['path']}")
    return problems


def prune(root: Path, keep: int) -> list[str]:
    """Remove all but the newest `keep` snapshots.  It removes only directories this tool made (a manifest inside, a snapshot-style name),
    and only links: the lake's own files stay, because the lake still holds its link to every inode."""
    names = list_snapshots(root)
    latest = (Path(root) / "snapshots" / "latest").resolve().name if (Path(root) / "snapshots" / "latest").exists() else None
    drop = [n for n in names[:-keep] if n != latest] if keep > 0 else []
    for n in drop:
        shutil.rmtree(Path(root) / "snapshots" / n)
    return drop


def status(root: Path, max_age_days: float = 2.0, now: Optional[datetime] = None) -> tuple[bool, str]:
    """(healthy, one line) for the health report: the latest snapshot's id, age, size and the newest tick day it holds."""
    now = now or datetime.now(timezone.utc)
    try:
        snap = resolve(root)
        m = _load_manifest(snap)
    except (SnapshotError, OSError, ValueError) as exc:
        return False, f"no usable snapshot under {root}: {exc}"
    age = (now - datetime.strptime(m["created_at"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)).total_seconds() / 86400
    ticks = [c["last"] for k, c in m["coverage"].items() if k.startswith("ticks") and c["last"]]
    line = (f"snapshot {m['snapshot_id']}: {age:.1f} days old, {len(m['files']):,} files, {m['bytes'] / 1e9:.2f} GB, newest tick day "
            f"{max(ticks) if ticks else 'none'}, {len(m['skipped'])} skipped, {len(m['warnings'])} warnings")
    return age <= max_age_days, line


# ---------------------------------------------------------------------------------------------- command line
def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", type=Path, default=DEFAULT_ROOT, help=f"research root (default {DEFAULT_ROOT}, or JARVIS_RESEARCH_ROOT)")
    sub = ap.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("create", help="snapshot the closed files of the lake")
    c.add_argument("--lake", type=Path, default=None, help="the production lake (default: JARVIS_LAKE or ./lake)")
    c.add_argument("--copy", action="store_true", help="copy instead of hard-linking (spends disk; for a root on another filesystem)")
    c.add_argument("--include-raw", action="store_true", help="also include the raw payload folders")
    c.add_argument("--keep", type=int, default=0, help="afterwards keep only the newest N snapshots (default: keep all)")
    s = sub.add_parser("status", help="latest snapshot and its age; exit 1 when missing or stale")
    s.add_argument("--max-age-days", type=float, default=2.0)
    v = sub.add_parser("verify", help="check a snapshot against its manifest")
    v.add_argument("snapshot", nargs="?", default="latest")
    v.add_argument("--deep", action="store_true", help="also recompute every checksum (reads every file)")
    sub.add_parser("list", help="the snapshots under the root")
    a = ap.parse_args(argv)
    try:
        if a.cmd == "create":
            os.umask(0o027)              # directories made here are group-readable (the research group, through the root's setgid bit), never world-readable
            from data_layer import lake as lake_mod
            m = create(a.lake or lake_mod.LAKE_DIR, a.root, a.copy, a.include_raw, keep=a.keep)
            print(f"snapshot {m['snapshot_id']} ({m['mode']}): {len(m['files']):,} files, {m['bytes'] / 1e9:.2f} GB; "
                  f"{len(m['skipped'])} skipped (open or in flight), {len(m['warnings'])} warnings")
            for w in m["warnings"][:10]:
                print("  WARNING:", w)
            for k in m["skipped"][:10]:
                print("  skipped:", k["path"], "-", k["reason"])
            return 0
        if a.cmd == "status":
            ok, line = status(a.root, a.max_age_days)
            print(("OK   " if ok else "STALE ") + line)
            return 0 if ok else 1
        if a.cmd == "verify":
            snap = resolve(a.root, a.snapshot)
            problems = verify(snap, a.deep)
            print(f"{snap.name}: " + ("matches its manifest" if not problems else f"{len(problems)} problems"))
            for p in problems[:20]:
                print("  ", p)
            return 0 if not problems else 1
        for n in list_snapshots(a.root):
            print(n)
        return 0
    except SnapshotError as exc:
        print(f"STOPPED: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
