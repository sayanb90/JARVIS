"""
Results of the viewer's slow whole-day calls, kept on disk for a day whose files have stopped changing (SCRUM-206, 18 Nov 2026).

On the VPS the market overview took 67 s (15 M ticks, every equity) and the F&O chain 20 s; each is a pure function of the day's tick, Greek and bar
files.  Once those files have been quiet for QUIET_S nobody writes to them again, so the answer is computed once, saved as JSON, and served from the file
(after a viewer restart too).  A changed file (name, size or modification time) gives another key, so a stale answer is never served.

  python -m webapp.daycache warm [--day D]      cron after the close: computes the overview and every underlying's chains of the day

The cache folder is $JARVIS_VIEWER_CACHE, else <lake>/.viewer-cache; nothing outside the viewer reads it, and a folder that cannot be written only
means the answers are computed each time, as before.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from pathlib import Path
from typing import Callable, Optional

log = logging.getLogger("webapp.daycache")

QUIET_S = 30 * 60                 # files untouched this long are a finished day
KEEP_PER_KIND = 1                 # older answers of the same question are deleted when a new one is saved


def folder(lake_path) -> Path:
    return Path(os.environ.get("JARVIS_VIEWER_CACHE") or (Path(lake_path) / ".viewer-cache"))


def settled(fingerprint: tuple, now: Optional[float] = None) -> bool:
    """True when the day's files exist and none was modified in the last QUIET_S seconds.  `fingerprint` is Lake.fingerprint(): tuples whose third
    element is the modification time in ns."""
    if not fingerprint:
        return False
    newest = max(int(x[2]) for x in fingerprint) / 1e9
    return (now if now is not None else time.time()) - newest >= QUIET_S


def _names(kind: str, key: tuple, fingerprint: tuple) -> tuple:
    k = hashlib.sha1(json.dumps([kind, list(key)], default=str).encode()).hexdigest()[:16]
    f = hashlib.sha1(json.dumps(fingerprint, default=str).encode()).hexdigest()[:16]
    return k, f


def get(lake_path, kind: str, key: tuple, fingerprint: tuple):
    k, f = _names(kind, key, fingerprint)
    path = folder(lake_path) / f"{kind}-{k}-{f}.json"
    try:
        if path.exists():
            return json.loads(path.read_text())
    except (OSError, ValueError) as exc:
        log.warning("day cache %s unreadable: %r", path, exc)
    return None


def put(lake_path, kind: str, key: tuple, fingerprint: tuple, value) -> None:
    k, f = _names(kind, key, fingerprint)
    root = folder(lake_path)
    try:
        root.mkdir(parents=True, exist_ok=True)
        path = root / f"{kind}-{k}-{f}.json"
        tmp = path.with_name(path.name + f".{os.getpid()}.tmp")
        tmp.write_text(json.dumps(value))
        tmp.replace(path)
        for old in sorted(root.glob(f"{kind}-{k}-*.json"), key=lambda p: p.stat().st_mtime, reverse=True)[KEEP_PER_KIND:]:
            old.unlink(missing_ok=True)
    except OSError as exc:
        log.warning("day cache not written (%s): %r", root, exc)


def cached(lake_path, kind: str, key: tuple, fingerprint: tuple, compute: Callable[[], object], now: Optional[float] = None):
    """The saved answer for a finished day, else compute it (and save it when the day is finished)."""
    done = settled(fingerprint, now)
    if done:
        hit = get(lake_path, kind, key, fingerprint)
        if hit is not None:
            return hit
    t0 = time.perf_counter()
    value = compute()
    if done:
        put(lake_path, kind, key, fingerprint, value)
    log.info("day cache %s %s: computed in %.1f s%s", kind, key, time.perf_counter() - t0, " and saved" if done else " (day not finished)")
    return value


def warm(lake_path, day: str, interval: str = "1s") -> dict:
    """Compute and save the overview and every underlying's chain (every expiry) of `day`."""
    from data_layer.query import Lake
    from webapp.app import chain_payload, overview_payload
    q = Lake(Path(lake_path))
    done = {}
    t0 = time.perf_counter()
    overview_payload(q, Path(lake_path), day, interval)
    done["overview"] = round(time.perf_counter() - t0, 1)
    for u in q.fo_underlyings(day, interval):
        first = chain_payload(q, Path(lake_path), day, u, None, interval)
        for e in (first or {}).get("expiries", []):
            chain_payload(q, Path(lake_path), day, u, e, interval)
        done[u] = round(time.perf_counter() - t0, 1)
    return done


def main(argv=None) -> int:
    import argparse
    ap = argparse.ArgumentParser(prog="python -m webapp.daycache")
    ap.add_argument("action", choices=["warm"])
    ap.add_argument("--day", default=None, help="default: the newest day with ticks")
    ap.add_argument("--lake", default=None)
    a = ap.parse_args(argv)
    from data_layer.lake import LAKE_DIR
    from data_layer.query import Lake
    lake_path = Path(a.lake or os.getenv("WEBAPP_LAKE") or os.getenv("JARVIS_LAKE") or LAKE_DIR)
    day = a.day or (sorted(Lake(lake_path).bar_days("1s"))[-1] if Lake(lake_path).bar_days("1s") else None)
    if not day:
        print("no days in", lake_path)
        return 1
    print(f"warming {day}: {warm(lake_path, str(day))}  (cache {folder(lake_path)})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
