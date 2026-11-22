"""
A day's signals, reduced once on disk, read in slices (SCRUM-208, 22 Nov 2026).

The Signals tab used to read a day's live files into the viewer's own memory (2.4 to 2.6 GB resident, five incidents in three days).  Here a short-lived
process does the reading and keeps only what the tab shows, as small Parquet files; the viewer holds an index and reads one signal's slice when asked:

  <store>/reduced/date=D/manifest.json                      files read so far, the keys held, progress, whether the day is complete
  <store>/reduced/date=D/<kind>/signal=S/bucket=B/underlying=U/part-*.parquet, merged-*.parquet
      kind single   a signal with one row per bucket (the chain and flow signals): every row of the day
           fut      a per-contract signal's futures rows, every bucket
           hist     its option rows at the full minute only, for the by-strike heatmap
           last     its newest snapshot
           events   block prints, the newest few hundred
  (the same reduction as webapp/signals_store.LiveDay, whose code it runs: one definition of "reduced")

  python -m webapp.reduced once   [--store S] [--day D]            read what is new, write it, exit (the viewer runs this as a capped child; cron may too)
  python -m webapp.reduced follow [--store S] [--day D] [--every 60] [--until 10:10]
  python -m webapp.reduced status | prune [--keep 20]

Incremental: a second run reads only files it has not read.  A day whose small files were replaced by the nightly compaction, or whose compacted file changed,
is reduced afresh.  The store is never written outside <store>/reduced/.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import threading
import time
from collections import OrderedDict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

from . import signals_store as ss

log = logging.getLogger("webapp.reduced")

SCHEMA = 3                        # bump when what is kept per signal changes: older reduced days are then read afresh
MANIFEST = "manifest.json"
CONSOLIDATE_AT = 24               # part files of one key before they are merged into one
STALE_S = 30.0                    # a running day's reduced copy older than this is refreshed (by a child) on the next request
RETRY_AFTER_FAILURE_S = 120.0
REDUCE_BATCH_ROWS = 49_152        # rows per streamed frame in the reducer (the viewer's loader uses 196,608): a smaller frame, a smaller peak
KEEP_DAYS = 20


def reduced_root(store) -> Path:
    return Path(store) / "reduced"


def reduced_dir(store, day) -> Path:
    return reduced_root(store) / f"date={day}"


def key_dir(base: Path, kind: str, sid: str, bucket: str, und: str) -> Path:
    return base / kind / f"signal={sid}" / f"bucket={bucket}" / f"underlying={und or '_'}"


def read_manifest(base: Path) -> Optional[dict]:
    try:
        m = json.loads((base / MANIFEST).read_text())
    except (OSError, ValueError):
        return None
    return m if m.get("schema") == SCHEMA else None


def reduced_days(store) -> list:
    """Days with a reduced copy that holds something, newest first."""
    root = reduced_root(store)
    out = []
    for p in (q for q in root.glob("date=*") if q.is_dir()) if root.is_dir() else []:
        m = read_manifest(p)
        if m and m.get("keys"):
            out.append(p.name.split("=", 1)[1])
    return sorted(out, reverse=True)


def resolve_store(arg=None) -> Path:
    env = os.getenv("JARVIS_SIGNAL_STORE")
    if arg:
        return Path(arg)
    if env:
        return Path(env)
    return Path.cwd() / "store" if (Path.cwd() / "store" / "live").is_dir() else Path("/srv/jarvis-research/store")


# ---------------------------------------------------------------------------------------------------------- writing
class Writer:
    """The sink of one reduction: appends each batch's kept frames as part files, merges a key's parts when there are many, and keeps the manifest."""

    def __init__(self, base: Path, ld: "ss.LiveDay", day, keys: set, fingerprint, started: float):
        self.base, self.ld, self.day, self.keys, self.fingerprint, self.started = base, ld, day, set(keys), fingerprint, started
        self.rows = 0

    def __call__(self, taken: dict) -> None:
        for (kind, sid, bucket, und), frames in taken.items():
            df = ss.merge_frames(kind, frames)
            if df is None or not len(df):
                continue
            d = key_dir(self.base, kind, sid, bucket, und)
            d.mkdir(parents=True, exist_ok=True)
            tmp = d / f"part-{time.time_ns()}.parquet.tmp"
            df.to_parquet(tmp, index=False)
            tmp.replace(d / tmp.name[:-4])
            self.keys.add((kind, sid, bucket, und))
            self.rows += len(df)
            files = sorted(d.glob("*.parquet"))
            if len(files) > CONSOLIDATE_AT:
                self._merge(d, kind, files)
        self.write(complete=False)

    @staticmethod
    def _merge(d: Path, kind: str, files: list) -> None:
        df = ss.merge_frames(kind, [pd.read_parquet(f) for f in files])
        if df is None:
            return
        tmp = d / f"merged-{time.time_ns()}.parquet.tmp"
        df.to_parquet(tmp, index=False)
        tmp.replace(d / tmp.name[:-4])
        for f in files:                                              # a reader that listed the folder before this sees the rows twice, which merge_frames drops
            f.unlink(missing_ok=True)

    def write(self, complete: bool, error: Optional[str] = None) -> None:
        m = {"schema": SCHEMA, "day": str(self.day), "updated": time.time(), "complete": bool(complete), "error": error,
             "files_done": len(self.ld.seen), "files_total": max(self.ld.files_total, len(self.ld.seen)), "seen": sorted(self.ld.seen),
             "fingerprint": self.fingerprint, "keys": sorted(list(k) for k in self.keys), "rows": self.rows}
        self.base.mkdir(parents=True, exist_ok=True)
        tmp = self.base / (MANIFEST + ".tmp")
        tmp.write_text(json.dumps(m))
        tmp.replace(self.base / MANIFEST)


def reduce_day(store, day, log_fn=print) -> dict:
    """Read the files of `day` that were not read before and add them to the reduced copy (all of them, on the first run).  Never raises."""
    store, day = Path(store), (day if isinstance(day, date) else date.fromisoformat(str(day)))
    base = reduced_dir(store, day)
    t0 = time.perf_counter()
    import fcntl
    lock_path = reduced_root(store) / f".lock-{day}"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with open(lock_path, "w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)                 # one reducer per day: the viewer's child, cron and a hand run must not write together
        except OSError:
            return {"day": str(day), "reduced": False, "note": "another reducer is running this day"}
        return _reduce_locked(store, day, base, t0, log_fn)


def _reduce_locked(store: Path, day: date, base: Path, t0: float, log_fn) -> dict:
    try:
        ld = ss.LiveDay(store, day)
        files = ld._files()
        if not files:
            return {"day": str(day), "files": 0, "reduced": False, "note": "no live files"}
        names = {f.name for f in files}
        compacted = ld._cacheable(files)
        fp = [list(x) for x in ld._fingerprint(files)] if compacted else None
        man = read_manifest(base)
        if man and ((compacted and man.get("fingerprint") != fp) or not set(man.get("seen", [])) <= names):
            shutil.rmtree(base, ignore_errors=True)                  # the small files were compacted under it, or the compacted file changed: read afresh
            man = None
        elif not man:
            shutil.rmtree(base, ignore_errors=True)                  # no manifest or another schema: nothing here can be trusted
        ld.seen = set(man["seen"]) if man else set()
        w = Writer(base, ld, day, {tuple(k) for k in man["keys"]} if man else set(), fp, t0)
        w.rows = int(man.get("rows", 0)) if man else 0
        ld.sink = w
        ss.BATCH_ROWS = min(ss.BATCH_ROWS, REDUCE_BATCH_ROWS)
        ld.refresh()
        quiet = time.time() - max(f.stat().st_mtime for f in files) > 900            # nothing written for 15 minutes: the engines have stopped
        complete = bool(compacted or quiet or day < datetime.now(timezone.utc).date())
        w.write(complete=complete and not ld.error, error=ld.error)
        res = {"day": str(day), "files": len(files), "read": len(ld.seen), "keys": len(w.keys), "rows": w.rows, "complete": complete and not ld.error,
               "error": ld.error, "seconds": round(time.perf_counter() - t0, 1)}
        log_fn(json.dumps(res))
        return res
    except Exception as exc:                                         # noqa: BLE001 -- the caller (a child process) reports it and the viewer falls back
        log.exception("reducing %s failed", day)
        return {"day": str(day), "error": repr(exc), "reduced": False}


def prune(store, keep: int = KEEP_DAYS) -> list:
    root = reduced_root(store)
    days = sorted(p for p in root.glob("date=*") if p.is_dir()) if root.is_dir() else []
    gone = []
    for p in days[:-keep] if keep > 0 else days:
        shutil.rmtree(p, ignore_errors=True)
        gone.append(p.name)
    return gone


# ---------------------------------------------------------------------------------------------------------- reading
def answer_cache_mb() -> float:
    try:
        return float(os.environ.get("JARVIS_VIEWER_ANSWER_CACHE_MB", 60))
    except ValueError:
        return 60.0


class ReducedDay:
    """What the Signals tab reads a day from: the same methods as `signals_store.LiveDay` (frame, signals, underlyings, progress, ensure), but over the
    reduced Parquet files, one slice at a time, with a small cache bounded in bytes.  Holds no day in memory."""

    def __init__(self, store, day):
        self.store, self.day = Path(store), day if isinstance(day, date) else date.fromisoformat(str(day))
        self.base = reduced_dir(self.store, self.day)
        self.lock = threading.RLock()
        self._man: Optional[dict] = None
        self._man_stamp = None
        self._cache: "OrderedDict[tuple, pd.DataFrame]" = OrderedDict()
        self._cache_bytes = 0
        self._cache_gen = None
        self.thread: Optional[threading.Thread] = None
        self.error: Optional[str] = None
        self.failed_at = 0.0
        self.settled = False                       # a reduction ended without error (a day with no files at all leaves no manifest)
        self._checked = 0.0

    # ------------------------------------------------------------------------------------------ the manifest
    def manifest(self) -> Optional[dict]:
        p = self.base / MANIFEST
        try:
            st = p.stat()
        except OSError:
            self._man = None
            return None
        stamp = (st.st_mtime_ns, st.st_size)
        if stamp != self._man_stamp:
            m = read_manifest(self.base)
            with self.lock:
                self._man, self._man_stamp = m, stamp
        return self._man

    def available(self) -> bool:
        m = self.manifest()
        return bool(m and m.get("keys"))

    def _source_changed(self, m: dict) -> bool:
        """Is the reduced copy of a finished day out of date: the live files were replaced (compaction) or the compacted file changed?"""
        ld = ss.LiveDay(self.store, self.day)
        files = ld._files()
        if not files:
            return False
        if ld._cacheable(files):
            return [list(x) for x in ld._fingerprint(files)] != m.get("fingerprint")
        return {f.name for f in files} != set(m.get("seen", []))

    def needs_refresh(self) -> bool:
        m = self.manifest()
        if m is None:
            return True
        if not m.get("complete"):
            return time.time() - m.get("updated", 0) >= STALE_S
        if time.time() - self._checked >= 60.0:                       # a finished day: look at its files once a minute at most
            self._checked = time.time()
            try:
                return self._source_changed(m)
            except OSError:
                return False
        return False

    def ensure(self, wait_s: float = 20.0) -> "ReducedDay":
        """Start a child that reduces what is new (when the copy is missing, partial or out of date) and, only when there is no copy yet, wait for it a while."""
        if self.needs_refresh() and time.time() - self.failed_at >= RETRY_AFTER_FAILURE_S:
            with self.lock:
                if self.thread is None or not self.thread.is_alive():
                    self.thread = threading.Thread(target=self._refresh_child, daemon=True, name=f"reduce-{self.day}")
                    self.thread.start()
        if self.manifest() is None and self.thread is not None:
            self.thread.join(wait_s)
        return self

    def _refresh_child(self) -> None:
        from . import heavy
        from data_layer.query import QueryBusy
        if not heavy.children_enabled():                              # JARVIS_VIEWER_CHILD=0: in this process (tests, or a fallback)
            res = reduce_day(self.store, self.day, log_fn=lambda *a: None)
            self.error = res.get("error")
            self.failed_at = time.time() if self.error else 0.0
            self.settled = not self.error
            return
        try:
            res = heavy.run_child(["-m", "webapp.reduced", "once", "--store", str(self.store), "--day", str(self.day)], timeout_s=900, wait_slot_s=2.0,
                                  cwd=str(Path(__file__).resolve().parent.parent))
        except QueryBusy:
            return                                                    # another heavy request holds the slot: the next request tries again
        except Exception as exc:                                      # noqa: BLE001
            self.error, self.failed_at = f"could not start the reducer: {exc!r}", time.time()
            return
        if not res.ok:
            self.error = f"the reducer stopped ({res.killed or 'exit ' + str(res.returncode)}): {res.stderr.strip()[-300:]}"
            self.failed_at = time.time()
            log.warning("signals tab: %s", self.error)
        else:
            self.error, self.settled = None, True

    # ------------------------------------------------------------------------------------------ slices
    def _read(self, kind: str, sid: str, bucket: str, und: str) -> Optional[pd.DataFrame]:
        d = key_dir(self.base, kind, sid, bucket, und)
        files = sorted(d.glob("*.parquet")) if d.is_dir() else []
        if not files:
            return None
        import duckdb
        q = "[" + ", ".join("'" + str(f).replace("'", "''") + "'" for f in files) + "]"
        con = duckdb.connect()
        try:
            con.execute("SET threads=1")
            con.execute("SET memory_limit='300MB'")
            df = con.execute(f"SELECT * FROM read_parquet({q}, union_by_name=true, hive_partitioning=false)").df()
        except Exception:                                             # noqa: BLE001 -- a file merged away between the listing and the read, or mixed types
            df = pd.concat([pd.read_parquet(f) for f in files if f.exists()], ignore_index=True)
        finally:
            con.close()
        return ss.merge_frames(kind, [df])

    def frame(self, kind: str, sid: str, bucket: str, und: str) -> Optional[pd.DataFrame]:
        m = self.manifest()
        if m is None:
            return None
        key = (kind, sid, bucket, und)
        with self.lock:
            if self._cache_gen != m.get("updated"):                   # the copy grew: what was cached is out of date
                self._cache.clear()
                self._cache_bytes, self._cache_gen = 0, m.get("updated")
            if key in self._cache:
                self._cache.move_to_end(key)
                return self._cache[key]
        if [kind, sid, bucket, und] not in m["keys"]:
            return None
        df = self._read(kind, sid, bucket, und)
        if df is None:
            return None
        size = int(df.memory_usage(deep=True).sum())
        cap = answer_cache_mb() * 2**20
        with self.lock:
            if size <= cap:
                self._cache[key] = df
                self._cache_bytes += size
                while self._cache_bytes > cap and len(self._cache) > 1:
                    _, old = self._cache.popitem(last=False)
                    self._cache_bytes -= int(old.memory_usage(deep=True).sum())
        return df

    def signals(self, und: str) -> list:
        m = self.manifest()
        return ss.signals_from_keys([tuple(k) for k in m["keys"]], und) if m else []

    def underlyings(self) -> list:
        m = self.manifest()
        return sorted({k[3] for k in m["keys"] if k[3]}) if m else []

    def progress(self) -> dict:
        m = self.manifest() or {}
        return {"loaded": bool(m.get("keys")) or self.settled, "files_done": int(m.get("files_done", 0)), "files_total": int(m.get("files_total", 0)),
                "error": m.get("error") or self.error}


_REDUCED: "OrderedDict[tuple, ReducedDay]" = OrderedDict()
_REDUCED_LOCK = threading.Lock()


def get_reduced(store, day) -> ReducedDay:
    key = (str(Path(store)), str(day))
    with _REDUCED_LOCK:
        rd = _REDUCED.pop(key, None) or ReducedDay(store, day)
        _REDUCED[key] = rd
        while len(_REDUCED) > 8:
            _REDUCED.popitem(last=False)
    return rd


def forget() -> None:
    with _REDUCED_LOCK:
        _REDUCED.clear()


def enabled() -> bool:
    """JARVIS_VIEWER_REDUCED=0 reads the day into the viewer's memory as before 22 Nov 2026 (a fallback)."""
    return os.environ.get("JARVIS_VIEWER_REDUCED", "1") != "0"


# ---------------------------------------------------------------------------------------------------------- command line
def main(argv=None) -> int:
    import argparse
    ap = argparse.ArgumentParser(prog="python -m webapp.reduced", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("action", choices=["once", "follow", "status", "prune"])
    ap.add_argument("--store", default=None)
    ap.add_argument("--day", type=date.fromisoformat, default=None, help="default: the newest day with live files")
    ap.add_argument("--every", type=float, default=60.0, help="follow: seconds between reads")
    ap.add_argument("--until", default="10:10", help="follow: stop at this UTC time (HH:MM)")
    ap.add_argument("--keep", type=int, default=KEEP_DAYS)
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    store = resolve_store(a.store)
    if a.action == "status":
        for d in reduced_days(store):
            m = read_manifest(reduced_dir(store, d)) or {}
            print(d, "complete" if m.get("complete") else "partial", f"files {m.get('files_done')}/{m.get('files_total')}", f"keys {len(m.get('keys', []))}",
                  f"rows {m.get('rows')}", "error: " + str(m.get("error")) if m.get("error") else "")
        return 0
    if a.action == "prune":
        print("removed:", prune(store, a.keep))
        return 0
    live = ss.live_days(store)
    day = a.day or (date.fromisoformat(live[0]) if live else None)
    if day is None:
        print("no live days in", store)
        return 1
    if a.action == "once":
        res = reduce_day(store, day)
        return 1 if res.get("error") else 0
    hh, mm = (int(x) for x in a.until.split(":"))
    while True:                                                       # follow: a running day, until the engines have stopped
        res = reduce_day(store, day)
        now = datetime.now(timezone.utc)
        if (now.hour, now.minute) >= (hh, mm) or res.get("complete"):
            return 1 if res.get("error") else 0
        time.sleep(a.every)


if __name__ == "__main__":
    raise SystemExit(main())
