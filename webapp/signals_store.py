"""
A compact, read-only, incremental copy of one day's live signal rows, for the viewer's Signals tab (SCRUM-206).

The engines write <store>/live/date=D/*.parquet, one small file every 15 s per process (thousands of files, hundreds of MB by the close; the per-contract
signals at 1 s are the bulk).  The first version of the tab read every file of the day into one pandas frame on every refresh, which the viewer's
1.5 GB memory cap does not allow.  Here each file is read once (DuckDB, batches of files), reduced to what the tab shows, and kept:

  single    a signal with one row per bucket (the chain signals, flow, noise band ...): every row of the day
  fut       a per-contract signal: the rows of the futures contract, every bucket (the front future's series)
  hist      a per-contract signal: the option rows at the full minute only (one snapshot a minute), for the by-strike heatmap
  last      a per-contract signal: the newest snapshot of the options
  events    block prints: the newest few hundred

After the first full pass a refresh reads only the files that are new.  The store is never written.  A day that is being read for the first time is
loaded by a background thread: callers wait up to `wait_s` and otherwise get what has been read so far (`loaded` is False).
"""

from __future__ import annotations

import gc
import logging
import os
import pickle
import re
import threading
import time
from collections import OrderedDict, defaultdict
from datetime import date
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

log = logging.getLogger("webapp.signals")

BATCH = 50                       # files per DuckDB read
TAIL_FILES = 4                   # the newest files are read whole (the newest snapshot of every per-contract signal is in them) ...
TAIL_MAX_BYTES = 50 * 2**20      # ... when they are small: a compacted day is one file of 1 GB and is always thinned
FLUSH_ROWS = 150_000             # the reducer hands over what it kept after this many rows, also inside one big file
BATCH_ROWS = 196_608             # rows per streamed pandas frame
MIN_REFRESH_S = 3.0              # look for new files at most this often
HIST_SECONDS = 60                # per-contract signals keep the snapshot at second 0 of each minute
EVENT_SIGNALS = {"block_prints"}
MAX_EVENTS = 500
MAX_DAYS = 3                     # days kept in memory
BUS_COLS = ("t_vendor", "t_recv", "t_emit", "provisional")
CACHE_SCHEMA = 2                 # bump when what is kept per signal changes: older caches are then ignored


def rss_mb() -> float:
    try:
        import os
        with open("/proc/self/statm") as f:
            return int(f.read().split()[1]) * os.sysconf("SC_PAGE_SIZE") / 2**20
    except (OSError, ValueError):
        return 0.0


def mem_guard_mb() -> float:
    """Stop reading files above this resident size.  The viewer service is capped at 3 GB (MemoryMax) and DuckDB gets 400 MB of it, so 2200 MB leaves room
    for a request in flight.  (1100 MB was the guard of the 1.5 GB cap of 18 Nov 2026; on 19 Nov the viewer itself was at 1.1-1.9 GB before loading
    anything, so a day could never finish loading.)"""
    try:
        return float(os.environ.get("JARVIS_VIEWER_MEM_GUARD_MB", 2200))
    except ValueError:
        return 2200.0


_STAMP = re.compile(r"-(\d{13})-")


def _by_time(path: Path) -> tuple:
    """Files in the order they were written (the millisecond stamp in the name), not by writer name: the futures book writer's files ('book-...') sort before
    every engine file ('engine-...'), so a read that stops early at a guard had only the four futures signals (19 Nov 2026), instead of the first part of
    the day for every signal."""
    m = _STAMP.search(path.name)
    return (int(m.group(1)) if m else 0, path.name)


def _give_back_memory() -> None:
    """Hand freed memory back to the system: after a read the viewer's resident size stayed at 2.4-2.6 GB although the day it keeps is small (20 Nov 2026),
    because glibc keeps freed pandas buffers; the guard and the service cap count resident memory."""
    gc.collect()
    try:
        import ctypes
        ctypes.CDLL("libc.so.6").malloc_trim(0)
    except Exception:                                 # noqa: BLE001 -- not glibc: nothing to hand back
        pass


def is_future(symbols: pd.Series) -> pd.Series:
    return symbols.astype(str).str.contains("_FUT_", regex=False)


def live_days(store) -> list:
    """Days that have live rows, newest first."""
    base = Path(store) / "live"
    days = {p.name.split("=", 1)[1] for p in base.glob("date=*") if any(p.glob("*.parquet"))} if base.is_dir() else set()
    from . import reduced
    return sorted(days | set(reduced.reduced_days(store)), reverse=True)        # a day whose live files were removed is still there, reduced


def merge_frames(kind: str, parts: list) -> Optional[pd.DataFrame]:
    """The frames kept for one (kind, signal, bucket, underlying) as one: ordered by time, a row repeated by a restarted engine kept once, the newest
    snapshot only for 'last', the newest few hundred for 'events'.  Used by the in-process loader and by the reduced day's reader, so both give the same rows."""
    parts = [p for p in parts if p is not None and len(p)]
    if not parts:
        return None
    df = pd.concat(parts, ignore_index=True) if len(parts) > 1 else parts[0]
    subset = ["known_at"] + [c for c in ("symbol", "expiry", "strike", "cp") if c in df.columns]
    df = df.sort_values("known_at", kind="stable").drop_duplicates(subset=subset, keep="last").reset_index(drop=True)
    if kind == "last":
        df = df[df["known_at"] == df["known_at"].max()].reset_index(drop=True)
    elif kind == "events":
        df = df.tail(MAX_EVENTS).reset_index(drop=True)
    return df


def signals_from_keys(keys, und: str) -> list:
    """[(signal, bucket, kind)] with kind single, contract or events, for the keys (kind, signal, bucket, underlying) a day holds."""
    out: dict = {}
    for kind, sid, bucket, u in keys:
        if u != und:
            continue
        k = (sid, bucket)
        rank = {"single": 0, "events": 1, "fut": 2, "hist": 2, "last": 2}[kind]
        label = {"single": "single", "events": "events", "fut": "contract", "hist": "contract", "last": "contract"}[kind]
        if k not in out or rank < out[k][0]:
            out[k] = (rank, label)
    return sorted(((s, b, v[1]) for (s, b), v in out.items()), key=lambda x: (x[0], pd.Timedelta(x[1]) if x[1] else pd.Timedelta(0)))


class LiveDay:
    def __init__(self, store, day: date):
        self.store, self.day = Path(store), day
        self.lock = threading.RLock()                 # state: chunks and counters, held briefly
        self.refresh_lock = threading.Lock()          # one reader of files at a time
        self.seen: set = set()
        self.chunks: dict = defaultdict(list)         # (kind, signal, bucket, underlying) -> [DataFrame]
        self.loaded = False
        self.files_total = 0
        self.last_refresh = 0.0
        self.error: Optional[str] = None
        self.thread: Optional[threading.Thread] = None
        self._kept = 0                                # rows kept since the last flush
        self.sink = None                              # set by the reducer: called with the kept chunks after every batch of files, which are then dropped from memory

    # ------------------------------------------------------------------------------------------ reading
    def _files(self) -> list:
        from signals.options.stream import live_dir
        folder = live_dir(self.store, self.day)
        return sorted(folder.glob("*.parquet"), key=_by_time) if folder.is_dir() else []

    @staticmethod
    def _batches(paths: list, thin: bool = False):
        """The rows of `paths` as pandas frames of at most BATCH_ROWS rows, streamed: a compacted day is ONE file of 60 M rows and 136 columns (1.1 GB, 18 Nov
        2026) and the whole result as one pandas frame was 2.7 GB even after the thinning, because every signal shares those 136 columns.  With `thin` the
        per-contract option rows that are not at a full minute are dropped inside DuckDB (what `hist` keeps anyway).  A frame is reduced by _ingest before the
        next is read, and _ingest keeps only each signal's own (not all-null) columns."""
        import duckdb
        q = "[" + ", ".join("'" + str(p).replace("'", "''") + "'" for p in paths) + "]"
        sql = f"SELECT * FROM read_parquet({q}, union_by_name=true)"
        where = " WHERE symbol IS NULL OR contains(symbol, '_FUT_') OR date_trunc('minute', known_at) = known_at OR signal = 'block_prints'"
        for with_filter in ((True, False) if thin else (False,)):
            con = None
            try:
                con = duckdb.connect()
                con.execute(f"SET threads={int(os.environ.get('JARVIS_REDUCE_THREADS', 2))}")
                con.execute(f"SET memory_limit='{int(os.environ.get('JARVIS_REDUCE_DUCKDB_MB', 400))}MB'")
                con.execute(sql + (where if with_filter else ""))
                while True:                                # chunks of about BATCH_ROWS rows (100 DuckDB vectors of 2048), never the whole result
                    chunk = con.fetch_df_chunk(max(BATCH_ROWS // 2048, 1))
                    if chunk is None or not len(chunk):
                        break
                    yield chunk
                con.close()
                return
            except Exception as exc:                  # noqa: BLE001 -- no symbol column in these files, mixed column types, or a half-written file
                log.info("signals tab: batch read (filter=%s) failed: %s", with_filter, str(exc)[:120])
                if con is not None:
                    try:
                        con.close()
                    except Exception:                 # noqa: BLE001
                        pass
                if with_filter is False or not thin:
                    break
        for p in paths:                               # last resort, file by file (small files only)
            try:
                if Path(p).stat().st_size < 64 * 2**20:
                    yield pd.read_parquet(p)
            except Exception:                         # noqa: BLE001
                continue

    @staticmethod
    def _read(paths: list, thin: bool = False) -> Optional[pd.DataFrame]:
        parts = list(LiveDay._batches(paths, thin))
        return pd.concat(parts, ignore_index=True) if parts else None

    # ------------------------------------------------------------------------------------------ the day's reduced copy on disk
    def cache_file(self) -> Path:
        base = Path(os.environ.get("JARVIS_VIEWER_CACHE") or (self.store / "viewer-cache"))
        return base / f"signals-date={self.day}.pkl"

    @staticmethod
    def _fingerprint(files: list) -> list:
        return [(f.name, f.stat().st_size, f.stat().st_mtime_ns) for f in files]

    @staticmethod
    def _cacheable(files: list) -> bool:
        """Only a finished day: every file is the nightly compaction (the live files of a running day change every 15 s)."""
        return bool(files) and all(f.name.startswith("live-compacted") for f in files)

    def _load_cache(self, files: list) -> bool:
        """The reduced day saved by an earlier read (or by `warm`), when the files it was made from are unchanged: seconds instead of a minute."""
        path = self.cache_file()
        if not self._cacheable(files) or not path.exists():
            return False
        try:
            with open(path, "rb") as fh:
                blob = pickle.load(fh)
            if blob.get("schema") != CACHE_SCHEMA or blob.get("fingerprint") != self._fingerprint(files):
                return False
            with self.lock:
                self.chunks = defaultdict(list, blob["chunks"])
                self.seen = {f.name for f in files}
                self.files_total = len(files)
                self.last_refresh = time.monotonic()
                self.loaded = True
                self.error = None
            log.info("signals tab: %s read from the cache %s", self.day, path)
            return True
        except Exception as exc:                      # noqa: BLE001 -- a damaged or foreign cache is just not used
            log.warning("signals tab: cache %s not used: %r", path, exc)
            return False

    def _save_cache(self, files: list) -> None:
        if not self._cacheable(files):
            return
        path = self.cache_file()
        try:
            merged = {}
            for key in list(self.chunks):
                df = self.frame(*key)
                if df is not None:
                    merged[key] = [df]
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_name(path.name + f".{os.getpid()}.tmp")
            with open(tmp, "wb") as fh:
                pickle.dump({"schema": CACHE_SCHEMA, "fingerprint": self._fingerprint(files), "chunks": merged}, fh, protocol=pickle.HIGHEST_PROTOCOL)
            tmp.replace(path)
            log.info("signals tab: cache written %s (%.0f MB)", path, path.stat().st_size / 2**20)
        except Exception as exc:                      # noqa: BLE001 -- the tab works without it
            log.warning("signals tab: cache not written: %r", exc)

    def _over_guard(self) -> bool:
        """True when the viewer is above the memory guard even after the other cached days are dropped (this one is being read)."""
        if rss_mb() <= mem_guard_mb():
            return False
        evict_others(self)
        gc.collect()
        return rss_mb() > mem_guard_mb()

    def refresh(self) -> None:
        with self.refresh_lock:
            all_files = self._files()
            if self.seen and self._cacheable(all_files) and not self.seen <= {f.name for f in all_files}:
                # the nightly job replaced the day's small files by one compacted file while this process held the day: what is held is stale, and the
                # compacted file must not be added on top of it (20 Nov 2026: 4350 small files held, 'read 4350 of 4351' repeated for hours)
                with self.lock:
                    self.chunks = defaultdict(list)
                    self.seen = set()
                    self.loaded = False
            if not self.seen and self.sink is None and self._load_cache(all_files):
                return
            files = [f for f in all_files if f.name not in self.seen]
            with self.lock:
                self.files_total = len(self.seen) + len(files)
            whole = set(files[-TAIL_FILES:]) if len(files) > 2 * TAIL_FILES else set(files)       # the newest few files (all of a short list) are read whole ...
            thin_of = {f: not (f in whole and f.stat().st_size <= TAIL_MAX_BYTES) for f in files}    # ... unless they are big: a compacted day is one 1 GB file
            for i in range(0, len(files), BATCH):
                if self._over_guard():                         # better a partial day and a message than a killed viewer
                    with self.lock:
                        self.error = f"memory guard: read {len(self.seen)} of {self.files_total} files, the viewer is at {rss_mb():.0f} MB (limit {mem_guard_mb():.0f} MB)"
                        self.loaded = True
                    log.warning("signals tab: %s", self.error)
                    return
                batch = files[i:i + BATCH]
                groups: list = []                                  # runs of files with the same treatment, in order
                for f in batch:
                    if groups and groups[-1][1] == thin_of[f]:
                        groups[-1][0].append(f)
                    else:
                        groups.append(([f], thin_of[f]))
                for chunk, thin in groups:
                    for df in self._batches(chunk, thin=thin):
                        if len(df):
                            self._ingest(df)
                            if self.sink is not None and self._kept >= FLUSH_ROWS:          # one compacted file is one batch: flush inside it too
                                self._flush()
                with self.lock:
                    self.seen.update(f.name for f in batch)
                self._flush()
                _give_back_memory()
            with self.lock:
                self.last_refresh = time.monotonic()
                self.loaded = True
                self.error = None
            if self.sink is None:
                self._save_cache(all_files)

    def _flush(self) -> None:
        """With a sink (the reducer): hand over what the last batch kept and forget it, so this process never holds the day."""
        if self.sink is None:
            return
        with self.lock:
            taken, self.chunks, self._kept = self.chunks, defaultdict(list), 0
        self.sink(taken)

    def ensure(self, wait_s: float = 20.0) -> "LiveDay":
        """First call: load in a background thread and wait up to `wait_s`.  Later calls: read the new files (cheap)."""
        if not self.loaded:
            with self.lock:
                if self.thread is None or not self.thread.is_alive():
                    self.thread = threading.Thread(target=self._load, daemon=True, name=f"signals-load-{self.day}")
                    self.thread.start()
            self.thread.join(wait_s)
        elif time.monotonic() - self.last_refresh >= MIN_REFRESH_S:
            try:
                self.refresh()
            except Exception as exc:                  # noqa: BLE001 -- the tab keeps what it has
                self.error = repr(exc)
                log.warning("signals tab: refresh failed: %r", exc)
        return self

    def _load(self) -> None:
        try:
            self.refresh()
        except Exception as exc:                      # noqa: BLE001
            self.error = repr(exc)
            log.exception("signals tab: loading %s failed", self.day)

    # ------------------------------------------------------------------------------------------ reducing
    def _add(self, kind: str, sid: str, bucket: str, und: str, frame: pd.DataFrame) -> None:
        if len(frame):
            with self.lock:
                self.chunks[(kind, sid, bucket, und)].append(frame)
                self._kept += len(frame)

    def _ingest(self, df: pd.DataFrame) -> None:
        df = df.drop(columns=[c for c in BUS_COLS if c in df.columns])
        if "signal" not in df.columns or "known_at" not in df.columns:
            return
        for col, default in (("bucket", "1min"), ("underlying", "")):
            df[col] = df[col].fillna(default) if col in df.columns else default
        df["known_at"] = pd.to_datetime(df["known_at"])
        for (sid, bucket, und), g in df.groupby(["signal", "bucket", "underlying"], sort=False):
            g = g.dropna(axis=1, how="all").drop(columns=["signal", "bucket", "underlying"])
            if sid in EVENT_SIGNALS:
                self._add("events", sid, bucket, und, g.tail(MAX_EVENTS))
            elif "symbol" in g.columns:                      # per contract: it has symbols (a chunk with one contract per bucket, as a few small live files have,
                                                             # must not become a 'single' series: the tab prefers it and shows only those chunks)
                fut = is_future(g["symbol"])
                self._add("fut", sid, bucket, und, g[fut])
                opt = g[~fut]
                if len(opt):
                    ka = opt["known_at"]
                    snap = opt[(ka.dt.second % HIST_SECONDS == 0) & (ka.dt.microsecond == 0)]
                    num = snap.select_dtypes("float64").columns
                    snap = snap.astype({c: "float32" for c in num})
                    self._add("hist", sid, bucket, und, snap)
                    self._add("last", sid, bucket, und, opt[ka == ka.max()])
            else:
                self._add("single", sid, bucket, und, g)

    # ------------------------------------------------------------------------------------------ reading what was kept
    def frame(self, kind: str, sid: str, bucket: str, und: str) -> Optional[pd.DataFrame]:
        key = (kind, sid, bucket, und)
        with self.lock:
            parts = self.chunks.get(key)
            if not parts:
                return None
            if len(parts) == 1 and parts[0].attrs.get("merged"):
                return parts[0]
            df = merge_frames(kind, parts)
            if df is None:
                return None
            df.attrs["merged"] = True
            self.chunks[key] = [df]
            return df

    def signals(self, und: str) -> list:
        """[(signal, bucket, kind)] with kind single, contract or events."""
        with self.lock:
            keys = list(self.chunks.keys())
        return signals_from_keys(keys, und)

    def underlyings(self) -> list:
        with self.lock:
            return sorted({k[3] for k in self.chunks if k[3]})

    def progress(self) -> dict:
        with self.lock:
            return {"loaded": self.loaded, "files_done": len(self.seen), "files_total": self.files_total, "error": self.error}


_CACHE: "OrderedDict[tuple, LiveDay]" = OrderedDict()
_CACHE_LOCK = threading.Lock()


def get_day(store, day: date, wait_s: float = 20.0):
    """The day's signals, from its reduced copy on disk (webapp/reduced.py: a child process reduces, this process reads slices), or, when that is switched
    off (JARVIS_VIEWER_REDUCED=0) or the reducer failed, read into this process's memory (the older path, bounded by the memory guard)."""
    from . import reduced
    if reduced.enabled():
        rd = reduced.get_reduced(store, day)
        rd.ensure(wait_s)
        if rd.available() or rd.settled or not rd.failed_at:
            return rd
        log.warning("signals tab: %s; reading %s in the viewer", rd.error, day)
    key = (str(Path(store)), day)
    with _CACHE_LOCK:
        ld = _CACHE.pop(key, None) or LiveDay(store, day)
        _CACHE[key] = ld
        while len(_CACHE) > MAX_DAYS:
            _CACHE.popitem(last=False)
    return ld.ensure(wait_s)


def evict_others(keep: "LiveDay") -> None:
    """Drop every cached day but `keep` (a day being read needs the memory more than yesterday's does)."""
    with _CACHE_LOCK:
        for k in [k for k, v in _CACHE.items() if v is not keep]:
            _CACHE.pop(k, None)


def forget() -> None:
    """Drop every cached day (tests)."""
    with _CACHE_LOCK:
        _CACHE.clear()
    from . import reduced
    reduced.forget()


def main(argv=None) -> int:
    """python -m webapp.signals_store warm [--day D] [--store S]: reduce a compacted day once (cron, after the compaction) so the viewer's first read of it
    takes seconds.  Run outside the viewer's service."""
    import argparse
    ap = argparse.ArgumentParser(prog="python -m webapp.signals_store")
    ap.add_argument("action", choices=["warm"])
    ap.add_argument("--day", type=date.fromisoformat, default=None, help="default: the newest day with live rows")
    ap.add_argument("--store", type=Path, default=None)
    a = ap.parse_args(argv)
    env = os.getenv("JARVIS_SIGNAL_STORE")
    store = a.store or (Path(env) if env else (Path.cwd() / "store" if (Path.cwd() / "store" / "live").is_dir() else Path("/srv/jarvis-research/store")))
    day = a.day or (date.fromisoformat(live_days(store)[0]) if live_days(store) else None)
    if day is None:
        print("no live days in", store)
        return 1
    t0 = time.perf_counter()
    ld = LiveDay(store, day)
    if not ld._cacheable(ld._files()):
        print(f"{day}: the day's rows are not compacted yet (the compaction has not run): nothing to warm")
        return 0
    from . import reduced
    res = reduced.reduce_day(store, day, log_fn=lambda *a: None)             # the same reduction the viewer's child runs, here in cron's own process
    print(f"{day}: {res} in {time.perf_counter() - t0:.0f} s; reduced copy {reduced.reduced_dir(store, day)}")
    return 0 if res.get("complete") and not res.get("error") else 1


if __name__ == "__main__":
    raise SystemExit(main())
