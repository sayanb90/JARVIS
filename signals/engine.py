"""
The live signal engine (SCRUM-201, SCRUM-159): event-driven, one process, every live signal at its configured cadence.

  python -m signals live                    # cron 03:44 UTC; until 10:02
  python -m signals feasibility             # cost of every live signal at 1 s, 10 s and 1 min on a synthetic 2,000-contract chain

  ticks ──(bus, or tick files when the bus is off)──► Engine ──► one incremental runner per cadence group (1s / 10s / 1min)
                                                         └─► signal rows ──► "signals" bus (now) and <store>/live/date=D/ (every 15 s)

- Cadence per signal from signals/cadence.toml (signals/cadence.py); each cadence group is a LiveRunner at that bucket width, so the
  chain, smile, tick chain and every signal of the group are built per bucket of that width, incrementally, within each signal's
  declared lookback (signals/lookbacks.py).
- Ticks come from the in-memory bus (JARVIS_TICK_BUS=1 on the feeds) or, when the bus is off, from new tick files (as before).
- A bucket closes when an event past its end has been seen, or when the wall clock is `grace` past its end (a quiet market).
- Guardrail: a group whose cycle takes longer than its cadence OVERRUNS_BEFORE_FALLBACK times in a row moves to the next coarser
  cadence for the rest of the session (logged); the engine caps its own memory (JARVIS_SIGNAL_LIVE_MAX_MB).
- The futures order-book signals computed inside the feed (JARVIS_LIVE_SIGNALS=1) are not computed again here.
"""

from __future__ import annotations

import logging
import resource
import time as _time
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd

from signals import cadence as cadence_mod
from signals.live_runner import (OVERRUNS_BEFORE_FALLBACK, LiveRunner, TickFiles, history_for, live_signal_ids, load_baselines,
                                 slot_volume_history)

log = logging.getLogger("signals.engine")

WRITE_EVERY_S = 15.0
IN_FEED = ("fut_ofi", "mlofi_future")              # computed tick by tick inside the feed when JARVIS_LIVE_SIGNALS=1
LAYER = {"1s": "1s", "10s": "10s", "1min": "1m"}


# ---------------------------------------------------------------------------------------------- sources
class BusSource:
    """Ticks from the in-memory bus (every feed publisher)."""

    def __init__(self, names: Optional[list[str]] = None, env=None):
        from data_layer import bus
        self._bus = bus
        self.sub = bus.Subscriber(names or bus.feed_names(env), topics=["tick"], env=env)

    def poll(self, timeout_ms: int = 100) -> pd.DataFrame:
        msgs = [m for _, m in self.sub.poll(timeout_ms=timeout_ms)]
        return self._bus.rows_frame(msgs) if msgs else pd.DataFrame()

    @property
    def missed(self) -> int:
        return self.sub.missed


class FileSource:
    """Ticks from the day's new tick files (the bus is off)."""

    def __init__(self, lake: Path, day: date, sleep=_time.sleep):
        self.files = TickFiles(lake, day)
        self._sleep = sleep
        self.missed = 0

    def poll(self, timeout_ms: int = 1000) -> pd.DataFrame:
        df = self.files.new()
        if not len(df):
            self._sleep(timeout_ms / 1000)
        return df


# ---------------------------------------------------------------------------------------------- the engine
def signal_rows(df: pd.DataFrame) -> tuple[list[str], list[list]]:
    """Signal rows for the bus: times as nanoseconds, dates as ISO text, NaN as None."""
    from data_layer.bus import to_ns
    cols = list(df.columns)
    rows = []
    for rec in df.itertuples(index=False):
        row = []
        for v in rec:
            if isinstance(v, (pd.Timestamp, datetime)):
                row.append(to_ns(v))
            elif isinstance(v, date):
                row.append(v.isoformat())
            elif v is None or (isinstance(v, float) and v != v):
                row.append(None)
            elif hasattr(v, "item"):
                row.append(v.item())
            else:
                row.append(v if isinstance(v, (int, float, str, bool)) else str(v))
        rows.append(row)
    return cols, rows


@dataclass
class Engine:
    underlyings: list[str]
    histories: dict                                    # underlying -> history raw (history_for)
    cadence: dict                                      # signal -> "1s" / "10s" / "1min"
    store: Optional[Path] = None
    publisher: object = None                           # data_layer.bus.Publisher for signal rows, or None
    grace: pd.Timedelta = pd.Timedelta(seconds=1)
    exclude: tuple = ()
    baselines: dict = field(default_factory=dict)     # underlying -> cadence -> baselines
    volume_slot_daily: dict = field(default_factory=dict)   # underlying -> table (live: built at the open)
    runners: dict = field(default_factory=dict)       # (underlying, cadence) -> LiveRunner
    pending: list = field(default_factory=list)
    last_write: float = field(default_factory=_time.monotonic)
    fallbacks: list = field(default_factory=list)

    def __post_init__(self):
        groups = cadence_mod.groups({k: v for k, v in self.cadence.items() if k not in self.exclude})
        for u in self.underlyings:
            for c, ids in groups.items():
                r = LiveRunner(u, self.histories[u], bucket=c, ids=ids)
                r.baselines = self.baselines.get(u, {}).get(c, {})
                r._vsd = self.volume_slot_daily.get(u)            # the 20-session volume baseline, built at the open
                self.runners[(u, c)] = r

    def on_ticks(self, df: pd.DataFrame) -> None:
        if df is None or not len(df):
            return
        for r in self.runners.values():
            r.ingest(df)

    def step(self, now: Optional[pd.Timestamp] = None) -> pd.DataFrame:
        """Close every bucket that is due, compute its signals, publish them; write to the store every WRITE_EVERY_S seconds."""
        now = pd.Timestamp(now) if now is not None else None
        out = []
        for key, r in list(self.runners.items()):
            if now is not None and r.watermark is not None:
                r.advance(now - self.grace)
            if r.fixed is None and getattr(r, "_vsd", None) is not None and r.watermark is not None:
                r.fixed = r._fixed_providers(r.watermark.normalize(), volume_slot_daily=r._vsd)
            t0 = _time.perf_counter()
            rows = r.cycle()
            secs = _time.perf_counter() - t0
            if len(rows):
                out.append(rows)
            if len(rows) and r.check_budget(secs, r.td.total_seconds()):
                self._coarsen(key, r)
        rows = pd.concat(out, ignore_index=True) if out else pd.DataFrame()
        if len(rows):
            if self.publisher is not None:
                for sid, g in rows.groupby("signal"):
                    cols, data = signal_rows(g.dropna(axis=1, how="all"))
                    self.publisher.publish("signal", cols, data)
            self.pending.append(rows)
        if self.store is not None and self.pending and _time.monotonic() - self.last_write >= WRITE_EVERY_S:
            self.flush()
        return rows

    def flush(self) -> int:
        from signals.options.stream import write_rows
        if not self.pending:
            return 0
        rows = pd.concat(self.pending, ignore_index=True)
        self.pending, self.last_write = [], _time.monotonic()
        n = 0
        for (u, b), g in rows.groupby(["underlying", "bucket"]):
            n += write_rows(g.to_dict("records"), self.store, prefix=f"engine-{u}-{LAYER.get(b, b)}")
        return n

    def _coarsen(self, key, r: LiveRunner) -> None:
        """Move an overrunning group to the next coarser cadence for the rest of the session."""
        u, c = key
        order = cadence_mod.ORDER
        if order.index(c) == len(order) - 1:
            log.warning("%s %s: cycles slower than %s, already the coarsest cadence", u, c, c)
            r.overruns = 0
            return
        nc = order[order.index(c) + 1]
        log.warning("%s: %s cycles overran %d times in a row; moving %d signals to %s for the rest of the session", u, c,
                    OVERRUNS_BEFORE_FALLBACK, len(r.ids), nc)
        self.fallbacks.append((u, c, nc, len(r.ids)))
        target = self.runners.get((u, nc))
        if target is None:
            target = LiveRunner(u, self.histories[u], bucket=nc, ids=list(r.ids))
            target.ingest(r.ticks)
            self.runners[(u, nc)] = target
        else:
            target.ids = sorted(set(target.ids) | set(r.ids))
        del self.runners[key]


def run_engine(engine: Engine, source, until: pd.Timestamp, clock=lambda: pd.Timestamp.utcnow().tz_localize(None),
               poll_ms: int = 100) -> dict:
    """The session loop: poll the source, feed the runners, close due buckets, until `until` (UTC)."""
    stats = {"cycles": 0, "rows": 0, "max_step_s": 0.0}
    while clock() < until:
        df = source.poll(timeout_ms=poll_ms)
        engine.on_ticks(df)
        t0 = _time.perf_counter()
        rows = engine.step(clock())
        secs = _time.perf_counter() - t0
        stats["cycles"] += 1
        stats["rows"] += len(rows)
        stats["max_step_s"] = max(stats["max_step_s"], secs)
        if len(rows):
            log.info("step %.3fs rows %d missed_on_bus %d peak_rss %.0fMB", secs, len(rows), getattr(source, "missed", 0),
                     resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024)
    engine.flush()
    stats["fallbacks"] = engine.fallbacks
    return stats


def start(lake: Path, store: Path, underlyings: Iterable[str], until_utc: str = "10:02", env=None) -> dict:
    """Load the history once, build the engine from the cadence config, run the session."""
    import os
    from data_layer import bus
    env = os.environ if env is None else env
    day = datetime.utcnow().date()
    ids = live_signal_ids()
    cad = cadence_mod.load(ids, env=env)
    exclude = IN_FEED if env.get("JARVIS_LIVE_SIGNALS", "").strip().lower() in ("1", "true", "yes", "on") else ()
    us = list(underlyings)
    hist = {u: history_for(lake, u, day) for u in us}
    groups = cadence_mod.groups(cad)
    base = {u: {c: load_baselines(store, u, day, ids_c, c) for c, ids_c in groups.items()} for u in us}
    vsd = {u: slot_volume_history(lake, u, day) for u in us}
    pub = bus.Publisher("signals", env=env) if bus.bus_enabled(env) else None
    eng = Engine(us, hist, cad, store=store, publisher=pub, exclude=exclude, baselines=base, volume_slot_daily=vsd)
    src = BusSource(env=env) if bus.bus_enabled(env) else FileSource(lake, day)
    log.info("live engine: %s; cadence groups %s; source %s", us, {c: len(v) for c, v in groups.items()}, type(src).__name__)
    until = pd.Timestamp(datetime.combine(day, datetime.strptime(until_utc, "%H:%M").time()))
    return run_engine(eng, src, until)
