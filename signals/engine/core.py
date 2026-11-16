"""
Engine v2 (SCRUM-201): ticks -> per-tick state -> operators, one process per underlying.

  ticks (bus or replay) ──► reorder window ──► Session.apply (OptionBook, BarBook; O(1) per tick) and native tape operators
                                     │
                                     └─ event time crosses a bucket end (per cadence) ──► Session.close ─► chain snapshot (numpy)
                                                                                                        └─► operators ─► final rows
  timer (JARVIS_SIGNAL_TICK_MS, default 250 ms) ──► provisional values of the open buckets (native operators) for the strategy

Operators (signals/engine/operators.py): every live signal is one.  Native operators keep their own state and read the snapshot arrays;
the others run the registered batch function on frames cut from the per-tick state, bounded by the signal's declared lookback
(signals/lookbacks.py) - the same inputs the pandas runner gave them, without rebuilding the chain.  Either way the final row of a
bucket equals the batch value (tests/unit/test_engine_v2.py replays a day against the runner).

Ordering: ticks are applied in event-time order.  Live, ticks are held for JARVIS_SIGNAL_REORDER_MS (default 200 ms) after the newest
event time seen, so a tick from another feed shard arriving slightly late still lands in its bucket; a tick older than a bucket
already closed is counted in `late` and dropped (the batch would have put it in that bucket).
"""

from __future__ import annotations

import logging
import math
import os
import time as _time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Iterable, Optional

import numpy as np
import pandas as pd

from signals import cadence as cadence_mod
from signals import lookbacks, normalise
from signals.engine import chain_ops, operators, surface
from signals.engine.state import EXPIRY_NS, NS_MIN, YEAR_NS, Session, td_ns
from signals.framework import REGISTRY, Context, normalise_output
from signals.live_runner import CHAIN_FREE, OVERRUNS_BEFORE_FALLBACK, LiveRunner, _last_sessions, bar_volume, daily_inputs, live_signal_ids
from signals.options.core import INDEX_SYMBOL, VIX_SYMBOL

log = logging.getLogger("signals.engine")

TICK_MS_DEFAULT = 250
REORDER_MS_DEFAULT = 200
HIST_PROVIDERS = ("spot", "vix", "futures", "fut_front")


def tick_ms(env=None) -> int:
    return int((os.environ if env is None else env).get("JARVIS_SIGNAL_TICK_MS", TICK_MS_DEFAULT))


def engine_kind(env=None) -> str:
    """JARVIS_SIGNAL_ENGINE: "operators" (engine v2, the default) or "runner" (the pandas runner per cadence group)."""
    v = (os.environ if env is None else env).get("JARVIS_SIGNAL_ENGINE", "operators").strip().lower()
    if v not in ("operators", "runner"):
        raise ValueError(f"JARVIS_SIGNAL_ENGINE={v!r}: use operators or runner")
    return v


# ---------------------------------------------------------------------------------------------- ticks kept for tape windows
class TickWindow:
    """This underlying's ticks of the last `keep` (plus each symbol's last earlier tick as an anchor), for operators that read ticks.

    Cost per call does not depend on the length of the window (on 16 Nov 2026 the first version copied the whole window, some 300,000
    rows, on every call, so a bucket took 280 ms at the start of the session and 520 ms twenty minutes later).  The ticks are kept in two
    parts, both in arrival order: `big` (older ticks, trimmed to `keep`, with `big_last`, the last tick per symbol in it) and `chunks` (the
    last few seconds, as they arrived).  A window that starts after every tick in `big` reads the chunks and takes its anchors from
    `big_last` and the chunks; any other window reads both parts.  The result is the same either way: the anchors come first, in arrival
    order, then the ticks of the window."""

    MERGE_AFTER = pd.Timedelta(seconds=15)       # move chunks to `big` once the oldest is this old (event time) ...
    MERGE_KEEP = pd.Timedelta(seconds=5)         # ... all but the last few seconds

    def __init__(self, keep: pd.Timedelta):
        self.keep = keep
        self.big = pd.DataFrame()
        self.big_last = pd.DataFrame()
        self.big_max: Optional[pd.Timestamp] = None
        self.chunks: list = []
        self._small: Optional[pd.DataFrame] = None
        self.fast = 0                            # windows served from the chunks alone / from both parts (for the tests and the status)
        self.slow = 0

    @property
    def df(self) -> pd.DataFrame:
        parts = [x for x in (self.big, self._small_df()) if len(x)]
        return pd.concat(parts, ignore_index=True) if len(parts) > 1 else (parts[0] if parts else pd.DataFrame())

    def add(self, df: pd.DataFrame) -> None:
        if len(df):
            self.chunks.append(df)
            self._small = None

    def _small_df(self) -> pd.DataFrame:
        if self._small is None:
            self._small = (pd.concat(self.chunks, ignore_index=True) if len(self.chunks) > 1 else self.chunks[0]) if self.chunks else pd.DataFrame()
        return self._small

    def trim(self, now: pd.Timestamp) -> None:
        if not self.chunks or not self.chunks[0]["fts"].max() < now - self.MERGE_AFTER:
            return
        moved = []
        while self.chunks and self.chunks[0]["fts"].max() < now - self.MERGE_KEEP:
            moved.append(self.chunks.pop(0))
        self._small = None
        m = pd.concat(moved, ignore_index=True) if len(moved) > 1 else moved[0]
        last = pd.concat([self.big_last, m], ignore_index=True) if len(self.big_last) else m
        self.big_last = last.groupby("symbol").tail(1)
        mx = m["fts"].max()
        self.big_max = mx if self.big_max is None else max(self.big_max, mx)
        self.big = pd.concat([self.big, m], ignore_index=True) if len(self.big) else m
        old = self.big["fts"] < now - self.keep
        if old.any():
            anchors = self.big[old].groupby("symbol").tail(1)
            self.big = pd.concat([anchors, self.big[~old]], ignore_index=True)

    def window(self, lo: pd.Timestamp, upto: pd.Timestamp) -> pd.DataFrame:
        small = self._small_df()
        if self.big_max is not None and self.big_max < lo:
            self.fast += 1
            before = small[small["fts"] < lo].groupby("symbol").tail(1) if len(small) else small
            anchors = self.big_last[~self.big_last["symbol"].isin(before["symbol"])] if len(before) else self.big_last
            inside = small[(small["fts"] >= lo) & (small["fts"] < upto)] if len(small) else small
            parts = [x for x in (anchors, before, inside) if len(x)]
            return pd.concat(parts, ignore_index=True) if parts else small
        self.slow += 1
        t = self.df
        if not len(t):
            return t
        keep = (t["fts"] >= lo) & (t["fts"] < upto)
        anchors = t[t["fts"] < lo].groupby("symbol").tail(1)
        return pd.concat([anchors, t[keep]], ignore_index=True)


# ---------------------------------------------------------------------------------------------- inputs loaded at the open
@dataclass
class Anchors:
    """Everything from earlier sessions, computed once: trimmed history, the fixed providers, the previous session's ATM, daily inputs,
    and the history part of the index / VIX / futures providers per cadence."""
    underlying: str
    history: dict
    day: pd.Timestamp
    ids: list
    volume_slot_daily: Optional[pd.DataFrame] = None
    daily_signals: Optional[dict] = None
    index_sessions: int = 30
    option_sessions: int = 1
    hist: dict = field(default_factory=dict)
    fixed: dict = field(default_factory=dict)
    hist_atm: pd.DataFrame = field(default_factory=pd.DataFrame)
    hist_prov: dict = field(default_factory=dict)

    def __post_init__(self):
        h = self.history
        raw = dict(h)
        for name, keep in (("index_1m", self.index_sessions), ("vix_1m", self.index_sessions), ("opt_1m", self.option_sessions),
                           ("fut_1m", self.index_sessions)):
            raw[name] = _last_sessions(h.get(name), keep)
        self.hist = raw
        ref = LiveRunner(self.underlying, h, ids=self.ids)
        self.fixed = ref._fixed_providers(self.day.normalize(), volume_slot_daily=self.volume_slot_daily)
        hist_opt = _last_sessions(h.get("opt_1m"), self.option_sessions)
        if hist_opt is not None and len(hist_opt):
            hc = Context(self.underlying, dict(h, opt_1m=hist_opt), cache=dict(self.fixed))
            self.hist_atm = hc.get("atm") if len(hc.get("chain")) else pd.DataFrame()
        if self.daily_signals is None:
            self.daily_signals = daily_inputs(self.underlying, h, self.ids)
        self.contract_table = Context(self.underlying, raw).get("contract_table")

    def providers(self, cadence: str) -> dict:
        if cadence not in self.hist_prov:
            c = Context(self.underlying, self.hist, bucket=cadence)
            self.hist_prov[cadence] = {k: c.get(k) for k in HIST_PROVIDERS}
        return self.hist_prov[cadence]


def _cat(a: pd.DataFrame, b: pd.DataFrame) -> pd.DataFrame:
    if a is None or not len(a):
        return b
    if b is None or not len(b):
        return a
    return pd.concat([a, b], ignore_index=True)


# ---------------------------------------------------------------------------------------------- one cadence
@dataclass
class Group:
    """The operators of one cadence and the frames they read, kept for the longest lookback in the group."""
    cadence: str
    ids: list
    td: int = 0
    b_open: Optional[int] = None                       # start of the open bucket (ns)
    since: Optional[pd.Timestamp] = None               # end of the last bucket closed (rows known after it are new)
    chain: list = field(default_factory=list)          # [(b, frame)] of the bar chain, within the longest lookback
    chain_live: list = field(default_factory=list)
    atm: list = field(default_factory=list)            # today's ATM rows (dicts), one per bucket
    atm_df: Optional[pd.DataFrame] = None
    bars: dict = field(default_factory=lambda: defaultdict(list))   # kind -> rows of today's index / VIX / future bars
    bars_df: dict = field(default_factory=dict)
    eras: Optional[pd.DataFrame] = None
    keep_ns: int = 0
    needs_chain: bool = True
    windows: dict = field(default_factory=dict)
    timings: dict = field(default_factory=dict)
    buckets: int = 0
    overruns: int = 0
    interval: dict = field(default_factory=lambda: {"n": 0, "sum_s": 0.0, "max_s": 0.0})   # bucket closes since the last status
    budget_s: Optional[float] = None                   # time a bucket close may take (default: the cadence)

    native: tuple = ()                                 # signals computed by native operators (signals/engine/operators.py)
    natives: object = None
    cnatives: object = None                            # native chain operators (signals/engine/chain_ops.py)

    def __post_init__(self):
        self.td = td_ns(self.cadence)
        tdd = pd.Timedelta(self.cadence)
        lbs = [lookbacks.lookback(sid, tdd) for sid in self.ids]
        for sid, lb in zip(self.ids, lbs):
            if sid not in self.native:
                self.windows.setdefault(lb, []).append(sid)
        self.keep_ns = (max(lbs) if lbs else pd.Timedelta(0)).value + 2 * self.td
        adapters = [s for s in self.ids if s not in self.native]
        self.needs_chain = not set(adapters) <= CHAIN_FREE           # chain frames for the adapters (chain natives read arrays)
        self.needs_live = self.needs_chain or bool({"signed_flow", "fair_value_gap", "spread_gate"} & set(self.native))


class OperatorEngine:
    """Engine v2 for one underlying and one session."""

    def __init__(self, underlying: str, history: dict, cadence: dict, day, baselines: Optional[dict] = None,
                 volume_slot_daily: Optional[pd.DataFrame] = None, daily_signals: Optional[dict] = None, reorder_ms: int = 0,
                 r: float = 0.065, native: Optional[Iterable[str]] = None):
        self.underlying = underlying
        self.day = pd.Timestamp(day).normalize()
        self.cadence = dict(cadence)
        ids = sorted(self.cadence)
        self.anchors = Anchors(underlying, history, self.day, ids, volume_slot_daily=volume_slot_daily, daily_signals=daily_signals)
        groups = cadence_mod.groups(self.cadence)
        self._nat = tuple(operators.NATIVE + chain_ops.CHAIN_NATIVE if native is None else native)
        self._meta = operators.contract_meta(self.anchors.contract_table)
        self._history = history
        # every cadence from the finest configured to the coarsest is kept in the state, so a group that overruns can move to a
        # coarser one mid-session with exact bucket volumes and bars (a cadence without signals only closes its buckets: cheap)
        order = cadence_mod.ORDER
        finest = min((order.index(c) for c in groups), default=len(order) - 1)
        cads = order[finest:]
        self.session = Session(underlying, history.get("contracts"), self.day, cadences=tuple(cads), r=r, rates=history.get("rates"))
        self.groups = {c: self._group(c, groups.get(c, [])) for c in cads}
        self.fallbacks: list = []                       # (from cadence, to cadence, signals, at)
        self.baselines = baselines or {}                # cadence -> signal -> baseline
        self.reorder_ns = int(reorder_ms) * 1_000_000
        self.pending = pd.DataFrame()                   # reorder buffer
        self.newest: Optional[int] = None               # newest event time seen (ns)
        self.closed_to: Optional[int] = None            # every bucket ending at or before this is closed, at every cadence
        keep = max((g.keep_ns for g in self.groups.values()), default=0)
        self.ticks = TickWindow(pd.Timedelta(keep + 2 * NS_MIN, unit="ns"))
        self.prefix = f"{underlying}_"
        self.symbols = {INDEX_SYMBOL[underlying], VIX_SYMBOL}
        self.late = 0
        self.stats = defaultdict(float)
        self.trigger = (0, 0)                           # (vendor time, receive time) of the tick that closed the last buckets
        self.last_tick_mono: Optional[float] = None
        self.errors = 0                                 # batches or timer steps that raised (logged, the session continues)

    def _group(self, c: str, sids: list) -> Group:
        g = Group(c, list(sids), native=tuple(s for s in sids if s in self._nat))
        self._attach(g)
        return g

    def _attach(self, g: Group) -> None:
        """(Re)build a group's native operators for its signals, keeping the state of operators that already exist."""
        tape = [s for s in g.native if s in operators.NATIVE]
        chain = [s for s in g.native if s in chain_ops.CHAIN_NATIVE]
        if tape:
            if g.natives is None:
                g.natives = operators.Natives(tape, g.td, self._meta)
            else:
                g.natives.ids = tape
        if chain:
            if g.cnatives is None:
                h = self._history
                g.cnatives = chain_ops.ChainNatives(chain, g.td, self.session.options, self.anchors.hist_atm, h.get("participant"),
                                                    daily=self.anchors.fixed.get("daily"),
                                                    vix_hist=self.anchors.providers(g.cadence).get("vix"), events=h.get("events"))
            else:
                g.cnatives.ids = chain
        if g.cnatives is not None and g.cnatives.needs_live:
            g.needs_live = True

    def _coarsen(self, g: Group, at: int) -> None:
        """Move an overrunning group's signals to the next coarser cadence for the rest of the session (logged, in the heartbeat and
        the health check).  The coarser cadence's buckets and bars are already in the state, so its values are exact from its next
        bucket on; operators with memory (skew smoothing, depth baseline, previous-bucket differences) warm up again there."""
        order = cadence_mod.ORDER
        if g.cadence == order[-1]:
            log.warning("%s: %s buckets keep overrunning and it is already the coarsest cadence", self.underlying, g.cadence)
            g.overruns = 0
            return
        nc = order[order.index(g.cadence) + 1]
        t = self.groups[nc]
        moved = list(g.ids)
        log.warning("%s: %s buckets overran %d times in a row; moving %d signals to %s for the rest of the session: %s",
                    self.underlying, g.cadence, OVERRUNS_BEFORE_FALLBACK, len(moved), nc, ", ".join(moved))
        self.fallbacks.append((g.cadence, nc, moved, str(pd.Timestamp(at))))
        t.ids = sorted(set(t.ids) | set(moved))
        t.native = tuple(s for s in t.ids if s in self._nat)
        nt = Group(nc, t.ids, native=t.native)                      # recompute windows, keep_ns and needs_* for the merged list
        t.windows, t.keep_ns, t.needs_chain, t.needs_live = nt.windows, nt.keep_ns, nt.needs_chain, nt.needs_live
        self._attach(t)
        self.ticks.keep = max(self.ticks.keep, pd.Timedelta(t.keep_ns + 2 * NS_MIN, unit="ns"))
        g.ids, g.native, g.windows, g.natives, g.cnatives = [], (), {}, None, None
        g.needs_chain = g.needs_live = False
        g.overruns = 0

    def status(self, now: pd.Timestamp, missed: int = 0) -> dict:
        """What the status line and the heartbeat report; resets the per-interval bucket timings."""
        import resource
        rss = 0.0
        try:
            with open("/proc/self/statm") as f:
                rss = int(f.read().split()[1]) * os.sysconf("SC_PAGE_SIZE") / 2**20
        except OSError:
            rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
        out = {"underlying": self.underlying, "ticks": int(self.stats.get("ticks", 0)), "late": self.late, "missed_on_bus": int(missed),
               "errors": self.errors,
               "rows": int(self.stats.get("rows", 0)), "lag_s": round((now.value - self.newest) / 1e9, 2) if self.newest else None,
               "rss_mb": round(rss), "fallbacks": [list(f[:2]) + [len(f[2]), f[3]] for f in self.fallbacks], "groups": {}}
        for c, g in self.groups.items():
            iv = g.interval
            out["groups"][c] = {"signals": len(g.ids), "buckets": g.buckets, "closed": iv["n"],
                                "mean_ms": round(1000 * iv["sum_s"] / iv["n"], 1) if iv["n"] else None, "max_ms": round(1000 * iv["max_s"], 1)}
            g.interval = {"n": 0, "sum_s": 0.0, "max_s": 0.0}
        return out

    # ------------------------------------------------------------------------------------------ input
    def on_ticks(self, df: pd.DataFrame) -> pd.DataFrame:
        """New ticks (any symbols, any order).  Returns the final rows of every bucket they closed."""
        if df is None or not len(df):
            return pd.DataFrame()
        sym = df["symbol"].astype(str)
        df = df[sym.str.startswith(self.prefix) | sym.isin(self.symbols)]
        if not len(df):
            return pd.DataFrame()
        df = df.copy()
        df["fts"] = pd.to_datetime(df["fts"] if "fts" in df.columns else df["ts"])
        newest = int(df["fts"].max().value)
        self.newest = newest if self.newest is None else max(self.newest, newest)
        self.last_tick_mono = _time.monotonic()
        buf = pd.concat([self.pending, df], ignore_index=True) if len(self.pending) else df
        cut = self.newest - self.reorder_ns
        ready = buf["fts"].astype("int64") <= cut if self.reorder_ns else pd.Series(True, index=buf.index)
        self.pending = buf[~ready]
        return self._apply(buf[ready])

    def advance(self, now: pd.Timestamp, grace: pd.Timedelta = pd.Timedelta(seconds=1)) -> pd.DataFrame:
        """Wall clock: release held ticks older than now - grace and close every bucket that ended before it (a quiet market)."""
        to = int((pd.Timestamp(now) - grace).value)
        out = []
        if len(self.pending):
            ready = self.pending["fts"].astype("int64") <= to
            rel, self.pending = self.pending[ready], self.pending[~ready]
            out.append(self._apply(rel))
        out.append(self._close_until(to))
        return _concat(out)

    def finish(self) -> pd.DataFrame:
        """End of the session: apply held ticks and close the open buckets."""
        out = [self._apply(self.pending)]
        self.pending = pd.DataFrame()
        ends = [g.b_open + g.td for g in self.groups.values() if g.b_open is not None]
        if ends:
            out.append(self._close_until(max(ends)))
        return _concat(out)

    def _apply(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None or not len(df):
            return pd.DataFrame()
        df = df.sort_values("fts", kind="stable").reset_index(drop=True)
        ts = df["fts"].to_numpy(dtype="datetime64[ns]").astype(np.int64)
        if self.closed_to is not None:
            late = ts < self.closed_to
            if late.any():
                self.late += int(late.sum())
                df, ts = df[~late].reset_index(drop=True), ts[~late]
        out = []
        i, n = 0, len(df)
        for g in self.groups.values():
            if g.b_open is None and n:
                g.b_open = int(ts[0] - ts[0] % g.td)
        while i < n:
            nxt = min(g.b_open + g.td for g in self.groups.values())
            j = int(np.searchsorted(ts, nxt, side="left"))
            if j > i:
                t0 = _time.perf_counter()
                part = df.iloc[i:j]
                self.session.apply_frame(part)
                self.ticks.add(part)
                self.stats["apply_s"] += _time.perf_counter() - t0
                self.stats["ticks"] += j - i
                i = j
            if i < n:
                rts = df["rts"].iloc[i] if "rts" in df.columns else None
                self.trigger = (int(ts[i]), int(pd.Timestamp(rts).value) if rts is not None and rts == rts else int(ts[i]))
                out.append(self._close_until(int(ts[i])))
        return _concat(out)

    # ------------------------------------------------------------------------------------------ buckets
    def _close_until(self, t: int) -> pd.DataFrame:
        """Close, in time order across cadences, every bucket that ends at or before `t`."""
        out = []
        while True:
            due = [(g.b_open + g.td, g.td, g) for g in self.groups.values() if g.b_open is not None and g.b_open + g.td <= t]
            if not due:
                break
            end = min(e for e, _, _ in due)
            for e, _, g in sorted(due, key=lambda x: x[1]):          # finer cadences first at a shared boundary
                if e == end:
                    c0 = _time.perf_counter()
                    out.append(self._close(g, g.b_open))
                    secs = _time.perf_counter() - c0
                    g.b_open += g.td
                    iv = g.interval
                    iv["n"] += 1
                    iv["sum_s"] += secs
                    iv["max_s"] = max(iv["max_s"], secs)
                    if g.ids:
                        g.overruns = g.overruns + 1 if secs > (g.budget_s if g.budget_s is not None else g.td / 1e9) else 0
                        if g.overruns >= OVERRUNS_BEFORE_FALLBACK:
                            self._coarsen(g, end)
            self.closed_to = end if self.closed_to is None else max(self.closed_to, end)
        if self.closed_to is not None:
            self.ticks.trim(pd.Timestamp(self.closed_to))
        res = _concat(out)
        self.stats["rows"] += len(res)
        return res

    def _today_bars(self, g: Group, b: int) -> None:
        """Today's index, VIX and future bars of bucket b, appended to the group's raw frames."""
        bb = self.session.bars
        for sym in bb.symbols:
            x = bb.bar(g.cadence, sym, b)
            if x is None:
                continue
            kind = "index_1m" if sym == bb.index_symbol else "vix_1m" if sym == VIX_SYMBOL else "fut_1m"
            g.bars[kind].append([x[0], sym] + x[1:])
        g.bars_df = {}

    def _raw_today(self, g: Group) -> dict:
        from signals.engine.state import BAR_COLS
        out = {}
        for kind in ("index_1m", "vix_1m", "fut_1m"):
            if kind not in g.bars_df:
                rows = g.bars.get(kind, [])
                df = pd.DataFrame(rows, columns=["ts", "symbol", *BAR_COLS])
                df["ts"] = pd.to_datetime(df["ts"].astype("int64"), unit="ns")
                df["volume"] = bar_volume(df) if len(df) else np.nan       # the day counter's change between bars (as the runner)
                df["vol_day"] = df["vol_last"]
                g.bars_df[kind] = df.sort_values(["ts", "symbol"], kind="stable").reset_index(drop=True)
            out[kind] = g.bars_df[kind]
        return out

    def _shared(self, g: Group, today: dict) -> dict:
        """contract_table, spot, vix, futures, fut_front, expiries, eras: the history part computed once, today's part per bucket."""
        a = self.anchors
        hp = a.providers(g.cadence)
        cols = ["ts", "symbol", "open", "high", "low", "close", "volume", "oi", "vol_day"]
        traw = {k: v.reindex(columns=cols) for k, v in today.items()}
        traw["contracts"] = a.history.get("contracts")
        for k in ("index_1m", "vix_1m", "fut_1m"):
            for c in ("open", "high", "low", "close", "volume", "oi", "vol_day"):
                traw[k][c] = pd.to_numeric(traw[k][c], errors="coerce").astype(float)
        c = Context(self.underlying, traw, bucket=g.cadence, cache={"contract_table": a.contract_table})
        out = {"contract_table": a.contract_table}
        for k in HIST_PROVIDERS:
            out[k] = _cat(hp[k], c.get(k)) if len(today[{"spot": "index_1m", "vix": "vix_1m"}.get(k, "fut_1m")]) else hp[k]
        full = Context(self.underlying, {}, bucket=g.cadence, cache=dict(out))
        out["expiries"] = full.get("expiries")
        if g.eras is None or (len(today["index_1m"]) and g.eras["date"].max() < self.day.date()):
            g.eras = full.get("eras")
        out["eras"] = g.eras
        return out

    def _close(self, g: Group, b: int) -> pd.DataFrame:
        t0 = _time.perf_counter()
        bk = self.session.close(g.cadence, b)
        self._today_bars(g, b)
        tdd = pd.Timedelta(g.td, unit="ns")
        ts = pd.Timestamp(b)
        upto = ts + tdd
        out = []
        if g.natives is not None:
            n0 = _time.perf_counter()
            tp = operators.TapeTable(self.ticks.window(ts, upto), g.natives.meta, b, g.td)
            res = g.natives.run(tp, chain_live=bk.chain("live") if g.needs_live else None, book=self.session.options)
            for sid, df in res.items():
                part = _select(normalise_output(REGISTRY[sid][0], df, self.underlying), g.since, upto, self.day)
                if part is not None:
                    out.append(self._finish_rows(g, sid, part))
            g.timings["native"] = g.timings.get("native", 0.0) + _time.perf_counter() - n0
        at = None
        if g.needs_chain or g.cnatives is not None:
            at = surface.atm_row(surface.smile_rows(self.session.options, bk.chain("bar")))
            if at is not None:
                g.atm.append(at)
        if g.needs_chain:
            ch, cl = bk.chain("bar").frame(self.session.options), bk.chain("live").frame(self.session.options)
            for lst, fr in ((g.chain, ch), (g.chain_live, cl)):
                if len(fr):
                    lst.append((b, fr))
                while lst and lst[0][0] < b - g.keep_ns:
                    lst.pop(0)
        t1 = _time.perf_counter()
        g.timings["state"] = g.timings.get("state", 0.0) + t1 - t0
        if g.cnatives is not None:
            vb = self.session.bars.bar(g.cadence, VIX_SYMBOL, b)
            if vb is not None:
                g.cnatives.on_vix_bar(b, vb[4])                     # bar layout: [start, open, high, low, close, ...]
            fut = self.session.bars.front_bar(g.cadence, b, self.session.day)
            t_fut = ((pd.Timestamp(fut[1]).value + EXPIRY_NS - (b + g.td)) / YEAR_NS) if fut[1] is not None else math.nan
            res = g.cnatives.run(b, bk.chain("bar"), bk.chain("live") if g.cnatives.needs_live else None, at, (fut[0], t_fut), g.since)
            for sid, df in res.items():
                part = _select(normalise_output(REGISTRY[sid][0], df, self.underlying), g.since, upto, self.day)
                if part is not None:
                    out.append(self._finish_rows(g, sid, part))
            g.timings["chain_native"] = g.timings.get("chain_native", 0.0) + _time.perf_counter() - t1
            t1 = _time.perf_counter()
        if not g.windows:                                  # every signal of the group is native: no frames, no shared inputs
            g.since = upto
            g.buckets += 1
            g.timings["total"] = g.timings.get("total", 0.0) + _time.perf_counter() - t0
            return _concat(out)
        today = self._raw_today(g)
        shared = self._shared(g, today)
        t2 = _time.perf_counter()
        g.timings["inputs"] = g.timings.get("inputs", 0.0) + t2 - t1
        raw = dict(self.anchors.hist)
        for k in ("index_1m", "vix_1m", "fut_1m"):
            raw[k] = _cat(self.anchors.hist.get(k), today[k].reindex(columns=["ts", "symbol", "open", "high", "low", "close", "volume", "oi", "vol_day"]))
        if g.atm and (g.atm_df is None or len(g.atm_df) < len(g.atm)):
            new = pd.DataFrame(g.atm[len(g.atm_df) if g.atm_df is not None else 0:], columns=surface.ATM_COLS)
            g.atm_df = _cat(g.atm_df, new)
        atm_all = _cat(self.anchors.hist_atm, g.atm_df if g.atm else pd.DataFrame())
        since = g.since
        first_new = since if since is not None else self.day
        for lb, sids in g.windows.items():
            lo = first_new - lb - tdd
            w = dict(raw)
            w["ticks_fo"] = self.ticks.window(lo, upto)
            ho = raw.get("opt_1m")
            w["opt_1m"] = ho[pd.to_datetime(ho["ts"]) >= lo] if ho is not None and len(ho) else ho
            cw = [f for x, f in g.chain if x >= lo.value]
            lw = [f for x, f in g.chain_live if x >= lo.value]
            # today's ATM rows within the lookback, like the chain, plus the previous session's (day-over-day changes: skew d_skew)
            today_atm = g.atm_df[g.atm_df["ts"] >= lo] if g.atm and g.atm_df is not None else pd.DataFrame()
            aw = _cat(self.anchors.hist_atm, today_atm)
            cache = dict(self.anchors.fixed, **shared, atm=aw)
            cache["chain"] = pd.concat(cw, ignore_index=True) if cw else pd.DataFrame()
            cache["chain_live"] = pd.concat(lw, ignore_index=True) if lw else pd.DataFrame(columns=["ts"])
            c = Context(self.underlying, w, bucket=g.cadence, snaps="all", since=since, signals=dict(self.anchors.daily_signals),
                        cache=cache)
            for sid in sids:
                s0 = _time.perf_counter()
                try:
                    df = c.compute(sid)
                except Exception:                                      # noqa: BLE001 -- one signal must not stop the others
                    log.exception("engine %s %s failed", self.underlying, sid)
                    continue
                finally:
                    g.timings[sid] = g.timings.get(sid, 0.0) + _time.perf_counter() - s0
                part = _select(df, since, upto, self.day)
                if part is not None:
                    out.append(self._finish_rows(g, sid, part))
        g.since = upto
        g.buckets += 1
        g.timings["total"] = g.timings.get("total", 0.0) + _time.perf_counter() - t0
        return _concat(out)


def _finish(self, g: Group, sid: str, part: pd.DataFrame) -> pd.DataFrame:
    base = self.baselines.get(g.cadence, {}).get(sid)
    if base is not None and len(base) and normalise.one_row_per_bucket(part):
        part = normalise.apply_tod_z(part, base, normalise.numeric_cols(part))
    if "signal" in part.columns:
        part = part.rename(columns={"signal": "signal_value"})
    part.insert(0, "signal", sid)
    part["bucket"] = g.cadence
    part["underlying"] = self.underlying
    return part


OperatorEngine._finish_rows = _finish


def _select(df: Optional[pd.DataFrame], since: Optional[pd.Timestamp], upto: pd.Timestamp, day: pd.Timestamp) -> Optional[pd.DataFrame]:
    """The rows first known in (since, upto] (from the session start when since is None), as the runner selects them."""
    if df is None or not len(df) or "known_at" not in df.columns:
        return None
    ka = pd.to_datetime(df["known_at"])
    sel = (ka <= upto) & ((ka > since) if since is not None else (ka >= day))
    if not sel.any():
        return None
    return df[sel].copy()


def _concat(parts: Iterable[pd.DataFrame]) -> pd.DataFrame:
    parts = [p for p in parts if p is not None and len(p)]
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def replay(underlying: str, history: dict, ticks: pd.DataFrame, cadence: dict, step: str = "1min", **kw) -> pd.DataFrame:
    """Replay a recorded day through engine v2, `step` of ticks at a time; every final row it would have written."""
    t = ticks.copy()
    t["fts"] = pd.to_datetime(t["fts"] if "fts" in t.columns else t["ts"])
    day = t["fts"].min().normalize()
    eng = OperatorEngine(underlying, history, cadence, day, **kw)
    out = []
    for _, part in t.sort_values("fts", kind="stable").groupby(t["fts"].dt.floor(step), sort=True):
        out.append(eng.on_ticks(part))
    out.append(eng.finish())
    res = _concat(out)
    res.attrs["engine"] = eng
    return res


# ---------------------------------------------------------------------------------------------- provisional values and the session loop
def provisional(eng: OperatorEngine, now: pd.Timestamp) -> pd.DataFrame:
    """Values of the open buckets so far, for the native operators (book and tape), without touching their state: the strategy sees
    the bucket as it forms every JARVIS_SIGNAL_TICK_MS.  The final row at the bucket close is the one stored and tested."""
    import copy
    out = []
    t = pd.Timestamp(now)
    for g in eng.groups.values():
        if g.natives is None or g.b_open is None or t.value <= g.b_open:
            continue
        nat = copy.deepcopy(g.natives)
        tp = operators.TapeTable(eng.ticks.window(pd.Timestamp(g.b_open), t), nat.meta, g.b_open, g.td)
        for sid, df in nat.run(tp).items():
            if df is not None and len(df):
                df = normalise_output(REGISTRY[sid][0], df, eng.underlying)
                df.insert(0, "signal", sid)
                df["bucket"], df["underlying"], df["provisional"] = g.cadence, eng.underlying, True
                out.append(df)
    return _concat(out)


def stamp(rows: pd.DataFrame, eng: OperatorEngine) -> pd.DataFrame:
    """Latency stamps for the bus (not stored): the triggering tick's vendor and receive times, and the engine's emit time."""
    if not len(rows):
        return rows
    rows = rows.copy()
    rows["t_vendor"], rows["t_recv"] = eng.trigger
    rows["t_emit"] = _time.time_ns()
    return rows


STATUS_EVERY_S = 60
LAG_WARN_S = 5.0                                    # newest tick more than this behind the wall clock
SILENCE_WARN_S = 30.0                               # no tick for this long during market hours
MARKET_UTC = (pd.Timedelta(hours=3, minutes=45), pd.Timedelta(hours=10))


def status_line(st: dict) -> str:
    """One log line per engine and minute: ticks, late, missed on the bus, rows, lag, memory, per cadence buckets closed and the
    mean / slowest close in the last minute, and any fallback."""
    gs = " ".join(f"{c}[sig={g['signals']} closed={g['closed']} mean={g['mean_ms']}ms max={g['max_ms']}ms]" for c, g in st["groups"].items())
    fb = f" FALLBACKS={st['fallbacks']}" if st["fallbacks"] else ""
    return (f"status {st['underlying']}: ticks={st['ticks']} late={st['late']} missed_on_bus={st['missed_on_bus']} "
            f"errors={st.get('errors', 0)} rows={st['rows']} "
            f"lag={st['lag_s']}s rss={st['rss_mb']}MB {gs}{fb}")


def run(engines: list, source, until: pd.Timestamp, publisher=None, store=None, tick_ms: Optional[int] = None,
        clock=lambda: pd.Timestamp.utcnow().tz_localize(None), grace: pd.Timedelta = pd.Timedelta(milliseconds=500),
        heartbeat=None, status_s: float = STATUS_EVERY_S) -> dict:
    """The session loop of engine v2: poll ticks, apply them, every tick_ms close due buckets (wall clock minus grace) and publish
    provisional values; final rows go to the bus ("signal") and to the store every 15 s."""
    from signals.engine.runner import WRITE_EVERY_S, signal_rows
    from signals.options.stream import write_rows
    tick_ms = tick_ms or TICK_MS_DEFAULT
    stats = {"loops": 0, "rows": 0, "provisional": 0, "max_close_s": 0.0}
    pending, last_write, last_timer = [], _time.monotonic(), 0.0
    last_status, start_mono, warned_missed = _time.monotonic(), _time.monotonic(), 0
    statuses: dict = {}

    def publish(rows, topic):
        if publisher is None or not len(rows):
            return
        for _, g in rows.groupby("signal"):
            cols, data = signal_rows(g.dropna(axis=1, how="all"))
            publisher.publish(topic, cols, data)

    while clock() < until:
        df = source.poll(timeout_ms=tick_ms)
        out = []
        for e in engines:
            try:
                out.append(stamp(e.on_ticks(df), e))
            except Exception:                                           # noqa: BLE001 -- one bad batch must not stop the session
                e.errors += 1
                log.exception("%s: applying a tick batch failed (%d errors so far); continuing", e.underlying, e.errors)
        if _time.monotonic() - last_timer >= tick_ms / 1000:
            last_timer = _time.monotonic()
            now = clock()
            for e in engines:
                t0 = _time.perf_counter()
                e.trigger = (now.value, now.value)
                try:
                    out.append(stamp(e.advance(now, grace), e))
                    stats["max_close_s"] = max(stats["max_close_s"], _time.perf_counter() - t0)
                    prov = stamp(provisional(e, now), e)
                    stats["provisional"] += len(prov)
                    publish(prov, "provisional")
                except Exception:                                       # noqa: BLE001
                    e.errors += 1
                    log.exception("%s: the timer step failed (%d errors so far); continuing", e.underlying, e.errors)
            if _time.monotonic() - last_status >= status_s:
                last_status = _time.monotonic()
                missed = int(getattr(source, "missed", 0) or 0)
                in_market = MARKET_UTC[0] <= now - now.normalize() < MARKET_UTC[1]
                for e in engines:
                    st = e.status(now, missed)
                    statuses[e.underlying] = st
                    log.info(status_line(st))
                    if st["lag_s"] is not None and st["lag_s"] > LAG_WARN_S and in_market:
                        log.warning("%s: newest tick is %.1fs behind the clock (feed slow or engine behind)", e.underlying, st["lag_s"])
                    quiet = _time.monotonic() - (e.last_tick_mono or start_mono)
                    if in_market and quiet > SILENCE_WARN_S:
                        log.warning("%s: no tick for %.0fs during market hours (feed down, bus off or not publishing?)", e.underlying, quiet)
                if missed > warned_missed:
                    log.warning("%d tick messages missed on the bus so far (subscriber too slow)", missed)
                    warned_missed = missed
            if heartbeat is not None:
                heartbeat(now, engines, statuses)
        rows = _concat(out)
        stats["loops"] += 1
        if len(rows):
            stats["rows"] += len(rows)
            publish(rows, "signal")
            pending.append(rows.drop(columns=["t_vendor", "t_recv", "t_emit"], errors="ignore"))
        if store is not None and pending and _time.monotonic() - last_write >= WRITE_EVERY_S:
            allr = _concat(pending)
            pending, last_write = [], _time.monotonic()
            for (u, b), g in allr.groupby(["underlying", "bucket"]):
                write_rows(g.to_dict("records"), store, prefix=f"engine-{u}-{ {'1s': '1s', '10s': '10s'}.get(b, '1m') }")
    for e in engines:
        rows = e.finish()
        if len(rows):
            pending.append(rows)
    if store is not None and pending:
        allr = _concat(pending)
        for (u, b), g in allr.groupby(["underlying", "bucket"]):
            write_rows(g.to_dict("records"), store, prefix=f"engine-{u}-{ {'1s': '1s', '10s': '10s'}.get(b, '1m') }")
    stats["late"] = sum(e.late for e in engines)
    stats["fallbacks"] = {e.underlying: e.fallbacks for e in engines}
    for e in engines:
        log.info("end of session " + status_line(e.status(clock(), int(getattr(source, "missed", 0) or 0))))
    return stats


def write_heartbeat(path) -> callable:
    """A heartbeat file for the health check: time, buckets closed and late ticks per engine, rewritten on every timer."""
    import json
    from pathlib import Path
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)

    def beat(now, engines, statuses=None):
        statuses = statuses or {}
        doc = {"at": str(now), "engines": {e.underlying: {"buckets": {c: g.buckets for c, g in e.groups.items()}, "late": e.late,
                                                          "ticks": int(e.stats.get("ticks", 0)), "errors": e.errors,
                                                          "lag_s": round((now.value - e.newest) / 1e9, 2) if e.newest else None,
                                                          "fallbacks": [list(f[:2]) + [len(f[2]), f[3]] for f in e.fallbacks],
                                                          "last_status": statuses.get(e.underlying)} for e in engines}}
        tmp = p.with_suffix(".tmp")
        tmp.write_text(json.dumps(doc))
        tmp.replace(p)
    return beat


def start(lake, store, underlyings: Iterable[str], until_utc: str = "10:02", env=None) -> dict:
    """Engine v2 for the session: history and anchors loaded once, the cadence config, ticks from the bus (or tick files)."""
    from datetime import datetime
    from pathlib import Path
    from data_layer import bus
    from signals.engine.runner import IN_FEED, BusSource, FileSource
    from signals.live_runner import history_for, load_baselines, slot_volume_history
    env = os.environ if env is None else env
    day = datetime.utcnow().date()
    ids = live_signal_ids()
    cad = cadence_mod.load(ids, env=env)
    if env.get("JARVIS_LIVE_SIGNALS", "").strip().lower() in ("1", "true", "yes", "on"):
        cad = {k: v for k, v in cad.items() if k not in IN_FEED}
    engines = []
    for u in underlyings:
        hist = history_for(Path(lake), u, day)
        groups = cadence_mod.groups(cad)
        base = {c: load_baselines(Path(store), u, day, s, c) for c, s in groups.items()}
        engines.append(OperatorEngine(u, hist, cad, day, baselines=base, volume_slot_daily=slot_volume_history(Path(lake), u, day),
                                      reorder_ms=int(env.get("JARVIS_SIGNAL_REORDER_MS", REORDER_MS_DEFAULT))))
    for e in engines:
        e.trigger = (0, 0)
    pub = bus.Publisher("signals-" + "-".join(underlyings), env=env) if bus.bus_enabled(env) else None
    src = BusSource(env=env) if bus.bus_enabled(env) else FileSource(Path(lake), day)
    until = pd.Timestamp(datetime.combine(day, datetime.strptime(until_utc, "%H:%M").time()))
    log.info("engine v2: %s; cadence groups %s; tick %d ms; source %s", list(underlyings),
             {c: len(v) for c, v in cadence_mod.groups(cad).items()}, tick_ms(env), type(src).__name__)
    hb = write_heartbeat(Path(store) / "live" / f"engine-{'-'.join(underlyings)}.heartbeat.json")
    try:
        return run(engines, src, until, publisher=pub, store=Path(store), tick_ms=tick_ms(env), heartbeat=hb)
    finally:                                            # free the bus endpoints, so a restart can bind them again
        for x in (pub, getattr(src, "sub", None)):
            if x is not None:
                try:
                    x.close()
                except Exception:                       # noqa: BLE001
                    pass


def supervise(start_fn, until: pd.Timestamp, max_restarts: int = 20, wait_s: float = 5.0,
              clock=lambda: pd.Timestamp.utcnow().tz_localize(None), sleep=_time.sleep) -> dict:
    """Run the engine; if it stops with an error before `until`, log it and start it again (history reloaded, state rebuilt from
    the ticks that arrive after the restart), at most `max_restarts` times.  A clean return ends the session."""
    restarts = 0
    while True:
        try:
            res = start_fn()
            res = dict(res or {}, restarts=restarts)
            return res
        except Exception:                                           # noqa: BLE001 -- MemoryError included
            log.exception("engine stopped with an error (restart %d of %d)", restarts + 1, max_restarts)
            if clock() >= until:
                return {"restarts": restarts, "stopped": "error after the end of the session"}
            restarts += 1
            if restarts > max_restarts:
                log.error("engine failed %d times; giving up for today", restarts)
                return {"restarts": restarts, "stopped": "too many restarts"}
            sleep(wait_s)



def read_tick_window(lake, day, underlying: str, t0: pd.Timestamp, t1: pd.Timestamp) -> pd.DataFrame:
    """This underlying's option and future ticks and its index and India VIX ticks with event time in [t0, t1), from the day's tick
    files (both segments, the older single folder too), ordered by event then arrival time.  DuckDB reads only the matching rows, so
    memory follows the window, not the day (a whole day of every symbol is 15 to 20 million rows)."""
    import duckdb
    from pathlib import Path
    from data_layer.tick_store import day_folders
    files = [str(f) for d in day_folders(Path(lake), pd.Timestamp(day).date()) for f in sorted(Path(d).glob("*.parquet"))
             if not f.name.endswith(".tmp")]
    if not files:
        return pd.DataFrame()
    con = duckdb.connect()
    con.execute("SET memory_limit='1GB'")
    q = "[" + ", ".join("'" + f.replace("'", "''") + "'" for f in files) + "]"
    cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet({q}, union_by_name=true)").fetchall()}
    tcol = "fts" if "fts" in cols else "ts"
    order = ", ".join(c for c in ("fts", "rts", "ts") if c in cols)
    df = con.execute(f"SELECT * FROM read_parquet({q}, union_by_name=true) WHERE (regexp_matches(symbol, ?) OR symbol IN (?, ?)) "
                     f"AND {tcol} >= ? AND {tcol} < ? ORDER BY {order}",
                     [f"^{underlying}_", INDEX_SYMBOL[underlying], VIX_SYMBOL, t0.to_pydatetime(), t1.to_pydatetime()]).df()
    con.close()
    if "fts" not in df.columns and "ts" in df.columns:
        df["fts"] = df["ts"]
    return df


SESSION_WINDOW_UTC = (pd.Timedelta(hours=3, minutes=30), pd.Timedelta(hours=10, minutes=30))


def replay_day(lake, store, underlying: str, day, cadence: dict, window: pd.Timedelta = pd.Timedelta(minutes=30), log=print) -> int:
    """After the close: replay the day's recorded ticks through engine v2 (every cadence group at once) and write the per-bucket rows
    to the signal store, so the history of the live era is what the live engine computed.  Memory stays bounded: ticks are read
    `window` at a time, and rows are written to a staging folder per window, then combined per signal by DuckDB.  About 0.1 s of
    engine time per 1 s bucket on 2,000 contracts.  Returns rows written."""
    import shutil
    import duckdb
    from pathlib import Path
    from signals.framework import output_dir
    from signals.live_runner import history_for, load_baselines, slot_volume_history
    d = pd.Timestamp(day).normalize()
    groups = cadence_mod.groups(cadence)
    base = {c: load_baselines(Path(store), underlying, d.date(), ids, c) for c, ids in groups.items()}
    eng = OperatorEngine(underlying, history_for(Path(lake), underlying, d.date()), cadence, d, baselines=base,
                         volume_slot_daily=slot_volume_history(Path(lake), underlying, d.date()))
    staging = Path(store) / "_staging" / f"replay-{underlying}-{d.date()}"
    if staging.exists():
        shutil.rmtree(staging)
    staged: dict = defaultdict(list)                      # (signal, bucket) -> staged files
    k = 0

    def stage(rows: pd.DataFrame) -> None:
        nonlocal k
        if rows is None or not len(rows):
            return
        for (sid, b), g in rows.groupby(["signal", "bucket"]):
            g = g.drop(columns=["signal"]).assign(underlying=underlying)
            if "ts" not in g.columns and "date" in g.columns:
                g["ts"] = pd.to_datetime(g["known_at"]) - pd.Timedelta(b)
            f = staging / f"{sid}--{b}" / f"part-{k:05d}.parquet"
            f.parent.mkdir(parents=True, exist_ok=True)
            g.to_parquet(f, index=False)
            staged[(sid, b)].append(f)
            k += 1

    t, ticks_seen, rows_staged = d + SESSION_WINDOW_UTC[0], 0, 0
    t_start = _time.perf_counter()
    log(f"replay {underlying} {d.date()}: start, ticks read {int(window.total_seconds() // 60)} minutes at a time")
    while t < d + SESSION_WINDOW_UTC[1]:
        w = read_tick_window(lake, d, underlying, t, t + window)
        ticks_seen += len(w)
        if len(w):
            w["fts"] = pd.to_datetime(w["fts"])
            for slot, part in w.groupby(w["fts"].dt.floor("5min"), sort=True):
                out = eng.on_ticks(part)
                rows_staged += len(out) if out is not None else 0
                stage(out)
                log(f"replay {underlying} {slot:%H:%M} UTC: {len(part):,} ticks, {ticks_seen:,} ticks and {rows_staged:,} rows so far, "
                    f"{_time.perf_counter() - t_start:,.0f} s")
        t += window
    stage(eng.finish())
    if not ticks_seen:
        return 0
    n = 0
    for (sid, b), files in staged.items():
        spec = REGISTRY[sid][0]
        folder = output_dir(Path(store), spec, underlying, d.date(), f"intraday/bucket={ {'1s': '1s', '10s': '10s'}.get(b, '1m') }")
        folder.mkdir(parents=True, exist_ok=True)
        tmp = folder / "part.parquet.tmp"
        q = "[" + ", ".join("'" + str(f).replace("'", "''") + "'" for f in files) + "]"
        try:
            con = duckdb.connect()
            con.execute("SET memory_limit='1GB'")
            con.execute(f"COPY (SELECT * FROM read_parquet({q}, union_by_name=true)) TO '{tmp}' (FORMAT PARQUET)")
            n += con.execute(f"SELECT count(*) FROM read_parquet('{tmp}')").fetchone()[0]
            con.close()
        except Exception:                                   # noqa: BLE001 -- mixed column types across windows: combine in pandas
            df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
            df.to_parquet(tmp, index=False)
            n += len(df)
        tmp.replace(folder / "part.parquet")
    shutil.rmtree(staging, ignore_errors=True)
    log(f"replay {underlying} {d.date()}: {ticks_seen:,} ticks, {n:,} rows, {len(staged)} signal layers")
    return n


def check_day(lake, underlying: str, day, minutes: int = 60, bucket: str = "1min", ids: Optional[list] = None, log=print) -> dict:
    """Real-data check before going live: replay the first `minutes` of a recorded session through engine v2 and compare every row
    with the batch functions run on the same ticks all at once (the whole-day reference of the parity tests).  Returns
    {signal: "ok" | (problem)}; nothing is written."""
    from pathlib import Path
    from signals.live_runner import LiveRunner, history_for
    d = pd.Timestamp(day)
    start = d.normalize() + MARKET_UTC[0]
    t = read_tick_window(lake, d, underlying, start - pd.Timedelta(minutes=10), start + pd.Timedelta(minutes=minutes))
    if not len(t):
        return {"_": f"no ticks recorded on {d.date()}"}
    t["fts"] = pd.to_datetime(t["fts"])
    hist = history_for(Path(lake), underlying, d.date())
    ids = ids or [s for s in live_signal_ids() if cadence_mod.ORDER.index(cadence_mod.MIN_CADENCE.get(s, ("1s", ""))[0])
                  <= cadence_mod.ORDER.index(bucket)]
    log(f"check {underlying} {d.date()}: {len(t):,} ticks, {minutes} minutes, {len(ids)} signals at {bucket}")
    r = LiveRunner(underlying, hist, bucket=bucket, ids=ids, tick_window=pd.Timedelta(days=1), windowed=False)
    r.ingest(t)
    r.watermark = r.watermark.floor(bucket) + pd.Timedelta(bucket)
    ref = r.cycle()
    got = replay(underlying, hist, t, {s: bucket for s in ids}, step="1min")
    out = {}
    for sid in ids:
        a = ref[ref["signal"] == sid].dropna(axis=1, how="all") if len(ref) else pd.DataFrame()
        b = got[got["signal"] == sid].dropna(axis=1, how="all") if len(got) else pd.DataFrame()
        if not len(a) and not len(b):
            out[sid] = "no rows (inputs absent in this window)"
            continue
        if len(a) != len(b):
            out[sid] = f"rows differ: batch {len(a)}, engine {len(b)}"
            continue
        keys = [k for k in ("ts", "known_at", "expiry", "symbol", "strike", "cp", "snap", "date") if k in a.columns]
        a = a.sort_values(keys, kind="stable").reset_index(drop=True)
        b = b.sort_values(keys, kind="stable").reset_index(drop=True)
        bad = []
        for c in sorted(set(a.columns) | set(b.columns)):
            if c not in a.columns or c not in b.columns:
                bad.append(f"{c} (missing on one side)")
                continue
            x, y = a[c], b[c]
            if pd.api.types.is_numeric_dtype(x) and pd.api.types.is_numeric_dtype(y):
                ok = np.isclose(x.astype(float), y.astype(float), rtol=1e-9, atol=1e-12) | (x.isna() & y.isna())
            else:
                ok = (x.astype(str) == y.astype(str)) | (x.isna() & y.isna())
            if not ok.all():
                bad.append(f"{c} ({int((~ok).sum())} rows)")
        out[sid] = "ok" if not bad else "differs: " + ", ".join(bad[:4])
    return out
