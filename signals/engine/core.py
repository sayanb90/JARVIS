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
from signals.engine import surface
from signals.engine.state import NS_MIN, Session, td_ns
from signals.framework import Context
from signals.live_runner import CHAIN_FREE, LiveRunner, _last_sessions, bar_volume, daily_inputs, live_signal_ids
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
    """This underlying's ticks of the last `keep` (plus each symbol's last earlier tick as an anchor), for operators that read ticks."""

    def __init__(self, keep: pd.Timedelta):
        self.keep = keep
        self.df = pd.DataFrame()
        self._parts: list = []

    def add(self, df: pd.DataFrame) -> None:
        if len(df):
            self._parts.append(df)

    def _merge(self) -> None:
        if self._parts:
            self.df = pd.concat([self.df] + self._parts, ignore_index=True) if len(self.df) else pd.concat(self._parts, ignore_index=True)
            self._parts = []

    def trim(self, now: pd.Timestamp) -> None:
        self._merge()
        if not len(self.df):
            return
        cutoff = now - self.keep
        old = self.df["fts"] < cutoff
        if old.any():
            anchors = self.df[old].groupby("symbol").tail(1)
            self.df = pd.concat([anchors, self.df[~old]], ignore_index=True)

    def window(self, lo: pd.Timestamp, upto: pd.Timestamp) -> pd.DataFrame:
        self._merge()
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

    def __post_init__(self):
        self.td = td_ns(self.cadence)
        tdd = pd.Timedelta(self.cadence)
        for sid in self.ids:
            self.windows.setdefault(lookbacks.lookback(sid, tdd), []).append(sid)
        self.keep_ns = (max(self.windows) if self.windows else pd.Timedelta(0)).value + 2 * self.td
        self.needs_chain = not set(self.ids) <= CHAIN_FREE


class OperatorEngine:
    """Engine v2 for one underlying and one session."""

    def __init__(self, underlying: str, history: dict, cadence: dict, day, baselines: Optional[dict] = None,
                 volume_slot_daily: Optional[pd.DataFrame] = None, daily_signals: Optional[dict] = None, reorder_ms: int = 0,
                 r: float = 0.065):
        self.underlying = underlying
        self.day = pd.Timestamp(day).normalize()
        self.cadence = dict(cadence)
        ids = sorted(self.cadence)
        self.anchors = Anchors(underlying, history, self.day, ids, volume_slot_daily=volume_slot_daily, daily_signals=daily_signals)
        groups = cadence_mod.groups(self.cadence)
        self.groups = {c: Group(c, sids) for c, sids in groups.items()}
        self.session = Session(underlying, history.get("contracts"), self.day, cadences=tuple(self.groups), r=r, rates=history.get("rates"))
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
                    out.append(self._close(g, g.b_open))
                    g.b_open += g.td
            self.closed_to = end if self.closed_to is None else max(self.closed_to, end)
        if self.closed_to is not None:
            self.ticks.trim(pd.Timestamp(self.closed_to))
        return _concat(out)

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
        if g.needs_chain:
            ch, cl = bk.chain("bar").frame(self.session.options), bk.chain("live").frame(self.session.options)
            for lst, fr in ((g.chain, ch), (g.chain_live, cl)):
                if len(fr):
                    lst.append((b, fr))
                while lst and lst[0][0] < b - g.keep_ns:
                    lst.pop(0)
            rows = surface.smile_rows(self.session.options, bk.chain("bar"))
            at = surface.atm_row(rows)
            if at is not None:
                g.atm.append(at)
        t1 = _time.perf_counter()
        g.timings["state"] = g.timings.get("state", 0.0) + t1 - t0
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
        out = []
        for lb, sids in g.windows.items():
            lo = first_new - lb - tdd
            w = dict(raw)
            w["ticks_fo"] = self.ticks.window(lo, upto)
            ho = raw.get("opt_1m")
            w["opt_1m"] = ho[pd.to_datetime(ho["ts"]) >= lo] if ho is not None and len(ho) else ho
            cw = [f for x, f in g.chain if x >= lo.value]
            lw = [f for x, f in g.chain_live if x >= lo.value]
            cache = dict(self.anchors.fixed, **shared, atm=atm_all)
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
                if part is None:
                    continue
                base = self.baselines.get(g.cadence, {}).get(sid)
                if base is not None and len(base) and normalise.one_row_per_bucket(part):
                    part = normalise.apply_tod_z(part, base, normalise.numeric_cols(part))
                if "signal" in part.columns:
                    part = part.rename(columns={"signal": "signal_value"})
                part.insert(0, "signal", sid)
                part["bucket"] = g.cadence
                part["underlying"] = self.underlying
                out.append(part)
        g.since = upto
        g.buckets += 1
        g.timings["total"] = g.timings.get("total", 0.0) + _time.perf_counter() - t0
        return _concat(out)


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
