"""
The live signal runner (SCRUM-197): every signal that can be computed during the session, every bucket (1 minute by default, 10 s with
JARVIS_SIGNAL_CADENCE=10s), from today's ticks, written to <store>/live/date=D/ for the viewer and the paper trader.

  python -m signals live [--underlying NIFTY,BANKNIFTY] [--cadence 1m|10s] [--until 10:05]   # in cron at 03:44 UTC

How it stays cheap (measured 2026-11-13: recomputing a whole live day each cycle would take about 40 s and 3-4 GB):
  - ticks are read once: only tick files not seen before, only this underlying's options, futures, index and VIX;
  - ticks become bars (open, high, low, close, volume, OI, last quote, vendor Greeks per bucket and contract) as they arrive, and raw
    ticks are kept only for the last TICK_WINDOW minutes (the tick-level signals look no further back);
  - the option chain is built only for buckets that just closed (the chain at a bucket depends on that bucket and the STALE_MAX before
    it) and appended to a cache, so the IV solve per cycle is one bucket of the chain (about 20 ms for 2,000 options);
  - history (index minutes for the realised-vol forecast, previous sessions' chain for the day-over-day signals) is loaded once at start.

Point in time by construction: a bucket is closed once the newest event time seen in any tick file is past its end, and every signal is
prefix-invariant (tests), so the value written for a bucket is the value the batch would compute with data up to that bucket.  The
after-close job calls replay() on the same day's ticks, so history for the live era is computed by exactly this code.

Guardrails: each cycle's wall time is measured; when 10 s cycles overrun the cadence three times in a row the runner falls back to 1 m
for the rest of the session and logs it.  The process runs at low priority (nice, from cron) and caps its own address space
(JARVIS_SIGNAL_LIVE_MAX_MB, default 2048): a cycle that hits the cap fails with MemoryError, is logged and skipped, and the collectors
are never starved.  Cycle time, rows and peak memory go to logs/signals-live.log.
"""

from __future__ import annotations

import logging
import os
import resource
import time as _time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable, Optional

import numpy as np
import pandas as pd

from signals import catalogue  # noqa: F401  (registers every signal)
from signals import normalise
from signals.framework import REGISTRY, Context, specs
from signals.options.core import INDEX_SYMBOL, STALE_MAX, VIX_SYMBOL, build_chain, rate_table

log = logging.getLogger("signals.live")

TICK_WINDOW = pd.Timedelta(minutes=30)
OVERRUNS_BEFORE_FALLBACK = 3
CADENCES = {"1m": "1min", "1min": "1min", "10s": "10s"}
# Inputs the runner cannot provide during the session (published after the close, or needing the constituents' feed).
NOT_LIVE = {"basket_fair_value", "constituent_flow", "option_positioning_hourly", "implied_corr_proxy", "expiry_inverted_v",
            "manipulation_flag", "cost_log"}


def cadence(env=None) -> str:
    v = (os.environ if env is None else env).get("JARVIS_SIGNAL_CADENCE", "1m").strip().lower()
    if v not in CADENCES:
        raise ValueError(f"JARVIS_SIGNAL_CADENCE must be 1m or 10s, not {v!r}")
    return CADENCES[v]


def live_signal_ids() -> list[str]:
    """Signals computed during the session: per-minute and snapshot signals (taken at every bucket), not labels or studies."""
    return [s.id for s in specs() if s.kind == "signal" and s.grid in ("1m", "snap") and s.id not in NOT_LIVE]


# ---------------------------------------------------------------------------------------------- ticks to bars
def ticks_to_bars(t: pd.DataFrame, bucket: str) -> pd.DataFrame:
    """Bars per (bucket, symbol) from ticks: OHLC of the last trade (the quote mid when there is none), the last cumulative volume
    (vol_last; volume itself comes from bar_volume), last OI, last quote and vendor Greeks.  Deterministic: the same ticks give the same
    bars whether they arrive in one batch or in many."""
    if t is None or not len(t):
        return pd.DataFrame()
    t = t.copy()
    t["t_evt"] = pd.to_datetime(t["fts"] if "fts" in t.columns else t["ts"])
    t = t.sort_values(["symbol", "t_evt"], kind="stable")
    px = t["ltp"].astype(float)
    if "bid" in t.columns:
        px = px.fillna((t["bid"].astype(float) + t["ask"].astype(float)) / 2)
    t["px"] = px
    t["bucket"] = t["t_evt"].dt.floor(bucket)
    agg = {"open": ("px", "first"), "high": ("px", "max"), "low": ("px", "min"), "close": ("px", "last")}
    for c, src in (("vol_last", "vol_today"), ("vol_first", "vol_today"), ("oi", "oi"), ("bid", "bid"), ("ask", "ask"), ("bid_qty", "bid_qty"),
                   ("ask_qty", "ask_qty"), ("iv_vendor", "iv"), ("delta_vendor", "delta"), ("gamma_vendor", "gamma")):
        if src in t.columns:
            agg[c] = (src, "first" if c == "vol_first" else "last")
    b = t.groupby(["bucket", "symbol"], sort=True).agg(**agg).reset_index().rename(columns={"bucket": "ts"})
    return b


def bar_volume(bars: pd.DataFrame) -> pd.Series:
    """Volume per bar from the day's cumulative counter: the change in the last cumulative volume since the symbol's previous bar that
    day; the first bar of a day counts from its first tick (as the batch chain_live does)."""
    if "vol_last" not in bars.columns:
        return pd.Series(0.0, index=bars.index)
    b = bars.sort_values(["symbol", "ts"], kind="stable")
    day = b["ts"].dt.normalize()
    prev = b.groupby(["symbol", day])["vol_last"].shift(1)
    v = (b["vol_last"] - prev).where(prev.notna(), b["vol_last"] - b["vol_first"])
    return v.clip(lower=0).fillna(0).reindex(bars.index)


def _last_sessions(df: Optional[pd.DataFrame], n: int) -> Optional[pd.DataFrame]:
    if df is None or not len(df):
        return df
    d = pd.to_datetime(df["ts"]).dt.normalize()
    days = sorted(d.unique())[-n:]
    return df[d.isin(days)]


def daily_dependencies(ids: Iterable[str]) -> list[str]:
    """Daily signals that the live signals read through ctx.signal (their `inputs`), transitively."""
    seen, todo = set(), list(ids)
    while todo:
        sid = todo.pop()
        for name in REGISTRY[sid][0].inputs if sid in REGISTRY else ():
            if name in REGISTRY and name not in seen:
                seen.add(name)
                todo.append(name)
    return sorted(x for x in seen if REGISTRY[x][0].grid == "daily")


def daily_inputs(underlying: str, history: dict, ids: Iterable[str], store: Optional[Path] = None) -> dict:
    """The daily signals the live ones need, known at the last close: from the nightly store when given, else computed once on the
    history (tests, or a store not filled yet)."""
    from signals.framework import read_output
    out = {}
    ctx = None
    for sid in daily_dependencies(ids):
        df = read_output(store, sid, underlying) if store is not None else pd.DataFrame()
        if df is None or not len(df):
            ctx = ctx or Context(underlying, history)
            df = ctx.compute(sid)
        out[sid] = df
    return out


# ---------------------------------------------------------------------------------------------- the runner
@dataclass
class CycleStats:
    at: datetime
    seconds: float
    buckets: int
    rows: int
    rss_mb: float


@dataclass
class LiveRunner:
    underlying: str
    history: dict                                    # raw frames before today (load_raw ... ticks=False); contracts included
    bucket: str = "1min"
    ids: Optional[list[str]] = None
    tick_window: pd.Timedelta = TICK_WINDOW
    ticks: pd.DataFrame = field(default_factory=pd.DataFrame)    # option and future ticks of the last tick_window
    bars: pd.DataFrame = field(default_factory=pd.DataFrame)     # every bucket of today, every symbol (options, futures, index, VIX)
    chain: pd.DataFrame = field(default_factory=pd.DataFrame)    # today's chain, closed buckets only
    hist_chain: Optional[pd.DataFrame] = None
    watermark: Optional[pd.Timestamp] = None         # newest event time seen
    emitted_to: Optional[pd.Timestamp] = None        # rows with known_at up to here have been written
    overruns: int = 0
    stats: list = field(default_factory=list)
    baselines: dict = field(default_factory=dict)    # signal -> time-of-day baseline from earlier sessions (signals.normalise)
    fixed: Optional[dict] = None                     # providers that only depend on earlier sessions, computed once a session
    daily_signals: Optional[dict] = None             # daily signals the live ones read (e.g. spot_vol_beta), from the nightly store
    index_sessions: int = 30                         # index and VIX history per cycle (noise_band looks back 14 sessions)
    option_sessions: int = 1                         # option and future history per cycle (day-over-day changes)

    def __post_init__(self):
        self.ids = self.ids or live_signal_ids()
        self.td = pd.Timedelta(self.bucket)
        self.index_symbol = INDEX_SYMBOL[self.underlying]
        ct = self.history.get("contracts")
        self.contracts = ct if ct is not None else pd.DataFrame()
        self._prefix = f"{self.underlying}_"

    # ----------------------------------------------------------------------------------------- input
    def ingest(self, t: pd.DataFrame) -> None:
        """New ticks (any order, any symbols): this underlying's options and futures, its index and India VIX are kept."""
        if t is None or not len(t):
            return
        keep = t["symbol"].astype(str).str.startswith(self._prefix) | t["symbol"].isin([self.index_symbol, VIX_SYMBOL])
        t = t[keep]
        if t.empty:
            return
        t = t.copy()
        t["fts"] = pd.to_datetime(t["fts"] if "fts" in t.columns else t["ts"])
        newest = t["fts"].max()
        self.watermark = newest if self.watermark is None else max(self.watermark, newest)
        first_bucket = t["fts"].min().floor(self.bucket)
        self.ticks = pd.concat([self.ticks, t], ignore_index=True) if len(self.ticks) else t.reset_index(drop=True)
        # rebuild the bars of every bucket the new ticks touch, from the ticks held (they cover at least the open buckets)
        part = self.ticks[self.ticks["fts"] >= first_bucket]
        fresh = ticks_to_bars(part, self.bucket)
        if len(self.bars):
            old = self.bars[self.bars["ts"] < first_bucket]
            # a symbol's first tick of the day may have scrolled out of the window: keep vol_first from the stored bar
            self.bars = pd.concat([old, fresh], ignore_index=True)
        else:
            self.bars = fresh
        cutoff = self.watermark.floor(self.bucket) - self.tick_window
        old_t = self.ticks[self.ticks["fts"] < cutoff]
        if len(old_t):
            # keep each contract's last tick before the window as an anchor: per-tick volume and the tick rule difference against it
            anchors = old_t.sort_values("fts", kind="stable").groupby("symbol").tail(1)
            self.ticks = pd.concat([anchors, self.ticks[self.ticks["fts"] >= cutoff]], ignore_index=True)

    # ----------------------------------------------------------------------------------------- compute
    def closed_until(self) -> Optional[pd.Timestamp]:
        """Start of the first bucket that is not closed yet: buckets ending at or before the newest event time are closed."""
        if self.watermark is None:
            return None
        return self.watermark.floor(self.bucket)

    def _today_raw(self, upto: pd.Timestamp) -> dict:
        b = self.bars[self.bars["ts"] < upto].copy()
        b["volume"] = bar_volume(b)
        sym = b["symbol"].astype(str)
        is_opt = sym.str.match(rf"^{self.underlying}_[0-9.]+_(CE|PE)_")
        is_fut = sym.str.startswith(f"{self.underlying}_FUT_")
        cols = ["ts", "symbol", "open", "high", "low", "close", "volume", "oi"]
        h = self.history

        def cat(name, today):
            prev = h.get(name)
            if not len(today):
                return prev if prev is not None else pd.DataFrame(columns=cols)
            today = today.reindex(columns=cols)
            out = pd.concat([prev, today], ignore_index=True) if prev is not None and len(prev) else today.reset_index(drop=True)
            for c in ("open", "high", "low", "close", "volume", "oi"):
                out[c] = pd.to_numeric(out[c], errors="coerce").astype(float)
            out["ts"] = pd.to_datetime(out["ts"])
            return out

        raw = dict(h)
        for name, keep in (("index_1m", self.index_sessions), ("vix_1m", self.index_sessions), ("opt_1m", self.option_sessions),
                           ("fut_1m", self.index_sessions)):                 # futures: noise_band and the lead-lag look back 14 sessions
            raw[name] = _last_sessions(h.get(name), keep)
        h = raw
        raw = dict(h)
        raw["index_1m"] = cat("index_1m", b[sym == self.index_symbol])
        raw["vix_1m"] = cat("vix_1m", b[sym == VIX_SYMBOL])
        raw["fut_1m"] = cat("fut_1m", b[is_fut])
        opt_today = b[is_opt]
        raw["opt_1m"] = cat("opt_1m", opt_today)
        raw["ticks_fo"] = self.ticks[self.ticks["fts"] < upto]
        return raw, opt_today

    def _extend_chain(self, ctx: Context, opt_today: pd.DataFrame, upto: pd.Timestamp) -> pd.DataFrame:
        """Append the chain of the buckets closed since the last cycle.  The chain at a bucket uses the option bars of that bucket and the
        STALE_MAX minutes before it (stale prints carried), the index and the front future at that bucket: so building it on that
        slice gives the same rows as building it on the whole day."""
        done = self.chain["ts"].max() + self.td if len(self.chain) else None
        start = done if done is not None else (opt_today["ts"].min() if len(opt_today) else upto)
        if start >= upto or not len(opt_today):
            return self.chain
        lo = start - pd.Timedelta(minutes=STALE_MAX) - self.td
        o = opt_today[(opt_today["ts"] >= lo) & (opt_today["ts"] < upto)]
        new = build_chain(o, ctx.get("contract_table"), ctx.get("spot"), ctx.get("fut_front"), ctx.r, rates=rate_table(ctx.raw.get("rates")))
        if len(new):
            new = new[(new["ts"] >= start) & (new["ts"] < upto)]
            self.chain = pd.concat([self.chain, new], ignore_index=True) if len(self.chain) else new.reset_index(drop=True)
        return self.chain

    def _fixed_providers(self, day: pd.Timestamp) -> dict:
        """The daily realised measures with the HAR forecast, and the time-of-day variance profile, depend only on sessions before
        today (every row a signal reads from them was known at the last close), so they are computed once at the start instead of
        every cycle.  Today's profile row is built from earlier sessions; a two-minute placeholder makes today a session to profile."""
        h = dict(self.history)
        stub = pd.DataFrame({"ts": [day + pd.Timedelta(hours=3, minutes=45), day + pd.Timedelta(hours=3, minutes=46)],
                             "symbol": self.index_symbol, "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "volume": 0, "oi": np.nan})
        idx = h.get("index_1m")
        h["index_1m"] = pd.concat([idx, stub], ignore_index=True) if idx is not None and len(idx) else stub
        c = Context(self.underlying, h)
        daily = c.get("daily")
        daily = daily[pd.to_datetime(daily["known_at"]) < day] if daily is not None and len(daily) else daily
        tod = c.get("tod")
        return {"daily": daily, "tod": tod}

    def cycle(self, now: Optional[datetime] = None) -> pd.DataFrame:
        """Compute every live signal for the buckets closed since the last cycle.  Returns the new rows (signal, ts, known_at, ...)."""
        t0 = _time.perf_counter()
        upto = self.closed_until()
        if upto is None or (self.emitted_to is not None and upto <= self.emitted_to):
            return pd.DataFrame()
        raw, opt_today = self._today_raw(upto)
        if self.fixed is None:
            self.fixed = self._fixed_providers(upto.normalize())
        ctx = Context(self.underlying, raw, bucket="1min", snaps="all", cache=dict(self.fixed))
        if self.hist_chain is None:
            hist_opt = _last_sessions(self.history.get("opt_1m"), self.option_sessions)
            self.hist_chain = (Context(self.underlying, dict(self.history, opt_1m=hist_opt)).get("chain")
                               if hist_opt is not None and len(hist_opt) else pd.DataFrame())
            if self.daily_signals is None:
                self.daily_signals = daily_inputs(self.underlying, self.history, self.ids)
        chain = self._extend_chain(ctx, opt_today, upto)
        since = self.emitted_to
        ctx = Context(self.underlying, raw, bucket="1min", snaps="all", since=since, signals=dict(self.daily_signals),
                      cache=dict(self.fixed, chain=pd.concat([self.hist_chain, chain], ignore_index=True) if len(self.hist_chain) else chain))
        tick_ctx = (Context(self.underlying, raw, bucket=self.bucket, snaps="all", since=since, cache=dict(self.fixed),
                            signals=dict(self.daily_signals)) if self.bucket != "1min" else ctx)
        out = []
        lo = self.emitted_to
        for sid in self.ids:
            spec = REGISTRY[sid][0]
            c = tick_ctx if (spec.family == "live" and self.bucket != "1min") else ctx
            try:
                df = c.compute(sid)
            except Exception:                                          # noqa: BLE001 -- one signal must not stop the others
                log.exception("live %s %s failed", self.underlying, sid)
                continue
            if df is None or not len(df) or "known_at" not in df.columns:
                continue
            ka = pd.to_datetime(df["known_at"])
            sel = (ka <= upto) & ((ka > lo) if lo is not None else (ka >= upto.normalize()))
            if sel.any():
                part = df[sel].copy()
                if "signal" in part.columns:                       # a signal's own column of that name (noise_band) keeps its value
                    part = part.rename(columns={"signal": "signal_value"})
                part.insert(0, "signal", sid)
                base = self.baselines.get(sid)
                if base is not None and len(base) and normalise.one_row_per_bucket(part):
                    part = normalise.apply_tod_z(part, base, normalise.numeric_cols(part))
                part["bucket"] = c.bucket
                part["underlying"] = self.underlying
                out.append(part)
        self.emitted_to = upto
        rows = pd.concat(out, ignore_index=True) if out else pd.DataFrame()
        secs = _time.perf_counter() - t0
        self.stats.append(CycleStats(datetime.utcnow(), secs, 1, len(rows), resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024))
        return rows

    # ----------------------------------------------------------------------------------------- guardrail
    def check_budget(self, seconds: float, cadence_s: float) -> bool:
        """True when the runner should fall back to 1-minute cycles: OVERRUNS_BEFORE_FALLBACK cycles in a row longer than the cadence."""
        self.overruns = self.overruns + 1 if seconds > cadence_s else 0
        return self.overruns >= OVERRUNS_BEFORE_FALLBACK


def replay(underlying: str, history: dict, ticks: pd.DataFrame, bucket: str = "1min", step: Optional[str] = None,
           ids: Optional[list[str]] = None, baselines: Optional[dict] = None) -> pd.DataFrame:
    """Run the live runner over a day's recorded ticks, `step` at a time (default: one bucket), and return every row it would have
    written.  The after-close job uses this for the live era; the tests compare it with the batch."""
    r = LiveRunner(underlying, history, bucket=bucket, ids=ids, baselines=baselines or {})
    t = ticks.copy()
    t["fts"] = pd.to_datetime(t["fts"] if "fts" in t.columns else t["ts"])
    t = t.sort_values("fts", kind="stable")
    stp = pd.Timedelta(step or bucket)
    out = []
    for _, g in t.groupby(t["fts"].dt.floor(stp), sort=True):
        r.ingest(g)
        rows = r.cycle()
        if len(rows):
            out.append(rows)
    # the session's last bucket closes at the end of the day
    if r.watermark is not None:
        r.watermark = r.watermark.floor(r.bucket) + r.td
        rows = r.cycle()
        if len(rows):
            out.append(rows)
    return pd.concat(out, ignore_index=True) if out else pd.DataFrame()


# ---------------------------------------------------------------------------------------------- reading the day's tick files
class TickFiles:
    """New tick files of one day: the F&O segment (options, futures) and the equity segment (index, VIX).  Each file is read once; a
    file still being written is skipped until it is complete (the feed writes a temporary name and renames it)."""

    def __init__(self, lake: Path, day: date):
        from data_layer import tick_store
        self.folders = [tick_store.segment_folder(Path(lake), day, "fo"), tick_store.segment_folder(Path(lake), day, "eq"),
                        tick_store.legacy_folder(Path(lake), day)]
        self.seen: set[str] = set()

    def new(self) -> pd.DataFrame:
        parts = []
        for f in self.folders:
            if not f.is_dir():
                continue
            for p in sorted(f.glob("*.parquet")):
                if p.name in self.seen or p.name == "ticks.parquet":       # the compacted day file is written after the session
                    continue
                try:
                    parts.append(pd.read_parquet(p))
                except Exception:                                          # noqa: BLE001 -- half-written: try again next cycle
                    continue
                self.seen.add(p.name)
        return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def load_baselines(store: Path, underlying: str, day: date, ids: Iterable[str], bucket: str = "1min",
                   window: int = normalise.WINDOW) -> dict:
    """Time-of-day baselines per signal from the per-bucket history in the store (the last `window` sessions before `day`)."""
    out = {}
    layer = f"intraday/bucket={'10s' if bucket == '10s' else '1m'}"
    for sid in ids:
        spec = REGISTRY[sid][0]
        base = Path(store) / "signals" / sid / f"v{spec.version}" / layer / f"underlying={underlying}"
        files = sorted(f for f in base.glob("date=*/part.parquet") if f.parent.name.split("=", 1)[1] < str(day))[-window:]
        if len(files) < normalise.MIN_SESSIONS:
            continue
        h = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
        if normalise.one_row_per_bucket(h[pd.to_datetime(h["ts"]).dt.normalize() == pd.to_datetime(h["ts"]).dt.normalize().max()]):
            out[sid] = normalise.baseline(h, normalise.numeric_cols(h), window)
    return out


def history_for(lake: Path, underlying: str, day: date) -> dict:
    """Everything before `day` the live signals look back at: index and VIX minutes for 400 days (realised-vol forecast, VIX regime),
    the last 5 sessions of option and future minutes (day-over-day changes), contracts, participant file, events, rates."""
    from signals.lakeio import load_raw
    raw = load_raw(lake, underlying, day - timedelta(days=400), day - timedelta(days=1), option_from=day - timedelta(days=40), ticks=False)
    return raw


MAX_MB_DEFAULT = 2048


def cap_memory(env=None) -> int:
    """Limit this process's address space to JARVIS_SIGNAL_LIVE_MAX_MB (default 2048 MB); returns the cap in MB."""
    mb = int((os.environ if env is None else env).get("JARVIS_SIGNAL_LIVE_MAX_MB", MAX_MB_DEFAULT))
    resource.setrlimit(resource.RLIMIT_AS, (mb * 1024 * 1024, resource.getrlimit(resource.RLIMIT_AS)[1]))
    return mb


def run_session(lake: Path, store: Path, underlyings: Iterable[str], bucket: str, until_utc: str = "10:02", clock=datetime.utcnow,
                sleep=_time.sleep, files=None) -> dict:
    """The session loop: every `bucket` read new tick files, compute the closed buckets, append rows to <store>/live/date=D/.  Falls back
    from 10 s to 1 m after OVERRUNS_BEFORE_FALLBACK slow cycles in a row.  Returns a summary."""
    from signals.options.stream import write_rows
    day = clock().date()
    runners = {}
    for u in underlyings:
        r = LiveRunner(u, history_for(lake, u, day), bucket=bucket)
        r.baselines = load_baselines(store, u, day, r.ids, bucket)
        r.daily_signals = daily_inputs(u, r.history, r.ids, store)
        runners[u] = r
    files = files or TickFiles(lake, day)
    stop = datetime.combine(day, datetime.strptime(until_utc, "%H:%M").time())
    cad = pd.Timedelta(bucket).total_seconds()
    summary = {"cycles": 0, "rows": 0, "fallback": None, "max_cycle_s": 0.0}
    while clock() < stop:
        t0 = _time.perf_counter()
        new = files.new()
        for u, r in runners.items():
            try:
                r.ingest(new)
                rows = r.cycle()
            except MemoryError:
                log.error("%s: cycle hit the memory cap and was skipped", u)
                summary["memory_errors"] = summary.get("memory_errors", 0) + 1
                continue
            if len(rows):
                summary["rows"] += write_rows(rows.to_dict("records"), store, prefix=f"chain-{u}")
        secs = _time.perf_counter() - t0
        summary["cycles"] += 1
        summary["max_cycle_s"] = max(summary["max_cycle_s"], secs)
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
        log.info("cycle %.2fs bucket=%s ticks_new=%d rows_total=%d peak_rss=%.0fMB", secs, bucket, len(new), summary["rows"], rss)
        if bucket != "1min" and any(r.check_budget(secs, cad) for r in runners.values()):
            log.warning("cycles over %.0fs %d times in a row: falling back to 1-minute buckets for the rest of the session", cad,
                        OVERRUNS_BEFORE_FALLBACK)
            summary["fallback"] = clock().isoformat(timespec="seconds")
            bucket, cad = "1min", 60.0
            for r in runners.values():
                r.bucket, r.td, r.overruns = "1min", pd.Timedelta("1min"), 0
        sleep(max(0.0, cad - (_time.perf_counter() - t0)))
    return summary


def replay_day(lake: Path, store: Path, underlying: str, day: date, bucket: str = "1min") -> int:
    """After the close: replay the day's recorded ticks through the runner and write the per-bucket rows to the signal store, so the
    history of the live era is exactly what the runner computed (or would have).  Returns rows written."""
    from signals.framework import write_output
    files = TickFiles(lake, day)
    files.folders = [f.parent / f"date={day}" for f in files.folders]                  # all files, including the compacted day file
    parts = []
    for f in files.folders:
        if f.is_dir():
            parts += [pd.read_parquet(p) for p in sorted(f.glob("*.parquet"))]
    if not parts:
        return 0
    ticks = pd.concat(parts, ignore_index=True)
    ids = live_signal_ids()
    rows = replay(underlying, history_for(lake, underlying, day), ticks, bucket=bucket, step="5min", ids=ids,
                  baselines=load_baselines(store, underlying, day, ids, bucket))
    if rows.empty:
        return 0
    n = 0
    for sid, g in rows.groupby("signal"):
        spec = REGISTRY[sid][0]
        g = g.drop(columns=["signal"]).assign(underlying=underlying)
        if "ts" not in g.columns and "date" in g.columns:
            g["ts"] = pd.to_datetime(g["known_at"]) - pd.Timedelta(bucket)
        n += write_output(store, spec, g, layer=f"intraday/bucket={'10s' if bucket == '10s' else '1m'}")
    return n
