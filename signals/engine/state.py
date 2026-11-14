"""
Per-tick state for the operator engine (SCRUM-201): what the pandas live runner rebuilt every bucket from tables, kept here as arrays
that each tick updates in place, so the chain at a bucket is a handful of vector operations instead of a grid fill, merges and sorts.

  OptionBook    one slot per option contract (numpy arrays): the last non-null value and its event time for every tick field, the
                last tick time, the day's first volume counter, and per cadence the volume in the open bucket.  update() is O(1).
  BarBook       index, India VIX and futures: today's bars per cadence (open, high, low, close, volume counter, OI, quote), as the
                runner's ticks_to_bars builds them.
  close_bucket  at each bucket end (per cadence, in time order, before any tick of a later bucket is applied): the per-contract values
                as of that bucket, with the carry rules of the batch chain.
  build_chain   the chain rows of one bucket from that slice: years to expiry, forward (put-call parity, then the front future, then the
                index), IV on fresh prints only, Greeks.  Equal to signals.options.core.build_chain's rows for that bucket (tests).

Rules carried over from the batch chain (signals/options/core.py), so live and history agree:
  - a contract's last values are carried for STALE_MAX minutes (max(1, 5 min / bucket) buckets) after the bucket they were last seen in;
  - age is minutes from the contract's last tick's bucket to this bucket; IV only where age <= IV_AGE and the premium >= MIN_IV_PRICE;
  - two flavours of the chain, as in the runner: "bar" (close = the last trade, else the quote mid, per tick: ticks_to_bars; volume =
    the change in the day counter between bars) and "live" (close = the last trade in the bucket, else the bucket's last quote mid;
    volume = the sum of positive counter increments; signals.options.live.chain_live);
  - at 1 minute and longer the grid is the index bars (a bucket without an index bar has no chain rows) and the forward falls back to
    the front future's bar, then the index bar, of that bucket; below a minute the grid is every bucket and the live chain's forward
    falls back to the front future's mid in the bucket.

Ticks must be applied in event-time order per contract; a tick older than the contract's last one is counted in `late` and ignored (the
engine orders ticks in a short reorder window before applying them, and closes every due bucket before applying a later tick).
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd

from analytics import black76
from signals.options.core import (INDEX_SYMBOL, IV_AGE, MIN_IV_PRICE, PARITY_AGE, PARITY_PAIRS, STALE_MAX, VIX_SYMBOL, rate_by_row,
                                  rate_table)

NONE = np.iinfo(np.int64).min                    # "never seen" for event times
NS_MIN = 60 * 10**9
YEAR_NS = pd.Timedelta(days=365).value
EXPIRY_NS = pd.Timedelta(hours=10).value         # 15:30 IST
FIELDS = ("ltp", "px", "vol_today", "oi", "bid", "ask", "bid_qty", "ask_qty", "iv", "delta", "gamma")
F = {f: i for i, f in enumerate(FIELDS)}
CARRIED = (("oi", "oi"), ("bid", "bid"), ("ask", "ask"), ("bid_qty", "bid_qty"), ("ask_qty", "ask_qty"), ("iv_vendor", "iv"),
           ("delta_vendor", "delta"), ("gamma_vendor", "gamma"), ("vol_day", "vol_today"))


def td_ns(bucket) -> int:
    return int(pd.Timedelta(bucket).value)


def stale_steps(td: int) -> int:
    """Buckets a value is carried: STALE_MAX minutes, at least one bucket (the runner's rule at every cadence)."""
    return max(1, int(STALE_MAX * NS_MIN // td))


def _f(v) -> float:
    if v is None:
        return math.nan
    try:
        return float(v)
    except (TypeError, ValueError):
        return math.nan


# ---------------------------------------------------------------------------------------------- options
@dataclass
class _Cadence:
    td: int
    limit: int
    dvsum: np.ndarray                             # live volume (positive counter increments) in the open bucket
    vmark: np.ndarray                             # day counter at the end of the contract's previous bar
    cl_val: np.ndarray                            # live close of the contract's last bar that had one, and that bar's start
    cl_at: np.ndarray


class OptionBook:
    """Every option contract of one underlying for one session, in arrays sorted by (expiry, strike, call before put)."""

    def __init__(self, contracts: pd.DataFrame, cadences=("1min",)):
        o = contracts[contracts["instrument_type"].isin(["CE", "PE"])].drop_duplicates("symbol").copy()
        o["expiry"] = pd.to_datetime(o["expiry"]).dt.date
        o["cp"] = np.where(o["instrument_type"] == "CE", "C", "P")
        o = o.sort_values(["expiry", "strike", "cp"], kind="stable").reset_index(drop=True)
        self.n = n = len(o)
        self.symbols = o["symbol"].astype(str).tolist()
        self.slot = {s: i for i, s in enumerate(self.symbols)}
        self.strike = o["strike"].to_numpy(dtype=float)
        self.expiry = np.array(o["expiry"].tolist() + [None], dtype=object)[:-1] if n else np.array([], dtype=object)
        self.exp_end = (pd.to_datetime(o["expiry"]).to_numpy(dtype="datetime64[ns]").astype(np.int64) + EXPIRY_NS) if n else np.zeros(0, np.int64)
        self.is_call = (o["cp"] == "C").to_numpy()
        self.cp = o["cp"].to_numpy(dtype=object)
        self.lot = pd.to_numeric(o["lot_size"], errors="coerce").to_numpy(dtype=float)
        # the other side at the same expiry and strike (-1 when not listed)
        key = {(e, k, c): i for i, (e, k, c) in enumerate(zip(o["expiry"], o["strike"], o["cp"]))}
        self.partner = np.array([key.get((e, k, "P" if c == "C" else "C"), -1) for e, k, c in zip(o["expiry"], o["strike"], o["cp"])], dtype=np.int64)
        self.val = np.full((len(FIELDS), n), np.nan)
        self.tns = np.full((len(FIELDS), n), NONE, dtype=np.int64)
        self.last_ns = np.full(n, NONE, dtype=np.int64)
        self.first_ns = np.full(n, NONE, dtype=np.int64)
        self.day_first_vol = np.full(n, np.nan)
        self.prev_vol = np.full(n, np.nan)                    # the previous tick's day counter (NaN included), for live volume
        self.cad = {c: _Cadence(td_ns(c), stale_steps(td_ns(c)), np.zeros(n), np.full(n, np.nan), np.full(n, np.nan),
                                np.full(n, NONE, dtype=np.int64)) for c in cadences}
        self.late = 0
        self.ticks = 0

    def update(self, sym: str, t: int, ltp=None, vol_today=None, oi=None, bid=None, ask=None, bid_qty=None, ask_qty=None, iv=None,
               delta=None, gamma=None) -> bool:
        """Apply one tick (event time `t` in ns).  Returns False when the symbol is not an option of this book or the tick is late."""
        i = self.slot.get(sym)
        if i is None:
            return False
        last = self.last_ns[i]
        if last != NONE and t < last:
            self.late += 1
            return False
        self.ticks += 1
        vals = (_f(ltp), math.nan, _f(vol_today), _f(oi), _f(bid), _f(ask), _f(bid_qty), _f(ask_qty), _f(iv), _f(delta), _f(gamma))
        b, a, ltp_f = vals[4], vals[5], vals[0]
        px = ltp_f if ltp_f == ltp_f else (b + a) / 2                   # ticks_to_bars: the trade, else the quote mid (NaN if none)
        val, tns = self.val, self.tns
        for j, v in enumerate(vals):
            if j == 1:
                v = px
            if v == v:
                val[j, i] = v
                tns[j, i] = t
        v = vals[2]
        if last == NONE:
            self.first_ns[i] = t
            self.day_first_vol[i] = v
            dv = 0.0
        else:
            dv = v - self.prev_vol[i]
            dv = dv if dv > 0 else 0.0                                   # NaN and decreases count as 0 (clip, fillna)
        self.prev_vol[i] = v
        if dv:
            for c in self.cad.values():
                c.dvsum[i] += dv
        self.last_ns[i] = t
        return True

    def close_bucket(self, cadence: str, b: int) -> "Slice":
        """The contracts' values as of the end of bucket [b, b + td) at `cadence`, with the batch carry rules.  Call once per bucket, in
        order, after every tick of the bucket and before any tick of a later one."""
        c = self.cad[cadence]
        td, span = c.td, c.limit * c.td
        last = self.last_ns
        seen = last != NONE
        lb = np.where(seen, last - last % td, NONE)
        has_bar = seen & (lb == b)
        rows = seen & (b - lb <= span) & (lb <= b)
        tf = self.tns
        fl = np.where(tf != NONE, tf - tf % td, NONE)
        valid = (tf != NONE) & (b - fl <= span) & (fl <= b)
        in_b = (fl == b)
        val = self.val
        # live close of this bar: the bucket's last trade, else its last quote mid
        ltp_b = np.where(in_b[F["ltp"]], val[F["ltp"]], np.nan)
        mid_b = np.where(in_b[F["bid"]] & in_b[F["ask"]], (val[F["bid"]] + val[F["ask"]]) / 2, np.nan)
        cl_new = np.where(np.isnan(ltp_b), mid_b, ltp_b)
        set_cl = has_bar & ~np.isnan(cl_new)
        c.cl_val[set_cl], c.cl_at[set_cl] = cl_new[set_cl], b
        cl_ok = (c.cl_at != NONE) & (b - c.cl_at <= span)
        close_live = np.where(cl_ok, c.cl_val, np.nan)
        # bar volume: the day counter's change since the contract's previous bar (the first bar counts from the day's first tick)
        vlast = np.where(in_b[F["vol_today"]], val[F["vol_today"]], np.nan)
        base = np.where(np.isnan(c.vmark), self.day_first_vol, c.vmark)
        vbar = np.where(has_bar, np.nan_to_num(np.clip(vlast - base, 0, None), nan=0.0), 0.0)
        c.vmark[has_bar] = vlast[has_bar]
        vlive = np.where(has_bar, c.dvsum, 0.0)
        c.dvsum[:] = 0.0
        out = Slice(cadence=cadence, b=b, td=td, rows=np.flatnonzero(rows))
        idx = out.rows
        out.age = (b - lb[idx]) / NS_MIN
        out.close_bar = np.where(valid[F["px"], idx], val[F["px"], idx], np.nan)
        out.close_live = close_live[idx]
        out.volume_bar = vbar[idx].astype(float)
        out.volume_live = vlive[idx].astype(float)
        out.carried = {name: np.where(valid[F[src], idx], val[F[src], idx], np.nan) for name, src in CARRIED}
        out.has_bar = has_bar[idx]
        return out


@dataclass
class Slice:
    cadence: str
    b: int
    td: int
    rows: np.ndarray                               # slots of the contracts that have a chain row at this bucket
    age: np.ndarray = None
    close_bar: np.ndarray = None
    close_live: np.ndarray = None
    volume_bar: np.ndarray = None
    volume_live: np.ndarray = None
    carried: dict = field(default_factory=dict)
    has_bar: np.ndarray = None


# ---------------------------------------------------------------------------------------------- index, VIX, futures
BAR_COLS = ("open", "high", "low", "close", "vol_last", "vol_first", "oi", "bid", "ask", "bid_qty", "ask_qty", "iv_vendor",
            "delta_vendor", "gamma_vendor", "mid_last", "ltp_last")
_BC = {c: i for i, c in enumerate(BAR_COLS)}


class BarBook:
    """Today's bars per cadence for the index, India VIX and the futures (ticks_to_bars rules: px = the trade, else the quote mid;
    every other column the last non-null value in the bar)."""

    def __init__(self, underlying: str, contracts: pd.DataFrame, cadences=("1min",)):
        self.index_symbol = INDEX_SYMBOL[underlying]
        f = contracts[contracts["instrument_type"] == "FUT"].drop_duplicates("symbol")
        self.fut_expiry = dict(zip(f["symbol"].astype(str), pd.to_datetime(f["expiry"]).dt.date))
        self.symbols = {self.index_symbol, VIX_SYMBOL} | set(self.fut_expiry)
        self.td = {c: td_ns(c) for c in cadences}
        self.cur: dict = {c: {} for c in cadences}          # cadence -> symbol -> [start, values...]
        self.done: dict = {c: {} for c in cadences}         # cadence -> symbol -> list of closed bars
        self.last_ns: dict = {}
        self.late = 0

    def update(self, sym: str, t: int, ltp=None, vol_today=None, oi=None, bid=None, ask=None, bid_qty=None, ask_qty=None, iv=None,
               delta=None, gamma=None) -> bool:
        if sym not in self.symbols:
            return False
        last = self.last_ns.get(sym)
        if last is not None and t < last:
            self.late += 1
            return False
        self.last_ns[sym] = t
        ltp_f, b, a = _f(ltp), _f(bid), _f(ask)
        mid = (b + a) / 2
        px = ltp_f if ltp_f == ltp_f else mid
        vol = _f(vol_today)
        rest = {"oi": _f(oi), "bid": b, "ask": a, "bid_qty": _f(bid_qty), "ask_qty": _f(ask_qty), "iv_vendor": _f(iv),
                "delta_vendor": _f(delta), "gamma_vendor": _f(gamma), "mid_last": mid, "ltp_last": ltp_f, "vol_last": vol}
        for c, td in self.td.items():
            start = t - t % td
            cur = self.cur[c].get(sym)
            if cur is None or cur[0] != start:
                if cur is not None:
                    self.done[c].setdefault(sym, []).append(cur)
                cur = [start] + [math.nan] * len(BAR_COLS)
                self.cur[c][sym] = cur
            if px == px:
                if cur[1 + _BC["open"]] != cur[1 + _BC["open"]]:
                    cur[1 + _BC["open"]] = px
                    cur[1 + _BC["high"]] = px
                    cur[1 + _BC["low"]] = px
                else:
                    cur[1 + _BC["high"]] = max(cur[1 + _BC["high"]], px)
                    cur[1 + _BC["low"]] = min(cur[1 + _BC["low"]], px)
                cur[1 + _BC["close"]] = px
            if vol == vol and cur[1 + _BC["vol_first"]] != cur[1 + _BC["vol_first"]]:
                cur[1 + _BC["vol_first"]] = vol
            for k, v in rest.items():
                if v == v:
                    cur[1 + _BC[k]] = v
        return True

    def bar(self, cadence: str, sym: str, b: int) -> Optional[list]:
        """The bar of `sym` that starts at `b`, or None."""
        cur = self.cur[cadence].get(sym)
        if cur is not None and cur[0] == b:
            return cur
        done = self.done[cadence].get(sym)
        if done:
            for x in reversed(done[-3:]):
                if x[0] == b:
                    return x
        return None

    def close(self, cadence: str, sym: str, b: int) -> float:
        x = self.bar(cadence, sym, b)
        return x[1 + _BC["close"]] if x is not None else math.nan

    def front_bar(self, cadence: str, b: int, day) -> tuple[float, Optional[object]]:
        """fut_front at bucket b: among the futures with a bar at b and an expiry after today, the nearest (its close and expiry)."""
        best = None
        for sym, e in self.fut_expiry.items():
            if e is None or e <= day:
                continue
            x = self.bar(cadence, sym, b)
            if x is not None and (best is None or e < best[1]):
                best = (x[1 + _BC["close"]], e)
        return best if best is not None else (math.nan, None)

    def front_mid(self, cadence: str, b: int) -> tuple[float, Optional[object]]:
        """The live chain's sub-minute fallback (_fut_buckets): the nearest-expiry future that has ticked today (its expiry day
        included), its last quote mid in the bucket, else its last trade."""
        ticked = [(e, s) for s, e in self.fut_expiry.items() if s in self.last_ns]
        if not ticked:
            return math.nan, None
        e, s = min(ticked)
        x = self.bar(cadence, s, b)
        if x is None:
            return math.nan, None
        m = x[1 + _BC["mid_last"]]
        return (m if m == m else x[1 + _BC["ltp_last"]]), e

    def frame(self, cadence: str, symbols=None, upto: Optional[int] = None) -> pd.DataFrame:
        """Today's bars as a frame (ts, symbol, open, high, low, close, vol_last, vol_first, oi, ...), bars starting before `upto`."""
        rows = []
        for sym in (symbols or self.symbols):
            for x in self.done[cadence].get(sym, []) + ([self.cur[cadence][sym]] if sym in self.cur[cadence] else []):
                if upto is None or x[0] < upto:
                    rows.append([x[0], sym] + x[1:])
        df = pd.DataFrame(rows, columns=["ts", "symbol", *BAR_COLS])
        df["ts"] = pd.to_datetime(df["ts"].astype("int64"), unit="ns") if len(df) else pd.Series(dtype="datetime64[ns]")
        return df.sort_values(["ts", "symbol"], kind="stable").reset_index(drop=True)


# ---------------------------------------------------------------------------------------------- the chain of one bucket
CHAIN_COLS = ["ts", "expiry", "strike", "cp", "close", "volume", "oi", "age", "lot", "t", "r", "fwd", "fwd_src", "k", "iv", "delta",
              "gamma", "vega", "bid", "ask", "bid_qty", "ask_qty", "iv_vendor", "delta_vendor", "gamma_vendor", "vol_day"]


@dataclass
class Chain:
    """The chain rows of one bucket as arrays (slots index the OptionBook), in (expiry, strike, cp) order."""
    b: int
    td: int
    kind: str
    slots: np.ndarray
    cols: dict

    def __len__(self) -> int:
        return len(self.slots)

    def frame(self, book: OptionBook) -> pd.DataFrame:
        """As build_chain returns it (same columns and order)."""
        s = self.slots
        d = {"ts": pd.Series(pd.Timestamp(self.b), index=range(len(s))), "expiry": book.expiry[s], "strike": book.strike[s],
             "cp": book.cp[s]}
        d.update(self.cols)
        df = pd.DataFrame(d)
        cols = CHAIN_COLS + (["src"] if self.kind == "live" else [])
        return df[cols]


def day_rate(day, rates: Optional[pd.DataFrame], r: float) -> float:
    """The rate in force on `day` (rate_by_row on the day's table)."""
    return float(rate_by_row(pd.Series([pd.Timestamp(day)]), rate_table(rates), r)[0])


def build_chain(book: OptionBook, sl: Slice, kind: str, r: float, fut_close: float = math.nan, t_fut: float = math.nan,
                spot_close: float = math.nan) -> Chain:
    """Chain rows of one bucket from a Slice: kind "bar" (the runner's `chain`) or "live" (`chain_live`).  `fut_close`/`t_fut` and
    `spot_close` are the forward fallbacks of that bucket (NaN when there is none)."""
    idx = sl.rows
    t = (book.exp_end[idx] - (sl.b + sl.td)) / YEAR_NS
    keep = t > 0
    idx, t = idx[keep], t[keep]
    close = (sl.close_bar if kind == "bar" else sl.close_live)[keep]
    volume = (sl.volume_bar if kind == "bar" else sl.volume_live)[keep]
    age = sl.age[keep]
    carried = {k: v[keep] for k, v in sl.carried.items()}
    n = len(idx)
    rr = np.full(n, float(r))
    strike = book.strike[idx]
    fwd = np.full(n, np.nan)
    src = np.full(n, None, dtype=object)
    # forward from put-call parity: per expiry, the PARITY_PAIRS strikes with the smallest |C - P| where both sides are fresh
    pos = {int(s): j for j, s in enumerate(idx)}
    fresh = age <= PARITY_AGE
    exps = book.expiry[idx]
    for e in dict.fromkeys(exps):
        sel = np.flatnonzero(exps == e)
        cs = [j for j in sel if book.is_call[idx[j]] and fresh[j] and not np.isnan(close[j])]
        pairs = []
        for j in cs:
            p = book.partner[idx[j]]
            q = pos.get(int(p), -1) if p >= 0 else -1
            if q >= 0 and fresh[q] and not np.isnan(close[q]):
                pairs.append((j, q))
        if not pairs:
            continue
        cj = np.array([a for a, _ in pairs])
        pj = np.array([b for _, b in pairs])
        diff = np.abs(close[cj] - close[pj])
        f = strike[cj] + np.exp(rr[cj] * t[cj]) * (close[cj] - close[pj])
        order = np.lexsort((np.arange(len(diff)), diff))[:PARITY_PAIRS]
        fwd[sel] = float(np.median(f[order]))
        src[sel] = "parity"
    use = np.isnan(fwd) & ~np.isnan(np.full(n, fut_close))
    if use.any():
        fb = fut_close * np.exp(-rr * (t_fut - t))
        fwd[use], src[use] = fb[use], "future"
    use = np.isnan(fwd) & ~np.isnan(np.full(n, spot_close))
    if use.any():
        sb = spot_close * np.exp(rr * t)
        fwd[use], src[use] = sb[use], "spot"
    with np.errstate(divide="ignore", invalid="ignore"):
        k = np.log(strike / fwd)
    is_call = book.is_call[idx]
    ok = (age <= IV_AGE) & (close >= MIN_IV_PRICE)
    iv = np.full(n, np.nan)
    if ok.any():
        iv[ok] = black76.implied_vol(is_call[ok], close[ok], fwd[ok], strike[ok], t[ok], rr[ok])
    with np.errstate(divide="ignore", invalid="ignore"):
        gk = black76.greeks(is_call, fwd, strike, t, iv, rr)
    cols = {"close": close, "volume": volume, "oi": carried["oi"], "age": age, "lot": book.lot[idx], "t": t, "r": rr, "fwd": fwd,
            "fwd_src": src, "k": k, "iv": iv, "delta": gk["delta"], "gamma": gk["gamma"], "vega": gk["vega"]}
    for name in ("bid", "ask", "bid_qty", "ask_qty", "iv_vendor", "delta_vendor", "gamma_vendor", "vol_day"):
        cols[name] = carried[name]
    if kind == "live":                                     # "tick" on the bucket the contract traded in; carried rows have none
        cols["src"] = np.where(sl.has_bar[keep], np.array("tick", dtype=object), np.array(np.nan, dtype=object))
    return Chain(sl.b, sl.td, kind, idx, cols)


# ---------------------------------------------------------------------------------------------- one session
TICK_COLS = ("ltp", "vol_today", "oi", "bid", "ask", "bid_qty", "ask_qty", "iv", "delta", "gamma")


class Bucket:
    """One closed bucket of one cadence: the option Slice and, built on first use, its two chains."""

    def __init__(self, session: "Session", cadence: str, b: int, sl: Slice):
        self.session, self.cadence, self.b, self.slice = session, cadence, b, sl
        self.td = sl.td
        self._chains: dict = {}

    @property
    def ts(self) -> pd.Timestamp:
        return pd.Timestamp(self.b)

    def chain(self, kind: str = "bar") -> Chain:
        if kind not in self._chains:
            self._chains[kind] = self.session._chain(self, kind)
        return self._chains[kind]


class Session:
    """Per-tick state of one underlying for one session: options (OptionBook) and index / VIX / futures (BarBook), at every cadence."""

    def __init__(self, underlying: str, contracts: pd.DataFrame, day, cadences=("1min",), r: float = 0.065,
                 rates: Optional[pd.DataFrame] = None):
        ct = contracts
        if "underlying" in ct.columns:
            ct = ct[ct["underlying"] == underlying]
        self.underlying = underlying
        self.day = pd.Timestamp(day).date()
        self.cadences = tuple(cadences)
        self.options = OptionBook(ct, self.cadences)
        self.bars = BarBook(underlying, ct, self.cadences)
        self.r = day_rate(self.day, rates, r)
        self.ticks = 0

    def apply(self, sym: str, t: int, **v) -> bool:
        self.ticks += 1
        return self.options.update(sym, t, **v) or self.bars.update(sym, t, **v)

    def apply_frame(self, df: pd.DataFrame) -> int:
        """Apply a frame of ticks in its row order (the caller sorts by event time).  Returns ticks applied."""
        if df is None or not len(df):
            return 0
        tcol = "fts" if "fts" in df.columns else "ts"
        ts = pd.to_datetime(df[tcol]).to_numpy(dtype="datetime64[ns]").astype(np.int64)
        syms = df["symbol"].astype(str).to_numpy()
        cols = [(c, df[c].to_numpy(dtype=float) if c in df.columns else None) for c in TICK_COLS]
        n = 0
        for i in range(len(df)):
            kw = {c: a[i] for c, a in cols if a is not None}
            n += self.apply(syms[i], int(ts[i]), **kw)
        return n

    def close(self, cadence: str, b: int) -> Bucket:
        return Bucket(self, cadence, b, self.options.close_bucket(cadence, b))

    def _chain(self, bk: Bucket, kind: str) -> Chain:
        b, td, c = bk.b, bk.td, bk.cadence
        sub_minute = td < NS_MIN
        spot = self.bars.close(c, self.bars.index_symbol, b)
        if not sub_minute and spot != spot:                       # at a minute and longer the index bars are the grid
            return build_chain(self.options, _empty_slice(bk.slice), kind, self.r)
        if kind == "live" and sub_minute:
            fut, e = self.bars.front_mid(c, b)
            spot = math.nan
        else:
            fut, e = self.bars.front_bar(c, b, self.day)
        t_fut = (pd.Timestamp(e).value + EXPIRY_NS - (b + td)) / YEAR_NS if e is not None else math.nan
        return build_chain(self.options, bk.slice, kind, self.r, fut_close=fut, t_fut=t_fut, spot_close=spot)


def _empty_slice(sl: Slice) -> Slice:
    z = np.zeros(0)
    return Slice(sl.cadence, sl.b, sl.td, np.zeros(0, np.int64), z, z, z, z, z, {k: z for k, _ in CARRIED}, np.zeros(0, bool))
