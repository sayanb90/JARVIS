"""
Native operators (SCRUM-201): live signals computed from the bucket's ticks and per-symbol state carried between buckets, in numpy, with
the formulas of the batch functions in signals/options/live.py and signals/options/flow.py.  A signal without a native operator runs as
a windowed adapter in engine v2 (signals/engine/core.py): its batch function on frames cut from the per-tick state, bounded by its
declared lookback.  tests/unit/test_engine_v2.py replays a day through both and compares every row.

The tape of one bucket (TapeTable): this underlying's option and future ticks in [b, b + td), plus each symbol's last earlier tick (the
anchor), sorted by event time; for every tick the index of the previous tick of the same symbol.  Every operator below reads it once.

  book_pressure    last microprice minus last mid over the mid, last queue imbalance; per symbol (options and futures)
  illiquidity      median relative spread x max(1, 10 lots / touch lots); per option
  spread_gate      median relative spread per option, its moneyness from the live chain, z against the slot baseline (when loaded)
  fut_ofi          best-level order flow imbalance of the front future over mean depth
  mlofi_future     the same at each of 5 levels, and their mean
  depth_features   5-level depth imbalance, book slope, depth, refills, depth against the median of the previous 5 minutes
  signed_flow      Lee-Ready signed option volume, delta x lot and vega weighted (live chain Greeks)
  trade_size_flow  Lee-Ready signed lots by trade-size bucket, options and futures
  opening_flow     option volume by what it did to OI (opening / closing, buys / sells), calls and puts
  block_prints     prints of 100+ option lots or 50+ future lots, with the largest OI change across the chain in the bucket
  fair_value_gap   option mid moved by delta x the front future's microprice change since the previous bucket, against the quote

Lee-Ready memory: the tick rule (used when a trade prints at the prevailing mid, or with no quote before it) remembers the last price
change of the session; the batch remembers it within its window (the bucket and the one before).  They differ only for a trade at the
mid after a quiet bucket; the replay test shows no difference on the synthetic day.
"""

from __future__ import annotations

import math
from collections import defaultdict, deque
from typing import Optional

import numpy as np
import pandas as pd

from signals.options.flow import BLOCK_LOTS_FUT, BLOCK_LOTS_OPT, SIZE_LABELS
from signals.options.live import LEVELS, TOUCH_REF

NATIVE = ("book_pressure", "illiquidity", "spread_gate", "fut_ofi", "mlofi_future", "depth_features", "signed_flow", "trade_size_flow",
          "opening_flow", "block_prints", "fair_value_gap")
WITHDRAW = pd.Timedelta(minutes=5)


def _num(df: pd.DataFrame, c: str) -> np.ndarray:
    return df[c].to_numpy(dtype=float) if c in df.columns else np.full(len(df), np.nan)


class TapeTable:
    """The bucket's ticks with contract facts and derived quote measures (ticks_meta), as arrays."""

    def __init__(self, ticks: pd.DataFrame, meta: dict, b: int, td: int):
        self.b, self.td = b, td
        t = ticks
        sym = t["symbol"].astype(str).to_numpy() if len(t) else np.array([], dtype=object)
        info = [meta.get(s) for s in sym]
        keep = np.array([x is not None for x in info], dtype=bool)          # contracts of this underlying (index, VIX dropped)
        t = t[keep]
        info = [x for x in info if x is not None]
        self.n = n = len(t)
        self.sym = sym[keep]
        self.t = t["fts"].to_numpy(dtype="datetime64[ns]").astype(np.int64) if n else np.zeros(0, np.int64)
        self.itype = np.array([x[0] for x in info], dtype=object)
        self.strike = np.array([x[1] for x in info], dtype=float)
        self.expiry = np.array([x[2] for x in info] + [None], dtype=object)[:-1]
        self.lot = np.array([x[3] for x in info], dtype=float)
        self.is_opt = np.isin(self.itype, ["CE", "PE"])
        self.is_fut = self.itype == "FUT"
        for c in ("ltp", "vol_today", "oi", "bid", "ask", "bid_qty", "ask_qty", "ltq"):
            setattr(self, c, _num(t, c))
        self.has_ltq = "ltq" in t.columns
        self.levels = {}
        for side in ("bid", "ask"):
            for i in range(2, LEVELS + 1):
                self.levels[(side, i, "px")] = _num(t, f"{side}{i}")
                self.levels[(side, i, "qty")] = _num(t, f"{side}{i}_qty")
        self.book = "bid2" in t.columns and "bid2_qty" in t.columns and bool(np.any(~np.isnan(self.levels[("bid", 2, "px")])))
        with np.errstate(divide="ignore", invalid="ignore"):
            self.mid = (self.bid + self.ask) / 2
            self.rel_spread = (self.ask - self.bid) / self.mid
            tot = self.bid_qty + self.ask_qty
            tot = np.where(tot == 0, np.nan, tot)
            self.microprice = (self.bid * self.ask_qty + self.ask * self.bid_qty) / tot
            self.qi = (self.bid_qty - self.ask_qty) / tot
        self.in_b = self.t >= b
        # previous tick of the same symbol (-1 when none in the table)
        codes, self.code = np.unique(self.sym, return_inverse=True) if n else (np.array([]), np.zeros(0, np.int64))
        self.codes = codes
        order = np.lexsort((np.arange(n), self.code))
        prev = np.full(n, -1, dtype=np.int64)
        if n > 1:
            same = self.code[order[1:]] == self.code[order[:-1]]
            prev[order[1:][same]] = order[:-1][same]
        self.prev = prev
        self.sym_order = order                                  # rows grouped by symbol, in time order

    def shifted(self, a: np.ndarray) -> np.ndarray:
        out = np.full(self.n, np.nan)
        ok = self.prev >= 0
        out[ok] = a[self.prev[ok]]
        return out

    def grouped(self, mask: np.ndarray) -> Optional[Grouped]:
        g = Grouped(self, mask)
        return g if g.n else None

    def groups(self, mask: np.ndarray):
        """Per symbol among rows in the bucket and `mask`: (symbol, row indices in time order), symbols in sorted order."""
        idx = np.flatnonzero(mask & self.in_b)
        if not len(idx):
            return []
        c = self.code[idx]
        order = np.lexsort((idx, c))
        idx, c = idx[order], c[order]
        cut = np.flatnonzero(np.diff(c)) + 1
        return [(self.codes[self.code[s[0]]], s) for s in np.split(idx, cut)]


class Grouped:
    """The rows of the bucket and a mask, grouped by symbol (symbols in sorted order, rows in time order): last, mean, median, sum and
    first of an array per symbol, each in one numpy pass.  The per-symbol Python loops this replaces (one np.median call per contract and
    bucket among them) took about 200 ms of every 1 s bucket close on the live feed (17 Nov 2026), with 300 contracts ticking a second."""

    def __init__(self, tp: "TapeTable", mask: np.ndarray):
        idx = np.flatnonzero(mask & tp.in_b)
        self.n = 0
        if not len(idx):
            return
        c = tp.code[idx]
        order = np.argsort(c, kind="stable")            # idx is ascending: a stable sort keeps time order within a symbol
        self.idx, c = idx[order], c[order]
        first = np.empty(len(c), dtype=bool)
        first[0] = True
        first[1:] = c[1:] != c[:-1]
        self.starts = np.flatnonzero(first)
        self.gid = np.cumsum(first) - 1
        self.n = len(self.starts)
        self.syms = tp.codes[c[self.starts]]
        self.first_rows = self.idx[self.starts]

    def _valid(self, a: np.ndarray):
        v = a[self.idx]
        return v, ~np.isnan(v)

    def last(self, a: np.ndarray) -> np.ndarray:
        """Last non-NaN value per symbol (NaN when none)."""
        v, ok = self._valid(a)
        pos = np.where(ok, np.arange(len(v)), -1)
        lp = np.maximum.reduceat(pos, self.starts)
        return np.where(lp >= 0, v[np.maximum(lp, 0)], np.nan)

    def mean(self, a: np.ndarray) -> np.ndarray:
        v, ok = self._valid(a)
        cnt = np.add.reduceat(ok.astype(float), self.starts)
        tot = np.add.reduceat(np.where(ok, v, 0.0), self.starts)
        with np.errstate(divide="ignore", invalid="ignore"):
            return np.where(cnt > 0, tot / cnt, np.nan)

    def median(self, a: np.ndarray) -> np.ndarray:
        v, ok = self._valid(a)
        if not ok.any():
            return np.full(self.n, np.nan)
        m = pd.Series(v[ok]).groupby(self.gid[ok]).median()
        out = np.full(self.n, np.nan)
        out[m.index.to_numpy()] = m.to_numpy()
        return out

    def sum(self, a: np.ndarray) -> np.ndarray:
        """Sum per symbol, NaN counted as 0 (np.nansum)."""
        v = np.nan_to_num(a[self.idx], nan=0.0)
        return np.add.reduceat(v, self.starts)


def _divv(a, b) -> np.ndarray:
    """Elementwise a / b with numpy semantics (x / 0 = +-inf, 0 / 0 = NaN)."""
    with np.errstate(divide="ignore", invalid="ignore"):
        return np.asarray(a, dtype=float) / np.asarray(b, dtype=float)


def _div(a, b) -> float:
    """a / b with numpy semantics (x / 0 = +-inf, 0 / 0 = NaN), as the pandas columns divide."""
    with np.errstate(divide="ignore", invalid="ignore"):
        return float(np.float64(a) / np.float64(b))


def _last(a: np.ndarray, rows: np.ndarray) -> float:
    v = a[rows]
    v = v[~np.isnan(v)]
    return float(v[-1]) if len(v) else math.nan


def _median(a: np.ndarray, rows: np.ndarray) -> float:
    v = a[rows]
    v = v[~np.isnan(v)]
    return float(np.median(v)) if len(v) else math.nan


def _mean(a: np.ndarray, rows: np.ndarray) -> float:
    v = a[rows]
    v = v[~np.isnan(v)]
    return float(v.mean()) if len(v) else math.nan


def _sum(a: np.ndarray, rows: np.ndarray) -> float:
    v = a[rows]
    return float(np.nansum(v))


class Natives:
    """State and per-bucket computation of every native operator of one cadence group."""

    def __init__(self, ids, td: int, meta: dict, slot_of=None, spread_base: Optional[pd.DataFrame] = None):
        self.ids = [s for s in ids if s in NATIVE]
        self.td = td
        self.meta = meta
        self.tick_sign: dict = {}                    # Lee-Ready tick rule memory per symbol
        self.prev_oi: dict = {}                      # last OI of the symbol's previous bucket with ticks (opening_flow, block_prints)
        self.prev_fvg: dict = {}                     # symbol -> (bucket, last mid, front future microprice of that bucket)
        self.n_withdraw = max(1, int(WITHDRAW.value // td))
        self._dh_row: dict = {}                      # depth_features: symbol -> row of the ring buffers below
        self._dh_val = np.full((64, self.n_withdraw), np.nan)    # depth of the symbol in each of its last n_withdraw buckets (slot = bucket number mod n)
        self._dh_bk = np.full((64, self.n_withdraw), -1, dtype=np.int64)   # the bucket start each slot holds
        self.ofi_prev: dict = {}                     # last front-future book (best and levels) carried across buckets
        self.spread_base = spread_base
        self._sg_lookup = None

    # ------------------------------------------------------------------------------------------ shared pieces
    def _lee_ready(self, tp: TapeTable, rows: np.ndarray):
        """dv and sign for `rows` (in the bucket), against each symbol's previous tick; updates the tick-rule memory."""
        pv = tp.shifted(tp.vol_today)
        dv = np.nan_to_num(np.clip(tp.vol_today - pv, 0, None), nan=0.0)
        prev_mid = (tp.shifted(tp.bid) + tp.shifted(tp.ask)) / 2
        pl = tp.shifted(tp.ltp)
        with np.errstate(invalid="ignore"):
            d = np.sign(tp.ltp - pl)
        sign = np.zeros(tp.n)
        for s, idx in tp.groups(np.ones(tp.n, bool)):
            mem = self.tick_sign.get(s, 0.0)
            for i in idx:
                if d[i] == d[i] and d[i] != 0:
                    mem = d[i]
                pm = prev_mid[i]
                if pm != pm:
                    sign[i] = mem
                elif tp.ltp[i] > pm:
                    sign[i] = 1.0
                elif tp.ltp[i] < pm:
                    sign[i] = -1.0
                else:
                    sign[i] = mem
            self.tick_sign[s] = mem
        return dv, sign

    def run(self, tp: TapeTable, chain_live=None, book=None) -> dict:
        """Rows (DataFrames before normalise_output) of every native operator for this bucket."""
        ts = pd.Timestamp(tp.b)
        ka = ts + pd.Timedelta(self.td, unit="ns")
        out = {}
        need_lr = {"signed_flow", "trade_size_flow", "opening_flow", "block_prints"} & set(self.ids)
        dv = sign = None
        if need_lr:
            dv, sign = self._lee_ready(tp, tp.in_b)
        greek = {}
        if chain_live is not None and len(chain_live) and book is not None:
            s = chain_live.slots
            for j, slot in enumerate(s):
                greek[book.symbols[slot]] = (chain_live.cols["delta"][j], chain_live.cols["vega"][j], chain_live.cols["k"][j])
        for sid in self.ids:
            fn = getattr(self, "_" + sid)
            out[sid] = fn(tp, ts, ka, dv=dv, sign=sign, greek=greek)
        self._carry(tp)
        return out

    def _carry(self, tp: TapeTable) -> None:
        for s, idx in tp.groups(tp.is_opt):
            v = _last(tp.oi, idx)
            if v == v:
                self.prev_oi[s] = v

    def _info(self, s):
        x = self.meta[s]
        return x[2], x[1], x[0]

    def _facts(self, syms):
        """(expiry, strike, type) of each symbol: lists and a float array, for building the rows of an operator in one go."""
        m = [self.meta[s] for s in syms]
        return [x[2] for x in m], np.array([x[1] for x in m], dtype=float), [x[0] for x in m]

    # ------------------------------------------------------------------------------------------ quotes
    def _book_pressure(self, tp, ts, ka, **_):
        g = tp.grouped(tp.is_opt | tp.is_fut)
        if g is None:
            return pd.DataFrame()
        syms = g.syms.tolist()
        e, k, cp = self._facts(syms)
        mid, mp = g.last(tp.mid), g.last(tp.microprice)
        return pd.DataFrame({"ts": ts, "known_at": ka, "symbol": syms, "expiry": e, "strike": k, "cp": cp,
                             "micro_rel": _divv(mp - mid, mid), "qi": g.last(tp.qi)})

    def _illiquidity(self, tp, ts, ka, **_):
        g = tp.grouped(tp.is_opt)
        if g is None:
            return pd.DataFrame()
        syms = g.syms.tolist()
        e, k, cp = self._facts(syms)
        rs = g.median(tp.rel_spread)
        bq, aq = g.last(tp.bid_qty), g.last(tp.ask_qty)
        lot = tp.lot[g.first_rows]
        touch_q = np.where(aq < bq, aq, bq)                       # min(bq, aq) as Python takes it: NaN only when bq is NaN
        with np.errstate(divide="ignore", invalid="ignore"):
            touch = np.where((lot == lot) & (lot != 0), touch_q / lot, np.nan)
            pen = np.where((touch == touch) & (touch != 0), TOUCH_REF / touch, np.nan)
            illiq = np.where(pen == pen, rs * np.maximum(1.0, pen), np.nan)
        return pd.DataFrame({"ts": ts, "known_at": ka, "symbol": syms, "expiry": e, "strike": k, "cp": cp, "rel_spread": rs, "illiq": illiq})

    def _spread_gate(self, tp, ts, ka, greek=None, **_):
        g = tp.grouped(tp.is_opt)
        if g is None:
            return pd.DataFrame()
        syms = g.syms.tolist()
        e, k, cp = self._facts(syms)
        nan3 = (math.nan, math.nan, math.nan)
        kk = np.array([greek.get(s, nan3)[2] for s in syms], dtype=float) if greek else np.full(len(syms), np.nan)
        rs = g.median(tp.rel_spread)
        return pd.DataFrame({"ts": ts, "known_at": ka, "symbol": syms, "expiry": e, "strike": k,
                             "cp": ["C" if c == "CE" else "P" for c in cp], "k": kk, "rel_spread": rs, "z": self._spread_z(ts, kk, rs)})

    def _spread_z(self, ts, k: np.ndarray, rs: np.ndarray) -> np.ndarray:
        """z of the relative spread against the 20-session baseline of the same 15-minute slot and moneyness band, per symbol."""
        out = np.full(len(rs), np.nan)
        b = self.spread_base
        if b is None or not len(b):
            return out
        if self._sg_lookup is None:                                 # (slot ns, band) -> (mean, std), the first match as the frame is read
            slots = pd.to_timedelta(b["slot"]).dt.total_seconds().to_numpy() * 1e9
            bands = b["band"].to_numpy(dtype=float)
            look: dict = {}
            for sl, bd, m, sd in zip(slots, bands, b["base_mean"].to_numpy(dtype=float), b["base_std"].to_numpy(dtype=float)):
                look.setdefault((int(round(sl)), float(bd)), (m, sd))
            self._sg_lookup = look
        slot = int((ts.floor("15min") - ts.normalize()).value)
        with np.errstate(invalid="ignore"):
            band = np.minimum(8.0, np.rint(np.abs(k) / 0.005))
        for i in np.flatnonzero(~np.isnan(k)):
            hit = self._sg_lookup.get((slot, float(band[i])))
            if hit is not None and hit[1] != 0:
                out[i] = (rs[i] - hit[0]) / hit[1]
        return out

    # ------------------------------------------------------------------------------------------ order flow on the front future
    def _front_rows(self, tp: TapeTable) -> np.ndarray:
        """The front future's rows (the nearest expiry among the futures in the table), in time order."""
        if not tp.is_fut.any():
            return np.zeros(0, np.int64)
        e = min(x for x in tp.expiry[tp.is_fut])
        return np.flatnonzero(tp.is_fut & (tp.expiry == e))

    @staticmethod
    def _ofi(b, a, qb, qa):
        pb, pa, pqb, pqa = (np.r_[np.nan, x[:-1]] for x in (b, a, qb, qa))
        with np.errstate(invalid="ignore"):
            e = (np.where(b >= pb, qb, 0) - np.where(b <= pb, pqb, 0) - np.where(a <= pa, qa, 0) + np.where(a >= pa, pqa, 0))
        return e, pb

    def _fut_ofi(self, tp, ts, ka, **_):
        f = self._front_rows(tp)
        if not len(f):
            return pd.DataFrame()
        e, pb = self._ofi(tp.bid[f], tp.ask[f], tp.bid_qty[f], tp.ask_qty[f])
        e = np.where(np.isnan(pb), 0, e)
        mine = tp.in_b[f]
        if not mine.any():
            return pd.DataFrame()
        ofi = float(np.nansum(e[mine]))
        depth = _mean((tp.bid_qty[f] + tp.ask_qty[f]) / 2, np.flatnonzero(mine))
        return pd.DataFrame([{"ts": ts, "ofi": ofi, "depth": depth, "mid": _last(tp.mid[f], np.flatnonzero(mine)),
                              "ofi_norm": ofi / depth if depth else math.nan, "known_at": ka}])

    def _level(self, tp, rows, side, i, what):
        if i == 1:
            return (getattr(tp, side) if what == "px" else getattr(tp, f"{side}_qty"))[rows]
        return tp.levels[(side, i, what)][rows]

    def _mlofi_future(self, tp, ts, ka, **_):
        if not tp.book:
            return pd.DataFrame()
        f = self._front_rows(tp)
        if not len(f) or not tp.in_b[f].any():
            return pd.DataFrame()
        mine = np.flatnonzero(tp.in_b[f])
        row = {"ts": ts}
        norms = []
        for i in range(1, LEVELS + 1):
            b, a = self._level(tp, f, "bid", i, "px"), self._level(tp, f, "ask", i, "px")
            qb, qa = self._level(tp, f, "bid", i, "qty"), self._level(tp, f, "ask", i, "qty")
            e, pb = self._ofi(b, a, qb, qa)
            e = np.where(np.isnan(pb), 0, np.nan_to_num(e))
            ofi = float(e[mine].sum())
            depth = _mean((qb + qa) / 2, mine)
            row[f"ofi{i}"], row[f"depth{i}"] = ofi, depth
            row[f"ofi{i}_norm"] = ofi / depth if depth else math.nan
            norms.append(row[f"ofi{i}_norm"])
        v = [x for x in norms if x == x]
        row["mlofi"] = float(np.mean(v)) if v else math.nan
        row["known_at"] = ka
        return pd.DataFrame([row])

    # ------------------------------------------------------------------------------------------ the 5-level book
    def _depth_row(self, syms) -> np.ndarray:
        """Rows of the depth ring buffers for `syms` (new symbols get a row; the buffers grow by doubling)."""
        rows = np.empty(len(syms), dtype=np.int64)
        for i, s in enumerate(syms):
            r = self._dh_row.get(s)
            if r is None:
                r = self._dh_row[s] = len(self._dh_row)
                if r >= len(self._dh_val):
                    pad = len(self._dh_val)
                    self._dh_val = np.vstack([self._dh_val, np.full((pad, self.n_withdraw), np.nan)])
                    self._dh_bk = np.vstack([self._dh_bk, np.full((pad, self.n_withdraw), -1, dtype=np.int64)])
            rows[i] = r
        return rows

    def _depth_features(self, tp, ts, ka, **_):
        if not tp.book:
            return pd.DataFrame()
        n = tp.n
        all_rows = np.arange(n)
        bq = sum(np.nan_to_num(self._level(tp, all_rows, "bid", i, "qty")) for i in range(1, LEVELS + 1))
        aq = sum(np.nan_to_num(self._level(tp, all_rows, "ask", i, "qty")) for i in range(1, LEVELS + 1))
        with np.errstate(divide="ignore", invalid="ignore"):
            tot = np.where(bq + aq == 0, np.nan, bq + aq)
            imb = (bq - aq) / tot
            depth = bq + aq
            span = (self._level(tp, all_rows, "ask", LEVELS, "px") - self._level(tp, all_rows, "bid", LEVELS, "px")) / tp.mid
            span = np.where(span == 0, np.nan, span)
            slope = (bq + aq) / span
        pbq, pbid, pvol = tp.shifted(tp.bid_qty), tp.shifted(tp.bid), tp.shifted(tp.vol_today)
        with np.errstate(invalid="ignore"):
            sold = (tp.ltp <= pbid) & ((tp.vol_today - pvol) > 0)
            refill = (sold & (tp.bid == pbid) & (tp.bid_qty >= pbq)).astype(int)
        g = tp.grouped(tp.is_opt | tp.is_fut)
        if g is None:
            return pd.DataFrame()
        syms = g.syms.tolist()
        e, k, cp = self._facts(syms)
        d = g.mean(depth)
        # the median of the symbol's depth over its last n_withdraw buckets with ticks (this bucket not yet included), when it has at
        # least min(5, n_withdraw) of them: a ring buffer per symbol, slot = bucket number mod n_withdraw
        r = self._depth_row(syms)
        lo = tp.b - self.n_withdraw * self.td
        val, bk = self._dh_val[r], self._dh_bk[r]
        ok = (bk >= lo) & ~np.isnan(val)
        cnt = ok.sum(axis=1)
        srt = np.sort(np.where(ok, val, np.inf), axis=1)
        a = np.arange(len(r))
        lower, upper = np.maximum(cnt - 1, 0) // 2, cnt // 2
        base = np.where(cnt >= min(5, self.n_withdraw), (srt[a, lower] + srt[a, np.minimum(upper, srt.shape[1] - 1)]) / 2, np.nan)
        slot = (tp.b // self.td) % self.n_withdraw
        self._dh_val[r, slot] = d
        self._dh_bk[r, slot] = tp.b
        return pd.DataFrame({"ts": ts, "symbol": syms, "depth_imb": g.mean(imb), "slope": g.median(slope), "depth": d,
                             "refills": g.sum(refill.astype(float)).astype(int), "cp": cp, "strike": k, "expiry": e,
                             "depth_change": _divv(d, base) - 1, "known_at": ka})

    # ------------------------------------------------------------------------------------------ the tape
    def _signed_flow(self, tp, ts, ka, dv=None, sign=None, greek=None, **_):
        o = np.flatnonzero(tp.is_opt & tp.in_b)
        if not len(o) or not greek:
            return pd.DataFrame()
        sv = df_ = vf = 0.0
        for s, idx in tp.groups(tp.is_opt):
            signed = float((sign[idx] * dv[idx]).sum())
            d_, v_, _ = greek.get(s, (math.nan, math.nan, math.nan))
            lot = tp.lot[idx[0]]
            lot = 1.0 if lot != lot else lot
            sv += signed
            df_ += signed * (0.0 if d_ != d_ else d_) * lot
            vf += signed * (0.0 if v_ != v_ else v_) * lot / 100.0
        return pd.DataFrame([{"ts": ts, "signed_volume": sv, "delta_flow": df_, "vega_flow": vf, "known_at": ka}])

    def _lots(self, tp, rows):
        with np.errstate(divide="ignore", invalid="ignore"):
            lot = np.where(tp.lot[rows] == 0, np.nan, tp.lot[rows])
            return tp.ltq[rows] / lot

    def _trade_size_flow(self, tp, ts, ka, dv=None, sign=None, **_):
        if not tp.has_ltq or np.isnan(tp.ltq).all():
            return pd.DataFrame()
        rows = np.flatnonzero(tp.in_b & (tp.is_opt | tp.is_fut) & (dv > 0))
        if not len(rows):
            return pd.DataFrame()
        lots = self._lots(tp, rows)
        lots = np.where(np.isnan(lots), 1.0, lots)
        out = {f"{k}_{b}": 0.0 for k in ("opt", "fut") for b in SIZE_LABELS}
        edges = [0, 1, 5, 20, np.inf]
        for j, r in enumerate(rows):
            x = lots[j]
            lab = next((SIZE_LABELS[i] for i in range(4) if edges[i] < x <= edges[i + 1]), None)
            if lab is None:
                continue
            kind = "fut" if tp.is_fut[r] else "opt"
            out[f"{kind}_{lab}"] += sign[r] * x
        return pd.DataFrame([{"ts": ts, **out, "known_at": ka}])

    def _opening_flow(self, tp, ts, ka, dv=None, sign=None, **_):
        cls_names = ("open_buy", "open_sell", "close_short", "close_long", "none")
        tot = {f"{c}_{s}": 0.0 for s in ("calls", "puts") for c in cls_names}
        any_row = False
        for s, idx in tp.groups(tp.is_opt):
            any_row = True
            signed = float((sign[idx] * dv[idx]).sum())
            vol = float(dv[idx].sum())
            oi = _last(tp.oi, idx)
            prev = self.prev_oi.get(s)
            d_oi = oi - prev if prev is not None and oi == oi else 0.0
            d_oi = 0.0 if d_oi != d_oi else d_oi
            c = ("open_buy" if d_oi > 0 and signed > 0 else "open_sell" if d_oi > 0 and signed < 0 else
                 "close_short" if d_oi < 0 and signed > 0 else "close_long" if d_oi < 0 and signed < 0 else "none")
            side = "calls" if self.meta[s][0] == "CE" else "puts"
            tot[f"{c}_{side}"] += vol
        if not any_row:
            return pd.DataFrame()
        for side in ("calls", "puts"):
            tot[f"net_open_buy_{side}"] = tot[f"open_buy_{side}"] - tot[f"open_sell_{side}"]
        return pd.DataFrame([{"ts": ts, **tot, "known_at": ka}])

    def _block_prints(self, tp, ts, ka, dv=None, sign=None, **_):
        if not tp.has_ltq:
            return pd.DataFrame()
        rows = np.flatnonzero(tp.in_b & (tp.is_opt | tp.is_fut))
        if not len(rows):
            return pd.DataFrame()
        lots = self._lots(tp, rows)
        big = (tp.is_fut[rows] & (lots >= BLOCK_LOTS_FUT)) | (tp.is_opt[rows] & (lots >= BLOCK_LOTS_OPT))
        ev = rows[big & (dv[rows] > 0)]
        if not len(ev):
            return pd.DataFrame()
        top = None
        for s, idx in tp.groups(tp.is_opt):
            oi = _last(tp.oi, idx)
            prev = self.prev_oi.get(s)
            if prev is None or oi != oi:
                continue
            d = oi - prev
            if top is None or abs(d) > abs(top[3]):
                e, k, cp = self._info(s)
                top = (s, k, cp, d)
        lot_of = dict(zip(rows, lots))
        out = []
        for r in ev:
            out.append({"ts": ts, "symbol": tp.sym[r], "instrument_type": tp.itype[r], "ltp": tp.ltp[r], "lots": lot_of[r],
                        "sign": sign[r], "oi_symbol": top[0] if top else None, "oi_strike": top[1] if top else math.nan,
                        "oi_cp": top[2] if top else None, "oi_jump": top[3] if top else math.nan,
                        "known_at": pd.Timestamp(int(tp.t[r]))})
        return pd.DataFrame(out)

    def _fair_value_gap(self, tp, ts, ka, greek=None, **_):
        fut = np.flatnonzero(tp.is_fut & tp.in_b)
        fmp = math.nan
        if len(fut):
            e = min(tp.expiry[fut])
            fr = fut[tp.expiry[fut] == e]
            fmp = _last(tp.microprice, fr)
        rows = []
        for s, idx in tp.groups(tp.is_opt):
            e, k, cp = self._info(s)
            mid, bid, ask = _last(tp.mid, idx), _last(tp.bid, idx), _last(tp.ask, idx)
            prev = self.prev_fvg.get(s)
            self.prev_fvg[s] = (tp.b, mid, fmp)
            if prev is None or prev[0] != tp.b - self.td or not greek:
                continue
            d_ = greek.get(s, (math.nan,))[0]
            fair = prev[1] + d_ * (fmp - prev[2])
            if fair != fair:
                continue
            rows.append({"ts": ts, "known_at": ka, "symbol": s, "expiry": e, "strike": k, "cp": "C" if cp == "CE" else "P", "fair": fair,
                         "buy_edge": _div(fair - ask, mid), "sell_edge": _div(bid - fair, mid)})
        return pd.DataFrame(rows)


def contract_meta(contract_table: pd.DataFrame) -> dict:
    """symbol -> (instrument_type, strike, expiry, lot_size) for options and futures."""
    c = contract_table[contract_table["instrument_type"].isin(["CE", "PE", "FUT"])]
    return {s: (it, float(k) if k == k else math.nan, e, float(l) if l == l else math.nan)
            for s, it, k, e, l in zip(c["symbol"].astype(str), c["instrument_type"], c["strike"], c["expiry"], c["lot_size"])}
