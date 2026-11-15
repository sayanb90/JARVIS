"""
Native chain operators (SCRUM-201): chain signals computed straight from one bucket's chain snapshot (signals/engine/state.py Chain)
and its numpy ATM row (signals/engine/surface.py), with the formulas of the batch functions, so no pandas table is built per bucket.
Each returns the rows the batch function would give for that bucket (before normalise_output); tests/unit/test_engine_v2.py replays
dense days through these and through the batch functions (the adapter path) and compares every row.

  cross-sections of the chain:   pcr_near, pcr_full, gex, gex_full, pin, pin_full, rr25, cp_iv_gap
  from the ATM row:              term_slope, curvature, skew (2-minute half-life average over the last 10 minutes, kept as state;
                                 the change against the previous session's same snapshot from the ATM rows loaded at the open),
                                 event_variance (scheduled events loaded at the open), implied_move, vrp, rv_over_iv, vol_size (the
                                 daily HAR forecast and realised moves loaded at the open; India VIX from today's bars)

Two flavours of the chain, as in the batch: "bar" (signals that read `chain`) and "live" (signals that read `chain_live`).
"""

from __future__ import annotations

import math
from collections import defaultdict, deque
from typing import Optional

import numpy as np
import pandas as pd

from analytics import black76
from analytics.realised import SESSIONS_PER_YEAR
from signals.engine.state import Chain, OptionBook

CHAIN_NATIVE = ("pcr_near", "pcr_full", "gex", "gex_full", "pin", "pin_full", "rr25", "cp_iv_gap", "term_slope", "curvature", "skew",
                "event_variance", "implied_move", "vrp", "rv_over_iv", "vol_size")
LIVE_FLAVOUR = {"pcr_full", "gex_full", "pin_full", "rr25"}
R_FLAT = 0.065                                         # Context.r: the flat rate the batch gex flip and cp_iv_gap use
IST = pd.Timedelta(hours=5, minutes=30)
SKEW_HALFLIFE_S, SKEW_WINDOW_S = 120.0, 600.0


def snap_label(known_at: pd.Timestamp, td: int) -> str:
    return (known_at + IST).strftime("%H:%M:%S" if td < 60 * 10**9 else "%H:%M")


def _first(a: np.ndarray):
    """pandas "first": the first non-null value, NaN when none."""
    for v in a:
        if v is not None and v == v:
            return v
    return math.nan


def _sum(a: np.ndarray) -> float:
    return float(np.nansum(a)) if len(a) else 0.0


def _div(a, b) -> float:
    with np.errstate(divide="ignore", invalid="ignore"):
        return float(np.float64(a) / np.float64(b))


class ChainNatives:
    """The native chain operators of one cadence group, with the state some of them carry between buckets."""

    def __init__(self, ids, td: int, book: OptionBook, hist_atm: Optional[pd.DataFrame] = None, participant: Optional[pd.DataFrame] = None,
                 daily: Optional[pd.DataFrame] = None, vix_hist: Optional[pd.DataFrame] = None, events: Optional[pd.DataFrame] = None):
        self.ids = [s for s in ids if s in CHAIN_NATIVE]
        self.td = td
        self.book = book
        self.skew_hist: dict = defaultdict(deque)        # expiry -> (seconds, skew1, skew2) of today's buckets, last 10 minutes
        self.prev_skew: dict = {}                         # snapshot label -> (skew1, skew2) of the previous session
        if hist_atm is not None and len(hist_atm):
            for r in hist_atm.sort_values("ts", kind="stable").itertuples(index=False):
                ka = pd.Timestamp(r.ts) + pd.Timedelta(td, unit="ns")
                self.prev_skew[snap_label(ka, td)] = (r.iv_m1 - r.iv_p1, r.iv_m2 - r.iv_p2)
        self._anchors(daily, vix_hist, events)
        self.participant = None
        if participant is not None and len(participant):
            pt = participant[participant["participant"] == "Client"].copy()
            pt["known_at"] = pd.to_datetime(pt["known_at"]).astype("datetime64[ns]")
            pt = pt.sort_values("known_at", kind="stable")
            self.participant = (pt["known_at"].to_numpy(), (pt["option_index_call_long"] - pt["option_index_call_short"]).to_numpy(float),
                                (pt["option_index_put_long"] - pt["option_index_put_short"]).to_numpy(float))

    def _anchors(self, daily, vix_hist, events) -> None:
        """What vrp, implied_move and event_variance read from earlier sessions, as arrays (once a session)."""
        from signals.options.surface import EVENT_KINDS
        self.fc = None
        if daily is not None and len(daily) and "har_5" in daily.columns:
            f = daily[["known_at", "har_5", "yz20"]].dropna(subset=["har_5"]).sort_values("known_at", kind="stable")
            self.fc = (pd.to_datetime(f["known_at"]).astype("datetime64[ns]").to_numpy(),
                       np.sqrt(f["har_5"].to_numpy(float) * SESSIONS_PER_YEAR), np.sqrt(f["yz20"].to_numpy(float) * SESSIONS_PER_YEAR))
        self.moves = None
        if daily is not None and len(daily):
            d = daily[["date", "known_at", "close"]].sort_values("date").reset_index(drop=True)
            self.moves = (np.log(d["close"]).to_numpy(), pd.to_datetime(d["known_at"]).astype("datetime64[ns]").to_numpy())
        self.memo: dict = {}
        self.vix_last = None                                         # (bar end ns, close) of the latest VIX bar
        self.has_vix = vix_hist is not None and len(vix_hist) > 0
        if self.has_vix:
            v = vix_hist.sort_values("ts")
            self.vix_last = (int(pd.Timestamp(v["ts"].iloc[-1]).value) + self.td, float(v["close"].iloc[-1]))
        self.event_days = set()
        if events is not None and len(events):
            self.event_days = set(pd.to_datetime(events[events["kind"].isin(EVENT_KINDS)]["date"]).dt.date)

    def on_vix_bar(self, start_ns: int, close: float) -> None:
        if close == close:
            self.vix_last = (start_ns + self.td, close)

    @property
    def needs_live(self) -> bool:
        return bool(LIVE_FLAVOUR & set(self.ids))

    def run(self, b: int, bar: Optional[Chain], live: Optional[Chain], atm: Optional[dict], fut: tuple = (math.nan, math.nan),
            since=None) -> dict:
        ts = pd.Timestamp(b)
        ka = ts + pd.Timedelta(self.td, unit="ns")
        ctx = {"ts": ts, "ka": ka, "date": ts.date(), "snap": snap_label(ka, self.td), "fut": fut, "since": since}
        out = {}
        for sid in self.ids:
            ch = live if sid in LIVE_FLAVOUR else bar
            out[sid] = getattr(self, "_" + sid)(ch, atm, ctx)
        return out

    # ------------------------------------------------------------------------------------------ helpers on a chain
    def _cols(self, ch: Chain) -> dict:
        s = ch.slots
        b = self.book
        return dict(ch.cols, expiry=b.expiry[s], strike=b.strike[s], cp=b.cp[s])

    @staticmethod
    def _front(c: dict) -> np.ndarray:
        e = c["expiry"]
        return e == min(e) if len(e) else np.zeros(0, bool)

    # ------------------------------------------------------------------------------------------ put/call ratios
    def _pcr(self, ch, ctx, max_k):
        if ch is None or not len(ch):
            return pd.DataFrame()
        c = self._cols(ch)
        with np.errstate(invalid="ignore"):
            m = self._front(c) & (np.abs(c["k"]) <= max_k)
        if not m.any():
            return pd.DataFrame()
        is_p, is_c = m & (c["cp"] == "P"), m & (c["cp"] == "C")
        if not is_p.any() or not is_c.any():                 # the batch's unstack has no column for the missing side
            return pd.DataFrame()
        vol = c["vol_day"]
        put_oi, call_oi = _sum(c["oi"][is_p]), _sum(c["oi"][is_c])
        put_vol, call_vol = _sum(vol[is_p]), _sum(vol[is_c])
        return pd.DataFrame([{"date": ctx["date"], "snap": ctx["snap"], "known_at": ctx["ka"], "put_oi": put_oi, "call_oi": call_oi,
                              "put_vol": put_vol, "call_vol": call_vol, "pcr_oi": _div(put_oi, call_oi),
                              "pcr_vol": put_vol / call_vol if call_vol != 0 else math.nan}])

    def _pcr_near(self, ch, atm, ctx):
        return self._pcr(ch, ctx, 0.03)

    def _pcr_full(self, ch, atm, ctx):
        return self._pcr(ch, ctx, np.inf)

    # ------------------------------------------------------------------------------------------ gamma exposure
    def _gex(self, ch, atm, ctx, max_k=0.03, gamma="gamma"):
        if ch is None or not len(ch):
            return pd.DataFrame()
        c = self._cols(ch)
        g = c["gamma"]
        if gamma == "gamma_use":
            gv = c["gamma_vendor"]
            g = np.where(~np.isnan(gv) & (gv != 0), gv, c["gamma"])
        with np.errstate(invalid="ignore"):
            m = (np.abs(c["k"]) <= max_k) & ~np.isnan(g) & ~np.isnan(c["oi"])
        if not m.any():
            return pd.DataFrame()
        g, oi, fwd, strike = g[m], c["oi"][m], c["fwd"][m], c["strike"][m]
        lot = np.where(np.isnan(c["lot"][m]), 1.0, c["lot"][m])
        is_c = c["cp"][m] == "C"
        unit = g * oi * lot * fwd ** 2 * 0.01
        gc, gp = np.where(is_c, unit, 0.0), np.where(~is_c, unit, 0.0)
        row = {"date": ctx["date"], "snap": ctx["snap"], "known_at": ctx["ka"], "gex_calls": _sum(gc), "gex_puts": _sum(gp),
               "fwd": _first(fwd)}
        row["gex_us"] = row["gex_calls"] - row["gex_puts"]
        ks, inv = np.unique(strike, return_inverse=True)
        bc, bp = np.zeros(len(ks)), np.zeros(len(ks))
        np.add.at(bc, inv, np.nan_to_num(gc))
        np.add.at(bp, inv, np.nan_to_num(gp))
        row["call_wall"], row["put_wall"] = float(ks[int(np.argmax(bc))]), float(ks[int(np.argmax(bp))])
        if self.participant is not None:
            at, nc, npu = self.participant
            i = int(np.searchsorted(at, np.datetime64(ctx["ka"].value, "ns"), side="right")) - 1
            if i >= 0:
                row.update(client_net_calls=nc[i], client_net_puts=npu[i], pt_at=pd.Timestamp(at[i]))
                row["gex_participant"] = -np.sign(nc[i]) * row["gex_calls"] - np.sign(npu[i]) * row["gex_puts"]
            else:
                row.update(client_net_calls=math.nan, client_net_puts=math.nan, pt_at=pd.NaT, gex_participant=math.nan)
        flip = math.nan
        f0 = float(fwd[0])
        grid = f0 * np.linspace(0.97, 1.03, 49)
        f2 = grid[:, None]
        with np.errstate(divide="ignore", invalid="ignore"):
            gk = black76.greeks(np.broadcast_to(is_c, (len(grid), len(g))), f2, strike[None, :], c["t"][m][None, :],
                                c["iv"][m][None, :], R_FLAT)["gamma"]
        u = np.nan_to_num(gk) * (oi * lot)[None, :] * f2 ** 2 * 0.01
        tot = np.where(is_c[None, :], u, -u).sum(axis=1)
        idx = np.where(np.diff(np.sign(tot)) != 0)[0]
        if len(idx):
            i = idx[np.argmin(np.abs(grid[idx] - f0))]
            flip = float(grid[i] - tot[i] * (grid[i + 1] - grid[i]) / (tot[i + 1] - tot[i]))
        row["gamma_flip"] = flip
        row["flip_dist"] = (row["fwd"] - flip) / row["fwd"]
        return pd.DataFrame([row])

    def _gex_full(self, ch, atm, ctx):
        return self._gex(ch, atm, ctx, max_k=np.inf, gamma="gamma_use")

    # ------------------------------------------------------------------------------------------ pin
    def _pin_common(self, ch, ctx, max_k, with_gamma):
        if ch is None or not len(ch):
            return None
        c = self._cols(ch)
        with np.errstate(invalid="ignore"):
            m = self._front(c) & (np.abs(c["k"]) <= max_k) if max_k < np.inf else self._front(c)
        if not m.any() or min(c["expiry"]) != ctx["date"]:            # expiry day only
            return None
        ks, inv = np.unique(c["strike"][m], return_inverse=True)
        oi = np.zeros(len(ks))
        np.add.at(oi, inv, np.nan_to_num(c["oi"][m]))
        fwd = _first(c["fwd"][m])
        out = {"pin_oi": float(ks[int(np.argmax(oi))]), "fwd": fwd}
        if with_gamma:
            goi = np.zeros(len(ks))
            np.add.at(goi, inv, np.nan_to_num(c["gamma"][m]) * np.nan_to_num(c["oi"][m]))
            out["pin_gamma"] = float(ks[int(np.argmax(goi))])
        return out

    def _pin(self, ch, atm, ctx):
        p = self._pin_common(ch, ctx, 0.03, True)
        if p is None:
            return pd.DataFrame()
        return pd.DataFrame([{"ts": ctx["ts"], "pin_oi": p["pin_oi"], "fwd": p["fwd"], "pin_gamma": p["pin_gamma"],
                              "dist_oi": (p["fwd"] - p["pin_oi"]) / p["fwd"], "dist_gamma": (p["fwd"] - p["pin_gamma"]) / p["fwd"],
                              "known_at": ctx["ka"]}])

    def _pin_full(self, ch, atm, ctx):
        p = self._pin_common(ch, ctx, np.inf, False)
        if p is None:
            return pd.DataFrame()
        return pd.DataFrame([{"ts": ctx["ts"], "known_at": ctx["ka"], "pin_oi": p["pin_oi"], "fwd": p["fwd"],
                              "dist_oi": (p["fwd"] - p["pin_oi"]) / p["fwd"]}])

    # ------------------------------------------------------------------------------------------ 25-delta risk reversal
    def _rr25(self, ch, atm, ctx):
        if ch is None or not len(ch):
            return pd.DataFrame()
        c = self._cols(ch)
        dv, ivv = c["delta_vendor"], c["iv_vendor"]
        ok = ~np.isnan(dv) & ~np.isnan(ivv)
        rows = []
        for e in sorted(set(c["expiry"][ok])):
            m = ok & (c["expiry"] == e)

            def at(side, target):
                s = m & (c["cp"] == side)
                d, v = dv[s], ivv[s]
                o = np.argsort(d, kind="quicksort")
                d, v = d[o], v[o]
                return float(np.interp(target, d, v)) if len(d) >= 2 and d.min() <= target <= d.max() else math.nan
            iv_c, iv_p = at("C", 0.25), at("P", -0.25)
            rows.append({"date": ctx["date"], "snap": ctx["snap"], "known_at": ctx["ka"], "expiry": e, "iv_25c": iv_c, "iv_25p": iv_p,
                         "rr25": iv_p - iv_c})
        return pd.DataFrame(rows)

    # ------------------------------------------------------------------------------------------ call-put IV gap and basis
    def _cp_iv_gap(self, ch, atm, ctx):
        if ch is None or not len(ch) or atm is None:
            return pd.DataFrame()
        c = self._cols(ch)
        with np.errstate(invalid="ignore"):
            m = self._front(c) & (np.abs(c["k"]) <= 0.01)
        if not m.any():
            return pd.DataFrame()
        iv, strike, cp = c["iv"][m], c["strike"][m], c["cp"][m]
        have = ~np.isnan(iv)
        if not (have & (cp == "C")).any() or not (have & (cp == "P")).any():
            return pd.DataFrame()
        ks = np.unique(strike[have])
        civ = {k: v for k, v, s, h in zip(strike, iv, cp, have) if s == "C" and h}
        piv = {k: v for k, v, s, h in zip(strike, iv, cp, have) if s == "P" and h}
        gaps = [civ[k] - piv[k] for k in ks if k in civ and k in piv]
        gap = float(np.median(gaps)) if gaps else math.nan
        fwd, t = _first(c["fwd"][m]), _first(c["t"][m])
        src = _first(c["fwd_src"][m])
        close, t_fut = ctx["fut"]
        fut_fwd = close * math.exp(-R_FLAT * (t_fut - t)) if close == close and t_fut == t_fut else math.nan
        basis = (fwd / fut_fwd - 1) * 1e4 if src == "parity" else math.nan
        return pd.DataFrame([{"date": ctx["date"], "snap": ctx["snap"], "known_at": ctx["ka"], "cp_gap": gap, "basis_bp": basis}])

    # ------------------------------------------------------------------------------------------ from the ATM row
    def _term_slope(self, ch, atm, ctx):
        if atm is None:
            return pd.DataFrame()
        t, nx = atm["t"], atm.get("next_atm_iv")
        if not (t * 365 >= 1) or nx is None or nx != nx:
            return pd.DataFrame()
        return pd.DataFrame([{"date": ctx["date"], "snap": ctx["snap"], "known_at": ctx["ka"], "expiry": atm["expiry"],
                              "next_expiry": atm["next_expiry"], "atm_iv": atm["atm_iv"], "next_atm_iv": nx,
                              "slope": nx - atm["atm_iv"], "ratio": _div(nx, atm["atm_iv"]),
                              "days_between": (atm["next_t"] - t) * 365}])

    def _curvature(self, ch, atm, ctx):
        if atm is None:
            return pd.DataFrame()
        return pd.DataFrame([{"date": ctx["date"], "snap": ctx["snap"], "known_at": ctx["ka"], "expiry": atm["expiry"],
                              "curv1": (atm["iv_m1"] + atm["iv_p1"]) / 2 - atm["atm_iv"],
                              "curv2": (atm["iv_m2"] + atm["iv_p2"]) / 2 - atm["atm_iv"]}])

    def _skew(self, ch, atm, ctx):
        if atm is None:
            return pd.DataFrame()
        s1, s2 = atm["iv_m1"] - atm["iv_p1"], atm["iv_m2"] - atm["iv_p2"]
        prev = self.prev_skew.get(ctx["snap"])
        d1, d2 = (s1 - prev[0], s2 - prev[1]) if prev else (math.nan, math.nan)
        now = (ctx["ka"] - pd.Timestamp("1970-01-01")).total_seconds()
        h = self.skew_hist[atm["expiry"]]
        h.append((now, s1, s2))
        while h and now - h[0][0] >= SKEW_WINDOW_S:
            h.popleft()
        sm = []
        for j in (1, 2):
            ok = [(x[0], x[j]) for x in h if x[j] == x[j]]
            if ok:
                w = np.array([0.5 ** ((now - t) / SKEW_HALFLIFE_S) for t, _ in ok])
                v = np.array([v for _, v in ok])
                sm.append(float((w * v).sum() / w.sum()))
            else:
                sm.append(math.nan)
        return pd.DataFrame([{"date": ctx["date"], "snap": ctx["snap"], "known_at": ctx["ka"], "expiry": atm["expiry"], "skew1": s1,
                              "skew2": s2, "d_skew1": d1, "d_skew2": d2, "skew1_s": sm[0], "skew2_s": sm[1]}])

    # ------------------------------------------------------------------------------------------ forward variance and events
    def _event_variance(self, ch, atm, ctx):
        if atm is None:
            return pd.DataFrame()
        t, nt, iv, niv = atm["t"], atm["next_t"], atm["atm_iv"], atm["next_atm_iv"]
        if niv is None or niv != niv or not (nt > t):
            return pd.DataFrame()
        w1, w2 = iv ** 2 * t, niv ** 2 * nt
        fwd_var = (w2 - w1) / (nt - t)
        fv = max(fwd_var, 0.0) if fwd_var == fwd_var else fwd_var
        d, x = ctx["date"], pd.Timestamp(atm["expiry"]).date()
        ev = any(d < e <= x for e in self.event_days) or (d in self.event_days and ctx["snap"] < "12:00")
        excess = w1 - fv * t
        evar = excess if ev else math.nan
        return pd.DataFrame([{"date": d, "snap": ctx["snap"], "known_at": ctx["ka"], "expiry": atm["expiry"],
                              "fwd_vol": math.sqrt(fv) if fv == fv else math.nan, "event_before_expiry": ev, "event_var": evar,
                              "event_move": math.sqrt(max(evar, 0.0)) if evar == evar else math.nan}])

    # ------------------------------------------------------------------------------------------ volatility risk premium family
    def _forecast(self, ka: pd.Timestamp):
        """The HAR forecast made at the latest close strictly before the snapshot minus a second (_with_forecast)."""
        at, rv, yz = self.fc
        i = int(np.searchsorted(at, np.datetime64((ka - pd.Timedelta(seconds=1)).value, "ns"), side="left")) - 1
        return (rv[i], yz[i]) if i >= 0 else (math.nan, math.nan)

    def _vrp_row(self, atm, ctx):
        if atm is None or self.fc is None:
            return None
        rv, yz = self._forecast(ctx["ka"])
        iv7 = atm["iv7"]
        row = {"date": ctx["date"], "snap": ctx["snap"], "known_at": ctx["ka"], "iv7": iv7, "iv7_how": atm["iv7_how"], "rv_fc": rv,
               "rv_yz20": yz, "vrp_diff": iv7 - rv, "vrp_ratio": _div(iv7, rv)}
        if self.has_vix:
            vx = math.nan
            if self.vix_last is not None:
                at, close = self.vix_last
                if at <= ctx["ka"].value and ctx["ka"].value - at <= 5 * 60 * 10**9:
                    vx = close
            row["vix"], row["vix_vrp_diff"] = vx, vx / 100.0 - rv
        return row

    def _vrp(self, ch, atm, ctx):
        r = self._vrp_row(atm, ctx)
        return pd.DataFrame([r]) if r else pd.DataFrame()

    def _rv_over_iv(self, ch, atm, ctx):
        r = self._vrp_row(atm, ctx)
        if not r:
            return pd.DataFrame()
        return pd.DataFrame([{k: r[k] for k in ("date", "snap", "known_at", "iv7", "rv_fc")} | {"trigger": bool((r["rv_fc"] - r["iv7"]) > 0.01)}])

    def _vol_size(self, ch, atm, ctx):
        r = self._vrp_row(atm, ctx)
        if not r:
            return pd.DataFrame()
        sc = 0.12 / r["rv_fc"] if r["rv_fc"] == r["rv_fc"] and r["rv_fc"] != 0 else math.nan
        sc = min(max(sc, 0.25), 2.0) if sc == sc else sc
        return pd.DataFrame([{k: r[k] for k in ("date", "snap", "known_at", "rv_fc")} | {"scale": sc}])

    def _implied_move(self, ch, atm, ctx):
        if atm is None or self.moves is None:
            return pd.DataFrame()
        logc, ka = self.moves
        t, straddle, fwd = atm["t"], atm["straddle"], atm["fwd"]
        im = straddle / fwd
        ivm = im / (MU1 * math.sqrt(t)) if t == t and t >= 0 else math.nan
        m = int(np.searchsorted(ka, np.datetime64(ctx["ka"].value, "ns"), side="left"))
        n = max(1, int(np.round(t * SESSIONS_PER_YEAR)))
        if (m, n) not in self.memo:
            if m < n * 4 + 1:
                self.memo[(m, n)] = math.nan
            else:
                lc = logc[:m]
                mv = np.abs(lc[n:] - lc[:-n])[-20:]
                self.memo[(m, n)] = float(mv.mean()) / (MU1 * np.sqrt(n / SESSIONS_PER_YEAR)) if len(mv) else math.nan
        rvm = self.memo[(m, n)]
        return pd.DataFrame([{"date": ctx["date"], "snap": ctx["snap"], "known_at": ctx["ka"], "expiry": atm["expiry"], "t": t,
                              "straddle": straddle, "fwd": fwd, "implied_move": im, "implied_vol_from_move": ivm,
                              "realised_vol_from_move": rvm, "move_ratio": _div(ivm, rvm)}])


MU1 = float(np.sqrt(2.0 / np.pi))
