"""
Shape of the volatility surface (SCRUM-148, step 5).  All from our own IVs (the chain), at the snapshot times unless said otherwise.

  spot_vol_beta     A7   per session: slope of 5-minute changes in front ATM IV on 5-minute index log returns, all, up and down moves;
                         at snapshots, the residual IV move since 09:20 net of the previous session's beta
  term_slope        A8   next-expiry ATM IV minus front ATM IV (front at least a day out), and the ratio
  event_variance    A9   forward variance between the two listed expiries, and the front expiry's variance in excess of it when a
                         scheduled event (RBI policy, Budget, election result) falls before the front expiry
  vol_of_vol        A10  standard deviation over 20 sessions of daily changes in log iv7 (15:15 snapshot), and of log India VIX
  skew              A11  IV 2 % below the forward minus IV 2 % above (and 1 %), and the change since the previous session's snapshot
  curvature         A13  mean of the 2 % wings minus ATM (and 1 %)
  surface_shocks    A14  daily changes of level (iv7), slope (1 % skew) and curvature (1 %), each as a z-score against the previous 60 sessions
  skew_stickiness   A15  per session: (d ATM IV / d ln S) / (d IV / d ln K): 0 is sticky strike, -1 sticky moneyness in this convention
  cp_iv_gap         A16  median call IV minus put IV at the same strike within 1 % of the forward, and the parity forward against the
                         future carried to the option's expiry (basis points)
  vrp_tilt          A17  20-session mean of the 15:15 vrp_diff, for a 1 to 3 month view
  iv_gap            -    iv7 at 09:20 minus iv7 at the previous 15:15, with the index's overnight gap (v1 catalogue)
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from signals.framework import Context, Spec, register, trailing_z
from signals.options.core import OPEN_UTC, snap_utc

EVENT_KINDS = ("rbi_mpc", "budget", "election_result")


def _snaps(ctx: Context) -> pd.DataFrame:
    s = ctx.get("atm_snaps")
    return s if s is not None else pd.DataFrame()


def _close_snap(ctx: Context) -> pd.DataFrame:
    s = _snaps(ctx)
    return s[s["snap"] == "15:15"].copy() if len(s) else s


def _five_min(ctx: Context) -> pd.DataFrame:
    a = ctx.get("atm")
    sp = ctx.get("spot")
    if a is None or not len(a) or not len(sp):
        return pd.DataFrame()
    m = a[["ts", "atm_iv"]].merge(sp[["ts", "close"]], on="ts").sort_values("ts")
    m["date"] = m["ts"].dt.date
    m["b"] = m["ts"].dt.floor("5min")
    g = m.groupby(["date", "b"], as_index=False).agg(iv=("atm_iv", "last"), close=("close", "last"))
    g["div"] = g.groupby("date")["iv"].diff()
    g["r"] = np.log(g["close"]).groupby(g["date"]).diff()
    return g.dropna(subset=["div", "r"])


def _slope(y: pd.Series, x: pd.Series) -> float:
    if len(x) < 10 or x.var() == 0:
        return np.nan
    return float(np.cov(x, y, ddof=1)[0, 1] / x.var())


@register(Spec(id="spot_vol_beta", version=1, family="surface", grid="daily", inputs=("atm", "spot"), tickets=("SCRUM-148",),
               doc="A7: per session, slope of 5-minute ATM IV changes on index returns (all, up, down)."))
def spot_vol_beta(ctx: Context, p: dict) -> pd.DataFrame:
    g = _five_min(ctx)
    if g.empty:
        return g
    rows = []
    for d, x in g.groupby("date"):
        rows.append({"date": d, "known_at": pd.Timestamp(d) + pd.Timedelta(hours=10), "beta": _slope(x["div"], x["r"]),
                     "beta_up": _slope(x.loc[x["r"] > 0, "div"], x.loc[x["r"] > 0, "r"]),
                     "beta_down": _slope(x.loc[x["r"] < 0, "div"], x.loc[x["r"] < 0, "r"]), "n": len(x)})
    return pd.DataFrame(rows)


@register(Spec(id="residual_iv", version=1, family="surface", grid="snap", inputs=("atm_snaps", "spot_vol_beta", "spot"), tickets=("SCRUM-148",),
               doc="A7: at 12:00 and 15:15, the change in front ATM IV since 09:20 net of the previous session's spot-vol beta times the "
                   "index return since 09:20."))
def residual_iv(ctx: Context, p: dict) -> pd.DataFrame:
    s = _snaps(ctx)
    b = ctx.signal("spot_vol_beta")
    sp = ctx.get("spot")
    if not len(s) or not len(b):
        return pd.DataFrame()
    px = sp.set_index("ts")["close"]
    base = s[s["snap"] == "09:20"].set_index("date")
    prev_beta = b.set_index("date")["beta"].shift(1)
    rows = []
    for _, r in s[s["snap"] != "09:20"].iterrows():
        d = r["date"]
        if d not in base.index or d not in prev_beta.index or np.isnan(prev_beta.get(d, np.nan)):
            continue
        p0, p1 = px.get(snap_utc(d, "09:20") - pd.Timedelta(minutes=1)), px.get(pd.Timestamp(r["known_at"]) - pd.Timedelta(minutes=1))
        if p0 is None or p1 is None:
            continue
        ret = np.log(p1 / p0)
        div = r["atm_iv"] - base.loc[d, "atm_iv"]
        rows.append({"date": d, "snap": r["snap"], "known_at": r["known_at"], "d_iv": div, "ret": ret, "beta_prev": prev_beta[d],
                     "residual": div - prev_beta[d] * ret})
    return pd.DataFrame(rows)


@register(Spec(id="term_slope", version=1, family="surface", grid="snap", inputs=("atm_snaps",), params={"min_front_days": 1}, tickets=("SCRUM-148",),
               doc="A8: next-expiry ATM IV minus front ATM IV (front at least a day away), and their ratio."))
def term_slope(ctx: Context, p: dict) -> pd.DataFrame:
    s = _snaps(ctx)
    if not len(s):
        return s
    s = s[(s["t"] * 365 >= p["min_front_days"]) & s["next_atm_iv"].notna()].copy()
    s["slope"] = s["next_atm_iv"] - s["atm_iv"]
    s["ratio"] = s["next_atm_iv"] / s["atm_iv"]
    s["days_between"] = (s["next_t"] - s["t"]) * 365
    return s[["date", "snap", "known_at", "expiry", "next_expiry", "atm_iv", "next_atm_iv", "slope", "ratio", "days_between"]]


@register(Spec(id="event_variance", version=1, family="surface", grid="snap", inputs=("atm_snaps", "events"), tickets=("SCRUM-148",),
               doc="A9: forward volatility between the two expiries and, when a scheduled event falls before the front expiry, the front's "
                   "variance in excess of the forward (the priced event move)."))
def event_variance(ctx: Context, p: dict) -> pd.DataFrame:
    s = _snaps(ctx)
    if not len(s):
        return s
    s = s[s["next_atm_iv"].notna() & (s["next_t"] > s["t"])].copy()
    w1, w2 = s["atm_iv"] ** 2 * s["t"], s["next_atm_iv"] ** 2 * s["next_t"]
    s["fwd_var"] = (w2 - w1) / (s["next_t"] - s["t"])
    s["fwd_vol"] = np.sqrt(s["fwd_var"].clip(lower=0))
    ev = ctx.raw.get("events")
    days = set()
    if ev is not None and len(ev):
        days = set(pd.to_datetime(ev[ev["kind"].isin(EVENT_KINDS)]["date"]).dt.date)
    s["event_before_expiry"] = [any(d < e <= x for e in days) or (d in days and r < "12:00")
                                for d, x, r in zip(s["date"], pd.to_datetime(s["expiry"]).dt.date, s["snap"])]
    excess = w1 - s["fwd_var"].clip(lower=0) * s["t"]
    s["event_var"] = np.where(s["event_before_expiry"], excess, np.nan)
    s["event_move"] = np.sqrt(np.clip(s["event_var"], 0, None))
    return s[["date", "snap", "known_at", "expiry", "fwd_vol", "event_before_expiry", "event_var", "event_move"]]


@register(Spec(id="vol_of_vol", version=1, family="surface", grid="daily", inputs=("atm_snaps", "vix"), params={"window": 20}, tickets=("SCRUM-148",),
               doc="A10: standard deviation over 20 sessions of daily changes in log iv7 (15:15) and in log India VIX (close)."))
def vol_of_vol(ctx: Context, p: dict) -> pd.DataFrame:
    s = _close_snap(ctx)
    out = pd.DataFrame()
    if len(s):
        out = s[["date", "known_at", "iv7"]].copy()
        out["vov_iv7"] = np.log(out["iv7"]).diff().rolling(p["window"], min_periods=p["window"] // 2).std()
    v = ctx.get("vix")
    if v is not None and len(v):
        d = v.assign(date=v["ts"].dt.date).groupby("date", as_index=False)["close"].last()
        d["vov_vix"] = np.log(d["close"]).diff().rolling(p["window"], min_periods=p["window"] // 2).std()
        d["known_at_v"] = pd.to_datetime(d["date"]) + pd.Timedelta(hours=10)
        if len(out):
            out = out.merge(d[["date", "vov_vix"]], on="date", how="left")
        else:
            out = d.rename(columns={"known_at_v": "known_at"})[["date", "known_at", "vov_vix"]]
    return out


@register(Spec(id="skew", version=2, family="surface", grid="snap", inputs=("atm_snaps",), tickets=("SCRUM-148", "SCRUM-198"),
               params={"halflife_min": 5},
               doc="A11: IV 2 % below the forward minus IV 2 % above (and at 1 %), front expiry, and the change since the previous session.  "
                   "v2 adds skew1_s and skew2_s: an exponential average over the session (half-life 5 minutes, on event time, gaps carried) "
                   "so one wing strike that was stale and prints again does not make the skew jump."))
def skew(ctx: Context, p: dict) -> pd.DataFrame:
    s = _snaps(ctx)
    if not len(s):
        return s
    s = s.copy()
    s["skew2"] = s["iv_m2"] - s["iv_p2"]
    s["skew1"] = s["iv_m1"] - s["iv_p1"]
    s = s.sort_values(["snap", "date"])
    s["d_skew1"] = s.groupby("snap")["skew1"].diff()
    s["d_skew2"] = s.groupby("snap")["skew2"].diff()
    s = s.sort_values("known_at")
    hl = pd.Timedelta(minutes=p["halflife_min"])
    for c in ("skew1", "skew2"):
        s[f"{c}_s"] = np.nan
        if ctx.snaps != "all":                  # a per-bar average: not defined on three snapshots a day
            continue
        for _, g in s.groupby(["date", "expiry"], sort=False):
            s.loc[g.index, f"{c}_s"] = g[c].ewm(halflife=hl, times=pd.to_datetime(g["known_at"]), ignore_na=True).mean().to_numpy()
    return s[["date", "snap", "known_at", "expiry", "skew1", "skew2", "d_skew1", "d_skew2", "skew1_s", "skew2_s"]].sort_values(["snap", "date"])


@register(Spec(id="curvature", version=1, family="surface", grid="snap", inputs=("atm_snaps",), tickets=("SCRUM-148",),
               doc="A13: mean of the wing IVs minus ATM IV, at 2 % and 1 % from the forward, front expiry."))
def curvature(ctx: Context, p: dict) -> pd.DataFrame:
    s = _snaps(ctx)
    if not len(s):
        return s
    s = s.copy()
    s["curv2"] = (s["iv_m2"] + s["iv_p2"]) / 2 - s["atm_iv"]
    s["curv1"] = (s["iv_m1"] + s["iv_p1"]) / 2 - s["atm_iv"]
    return s[["date", "snap", "known_at", "expiry", "curv1", "curv2"]]


@register(Spec(id="surface_shocks", version=1, family="surface", grid="daily", inputs=("atm_snaps",), params={"window": 60}, tickets=("SCRUM-148",),
               doc="A14: daily changes of level (iv7), slope (1 % skew) and curvature (1 %) at 15:15, as z-scores against the previous 60."))
def surface_shocks(ctx: Context, p: dict) -> pd.DataFrame:
    s = _close_snap(ctx)
    if not len(s):
        return s
    s = s.sort_values("date").copy()
    s["level"] = s["iv7"]
    s["slope"] = s["iv_m1"] - s["iv_p1"]
    s["curv"] = (s["iv_m1"] + s["iv_p1"]) / 2 - s["atm_iv"]
    for c in ("level", "slope", "curv"):
        s[f"d_{c}"] = s[c].diff()
        s[f"z_{c}"] = trailing_z(s[f"d_{c}"], p["window"], min_periods=20)
    return s[["date", "known_at", "d_level", "d_slope", "d_curv", "z_level", "z_slope", "z_curv"]]


@register(Spec(id="skew_stickiness", version=1, family="surface", grid="daily", inputs=("atm", "spot"), tickets=("SCRUM-148",),
               doc="A15: (d ATM IV / d ln S) / (d IV / d ln K) per session; near 0 sticky strike, near -1 sticky moneyness."))
def skew_stickiness(ctx: Context, p: dict) -> pd.DataFrame:
    g = _five_min(ctx)
    a = ctx.get("atm")
    if g.empty or a is None or not len(a):
        return pd.DataFrame()
    a = a.assign(date=pd.to_datetime(a["ts"]).dt.date)
    a["smile_slope"] = (a["iv_p1"] - a["iv_m1"]) / (np.log(1.01) - np.log(0.99))
    sl = a.groupby("date")["smile_slope"].median()
    rows = []
    for d, x in g.groupby("date"):
        b = _slope(x["div"], x["r"])
        ss = sl.get(d, np.nan)
        rows.append({"date": d, "known_at": pd.Timestamp(d) + pd.Timedelta(hours=10), "atm_beta": b, "smile_slope": ss,
                     "ssr": b / ss if ss and not np.isnan(ss) and abs(ss) > 0.05 else np.nan})
    return pd.DataFrame(rows)


@register(Spec(id="cp_iv_gap", version=1, family="surface", grid="snap", inputs=("chain", "fut_front"), params={"max_k": 0.01}, tickets=("SCRUM-148",),
               doc="A16: median call minus put IV at the same strike within 1 % of the forward (front expiry), and the parity forward "
                   "against the front future carried to the option's expiry, in basis points."))
def cp_iv_gap(ctx: Context, p: dict) -> pd.DataFrame:
    ch = ctx.get("chain")
    s = _snaps(ctx)
    if ch is None or not len(ch) or not len(s):
        return pd.DataFrame()
    want = set(pd.to_datetime(s["known_at"]) - pd.Timedelta(minutes=1))
    c = ch[ch["ts"].isin(want)]
    first = c.groupby("ts")["expiry"].transform("min")
    c = c[(c["expiry"] == first) & (c["k"].abs() <= p["max_k"])]
    piv = c.pivot_table(index=["ts", "strike"], columns="cp", values="iv")
    if not {"C", "P"} <= set(piv.columns):
        return pd.DataFrame()
    gap = (piv["C"] - piv["P"]).groupby(level="ts").median().rename("cp_gap")
    fw = c.groupby("ts").agg(fwd=("fwd", "first"), t=("t", "first"), src=("fwd_src", "first"))
    f = ctx.get("fut_front")
    out = fw.join(gap)
    if f is not None and len(f):
        ff = f.set_index("ts")[["close", "t_fut"]]
        out = out.join(ff)
        out["fut_fwd"] = out["close"] * np.exp(-ctx.r * (out["t_fut"] - out["t"]))
        out["basis_bp"] = np.where(out["src"] == "parity", (out["fwd"] / out["fut_fwd"] - 1) * 1e4, np.nan)
    out = out.reset_index()
    out["known_at"] = out["ts"] + pd.Timedelta(minutes=1)
    out = out.merge(s[["date", "snap", "known_at"]], on="known_at")
    return out[["date", "snap", "known_at", "cp_gap"] + [c for c in ("basis_bp",) if c in out.columns]]


@register(Spec(id="vrp_tilt", version=1, family="surface", grid="daily", inputs=("vrp",), params={"window": 20}, tickets=("SCRUM-148",),
               doc="A17: 20-session mean of the 15:15 vrp_diff (iv7 minus forecast realised volatility), a 1 to 3 month index view."))
def vrp_tilt(ctx: Context, p: dict) -> pd.DataFrame:
    v = ctx.signal("vrp")
    if not len(v):
        return v
    v = v[v["snap"] == "15:15"].sort_values("date").copy()
    v["tilt"] = v["vrp_diff"].rolling(p["window"], min_periods=p["window"] // 2).mean()
    return v[["date", "known_at", "vrp_diff", "tilt"]]


@register(Spec(id="iv_gap", version=1, family="surface", grid="daily", inputs=("atm_snaps", "daily"), tickets=("SCRUM-148",),
               doc="v1 A7: iv7 at 09:20 minus iv7 at the previous session's 15:15, with the index's overnight gap."))
def iv_gap(ctx: Context, p: dict) -> pd.DataFrame:
    s = _snaps(ctx)
    d = ctx.get("daily")
    if not len(s) or d is None or not len(d):
        return pd.DataFrame()
    m = s[s["snap"] == "09:20"][["date", "known_at", "iv7"]].rename(columns={"iv7": "iv7_open"}).sort_values("known_at")
    c = s[s["snap"] == "15:15"][["known_at", "iv7"]].rename(columns={"iv7": "iv7_prev_close", "known_at": "prev_at"}).sort_values("prev_at")
    m = pd.merge_asof(m, c, left_on="known_at", right_on="prev_at", direction="backward", allow_exact_matches=False)
    m = m.dropna(subset=["iv7_prev_close"]).drop(columns=["prev_at"])
    m = m.merge(d[["date", "r_on"]], on="date", how="left")
    m["iv_gap"] = m["iv7_open"] - m["iv7_prev_close"]
    return m
