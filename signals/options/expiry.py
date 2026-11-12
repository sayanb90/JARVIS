"""
Expiry-day and short-dated straddle signals (SCRUM-179).

  fair_straddle         A5   on the last two sessions before an expiry, each minute: the Black-76 value of the front ATM straddle at the
                             volatility the remaining variance implies (time-of-day share still to come today x the HAR forecast of the
                             session's intraday variance, plus a night and a full session when expiry is tomorrow) against its price
  expiry_last_hour      B8   per session: the share of the session's variance in the last 60 minutes; trailing means on expiry and other days
  pin                   B1   expiry-day minutes: the near-money strike with the most open interest (calls plus puts) and the one with the
                             most gamma x OI, and the forward's distance from each
  pin_test (study)      B1   per session at 15:29 IST: distance of the index from the max-OI strike seen at 13:30, expiry and placebo days
  gamma_dampening (study) B9 expiry-day hours: near-money gamma exposure at the start of the hour, realised variance of that hour and the
                             hour before
  vix_expiry_dip        B10  India VIX change on expiry days against other days (trailing), and VIX net of the usual expiry dip
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from analytics import black76
from signals.framework import Context, Spec, register
from signals.options.core import era_of, minute_of_session

SESSION_MINUTES = 375


def _intraday_var_fc(ctx: Context) -> pd.DataFrame:
    """Per session: the HAR forecast of the next session's intraday variance (made at this close) and the trailing mean overnight variance."""
    d = ctx.get("daily")
    if d is None or not len(d):
        return pd.DataFrame()
    from analytics.realised import har_forecast
    h = har_forecast(d, horizons=(1,), col="rv").rename(columns={"har_1": "rv_fc_1"})
    out = d[["date", "known_at", "r_on"]].merge(h[["date", "rv_fc_1"]], on="date")
    out["on_var"] = (out["r_on"] ** 2).rolling(60, min_periods=20).mean()
    return out


@register(Spec(id="fair_straddle", version=1, family="expiry", grid="1m", inputs=("atm", "tod", "daily", "eras"), params={"max_dte": 1},
               tickets=("SCRUM-179",),
               doc="A5: front ATM straddle against its fair value from the remaining variance (time-of-day profile x HAR forecast)."))
def fair_straddle(ctx: Context, p: dict) -> pd.DataFrame:
    a = ctx.get("atm")
    prof = ctx.get("tod")
    fc = _intraday_var_fc(ctx)
    if a is None or not len(a) or prof is None or not len(prof) or not len(fc):
        return pd.DataFrame()
    a = a.copy()
    a["date"] = pd.to_datetime(a["ts"]).dt.date
    a["dte"] = (pd.to_datetime(a["expiry"]) - pd.to_datetime(a["date"])).dt.days
    a = a[(a["dte"] <= p["max_dte"]) & a["straddle"].notna()]
    if a.empty:
        return pd.DataFrame()
    a["minute"] = minute_of_session(a["ts"])
    a = a.merge(prof[["date", "minute", "remaining"]], on=["date", "minute"], how="inner")
    fc = fc.sort_values("known_at")
    a["_q"] = pd.to_datetime(a["ts"])
    a = pd.merge_asof(a.sort_values("_q"), fc[["known_at", "rv_fc_1", "on_var"]].rename(columns={"known_at": "fc_at"}), left_on="_q",
                      right_on="fc_at", direction="backward", allow_exact_matches=False)
    var = a["remaining"] * a["rv_fc_1"] + np.where(a["dte"] >= 1, a["on_var"].fillna(0) + a["rv_fc_1"], 0.0) * (a["dte"] >= 1)
    a["var_left"] = var
    sig = np.sqrt(np.clip(var, 0, None) / a["t"])
    is_c = np.ones(len(a), dtype=bool)
    a["fair"] = (black76.price(is_c, a["fwd"], a["atm_strike"], a["t"], sig, ctx.r) +
                 black76.price(~is_c, a["fwd"], a["atm_strike"], a["t"], sig, ctx.r))
    a["rich"] = a["straddle"] / a["fair"] - 1
    a["known_at"] = pd.to_datetime(a["ts"]) + pd.Timedelta(minutes=1)
    return a[["ts", "known_at", "expiry", "dte", "minute", "straddle", "fair", "rich", "var_left"]]


@register(Spec(id="expiry_last_hour", version=1, family="expiry", grid="daily", inputs=("spot", "eras"), params={"n_exp": 12, "n_other": 60},
               tickets=("SCRUM-179",),
               doc="B8: share of the session's variance in the last 60 minutes; trailing means on expiry days and other days, and their ratio."))
def expiry_last_hour(ctx: Context, p: dict) -> pd.DataFrame:
    sp = ctx.get("spot")
    er = ctx.get("eras")
    if not len(sp) or not len(er):
        return pd.DataFrame()
    m = sp.copy()
    m["date"] = m["ts"].dt.date
    m["r2"] = np.log(m["close"]).groupby(m["date"]).diff() ** 2
    m["minute"] = minute_of_session(m["ts"])
    g = m.groupby("date")
    d = pd.DataFrame({"rv": g["r2"].sum(), "rv_last60": m[m["minute"] >= SESSION_MINUTES - 60].groupby("date")["r2"].sum()}).reset_index()
    d["share_last60"] = d["rv_last60"] / d["rv"]
    d = d.merge(er[["date", "is_expiry", "era"]], on="date", how="left")
    d["known_at"] = pd.to_datetime(d["date"]) + pd.Timedelta(hours=10)
    exp_mean = d["share_last60"].where(d["is_expiry"] == True)  # noqa: E712
    oth_mean = d["share_last60"].where(d["is_expiry"] != True)  # noqa: E712
    d["mean_expiry"] = exp_mean.rolling(len(d), min_periods=1).apply(lambda x: np.nanmean(x[~np.isnan(x)][-p["n_exp"]:]) if (~np.isnan(x)).any() else np.nan, raw=True)
    d["mean_other"] = oth_mean.rolling(len(d), min_periods=1).apply(lambda x: np.nanmean(x[~np.isnan(x)][-p["n_other"]:]) if (~np.isnan(x)).any() else np.nan, raw=True)
    d["ratio"] = d["mean_expiry"] / d["mean_other"]
    return d[["date", "known_at", "is_expiry", "era", "share_last60", "mean_expiry", "mean_other", "ratio"]]


def _front_near(ch: pd.DataFrame, max_k: float) -> pd.DataFrame:
    ch = ch.copy()
    ch["date"] = ch["ts"].dt.date
    first = ch.groupby("ts")["expiry"].transform("min")
    return ch[(ch["expiry"] == first) & (ch["k"].abs() <= max_k)]


@register(Spec(id="pin", version=1, family="expiry", grid="1m", inputs=("chain",), params={"max_k": 0.03}, tickets=("SCRUM-179",),
               doc="B1: on expiry day, the near-money strike with the most OI (calls + puts) and the one with the most gamma x OI, and the "
                   "forward's distance from each."))
def pin(ctx: Context, p: dict) -> pd.DataFrame:
    ch = ctx.get("chain")
    if ch is None or not len(ch):
        return pd.DataFrame()
    n = _front_near(ch, p["max_k"])
    n = n[pd.to_datetime(n["expiry"]) == pd.to_datetime(n["date"])]
    if n.empty:
        return pd.DataFrame()
    n = n.assign(goi=n["gamma"].fillna(0) * n["oi"].fillna(0))
    s = n.groupby(["ts", "strike"], as_index=False).agg(oi=("oi", "sum"), goi=("goi", "sum"), fwd=("fwd", "first"))
    top_oi = s.loc[s.groupby("ts")["oi"].idxmax(), ["ts", "strike", "fwd"]].rename(columns={"strike": "pin_oi"})
    top_g = s.loc[s.groupby("ts")["goi"].idxmax(), ["ts", "strike"]].rename(columns={"strike": "pin_gamma"})
    out = top_oi.merge(top_g, on="ts")
    out["dist_oi"] = (out["fwd"] - out["pin_oi"]) / out["fwd"]
    out["dist_gamma"] = (out["fwd"] - out["pin_gamma"]) / out["fwd"]
    out["known_at"] = pd.to_datetime(out["ts"]) + pd.Timedelta(minutes=1)
    return out


@register(Spec(id="pin_test", version=1, family="expiry", grid="daily", kind="study", inputs=("chain", "spot", "eras"),
               params={"max_k": 0.03, "at": "13:30"}, tickets=("SCRUM-179",),
               doc="B1 test: per session, |index at 15:29 - max-OI strike of the front expiry at 13:30| / index; expiry and placebo days."))
def pin_test(ctx: Context, p: dict) -> pd.DataFrame:
    ch = ctx.get("chain")
    sp = ctx.get("spot")
    er = ctx.get("eras")
    if ch is None or not len(ch) or not len(sp):
        return pd.DataFrame()
    h, mnt = (int(x) for x in p["at"].split(":"))
    n = _front_near(ch, p["max_k"])
    rows = []
    for d, g in n.groupby("date"):
        at = pd.Timestamp(d) + pd.Timedelta(hours=h, minutes=mnt) - pd.Timedelta(hours=5, minutes=30) - pd.Timedelta(minutes=1)
        snap = g[g["ts"] == at]
        close = sp[(sp["ts"].dt.date == d) & (sp["ts"] == pd.Timestamp(d) + pd.Timedelta(hours=9, minutes=59))]
        if snap.empty or close.empty:
            continue
        s = snap.groupby("strike")["oi"].sum()
        k = float(s.idxmax())
        c = float(close["close"].iloc[0])
        rows.append({"date": d, "known_at": pd.Timestamp(d) + pd.Timedelta(hours=10), "pin_strike": k, "close": c, "dist": abs(c - k) / c})
    out = pd.DataFrame(rows)
    if len(out) and len(er):
        out = out.merge(er[["date", "is_expiry", "era"]], on="date", how="left")
    return out


@register(Spec(id="gamma_dampening", version=1, family="expiry", grid="daily", kind="study", inputs=("chain", "spot", "eras"),
               params={"max_k": 0.01, "hours_ist": ("10:15", "11:15", "12:15", "13:15", "14:15")}, tickets=("SCRUM-179",),
               doc="B9: expiry and other days, each hour: near-money gamma exposure (front expiry, US dealer convention: long calls, short "
                   "puts) at the start, and realised variance of the hour before and the hour after."))
def gamma_dampening(ctx: Context, p: dict) -> pd.DataFrame:
    ch = ctx.get("chain")
    sp = ctx.get("spot")
    er = ctx.get("eras")
    if ch is None or not len(ch) or not len(sp):
        return pd.DataFrame()
    n = _front_near(ch, p["max_k"])
    sign = np.where(n["cp"] == "C", 1.0, -1.0)
    n = n.assign(gex=sign * n["gamma"].fillna(0) * n["oi"].fillna(0) * n["lot"].fillna(1) * n["fwd"] ** 2 * 0.01)
    gex = n.groupby("ts")["gex"].sum()
    r = sp.assign(r2=np.log(sp["close"]).groupby(sp["ts"].dt.date).diff() ** 2).set_index("ts")["r2"]
    rows = []
    for d in sorted(n["date"].unique()):
        for hh in p["hours_ist"]:
            h, m = (int(x) for x in hh.split(":"))
            t0 = pd.Timestamp(d) + pd.Timedelta(hours=h, minutes=m) - pd.Timedelta(hours=5, minutes=30)
            g = gex.get(t0 - pd.Timedelta(minutes=1))
            if g is None:
                continue
            before = r[(r.index >= t0 - pd.Timedelta(minutes=60)) & (r.index < t0)].sum()
            after = r[(r.index >= t0) & (r.index < t0 + pd.Timedelta(minutes=60))].sum()
            rows.append({"date": d, "hour": hh, "known_at": t0 + pd.Timedelta(minutes=60), "gex": float(g), "rv_before": before, "rv_after": after})
    out = pd.DataFrame(rows)
    if len(out) and len(er):
        out = out.merge(er[["date", "is_expiry", "era"]], on="date", how="left")
    return out


@register(Spec(id="vix_expiry_dip", version=1, family="expiry", grid="daily", inputs=("vix", "eras"), params={"n_exp": 12}, tickets=("SCRUM-179",),
               doc="B10: India VIX change on expiry days (trailing mean of the last 12) and VIX net of that usual dip on an expiry day."))
def vix_expiry_dip(ctx: Context, p: dict) -> pd.DataFrame:
    v = ctx.get("vix")
    er = ctx.get("eras")
    if v is None or not len(v) or not len(er):
        return pd.DataFrame()
    d = v.assign(date=v["ts"].dt.date).groupby("date", as_index=False)["close"].last().rename(columns={"close": "vix"})
    d = d.merge(er[["date", "is_expiry"]], on="date", how="left")
    d["chg"] = d["vix"].diff()
    exp = d["chg"].where(d["is_expiry"] == True)  # noqa: E712
    vals = exp.to_numpy()
    mean_prev = np.full(len(d), np.nan)
    hist: list[float] = []
    for i, x in enumerate(vals):
        mean_prev[i] = np.mean(hist[-p["n_exp"]:]) if hist else np.nan
        if not np.isnan(x):
            hist.append(float(x))
    d["dip_mean"] = mean_prev
    d["vix_ex_dip"] = np.where(d["is_expiry"] == True, d["vix"] - d["dip_mean"].fillna(0), d["vix"])  # noqa: E712
    d["known_at"] = pd.to_datetime(d["date"]) + pd.Timedelta(hours=10)
    d["era"] = d["date"].map(era_of)
    return d
