"""
The direction layer on the index future (SCRUM-180).  These say which way; the volatility gate says whether to use options for it.

  last30_momentum   B6  the return from the previous close to 09:45 IST and to 15:00 IST, known at 15:00, as predictors of the last 30
                        minutes (Gao, Han, Li and Zhou 2018; Baltussen et al. 2021); expiry days flagged
  last30_outcome    label   the 15:00 to 15:30 return
  noise_band        B7  the day's open (or the previous close, whichever is further) widened by the average absolute move from the open
                        at that minute over the previous 14 sessions; +1 above the band and above VWAP, -1 below both (Zarattini, Aziz,
                        Barbon 2024)
  future_lead       C9  per session: correlation of the index's 1-minute return with the future's return one minute earlier, and the
                        future's basis to the index (annualised carry)
Uses the continuous front future (`fut_front`); without futures, the index.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from signals.framework import Context, Spec, register
from signals.options.core import minute_of_session

BAR_0945 = pd.Timedelta(hours=4, minutes=14)     # the bar ending 09:45 IST
BAR_1500 = pd.Timedelta(hours=9, minutes=29)     # the bar ending 15:00 IST
BAR_1529 = pd.Timedelta(hours=9, minutes=59)


def _px(ctx: Context) -> pd.DataFrame:
    f = ctx.get("fut_front")
    if f is not None and len(f):
        out = f[["ts", "close", "volume"]].copy()
    else:
        sp = ctx.get("spot")
        out = sp[["ts", "close"]].assign(volume=1.0)
    out["date"] = out["ts"].dt.date
    return out


def _eras(ctx: Context) -> pd.DataFrame:
    er = ctx.get("eras")
    return er[["date", "is_expiry", "era"]] if er is not None and len(er) else pd.DataFrame(columns=["date", "is_expiry", "era"])


@register(Spec(id="last30_momentum", version=1, family="direction", grid="daily", inputs=("fut_front", "eras"), tickets=("SCRUM-180",),
               doc="B6: return from the previous close to 09:45 and to 15:00 IST on the front future, known at 15:00."))
def last30_momentum(ctx: Context, p: dict) -> pd.DataFrame:
    px = _px(ctx)
    if px.empty:
        return pd.DataFrame()
    closes = px.groupby("date")["close"].last()
    rows = []
    days = sorted(px["date"].unique())
    by_ts = px.set_index("ts")["close"]
    for prev, d in zip(days[:-1], days[1:]):
        base = pd.Timestamp(d)
        a, b = by_ts.get(base + BAR_0945), by_ts.get(base + BAR_1500)
        if a is None or b is None:
            continue
        pc = closes[prev]
        rows.append({"date": d, "known_at": base + BAR_1500 + pd.Timedelta(minutes=1), "r_first30": np.log(a / pc), "r_to_1500": np.log(b / pc)})
    out = pd.DataFrame(rows)
    if len(out):
        out["signal"] = np.sign(out["r_to_1500"])
        out = out.merge(_eras(ctx), on="date", how="left")
    return out


@register(Spec(id="last30_outcome", version=1, family="direction", grid="daily", kind="label", inputs=("fut_front",), tickets=("SCRUM-180",),
               doc="Label for B6: the 15:00 to 15:29 IST return of the front future."))
def last30_outcome(ctx: Context, p: dict) -> pd.DataFrame:
    px = _px(ctx)
    if px.empty:
        return pd.DataFrame()
    by_ts = px.set_index("ts")["close"]
    rows = []
    for d in sorted(px["date"].unique()):
        base = pd.Timestamp(d)
        a, b = by_ts.get(base + BAR_1500), by_ts.get(base + BAR_1529)
        if a is not None and b is not None:
            rows.append({"date": d, "known_at": base + BAR_1529 + pd.Timedelta(minutes=1), "r_last30": np.log(b / a)})
    return pd.DataFrame(rows)


@register(Spec(id="noise_band", version=1, family="direction", grid="1m", inputs=("fut_front",), params={"lookback": 14}, tickets=("SCRUM-180",),
               doc="B7: breakout of a band around the open sized by the average absolute move from the open at that minute over the past "
                   "14 sessions, confirmed by VWAP."))
def noise_band(ctx: Context, p: dict) -> pd.DataFrame:
    px = _px(ctx)
    if px.empty:
        return pd.DataFrame()
    px = px.sort_values("ts").copy()
    px["minute"] = minute_of_session(px["ts"])
    px["open"] = px.groupby("date")["close"].transform("first")
    px["move"] = (px["close"] / px["open"] - 1).abs()
    piv = px.pivot_table(index="date", columns="minute", values="move")
    sigma = piv.shift(1).rolling(p["lookback"], min_periods=p["lookback"]).mean()
    sig_long = sigma.stack().rename("sigma").reset_index()
    px = px.merge(sig_long, on=["date", "minute"], how="left")
    prev_close = px.groupby("date")["close"].last().shift(1)
    px["prev_close"] = px["date"].map(prev_close)
    px["upper"] = np.maximum(px["open"], px["prev_close"]) * (1 + px["sigma"])
    px["lower"] = np.minimum(px["open"], px["prev_close"]) * (1 - px["sigma"])
    v = px["volume"].fillna(0).clip(lower=0) + 1e-9
    px["vwap"] = (px["close"] * v).groupby(px["date"]).cumsum() / v.groupby(px["date"]).cumsum()
    up = (px["close"] > px["upper"]) & (px["close"] > px["vwap"])
    dn = (px["close"] < px["lower"]) & (px["close"] < px["vwap"])
    px["signal"] = np.where(up, 1, np.where(dn, -1, 0))
    px.loc[px["sigma"].isna(), "signal"] = np.nan
    px["known_at"] = px["ts"] + pd.Timedelta(minutes=1)
    return px[["ts", "known_at", "close", "open", "upper", "lower", "vwap", "sigma", "signal"]]


@register(Spec(id="future_lead", version=1, family="direction", grid="daily", inputs=("fut_front", "spot"), tickets=("SCRUM-180",),
               doc="C9: per session, correlation of the index's 1-minute return with the future's return a minute earlier (and in the same "
                   "minute), and the future's annualised carry to the index at the close."))
def future_lead(ctx: Context, p: dict) -> pd.DataFrame:
    f = ctx.get("fut_front")
    sp = ctx.get("spot")
    if f is None or not len(f) or not len(sp):
        return pd.DataFrame()
    m = sp[["ts", "close"]].merge(f[["ts", "close", "t_fut"]], on="ts", suffixes=("_i", "_f")).sort_values("ts")
    m["date"] = m["ts"].dt.date
    m["ri"] = np.log(m["close_i"]).groupby(m["date"]).diff()
    m["rf"] = np.log(m["close_f"]).groupby(m["date"]).diff()
    m["rf_lag"] = m.groupby("date")["rf"].shift(1)
    rows = []
    for d, g in m.groupby("date"):
        g = g.dropna(subset=["ri", "rf", "rf_lag"])
        if len(g) < 30:
            continue
        last = g.iloc[-1]
        rows.append({"date": d, "known_at": pd.Timestamp(d) + pd.Timedelta(hours=10), "corr_lag1": g["ri"].corr(g["rf_lag"]),
                     "corr_same": g["ri"].corr(g["rf"]), "carry": float(np.log(last["close_f"] / last["close_i"]) / last["t_fut"])})
    return pd.DataFrame(rows)
