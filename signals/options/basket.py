"""
Basket fair value and the expiry-day manipulation detector (SCRUM-191).

SEBI's interim order of 3 July 2025 against Jane Street describes, on BANKNIFTY expiry days: aggressive buying of the index's constituent
stocks (and stock futures) in the morning that lifts the index, a much larger bearish option position built at the same time, and selling
of the constituents into the close.  These signals rebuild that picture from what we hold: constituent 1-minute bars and dated weights
(SCRUM-183), the index, its future, the option chain.

  basket_fair_value        per minute: the index rebuilt from its constituents, I_prev_close x sum_i w_i P_i,t / P_i,prev_close
                           (weights from the latest snapshot before the day, renormalised over the members present), against the index,
                           the future carried to spot and the option-implied spot (parity forward), in basis points; coverage = weight
                           present.  The gap between the basket and the index or future is the index-arbitrage signal desks trade.
  constituent_flow         per session and hour (IST): weighted signed turnover of the constituents (tick rule on 1-minute bars), as a
                           net buy share in [-1, 1], and the hour's share of the session's constituent turnover
  option_positioning_hourly per session and hour: change in near-money put and call OI (front expiry) and BVC-signed option volume x delta
  expiry_inverted_v        per session: index return from the open to 12:15 IST and from 12:15 to the close, each over the trailing daily
                           volatility; the morning share of constituent turnover against its trailing norm; expiry flag
  manipulation_flag        per session: the pattern on an expiry day - a morning push and an afternoon reversal each beyond
                           PUSH_Z, constituents bought into the push and sold into the reversal, and option positioning built against the
                           push (put OI up while the index is pushed up, or calls while it is pushed down); fixed thresholds
Use: a risk flag (do not hold short gamma into such an expiry) and, through the basket gap, a trade signal.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from analytics import black76
from signals.framework import Context, Spec, register
from signals.options.core import minute_of_session

MORNING_MIN = 180            # 09:15 to 12:15 IST
PUSH_Z = 1.0                 # morning and afternoon moves each beyond 1 trailing daily sigma
FLOW_MIN = 0.05              # constituent net buy share beyond 5 %
TICKETS = ("SCRUM-191",)


def _weights_for(ctx: Context) -> pd.DataFrame:
    w = ctx.raw.get("weights")
    return w if w is not None else pd.DataFrame(columns=["date", "symbol", "weight", "known_at"])


def _stocks(ctx: Context) -> pd.DataFrame:
    s = ctx.raw.get("stocks_1m")
    if s is None or not len(s):
        return pd.DataFrame(columns=["ts", "symbol", "close", "volume", "date"])
    s = s[["ts", "symbol", "close", "volume"]].copy()
    s["ts"] = pd.to_datetime(s["ts"])
    s["date"] = s["ts"].dt.date
    return s.sort_values(["symbol", "ts"])


def weights_asof(weights: pd.DataFrame, day) -> pd.Series:
    """{symbol: weight} from the latest snapshot dated before `day` (point in time: a snapshot is known at its close)."""
    if weights is None or not len(weights):
        return pd.Series(dtype=float)
    w = weights[pd.to_datetime(weights["date"]) < pd.Timestamp(day)]
    if not len(w):
        return pd.Series(dtype=float)
    last = w[w["date"] == w["date"].max()]
    return last.groupby("symbol")["weight"].sum()


@register(Spec(id="basket_fair_value", version=1, family="basket", grid="1m", inputs=("stocks_1m", "weights", "spot", "fut_front", "atm"),
               tickets=TICKETS, doc="Index rebuilt each minute from its weighted constituents, against the index, the future and the "
                                     "option-implied spot, in basis points."))
def basket_fair_value(ctx: Context, p: dict) -> pd.DataFrame:
    st = _stocks(ctx)
    sp = ctx.get("spot")
    wts = _weights_for(ctx)
    if st.empty or not len(sp) or not len(wts):
        return pd.DataFrame()
    prev_close = st.groupby(["symbol", "date"])["close"].last().groupby(level=0).shift(1).rename("prev")
    st = st.join(prev_close, on=["symbol", "date"])
    idx = sp.assign(date=sp["ts"].dt.date)
    idx_prev = idx.groupby("date")["close"].last().shift(1)
    rows = []
    for d, g in st.groupby("date"):
        w = weights_asof(wts, d)
        if w.empty or pd.isna(idx_prev.get(d)):
            continue
        g = g[g["symbol"].isin(w.index) & g["prev"].notna()]
        if g.empty:
            continue
        g = g.assign(w=g["symbol"].map(w))
        g = g.assign(wrel=g["w"] * g["close"] / g["prev"])
        agg = g.groupby("ts").agg(wrel=("wrel", "sum"), wsum=("w", "sum"))
        agg["ratio"] = agg["wrel"] / agg["wsum"]
        agg["coverage"] = agg["wsum"] / w.sum()
        agg = agg.drop(columns=["wrel", "wsum"])
        agg["basket"] = idx_prev[d] * agg["ratio"]
        rows.append(agg.reset_index())
    if not rows:
        return pd.DataFrame()
    out = pd.concat(rows, ignore_index=True).merge(sp[["ts", "close"]].rename(columns={"close": "index"}), on="ts", how="inner")
    out["gap_index_bp"] = (out["basket"] / out["index"] - 1) * 1e4
    f = ctx.get("fut_front")
    if f is not None and len(f):
        ff = f[["ts", "close", "t_fut"]].copy()
        ff["fut_spot"] = ff["close"] * np.exp(-ctx.r * ff["t_fut"])
        out = out.merge(ff[["ts", "fut_spot"]], on="ts", how="left")
        out["gap_future_bp"] = (out["fut_spot"] / out["basket"] - 1) * 1e4
    a = ctx.get("atm")
    if a is not None and len(a):
        aa = a[["ts", "fwd", "t"]].copy()
        aa["opt_spot"] = aa["fwd"] * np.exp(-ctx.r * aa["t"])
        out = out.merge(aa[["ts", "opt_spot"]], on="ts", how="left")
        out["gap_options_bp"] = (out["opt_spot"] / out["basket"] - 1) * 1e4
    out["known_at"] = out["ts"] + pd.Timedelta(minutes=1)
    return out.drop(columns=["ratio"])


def _ist_hour(ts: pd.Series) -> pd.Series:
    m = minute_of_session(ts)
    return (m // 60).clip(upper=6)                     # 0 = 09:15-10:15 ... 6 = 15:15-15:30


@register(Spec(id="constituent_flow", version=1, family="basket", grid="daily", inputs=("stocks_1m", "weights"), tickets=TICKETS,
               doc="Per session and hour: weighted signed constituent turnover (tick rule) as a net buy share, and turnover against the same hour's norm."))
def constituent_flow(ctx: Context, p: dict) -> pd.DataFrame:
    st = _stocks(ctx)
    wts = _weights_for(ctx)
    if st.empty or not len(wts):
        return pd.DataFrame()
    st["dp"] = st.groupby(["symbol", "date"])["close"].diff()
    st["sign"] = np.sign(st["dp"]).replace(0, np.nan)
    st["sign"] = st.groupby(["symbol", "date"])["sign"].ffill().fillna(0)
    st["hour"] = _ist_hour(st["ts"])
    rows = []
    for d, g in st.groupby("date"):
        w = weights_asof(wts, d)
        if w.empty:
            continue
        g = g[g["symbol"].isin(w.index)]
        if g.empty:
            continue
        val = g["close"] * g["volume"] * g["symbol"].map(w)
        g = g.assign(val=val, sval=val * g["sign"])
        h = g.groupby("hour").agg(turnover=("val", "sum"), signed=("sval", "sum")).reset_index()
        h["net_buy_share"] = h["signed"] / h["turnover"].replace(0, np.nan)
        h["date"] = d
        rows.append(h)
    if not rows:
        return pd.DataFrame()
    out = pd.concat(rows, ignore_index=True).sort_values(["hour", "date"])
    norm = out.groupby("hour")["turnover"].transform(lambda x: x.shift(1).rolling(20, min_periods=5).mean())
    out["turnover_vs_norm"] = out["turnover"] / norm                    # this hour's turnover against the same hour of the last 20 sessions
    out = out.sort_values(["date", "hour"])
    out["known_at"] = pd.to_datetime(out["date"]) + pd.Timedelta(hours=3, minutes=45) + pd.to_timedelta((out["hour"] + 1) * 60, unit="min")
    out["known_at"] = out["known_at"].clip(upper=pd.to_datetime(out["date"]) + pd.Timedelta(hours=10))
    return out[["date", "known_at", "hour", "turnover", "net_buy_share", "turnover_vs_norm"]]


@register(Spec(id="option_positioning_hourly", version=1, family="basket", grid="daily", inputs=("chain",), params={"max_k": 0.03},
               tickets=TICKETS, doc="Per session and hour: change in near-money put and call OI (front expiry) and BVC-signed option "
                                     "volume x delta x lot."))
def option_positioning_hourly(ctx: Context, p: dict) -> pd.DataFrame:
    ch = ctx.get("chain")
    if ch is None or not len(ch):
        return pd.DataFrame()
    c = ch[ch["k"].abs() <= p["max_k"]].copy()
    first = c.groupby("ts")["expiry"].transform("min")
    c = c[c["expiry"] == first].sort_values("ts")
    c["date"] = c["ts"].dt.date
    c["hour"] = _ist_hour(c["ts"])
    key = ["date", "expiry", "strike", "cp"]
    c["d_oi"] = c.groupby(key)["oi"].diff().fillna(0)
    dp = c.groupby(key)["close"].diff()
    sd = dp.groupby([c[k] for k in key]).transform(lambda x: x.expanding(min_periods=5).std()).replace(0, np.nan)   # only minutes so far
    c["signed"] = c["volume"] * (2 * black76.ncdf((dp / sd).fillna(0).to_numpy()) - 1)
    c["delta_flow"] = c["signed"] * c["delta"].fillna(0) * c["lot"].fillna(1)
    g = c.groupby(["date", "hour", "cp"]).agg(d_oi=("d_oi", "sum"), delta_flow=("delta_flow", "sum")).unstack("cp").fillna(0)
    g.columns = [f"{a}_{'calls' if b == 'C' else 'puts'}" for a, b in g.columns]
    g = g.reset_index()
    for col in ("d_oi_calls", "d_oi_puts", "delta_flow_calls", "delta_flow_puts"):
        if col not in g.columns:
            g[col] = 0.0
    g["delta_flow"] = g["delta_flow_calls"] + g["delta_flow_puts"]
    g["known_at"] = (pd.to_datetime(g["date"]) + pd.Timedelta(hours=3, minutes=45) + pd.to_timedelta((g["hour"] + 1) * 60, unit="min")).clip(
        upper=pd.to_datetime(g["date"]) + pd.Timedelta(hours=10))
    return g


@register(Spec(id="expiry_inverted_v", version=1, family="basket", grid="daily", inputs=("spot", "stocks_1m", "eras"), params={"window": 20},
               tickets=TICKETS, doc="Per session: open to 12:15 and 12:15 to close index returns over trailing daily volatility, and the "
                                     "morning share of constituent turnover against its norm; expiry flag."))
def expiry_inverted_v(ctx: Context, p: dict) -> pd.DataFrame:
    sp = ctx.get("spot")
    if not len(sp):
        return pd.DataFrame()
    m = sp.assign(date=sp["ts"].dt.date, minute=minute_of_session(sp["ts"]))
    g = m.groupby("date")
    d = pd.DataFrame({"open": g["close"].first(), "close": g["close"].last(),
                      "mid": m[m["minute"] == MORNING_MIN - 1].groupby("date")["close"].last()}).dropna().reset_index()
    d["r_am"] = np.log(d["mid"] / d["open"])
    d["r_pm"] = np.log(d["close"] / d["mid"])
    d["sigma"] = np.log(d["close"]).diff().shift(1).rolling(p["window"], min_periods=10).std()
    d["am_z"], d["pm_z"] = d["r_am"] / d["sigma"], d["r_pm"] / d["sigma"]
    d["reversal"] = np.where(np.sign(d["r_am"]) != np.sign(d["r_pm"]), -d["am_z"] * d["pm_z"], 0.0)
    st = _stocks(ctx)
    if len(st):
        st["minute"] = minute_of_session(st["ts"])
        val = st["close"] * st["volume"]
        am = val[st["minute"] < MORNING_MIN].groupby(st.loc[st["minute"] < MORNING_MIN, "date"]).sum()
        tot = val.groupby(st["date"]).sum()
        share = (am / tot).rename("am_turnover_share")
        d = d.merge(share, left_on="date", right_index=True, how="left")
        d["am_share_vs_norm"] = d["am_turnover_share"] / d["am_turnover_share"].shift(1).rolling(p["window"], min_periods=5).mean()
    er = ctx.get("eras")
    if er is not None and len(er):
        d = d.merge(er[["date", "is_expiry", "era"]], on="date", how="left")
    d["known_at"] = pd.to_datetime(d["date"]) + pd.Timedelta(hours=10)
    return d.drop(columns=["open", "close", "mid"])


@register(Spec(id="manipulation_flag", version=1, family="basket", grid="daily",
               inputs=("expiry_inverted_v", "constituent_flow", "option_positioning_hourly"),
               params={"push_z": PUSH_Z, "flow_min": FLOW_MIN}, tickets=TICKETS,
               doc="Expiry-day pattern of SEBI's 2025 order: morning push and afternoon reversal beyond 1 sigma, constituents bought into "
                   "the push and sold into the reversal, option positioning built against the push."))
def manipulation_flag(ctx: Context, p: dict) -> pd.DataFrame:
    v = ctx.signal("expiry_inverted_v")
    if not len(v):
        return pd.DataFrame()
    out = v[["date", "known_at", "am_z", "pm_z", "reversal"] + [c for c in ("is_expiry", "era") if c in v.columns]].copy()
    cf = ctx.signal("constituent_flow")
    if len(cf):
        w = cf.assign(part=np.where(cf["hour"] < MORNING_MIN // 60, "am", "pm"))
        w = w.assign(sv=w["net_buy_share"] * w["turnover"])
        agg = w.groupby(["date", "part"]).apply(lambda x: x["sv"].sum() / x["turnover"].sum(), include_groups=False).unstack("part")
        agg.columns = [f"flow_{c}" for c in agg.columns]
        out = out.merge(agg.reset_index(), on="date", how="left")
    op = ctx.signal("option_positioning_hourly")
    if len(op):
        am = op[op["hour"] < MORNING_MIN // 60].groupby("date")[["d_oi_puts", "d_oi_calls"]].sum()
        am.columns = ["am_put_oi", "am_call_oi"]
        out = out.merge(am.reset_index(), on="date", how="left")
    push = out["am_z"].abs() > p["push_z"]
    rev = (np.sign(out["am_z"]) != np.sign(out["pm_z"])) & (out["pm_z"].abs() > p["push_z"])
    up = np.sign(out["am_z"])
    flow_ok = pd.Series(False, index=out.index)
    if {"flow_am", "flow_pm"} <= set(out.columns):
        flow_ok = (up * out["flow_am"] > p["flow_min"]) & (-up * out["flow_pm"] > p["flow_min"])
    opt_ok = pd.Series(False, index=out.index)
    if {"am_put_oi", "am_call_oi"} <= set(out.columns):
        opt_ok = np.where(up > 0, out["am_put_oi"] > out["am_call_oi"], out["am_call_oi"] > out["am_put_oi"])
        opt_ok = pd.Series(opt_ok, index=out.index)
    expiry = out["is_expiry"].fillna(False).astype(bool) if "is_expiry" in out.columns else pd.Series(True, index=out.index)
    out["n_conditions"] = push.astype(int) + rev.astype(int) + flow_ok.astype(int) + opt_ok.astype(int)
    out["flag"] = expiry & push & rev & flow_ok & opt_ok
    return out
