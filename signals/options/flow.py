"""
Flow proxies for what India does not publish (SCRUM-192): no customer/dealer split, no opening/closing flags, no trade-size disclosure.

  opening_flow        live, per minute over the chain: option volume classified by what it did to open interest.  Per contract-minute,
                      Lee-Ready signed volume and the OI change: OI up with net buying = opening buys, OI up with net selling = opening
                      sells (writing), OI down with net buying = closing shorts, OI down with net selling = closing longs.  Calls and puts
                      separately (the stand-in for Pan and Poteshman's open-buy volume)
  opening_flow_daily  the same classification from 1-minute candles (bulk-volume-classification sign), per session: the history version
  trade_size_flow     live, per minute: signed lots by trade-size bucket (last-trade quantity in lots: 1, 2-5, 6-20, 21+), options and
                      futures separately; small lots read as retail, large as institutional
  block_prints        live events: prints of at least BLOCK_LOTS_OPT (options) or BLOCK_LOTS_FUT (futures) lots, with the largest OI
                      change across the chain in the same minute (the strike a large trader may be building)
  implied_corr_proxy  per session: implied correlation of the index from its 7-day ATM IV and the constituents' realised volatilities
                      (20 sessions): rho = (s_I^2 - sum w^2 s^2) / ((sum w s)^2 - sum w^2 s^2); realised stands in for the constituents'
                      implied volatility until heavyweight stock options are collected (SCRUM-194).  Because index implied volatility
                      carries a variance premium that realised volatility does not, the proxy runs high (it can exceed 1): read it
                      against its own history (changes, percentiles), not as a correlation level
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from analytics import black76
from analytics.realised import SESSIONS_PER_YEAR
from signals.framework import Context, Spec, register
from signals.options.basket import _stocks, _weights_for, weights_asof
from signals.options.live import MIN_SAMPLE, lee_ready

BLOCK_LOTS_OPT = 100
BLOCK_LOTS_FUT = 50
SIZE_BUCKETS = [0, 1, 5, 20, np.inf]
SIZE_LABELS = ["1", "2_5", "6_20", "21_plus"]
TICKETS = ("SCRUM-192",)


def _live(sid, doc, grid="1m", params=None):
    return Spec(id=sid, version=1, family="flow", grid=grid, inputs=("ticks_fo",), params=params or {}, live_only=True, min_sample=MIN_SAMPLE,
                tickets=TICKETS, doc=doc)


def classify(signed: pd.Series, d_oi: pd.Series, vol: pd.Series) -> pd.Series:
    """open_buy, open_sell, close_short (OI down, net buying), close_long (OI down, net selling) or none."""
    return pd.Series(np.select([(d_oi > 0) & (signed > 0), (d_oi > 0) & (signed < 0), (d_oi < 0) & (signed > 0), (d_oi < 0) & (signed < 0)],
                               ["open_buy", "open_sell", "close_short", "close_long"], "none"), index=signed.index)


def _tabulate(m: pd.DataFrame, key: list[str]) -> pd.DataFrame:
    m = m.assign(cls=classify(m["signed"], m["d_oi"], m["vol"]), side=np.where(m["cp"].isin(["C", "CE"]), "calls", "puts"))
    t = m.groupby(key + ["side", "cls"])["vol"].sum().unstack(["side", "cls"]).fillna(0)
    t.columns = [f"{c}_{s}" for s, c in t.columns]
    t = t.reset_index()
    for side in ("calls", "puts"):
        for c in ("open_buy", "open_sell", "close_short", "close_long"):
            if f"{c}_{side}" not in t.columns:
                t[f"{c}_{side}"] = 0.0
        t[f"net_open_buy_{side}"] = t[f"open_buy_{side}"] - t[f"open_sell_{side}"]
    return t


@register(_live("opening_flow", "Per minute: option volume split into opening buys, opening sells, closing shorts and closing longs (Lee-Ready "
                "sign with the OI change), calls and puts."))
def opening_flow(ctx: Context, p: dict) -> pd.DataFrame:
    t = ctx.get("ticks_meta")
    if t is None or not len(t):
        return pd.DataFrame()
    o = lee_ready(t[t["instrument_type"].isin(["CE", "PE"])])
    m = o.groupby(["minute", "symbol"]).agg(signed=("signed", "sum"), vol=("dv", "sum"), oi=("oi", "last"), cp=("instrument_type", "first")).reset_index()
    m = m.sort_values("minute")
    m["day"] = m["minute"].dt.normalize()
    m["d_oi"] = m.groupby(["day", "symbol"])["oi"].diff().fillna(0)
    out = _tabulate(m, ["minute"]).rename(columns={"minute": "ts"})
    out["known_at"] = out["ts"] + pd.Timedelta(minutes=1)
    return out


@register(Spec(id="opening_flow_daily", version=1, family="flow", grid="daily", inputs=("chain",), params={"max_k": 0.05}, tickets=TICKETS,
               doc="History proxy: per session, near-money option volume split into opening and closing buys and sells from candles "
                   "(bulk-volume-classification sign with the OI change)."))
def opening_flow_daily(ctx: Context, p: dict) -> pd.DataFrame:
    ch = ctx.get("chain")
    if ch is None or not len(ch):
        return pd.DataFrame()
    c = ch[(ch["k"].abs() <= p["max_k"]) & (ch["age"] == 0)].sort_values("ts").copy()
    c["date"] = c["ts"].dt.date
    key = ["date", "expiry", "strike", "cp"]
    dp = c.groupby(key)["close"].diff()
    sd = dp.groupby([c[k] for k in key]).transform("std").replace(0, np.nan)
    c["signed"] = c["volume"] * (2 * black76.ncdf((dp / sd).fillna(0).to_numpy()) - 1)
    c["d_oi"] = c.groupby(key)["oi"].diff().fillna(0)
    c["vol"] = c["volume"]
    out = _tabulate(c, ["date"])
    out["known_at"] = pd.to_datetime(out["date"]) + pd.Timedelta(hours=10)
    return out


@register(_live("trade_size_flow", "Per minute: Lee-Ready signed lots by trade-size bucket (1, 2-5, 6-20, 21+ lots), options and futures."))
def trade_size_flow(ctx: Context, p: dict) -> pd.DataFrame:
    t = ctx.get("ticks_meta")
    if t is None or not len(t) or "ltq" not in t.columns or t["ltq"].isna().all():
        return pd.DataFrame()
    s = lee_ready(t)
    s = s[s["dv"] > 0].copy()
    lots = (s["ltq"] / s["lot_size"].replace(0, np.nan)).fillna(1)
    s["bucket"] = pd.cut(lots, SIZE_BUCKETS, labels=SIZE_LABELS).astype(str)
    s["kind"] = np.where(s["instrument_type"] == "FUT", "fut", "opt")
    s["signed_lots"] = s["sign"] * lots
    g = s.groupby(["minute", "kind", "bucket"])["signed_lots"].sum().unstack(["kind", "bucket"]).fillna(0)
    g.columns = [f"{k}_{b}" for k, b in g.columns]
    g = g.reindex(columns=[f"{k}_{b}" for k in ("opt", "fut") for b in SIZE_LABELS], fill_value=0.0)   # the same columns every time
    g = g.reset_index().rename(columns={"minute": "ts"})
    g["known_at"] = g["ts"] + pd.Timedelta(minutes=1)
    return g


@register(_live("block_prints", "Events: option prints of 100+ lots and futures prints of 50+ lots, with the largest OI change across the chain "
                "in the same minute.", params={"opt_lots": BLOCK_LOTS_OPT, "fut_lots": BLOCK_LOTS_FUT}))
def block_prints(ctx: Context, p: dict) -> pd.DataFrame:
    t = ctx.get("ticks_meta")
    if t is None or not len(t) or "ltq" not in t.columns:
        return pd.DataFrame()
    s = lee_ready(t)
    lots = s["ltq"] / s["lot_size"].replace(0, np.nan)
    big = ((s["instrument_type"] == "FUT") & (lots >= p["fut_lots"])) | (s["instrument_type"].isin(["CE", "PE"]) & (lots >= p["opt_lots"]))
    ev = s[big & (s["dv"] > 0)].assign(lots=lots[big])
    if ev.empty:
        return pd.DataFrame()
    o = t[t["instrument_type"].isin(["CE", "PE"])].sort_values("t_evt")
    oi = o.groupby(["minute", "symbol"]).agg(oi=("oi", "last"), strike=("strike", "first"), cp=("instrument_type", "first")).reset_index()
    oi["d_oi"] = oi.groupby("symbol")["oi"].diff()
    top = oi.loc[oi.dropna(subset=["d_oi"]).groupby("minute")["d_oi"].apply(lambda x: x.abs().idxmax())]
    top = top[["minute", "symbol", "strike", "cp", "d_oi"]].rename(columns={"symbol": "oi_symbol", "strike": "oi_strike", "cp": "oi_cp", "d_oi": "oi_jump"})
    out = ev[["t_evt", "minute", "symbol", "instrument_type", "ltp", "lots", "sign"]].merge(top, on="minute", how="left")
    out = out.rename(columns={"minute": "ts"})
    out["known_at"] = out["t_evt"]
    return out.drop(columns=["t_evt"])


@register(Spec(id="implied_corr_proxy", version=1, family="flow", grid="daily", inputs=("atm_snaps", "stocks_1m", "weights"),
               params={"window": 20}, tickets=TICKETS,
               doc="Implied correlation of the index from its 7-day ATM IV and the constituents' 20-session realised volatilities."))
def implied_corr_proxy(ctx: Context, p: dict) -> pd.DataFrame:
    s = ctx.get("atm_snaps")
    st = _stocks(ctx)
    wts = _weights_for(ctx)
    if s is None or not len(s) or st.empty or not len(wts):
        return pd.DataFrame()
    closes = st.groupby(["date", "symbol"])["close"].last().unstack("symbol").sort_index()
    vol = np.log(closes).diff().rolling(p["window"], min_periods=p["window"] // 2).std() * np.sqrt(SESSIONS_PER_YEAR)
    snap = s[s["snap"] == "15:15"][["date", "known_at", "iv7"]]
    rows = []
    for _, r in snap.iterrows():
        d = r["date"]
        w = weights_asof(wts, d)
        prev = vol[vol.index < d]
        if w.empty or prev.empty or pd.isna(r["iv7"]):
            continue
        sig = prev.iloc[-1].reindex(w.index).dropna()
        ww = w.reindex(sig.index)
        if ww.sum() <= 0:
            continue
        ww = ww / ww.sum()
        own = float((ww ** 2 * sig ** 2).sum())
        cross = float((ww * sig).sum() ** 2 - own)
        rho = (r["iv7"] ** 2 - own) / cross if cross > 0 else np.nan
        rows.append({"date": d, "known_at": r["known_at"], "iv7": r["iv7"], "basket_vol": float((ww * sig).sum()), "implied_corr": rho})
    return pd.DataFrame(rows)
