"""
Positioning, open interest, gamma and option volume (SCRUM-148, step 6; logged but not trusted: the literature support is weakest here).
History covers only the strikes near the money (about 5 strikes either side of each contract's range), so every history version is a
near-money measure; full-chain versions come from the live feed (signals.options.live) under their own names.

  pcr_near              B2   put OI / call OI and put volume / call volume (session so far) within 3 % of the forward, front expiry
  oi_quadrant           B3   per session: near-money OI change split by the price move (long build-up, short build-up, long unwinding,
                             short covering), calls and puts; max pain within the band
  vol_demand_proxy      B4   per session: volume signed by bulk volume classification (Easley, Lopez de Prado, O'Hara), weighted by vega
                             and by delta x lot: a crude volatility-demand and delta-flow proxy from candles
  gex                   B5   near-money gamma exposure (total, and the call and put walls: the strikes with the most call and put gamma
                             exposure), US dealer convention (dealers long calls, short puts) and an Indian convention
                             from participant OI (dealers on the other side of clients' net position, from the previous session's file);
                             the gamma flip level (where the US-convention exposure changes sign as the forward moves +/- 3 %)
  participant_positioning B11 net long minus short index calls, puts and futures per participant, the change, and a 250-session percentile;
                             known the evening the file is published
  basis_roll            B12  future against the index (annualised carry) at the close, and the next contract's share of futures OI
  option_volume         E1   option volume per 15-minute bucket and moneyness group against its 20-session average in the same bucket
  premium_turnover      E2   price x volume x lot per 15-minute bucket, calls and puts, and their ratio
  volume_burst          E3   minutes where a short-dated out-of-the-money contract trades 5 times its trailing hourly pace
"""

from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd

from analytics import black76
from signals.framework import Context, Spec, register, trailing_pct
from signals.options.core import snap_utc, SNAP_TIMES_IST

BUCKET = "15min"


def _front(ch: pd.DataFrame) -> pd.DataFrame:
    first = ch.groupby("ts")["expiry"].transform("min")
    return ch[ch["expiry"] == first]


def _at_snaps(ch: pd.DataFrame, snaps: str = "fixed", bucket: pd.Timedelta = pd.Timedelta(minutes=1)) -> pd.DataFrame:
    """Rows of the chain at the snapshots: the bars ending at 09:20, 12:00 and 15:15 IST ("fixed"), or every bar ("all", SCRUM-198).
    Adds date, snap (IST time the bar ends) and known_at (the bar's end)."""
    if snaps == "all":
        c = ch.copy()
        c["known_at"] = c["ts"] + bucket
        c["date"] = c["ts"].dt.date
        c["snap"] = (c["known_at"] + pd.Timedelta(hours=5, minutes=30)).dt.strftime("%H:%M:%S" if bucket < pd.Timedelta(minutes=1) else "%H:%M")
        return c
    days = sorted(ch["ts"].dt.date.unique())
    want = {snap_utc(d, s) - bucket: (d, s) for d in days for s in SNAP_TIMES_IST}
    c = ch[ch["ts"].isin(want.keys())].copy()
    c["date"] = c["ts"].map(lambda t: want[t][0])
    c["snap"] = c["ts"].map(lambda t: want[t][1])
    c["known_at"] = c["ts"] + bucket
    return c


def pcr_table(ch: pd.DataFrame, max_k: float, snaps: str = "fixed", bucket: pd.Timedelta = pd.Timedelta(minutes=1)) -> pd.DataFrame:
    """Put/call OI and session-volume ratios at the snapshots, front expiry, |k| <= max_k (np.inf for the full chain)."""
    if ch is None or not len(ch):
        return pd.DataFrame()
    c = ch.copy()
    c["day"] = c["ts"].dt.date
    if "vol_day" in c.columns and c["vol_day"].notna().any():      # live chain: the feed's own day counter (no need for the whole day)
        c["cumvol"] = c["vol_day"]
    else:
        c["cumvol"] = c.sort_values("ts").groupby(["day", "expiry", "strike", "cp"])["volume"].cumsum()
    s = _at_snaps(_front(c), snaps, bucket)
    s = s[s["k"].abs() <= max_k]
    if s.empty:
        return pd.DataFrame()
    g = s.groupby(["date", "snap", "known_at", "cp"]).agg(oi=("oi", "sum"), vol=("cumvol", "sum")).unstack("cp")
    out = pd.DataFrame({"put_oi": g[("oi", "P")], "call_oi": g[("oi", "C")], "put_vol": g[("vol", "P")], "call_vol": g[("vol", "C")]})
    out["pcr_oi"] = out["put_oi"] / out["call_oi"]
    out["pcr_vol"] = out["put_vol"] / out["call_vol"].replace(0, np.nan)
    return out.reset_index()


@register(Spec(id="pcr_near", version=1, family="positioning", grid="snap", inputs=("chain",), params={"max_k": 0.03}, tickets=("SCRUM-148",),
               doc="B2: put/call OI and volume ratios within 3 % of the forward, front expiry (near-money only)."))
def pcr_near(ctx: Context, p: dict) -> pd.DataFrame:
    return pcr_table(ctx.get("chain"), p["max_k"], ctx.snaps)


def _close_rows(ch: pd.DataFrame) -> pd.DataFrame:
    c = ch.copy()
    c["date"] = c["ts"].dt.date
    last = c.groupby(["date", "expiry", "strike", "cp"])["ts"].transform("max")
    return c[c["ts"] == last]


def max_pain(oi: pd.DataFrame) -> float:
    """Strike that minimises what option holders are paid at expiry, over the strikes given (columns strike, cp, oi)."""
    ks = np.sort(oi["strike"].unique())
    if not len(ks):
        return np.nan
    calls = oi[oi["cp"] == "C"]
    puts = oi[oi["cp"] == "P"]
    pain = [float((calls["oi"] * np.maximum(k - calls["strike"], 0)).sum() + (puts["oi"] * np.maximum(puts["strike"] - k, 0)).sum()) for k in ks]
    return float(ks[int(np.argmin(pain))])


@register(Spec(id="oi_quadrant", version=1, family="positioning", grid="daily", inputs=("chain",), params={"max_k": 0.03}, tickets=("SCRUM-148",),
               doc="B3: near-money OI change split by price direction (build-up and unwinding), calls and puts, and max pain in the band."))
def oi_quadrant(ctx: Context, p: dict) -> pd.DataFrame:
    ch = ctx.get("chain")
    if ch is None or not len(ch):
        return pd.DataFrame()
    c = _close_rows(ch).sort_values("date")
    c = c[c["k"].abs() <= p["max_k"]]
    c["d_oi"] = c.groupby(["expiry", "strike", "cp"])["oi"].diff()
    c["d_px"] = c.groupby(["expiry", "strike", "cp"])["close"].diff()
    c = c.dropna(subset=["d_oi", "d_px"])
    c["quad"] = np.select([(c["d_px"] > 0) & (c["d_oi"] > 0), (c["d_px"] < 0) & (c["d_oi"] > 0), (c["d_px"] < 0) & (c["d_oi"] < 0),
                           (c["d_px"] > 0) & (c["d_oi"] < 0)], ["long_buildup", "short_buildup", "long_unwinding", "short_covering"], "flat")
    g = c.groupby(["date", "cp", "quad"])["d_oi"].sum().unstack(["cp", "quad"]).fillna(0)
    g.columns = [f"{cp.lower()}_{q}" for cp, q in g.columns]
    g = g.reset_index()
    fr = _front(_close_rows(ch).assign(ts=lambda x: x["ts"]))
    mp = fr[fr["k"].abs() <= p["max_k"]].groupby("date").apply(lambda x: pd.Series({"max_pain": max_pain(x), "fwd": x["fwd"].iloc[0]}),
                                                                include_groups=False).reset_index()
    out = g.merge(mp, on="date", how="outer")
    out["max_pain_dist"] = (out["fwd"] - out["max_pain"]) / out["fwd"]
    out["known_at"] = pd.to_datetime(out["date"]) + pd.Timedelta(hours=10)
    return out


@register(Spec(id="vol_demand_proxy", version=1, family="positioning", grid="daily", inputs=("chain",), params={"max_k": 0.05}, tickets=("SCRUM-148",),
               doc="B4 proxy: option volume signed by bulk volume classification, weighted by vega and by delta x lot, per session."))
def vol_demand_proxy(ctx: Context, p: dict) -> pd.DataFrame:
    ch = ctx.get("chain")
    if ch is None or not len(ch):
        return pd.DataFrame()
    c = ch[(ch["k"].abs() <= p["max_k"]) & (ch["age"] == 0)].sort_values("ts").copy()
    c["date"] = c["ts"].dt.date
    key = ["date", "expiry", "strike", "cp"]
    c["dp"] = c.groupby(key)["close"].diff()
    sd = c.groupby(key)["dp"].transform("std")
    z = c["dp"] / sd.replace(0, np.nan)
    c["buy_frac"] = black76.ncdf(z.fillna(0).to_numpy())
    c["signed"] = c["volume"] * (2 * c["buy_frac"] - 1)
    lot = c["lot"].fillna(1)
    c["vega_flow"] = c["signed"] * c["vega"].fillna(0) * lot / 100.0
    c["delta_flow"] = c["signed"] * c["delta"].fillna(0) * lot
    out = c.groupby("date").agg(signed_volume=("signed", "sum"), vega_flow=("vega_flow", "sum"), delta_flow=("delta_flow", "sum")).reset_index()
    out["known_at"] = pd.to_datetime(out["date"]) + pd.Timedelta(hours=10)
    return out


def gex_table(ch: pd.DataFrame, participant: pd.DataFrame | None, max_k: float, r: float, gamma_col: str = "gamma", snaps: str = "fixed",
              bucket: pd.Timedelta = pd.Timedelta(minutes=1), since: Optional[pd.Timestamp] = None) -> pd.DataFrame:
    """Gamma exposure at the snapshots over strikes with |k| <= max_k (all expiries present): US convention and, when participant data
    is given, the Indian convention; and the gamma flip level for the US convention."""
    if ch is None or not len(ch):
        return pd.DataFrame()
    s = _at_snaps(ch, snaps, bucket)
    s = s[(s["k"].abs() <= max_k) & s[gamma_col].notna() & s["oi"].notna()]
    if s.empty:
        return pd.DataFrame()
    lot = s["lot"].fillna(1)
    unit = s[gamma_col] * s["oi"] * lot * s["fwd"] ** 2 * 0.01
    s = s.assign(gex_c=np.where(s["cp"] == "C", unit, 0.0), gex_p=np.where(s["cp"] == "P", unit, 0.0))
    out = s.groupby(["date", "snap", "known_at"]).agg(gex_calls=("gex_c", "sum"), gex_puts=("gex_p", "sum"), fwd=("fwd", "first")).reset_index()
    out["gex_us"] = out["gex_calls"] - out["gex_puts"]
    by_k = s.groupby(["date", "snap", "strike"], as_index=False).agg(gc=("gex_c", "sum"), gp=("gex_p", "sum"))
    cw = by_k.loc[by_k.groupby(["date", "snap"])["gc"].idxmax(), ["date", "snap", "strike"]].rename(columns={"strike": "call_wall"})
    pw = by_k.loc[by_k.groupby(["date", "snap"])["gp"].idxmax(), ["date", "snap", "strike"]].rename(columns={"strike": "put_wall"})
    out = out.merge(cw, on=["date", "snap"], how="left").merge(pw, on=["date", "snap"], how="left")
    if participant is not None and len(participant):
        pt = participant[participant["participant"] == "Client"].copy()
        pt["client_net_calls"] = pt["option_index_call_long"] - pt["option_index_call_short"]
        pt["client_net_puts"] = pt["option_index_put_long"] - pt["option_index_put_short"]
        pt = pt.sort_values("known_at")[["known_at", "client_net_calls", "client_net_puts"]].rename(columns={"known_at": "pt_at"})
        out = pd.merge_asof(out.sort_values("known_at"), pt, left_on="known_at", right_on="pt_at", direction="backward")
        out["gex_participant"] = -np.sign(out["client_net_calls"]) * out["gex_calls"] - np.sign(out["client_net_puts"]) * out["gex_puts"]
    flips = []
    if since is not None:                        # live: the flip search only for the rows that will be written (Context.since)
        s = s[s["known_at"] > since]
    for (d, sn), g in s.groupby(["date", "snap"]):
        f0 = float(g["fwd"].iloc[0])
        grid = f0 * np.linspace(0.97, 1.03, 49)
        is_c = (g["cp"] == "C").to_numpy()
        f2 = grid[:, None]                                            # every grid forward at once: (49, contracts)
        gk = black76.greeks(np.broadcast_to(is_c, (len(grid), len(g))), f2, g["strike"].to_numpy()[None, :], g["t"].to_numpy()[None, :],
                            g["iv"].to_numpy()[None, :], r)["gamma"]
        u = np.nan_to_num(gk) * (g["oi"].to_numpy() * g["lot"].fillna(1).to_numpy())[None, :] * f2 ** 2 * 0.01
        tot = np.where(is_c[None, :], u, -u).sum(axis=1)
        sgn = np.sign(tot)
        ch_idx = np.where(np.diff(sgn) != 0)[0]
        flip = np.nan
        if len(ch_idx):
            i = ch_idx[np.argmin(np.abs(grid[ch_idx] - f0))]
            flip = float(grid[i] - tot[i] * (grid[i + 1] - grid[i]) / (tot[i + 1] - tot[i]))
        flips.append({"date": d, "snap": sn, "gamma_flip": flip})
    out = out.merge(pd.DataFrame(flips, columns=["date", "snap", "gamma_flip"]), on=["date", "snap"], how="left")
    out["flip_dist"] = (out["fwd"] - out["gamma_flip"]) / out["fwd"]
    return out


@register(Spec(id="gex", version=1, family="positioning", grid="snap", inputs=("chain", "participant"), params={"max_k": 0.03}, tickets=("SCRUM-148",),
               doc="B5: near-money gamma exposure (US convention; Indian convention from the previous participant file) and the flip level."))
def gex(ctx: Context, p: dict) -> pd.DataFrame:
    ch = ctx.get("chain")
    if ch is None or not len(ch):
        return pd.DataFrame()
    return gex_table(ch, ctx.raw.get("participant"), p["max_k"], ctx.r, snaps=ctx.snaps, since=ctx.since)


@register(Spec(id="participant_positioning", version=1, family="positioning", grid="daily", inputs=("participant",), params={"window": 250},
               tickets=("SCRUM-148", "SCRUM-175"),
               doc="B11: net long minus short index calls, puts and futures per participant, its change and 250-session percentile."))
def participant_positioning(ctx: Context, p: dict) -> pd.DataFrame:
    pt = ctx.raw.get("participant")
    if pt is None or not len(pt):
        return pd.DataFrame()
    x = pt.copy().sort_values(["participant", "date"])
    x["net_calls"] = x["option_index_call_long"] - x["option_index_call_short"]
    x["net_puts"] = x["option_index_put_long"] - x["option_index_put_short"]
    x["net_futures"] = x["future_index_long"] - x["future_index_short"]
    x["net_option_delta_proxy"] = x["net_calls"] - x["net_puts"]
    for c in ("net_calls", "net_puts", "net_futures", "net_option_delta_proxy"):
        x[f"d_{c}"] = x.groupby("participant")[c].diff()
    x["pct_net_option_delta_proxy"] = x.groupby("participant")["net_option_delta_proxy"].transform(lambda s: trailing_pct(s, p["window"], 20))
    if "known_at" not in x.columns:
        x["known_at"] = pd.to_datetime(x["date"]) + pd.Timedelta(hours=16)
    return x[["date", "known_at", "participant", "net_calls", "net_puts", "net_futures", "net_option_delta_proxy", "d_net_calls", "d_net_puts",
              "d_net_futures", "d_net_option_delta_proxy", "pct_net_option_delta_proxy"]]


@register(Spec(id="basis_roll", version=1, family="positioning", grid="daily", inputs=("fut_front", "spot"), tickets=("SCRUM-148",),
               doc="B12: front future against the index at the close (annualised carry) and the next contract's share of futures OI."))
def basis_roll(ctx: Context, p: dict) -> pd.DataFrame:
    f = ctx.get("fut_front")
    sp = ctx.get("spot")
    if f is None or not len(f) or not len(sp):
        return pd.DataFrame()
    m = f.merge(sp[["ts", "close"]].rename(columns={"close": "spot"}), on="ts")
    m["date"] = m["ts"].dt.date
    last = m.sort_values("ts").groupby("date").tail(1)
    out = last[["date", "close", "spot", "t_fut", "oi", "next_oi"]].copy()
    out["basis"] = out["close"] / out["spot"] - 1
    out["carry"] = np.log(out["close"] / out["spot"]) / out["t_fut"]
    out["roll_share"] = out["next_oi"] / (out["oi"] + out["next_oi"])
    out["known_at"] = pd.to_datetime(out["date"]) + pd.Timedelta(hours=10)
    return out


def _bucketed(ch: pd.DataFrame) -> pd.DataFrame:
    c = _front(ch).copy()
    c["date"] = c["ts"].dt.date
    c["bucket"] = c["ts"].dt.floor(BUCKET)
    c["group"] = np.select([c["k"] < -0.005, c["k"] > 0.005], ["otm_put_side", "otm_call_side"], "atm")
    c["group"] = np.where((c["group"] == "otm_put_side") & (c["cp"] == "C"), "itm_call", c["group"])
    c["group"] = np.where((c["group"] == "otm_call_side") & (c["cp"] == "P"), "itm_put", c["group"])
    return c


@register(Spec(id="option_volume", version=1, family="positioning", grid="daily", inputs=("chain",), params={"window": 20}, tickets=("SCRUM-148",),
               doc="E1: front-expiry option volume per 15-minute bucket and moneyness group against its 20-session average in that bucket."))
def option_volume(ctx: Context, p: dict) -> pd.DataFrame:
    ch = ctx.get("chain")
    if ch is None or not len(ch):
        return pd.DataFrame()
    c = _bucketed(ch)
    g = c.groupby(["date", "bucket", "group"], as_index=False)["volume"].sum()
    g["slot"] = g["bucket"] - pd.to_datetime(g["date"])
    g = g.sort_values("date")
    g["avg_prev"] = g.groupby(["slot", "group"])["volume"].transform(lambda s: s.shift(1).rolling(p["window"], min_periods=5).mean())
    g["rel_volume"] = g["volume"] / g["avg_prev"]
    g["known_at"] = g["bucket"] + pd.Timedelta(BUCKET)
    return g.drop(columns=["slot"])


@register(Spec(id="premium_turnover", version=1, family="positioning", grid="daily", inputs=("chain",), tickets=("SCRUM-148",),
               doc="E2: premium traded (price x volume x lot) per 15-minute bucket, calls and puts (front expiry), and the call/put ratio."))
def premium_turnover(ctx: Context, p: dict) -> pd.DataFrame:
    ch = ctx.get("chain")
    if ch is None or not len(ch):
        return pd.DataFrame()
    c = _front(ch).copy()
    c["date"] = c["ts"].dt.date
    c["bucket"] = c["ts"].dt.floor(BUCKET)
    c["prem"] = c["close"] * c["volume"] * c["lot"].fillna(1)
    g = c.groupby(["date", "bucket", "cp"])["prem"].sum().unstack("cp").fillna(0).reset_index()
    g = g.rename(columns={"C": "call_premium", "P": "put_premium"})
    for col in ("call_premium", "put_premium"):
        if col not in g.columns:
            g[col] = 0.0
    g["call_put_ratio"] = g["call_premium"] / g["put_premium"].replace(0, np.nan)
    g["known_at"] = g["bucket"] + pd.Timedelta(BUCKET)
    return g


@register(Spec(id="volume_burst", version=1, family="positioning", grid="1m", inputs=("chain",),
               params={"ratio": 5.0, "max_dte": 2, "min_k": 0.005, "min_volume": 100}, tickets=("SCRUM-148",),
               doc="E3: minutes where a short-dated out-of-the-money contract's 5-minute volume is 5 times its trailing hourly pace."))
def volume_burst(ctx: Context, p: dict) -> pd.DataFrame:
    ch = ctx.get("chain")
    if ch is None or not len(ch):
        return pd.DataFrame()
    c = ch.sort_values("ts").copy()
    c["date"] = c["ts"].dt.date
    key = ["date", "expiry", "strike", "cp"]
    c["v5"] = c.groupby(key)["volume"].transform(lambda s: s.rolling(5, min_periods=1).sum())
    c["pace"] = c.groupby(key)["volume"].transform(lambda s: s.shift(5).rolling(60, min_periods=20).mean() * 5)
    otm = ((c["cp"] == "C") & (c["k"] > p["min_k"])) | ((c["cp"] == "P") & (c["k"] < -p["min_k"]))
    dte = (pd.to_datetime(c["expiry"]) - pd.to_datetime(c["date"])).dt.days
    hit = otm & (dte <= p["max_dte"]) & (c["v5"] >= p["min_volume"]) & (c["v5"] >= p["ratio"] * c["pace"])
    out = c[hit][["ts", "expiry", "strike", "cp", "k", "v5", "pace", "oi"]].copy()
    out["ratio"] = out["v5"] / out["pace"]
    out["known_at"] = out["ts"] + pd.Timedelta(minutes=1)
    return out
