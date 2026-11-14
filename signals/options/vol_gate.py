"""
The volatility gate and sizing (SCRUM-177): is premium cheap enough to buy, and how much.

Signals (all at the snapshot times 09:20, 12:00, 15:15 IST unless said otherwise):
  vrp            A1   iv7 (7-day constant-maturity ATM IV) against the HAR forecast of realised volatility over the next 5 sessions made
                      at the previous close: vrp_diff = iv7 - rv_fc, vrp_ratio = iv7 / rv_fc; NIFTY also against India VIX
  rv_over_iv     A2   buy trigger: rv_fc exceeds iv7 by MARGIN volatility points
  implied_move   A3   front straddle / forward (the market's expected absolute move to expiry, scaled by sqrt(2/pi) to a volatility)
                      against the realised absolute move over the same number of past sessions
  implied_move_outcome (label)   the realised move from the snapshot to expiry, for research only
  vix_regime     A6   India VIX level, 1-day change, percentile over 250 sessions, regime (low, mid, high by tercile); daily at the close
  vol_size       A18  position scale = target volatility / rv_fc, clipped
Volatilities are annualised fractions (0.13 = 13 %).
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from analytics import realised
from analytics.realised import SESSIONS_PER_YEAR
from signals.framework import Context, Spec, register, trailing_pct
from signals.options.core import OPEN_UTC

MU1 = float(np.sqrt(2.0 / np.pi))


def _with_forecast(ctx: Context, snaps: pd.DataFrame, horizon: int = 5) -> pd.DataFrame:
    """Attach the HAR forecast made at the latest close before each snapshot (annualised), so a snapshot never uses its own session."""
    d = ctx.get("daily")
    if snaps is None or not len(snaps) or d is None or not len(d):
        return pd.DataFrame()
    f = d[["known_at", f"har_{horizon}", "yz20"]].dropna(subset=[f"har_{horizon}"]).copy()
    f["rv_fc"] = np.sqrt(f[f"har_{horizon}"] * SESSIONS_PER_YEAR)
    f["rv_yz20"] = np.sqrt(f["yz20"] * SESSIONS_PER_YEAR)
    f = f.rename(columns={"known_at": "fc_at"}).sort_values("fc_at")
    s = snaps.copy()
    s["_q"] = pd.to_datetime(s["known_at"]) - pd.Timedelta(seconds=1)
    s = pd.merge_asof(s.sort_values("_q"), f[["fc_at", "rv_fc", "rv_yz20"]], left_on="_q", right_on="fc_at", direction="backward",
                      allow_exact_matches=False)
    return s.drop(columns=["_q"])


@register(Spec(id="vrp", version=1, family="vol_gate", grid="snap", inputs=("atm_snaps", "daily", "vix"), tickets=("SCRUM-177",),
               params={"horizon": 5}, warmup_days=0,
               doc="A1: iv7 against the HAR forecast of realised volatility (5 sessions); NIFTY also India VIX against the forecast."))
def vrp(ctx: Context, p: dict) -> pd.DataFrame:
    s = _with_forecast(ctx, ctx.get("atm_snaps"), p["horizon"])
    if not len(s):
        return s
    s["vrp_diff"] = s["iv7"] - s["rv_fc"]
    s["vrp_ratio"] = s["iv7"] / s["rv_fc"]
    v = ctx.get("vix")
    if v is not None and len(v):
        vv = v[["ts", "close"]].rename(columns={"close": "vix"}).sort_values("ts")
        vv["_at"] = vv["ts"] + pd.Timedelta(minutes=1)
        s = pd.merge_asof(s.sort_values("known_at"), vv[["_at", "vix"]], left_on="known_at", right_on="_at", direction="backward",
                          tolerance=pd.Timedelta(minutes=5)).drop(columns=["_at"])
        s["vix_vrp_diff"] = s["vix"] / 100.0 - s["rv_fc"]
    return s[["date", "snap", "known_at", "iv7", "iv7_how", "rv_fc", "rv_yz20", "vrp_diff", "vrp_ratio"] +
             [c for c in ("vix", "vix_vrp_diff") if c in s.columns]]


@register(Spec(id="vrp_intraday", version=1, family="vol_gate", grid="snap", inputs=("atm_snaps", "daily", "tod", "spot"),
               tickets=("SCRUM-198",), params={"horizon": 5, "frac_window": 20}, warmup_days=0,
               doc="A1 intraday: the daily HAR forecast as the anchor, revised during the session by what has been realised so far "
                   "against what the time-of-day profile says should have been by now.  rv_so_far_ratio > 1 means the day is running "
                   "hot; rv_fc_upd is the 5-session forecast with today's revision spread over its 5 sessions; vrp_intraday = iv7 - "
                   "rv_fc_upd."))
def vrp_intraday(ctx: Context, p: dict) -> pd.DataFrame:
    s = _with_forecast(ctx, ctx.get("atm_snaps"), p["horizon"])
    d, prof, sp = ctx.get("daily"), ctx.get("tod"), ctx.get("spot")
    if not len(s) or d is None or not len(d) or prof is None or not len(prof) or not len(sp):
        return pd.DataFrame()
    # the 1-session forecast of total variance and the within-day share of it (trailing mean of rv / tv over earlier sessions)
    f = d[["known_at", "har_1", "rv", "tv"]].copy()
    f["in_frac"] = (f["rv"] / f["tv"]).clip(0.05, 1.0).rolling(p["frac_window"], min_periods=5).mean()
    f = f.dropna(subset=["har_1", "in_frac"]).rename(columns={"known_at": "fc1_at"})[["fc1_at", "har_1", "in_frac"]].sort_values("fc1_at")
    s["_q"] = pd.to_datetime(s["known_at"]) - pd.Timedelta(seconds=1)
    s = pd.merge_asof(s.sort_values("_q"), f, left_on="_q", right_on="fc1_at", direction="backward", allow_exact_matches=False)
    # realised variance so far today, from the index minutes that ended by the bucket's end
    r = realised.minute_returns(sp)
    r["date"] = r["day"].dt.date
    r["end"] = r["ts"] + pd.Timedelta(minutes=1)
    r = r.sort_values("end")
    r["rv_cum"] = (r["r"] ** 2).groupby(r["date"]).cumsum()
    s = pd.merge_asof(s.sort_values("known_at"), r[["end", "date", "rv_cum", "minute"]].rename(columns={"date": "_d"}),
                      left_on="known_at", right_on="end", direction="backward")
    s.loc[s["_d"] != s["date"], ["rv_cum", "minute"]] = np.nan          # nothing realised yet today
    s["rv_cum"] = s["rv_cum"].fillna(0.0)
    s["minute"] = s["minute"].fillna(-1).astype(int)
    rem = prof[["date", "minute", "remaining"]]
    s = s.merge(rem, on=["date", "minute"], how="left")
    s.loc[s["minute"] < 0, "remaining"] = 1.0
    exp_in = s["har_1"] * s["in_frac"]                                # expected within-day variance today
    s["rv_so_far_ratio"] = s["rv_cum"] / (exp_in * (1 - s["remaining"])).where(s["remaining"] < 1)
    day_var = s["rv_cum"] + s["remaining"] * exp_in + s["har_1"] * (1 - s["in_frac"])     # so far + rest of today + overnight share
    s["rv_day_exp"] = np.sqrt(day_var * SESSIONS_PER_YEAR)
    s["rv_fc_upd"] = np.sqrt((s["rv_fc"] ** 2 + (day_var - s["har_1"]) * SESSIONS_PER_YEAR / p["horizon"]).clip(lower=0))
    s["vrp_intraday"] = s["iv7"] - s["rv_fc_upd"]
    return s[["date", "snap", "known_at", "iv7", "rv_fc", "rv_cum", "remaining", "rv_so_far_ratio", "rv_day_exp", "rv_fc_upd",
              "vrp_intraday"]].sort_values("known_at").reset_index(drop=True)


@register(Spec(id="rv_over_iv", version=1, family="vol_gate", grid="snap", inputs=("vrp",), params={"margin": 0.01}, tickets=("SCRUM-177",),
               doc="A2: buy trigger when the realised-volatility forecast exceeds iv7 by `margin` (1 volatility point)."))
def rv_over_iv(ctx: Context, p: dict) -> pd.DataFrame:
    v = ctx.signal("vrp")
    if not len(v):
        return v
    out = v[["date", "snap", "known_at", "iv7", "rv_fc"]].copy()
    out["trigger"] = (out["rv_fc"] - out["iv7"]) > p["margin"]
    return out


def _realised_abs_move(ctx: Context) -> pd.DataFrame:
    d = ctx.get("daily")
    if d is None or not len(d):
        return pd.DataFrame()
    return d[["date", "known_at", "close"]].copy()


@register(Spec(id="implied_move", version=1, family="vol_gate", grid="snap", inputs=("atm_snaps", "daily"), params={"min_days": 1},
               tickets=("SCRUM-177",),
               doc="A3: front straddle / forward as an annualised volatility (straddle ~ F sigma sqrt(T) sqrt(2/pi)) against the realised "
                   "volatility of absolute close-to-close moves over the past sessions matching the days to expiry."))
def implied_move(ctx: Context, p: dict) -> pd.DataFrame:
    s = ctx.get("atm_snaps")
    d = _realised_abs_move(ctx)
    if s is None or not len(s) or not len(d):
        return pd.DataFrame()
    s = s.copy()
    s["implied_move"] = s["straddle"] / s["fwd"]
    s["implied_vol_from_move"] = s["implied_move"] / (MU1 * np.sqrt(s["t"]))
    d = d.sort_values("date").reset_index(drop=True)
    logc = np.log(d["close"]).to_numpy()
    ka = pd.to_datetime(d["known_at"]).to_numpy()
    n_past = np.searchsorted(ka, pd.to_datetime(s["known_at"]).to_numpy(), side="left")   # sessions known strictly before the snapshot
    horizon = np.maximum(p["min_days"], np.round(s["t"].to_numpy() * SESSIONS_PER_YEAR).astype(int))
    memo: dict[tuple[int, int], float] = {}                 # every minute of a session shares its past: compute each (past, n) once

    def rv_move(m: int, n: int) -> float:
        if (m, n) not in memo:
            if m < n * 4 + 1:
                memo[(m, n)] = np.nan
            else:
                lc = logc[:m]
                moves = np.abs(lc[n:] - lc[:-n])[-20:]
                memo[(m, n)] = float(moves.mean()) / (MU1 * np.sqrt(n / SESSIONS_PER_YEAR)) if len(moves) else np.nan
        return memo[(m, n)]

    s["realised_vol_from_move"] = [rv_move(int(m), int(n)) for m, n in zip(n_past, horizon)]
    s["move_ratio"] = s["implied_vol_from_move"] / s["realised_vol_from_move"]
    return s[["date", "snap", "known_at", "expiry", "t", "straddle", "fwd", "implied_move", "implied_vol_from_move", "realised_vol_from_move", "move_ratio"]]


@register(Spec(id="implied_move_outcome", version=1, family="vol_gate", grid="snap", kind="label", inputs=("atm_snaps", "spot"),
               tickets=("SCRUM-177",),
               doc="Label for A3: the realised absolute move from the snapshot to the expiry close, as a fraction of the forward."))
def implied_move_outcome(ctx: Context, p: dict) -> pd.DataFrame:
    s = ctx.get("atm_snaps")
    sp = ctx.get("spot")
    if s is None or not len(s) or not len(sp):
        return pd.DataFrame()
    closes = sp.assign(date=sp["ts"].dt.date).groupby("date")["close"].last()
    out = s[["date", "snap", "known_at", "expiry", "fwd", "straddle"]].copy()
    out["spot_at_expiry"] = out["expiry"].map(closes)
    out["realised_move"] = (out["spot_at_expiry"] / (out["fwd"]) - 1).abs()
    out["known_at"] = pd.to_datetime(out["expiry"]) + pd.Timedelta(hours=10)
    return out


@register(Spec(id="vix_regime", version=1, family="vol_gate", grid="daily", inputs=("vix",), params={"window": 250}, tickets=("SCRUM-177",),
               doc="A6: India VIX close, 1-day change, percentile over 250 sessions and tercile regime."))
def vix_regime(ctx: Context, p: dict) -> pd.DataFrame:
    v = ctx.get("vix")
    if v is None or not len(v):
        return pd.DataFrame()
    d = v.assign(date=v["ts"].dt.date).groupby("date", as_index=False)["close"].last().rename(columns={"close": "vix"})
    d["known_at"] = pd.to_datetime(d["date"]) + pd.Timedelta(hours=10)
    d["vix_chg"] = d["vix"].diff()
    d["vix_pct"] = trailing_pct(d["vix"], p["window"])
    d["regime"] = pd.cut(d["vix_pct"], [-0.01, 1 / 3, 2 / 3, 1.01], labels=["low", "mid", "high"]).astype(str).replace("nan", None)
    return d


@register(Spec(id="vol_size", version=1, family="vol_gate", grid="snap", inputs=("vrp",), params={"target": 0.12, "lo": 0.25, "hi": 2.0},
               tickets=("SCRUM-177",), doc="A18: position scale = target volatility / forecast realised volatility, clipped to [0.25, 2]."))
def vol_size(ctx: Context, p: dict) -> pd.DataFrame:
    v = ctx.signal("vrp")
    if not len(v):
        return v
    out = v[["date", "snap", "known_at", "rv_fc"]].copy()
    out["scale"] = (p["target"] / out["rv_fc"]).clip(p["lo"], p["hi"])
    return out
