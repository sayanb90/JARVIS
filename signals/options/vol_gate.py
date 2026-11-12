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
    logc = np.log(d["close"])
    rows = []
    for _, r in s.iterrows():
        past = d[pd.to_datetime(d["known_at"]) < pd.to_datetime(r["known_at"])]
        n = max(p["min_days"], int(round(r["t"] * SESSIONS_PER_YEAR)))
        if len(past) < n * 4 + 1:
            rows.append(np.nan)
            continue
        lc = logc.iloc[past.index]
        moves = (lc.diff(n).abs()).dropna().iloc[-20:]
        rows.append(float(moves.mean()) / (MU1 * np.sqrt(n / SESSIONS_PER_YEAR)) if len(moves) else np.nan)
    s["realised_vol_from_move"] = rows
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
