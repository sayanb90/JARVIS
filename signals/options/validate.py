"""
The IV gate (SCRUM-173): on the days where both exist, our IV backed out of 1-minute candle closes against the IV Upstox sends on the
live feed.  Every history signal inherits the chain's error, so the history signals are not trusted until this passes.

compare_iv(ctx) pairs, for each option minute present in both the candle chain (raw opt_1m) and the live chain (ticks), our candle IV with
the vendor IV at the end of that minute, over strikes within MAX_K of the forward; gate() applies the thresholds.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from signals.framework import Context

MAX_K = 0.02
GATE_MEDIAN = 0.005          # 0.5 volatility points
GATE_P95 = 0.02              # 2 volatility points


def compare_iv(ctx: Context, max_k: float = MAX_K) -> pd.DataFrame:
    hist = ctx.get("chain")
    live = ctx.get("chain_live")
    if hist is None or not len(hist) or live is None or not len(live):
        return pd.DataFrame()
    h = hist[(hist["k"].abs() <= max_k) & hist["iv"].notna()][["ts", "expiry", "strike", "cp", "k", "t", "iv", "fwd_src"]]
    lv = live[live["iv_vendor"].notna()][["ts", "expiry", "strike", "cp", "iv_vendor", "iv"]].rename(columns={"iv": "iv_ours_on_ticks"})
    m = h.merge(lv, on=["ts", "expiry", "strike", "cp"])
    m["gap"] = m["iv"] - m["iv_vendor"]
    m["gap_ticks"] = m["iv_ours_on_ticks"] - m["iv_vendor"]
    return m


def gate(m: pd.DataFrame, median_max: float = GATE_MEDIAN, p95_max: float = GATE_P95) -> dict:
    """Summary of |our IV - Upstox IV|: median, 95th percentile, count, by days-to-expiry bucket too, and pass or fail."""
    if m is None or not len(m):
        return {"pass": False, "n": 0, "reason": "no overlap between candle IV and vendor IV"}
    a = m["gap"].abs()
    out = {"n": int(len(m)), "median_abs": float(a.median()), "p95_abs": float(a.quantile(0.95)), "bias": float(m["gap"].median()),
           "median_abs_on_ticks": float(m["gap_ticks"].abs().median()) if "gap_ticks" in m else np.nan}
    dte = pd.cut(m["t"] * 365, [0, 1, 3, 7, 40], labels=["0-1", "1-3", "3-7", "7+"])
    out["by_dte"] = {str(k): {"n": int(len(g)), "median_abs": float(g["gap"].abs().median())} for k, g in m.groupby(dte, observed=True)}
    out["pass"] = out["median_abs"] <= median_max and out["p95_abs"] <= p95_max
    return out
