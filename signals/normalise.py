"""
Time-of-day normalisation (SCRUM-198): a per-bucket value against the same part of the day in earlier sessions.

Implied vol, spreads, volume, PCR and skew all move through the session in a U shape, so a raw value at 09:16 IST looks extreme every
day.  Each value is turned into a z-score against the same slot (default 5 minutes, so about 100 earlier values over 20 sessions) of
the previous `window` sessions:

  baseline(history, cols, window=20, slot="5min")   ->  per slot: mean and std of each column over the sessions in `history`
  apply_tod_z(df, baseline, cols, slot="5min")      ->  df with <col>_tod_z added
  tod_zscore(df, cols, window=20, slot="5min")      ->  the same over a multi-session frame, each session against the sessions before
                                                        it only (point in time; used by the batch and the tests)

Applied to signals with one row per bucket (index-level): vrp, rv_over_iv, implied_move, skew, curvature, term_slope, pcr_near, gex ...
Per-contract signals carry their own normalisation (spread_gate's z against the same 15-minute slot and moneyness band).
"""

from __future__ import annotations

import numpy as np
import pandas as pd

SLOT = "5min"
WINDOW = 20
MIN_SESSIONS = 5


def _slot(ts: pd.Series, slot: str) -> pd.Series:
    t = pd.to_datetime(ts)
    return t.dt.floor(slot) - t.dt.normalize()


def numeric_cols(df: pd.DataFrame, exclude=("ts", "known_at", "date", "strike", "t", "next_t", "days_between", "atm_strike")) -> list[str]:
    return [c for c in df.columns if c not in exclude and pd.api.types.is_float_dtype(df[c]) and not c.endswith("_tod_z")]


def baseline(history: pd.DataFrame, cols: list[str], window: int = WINDOW, slot: str = SLOT) -> pd.DataFrame:
    """Mean and std per slot of each column over the last `window` sessions of `history` (all rows of history are earlier sessions)."""
    if history is None or not len(history):
        return pd.DataFrame()
    h = history.copy()
    h["_day"] = pd.to_datetime(h["ts"]).dt.normalize()
    days = sorted(h["_day"].unique())[-window:]
    if len(days) < MIN_SESSIONS:
        return pd.DataFrame()
    h = h[h["_day"].isin(days)]
    h["_slot"] = _slot(h["ts"], slot)
    cols = [c for c in cols if c in h.columns]
    g = h.groupby("_slot")[cols].agg(["mean", "std"])
    g.columns = [f"{c}__{s}" for c, s in g.columns]
    return g


def apply_tod_z(df: pd.DataFrame, base: pd.DataFrame, cols: list[str], slot: str = SLOT) -> pd.DataFrame:
    if df is None or not len(df) or base is None or not len(base):
        return df
    out = df.copy()
    s = _slot(out["ts"], slot)
    for c in cols:
        if f"{c}__mean" not in base.columns:
            continue
        m = s.map(base[f"{c}__mean"])
        sd = s.map(base[f"{c}__std"]).replace(0, np.nan)
        out[f"{c}_tod_z"] = (out[c] - m) / sd
    return out


def tod_zscore(df: pd.DataFrame, cols: list[str], window: int = WINDOW, slot: str = SLOT) -> pd.DataFrame:
    """Every session against the `window` sessions before it (never itself or later ones)."""
    if df is None or not len(df):
        return df
    d = pd.to_datetime(df["ts"]).dt.normalize()
    parts = []
    days = sorted(d.unique())
    for i, day in enumerate(days):
        cur = df[d == day]
        prev = df[d.isin(days[max(0, i - window):i])]
        parts.append(apply_tod_z(cur, baseline(prev, cols, window, slot), cols, slot) if len(prev) else cur)
    return pd.concat(parts).loc[df.index] if parts else df


def one_row_per_bucket(df: pd.DataFrame) -> bool:
    return df is not None and len(df) > 0 and "ts" in df.columns and not df["ts"].duplicated().any()
