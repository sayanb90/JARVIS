"""
The composite permission score for buying options (SCRUM-182).  No peer-reviewed composite exists: this is a pre-registered, equally
weighted sum of cited components, each standardised against its own previous 120 sessions (15:15 snapshot):

  z(-vrp_ratio)       options cheap against forecast realised volatility                   (Bollerslev, Tauchen, Zhou; Goyal, Saretto)
  z(-term slope)      front IV above the next expiry's: inverted term structure            (Johnson 2017)
  z(-gex_us)          negative dealer gamma: moves extend rather than revert               (Baltussen et al. 2021; Barbon, Buraschi)
  z(vov_iv7)          rising volatility of volatility                                      (Branger et al.)
score = mean of the available z-scores (at least MIN_COMPONENTS).  A higher score means buying premium is more permitted.  The weights are
fixed here; there is no optimisation code path.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from signals.framework import Context, Spec, register, trailing_z

COMPONENTS = ("vrp_ratio", "term_slope", "gex_us", "vov_iv7")
SIGNS = {"vrp_ratio": -1.0, "term_slope": -1.0, "gex_us": -1.0, "vov_iv7": 1.0}
MIN_COMPONENTS = 2


@register(Spec(id="buy_permission", version=1, family="composite", grid="daily", inputs=("vrp", "term_slope", "gex", "vol_of_vol"),
               params={"window": 120, "weights": "equal", "components": COMPONENTS}, tickets=("SCRUM-182",),
               doc="Equal-weighted z-score sum: cheap premium (low VRP ratio), inverted term structure, negative dealer gamma, rising "
                   "vol-of-vol."))
def buy_permission(ctx: Context, p: dict) -> pd.DataFrame:
    parts = []
    v = ctx.signal("vrp")
    if len(v):
        parts.append(v[v["snap"] == "15:15"][["date", "known_at", "vrp_ratio"]])
    t = ctx.signal("term_slope")
    if len(t):
        parts.append(t[t["snap"] == "15:15"][["date", "known_at", "slope"]].rename(columns={"slope": "term_slope"}))
    g = ctx.signal("gex")
    if len(g):
        parts.append(g[g["snap"] == "15:15"][["date", "known_at", "gex_us"]])
    w = ctx.signal("vol_of_vol")
    if len(w) and "vov_iv7" in w.columns:
        parts.append(w[["date", "known_at", "vov_iv7"]])
    if not parts:
        return pd.DataFrame()
    df = parts[0]
    for x in parts[1:]:
        df = df.merge(x.drop(columns=["known_at"]), on="date", how="outer")
    df = df.sort_values("date").reset_index(drop=True)
    df["known_at"] = pd.to_datetime(df["date"]) + pd.Timedelta(hours=10)
    zs = []
    for c in COMPONENTS:
        if c in df.columns:
            df[f"z_{c}"] = SIGNS[c] * trailing_z(df[c].astype(float), p["window"], min_periods=20)
            zs.append(f"z_{c}")
    n = df[zs].notna().sum(axis=1)
    df["n_components"] = n
    df["score"] = np.where(n >= MIN_COMPONENTS, df[zs].mean(axis=1), np.nan)
    return df
