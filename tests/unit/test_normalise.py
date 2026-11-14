"""Time-of-day z-scores (SCRUM-198): against the same slot of earlier sessions only."""

import numpy as np
import pandas as pd

from signals.normalise import apply_tod_z, baseline, tod_zscore


def u_shaped(days=12, seed=0):
    rng = np.random.default_rng(seed)
    rows = []
    for d in pd.bdate_range("2026-10-01", periods=days):
        for m in range(375):
            ts = d + pd.Timedelta(hours=3, minutes=45 + m)
            shape = 0.20 + 0.05 * ((m - 187) / 187) ** 2            # high at the open and the close
            rows.append({"ts": ts, "iv": shape + rng.normal(0, 0.002)})
    return pd.DataFrame(rows)


def test_seasonality_is_removed():
    df = tod_zscore(u_shaped(), ["iv"])
    last = df[df["ts"].dt.normalize() == df["ts"].dt.normalize().max()]
    raw_open_vs_mid = last["iv"].iloc[:10].mean() - last["iv"].iloc[180:190].mean()
    z_open_vs_mid = last["iv_tod_z"].iloc[:10].mean() - last["iv_tod_z"].iloc[180:190].mean()
    assert raw_open_vs_mid > 0.04 and abs(z_open_vs_mid) < 1.0


def test_only_earlier_sessions_are_used():
    df = u_shaped()
    full = tod_zscore(df, ["iv"])
    day5 = sorted(df["ts"].dt.normalize().unique())[6]
    cut = tod_zscore(df[df["ts"].dt.normalize() <= day5], ["iv"])
    a = full[full["ts"].dt.normalize() == day5]["iv_tod_z"].to_numpy()
    b = cut[cut["ts"].dt.normalize() == day5]["iv_tod_z"].to_numpy()
    np.testing.assert_allclose(a, b)
    first = full[full["ts"].dt.normalize() == df["ts"].dt.normalize().min()]
    assert "iv_tod_z" not in first.columns or first["iv_tod_z"].isna().all()


def test_a_shock_stands_out():
    df = u_shaped()
    hist, today = df[df["ts"].dt.normalize() < df["ts"].max().normalize()], df[df["ts"].dt.normalize() == df["ts"].max().normalize()].copy()
    today.loc[today.index[200], "iv"] += 0.02
    z = apply_tod_z(today, baseline(hist, ["iv"]), ["iv"])
    assert z["iv_tod_z"].iloc[200] > 5 and z["iv_tod_z"].drop(z.index[200]).abs().max() < 5
