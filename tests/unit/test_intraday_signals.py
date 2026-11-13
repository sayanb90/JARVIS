"""Snapshot signals at every bar (SCRUM-198): point in time, and the fixed-time values are just a read of the per-bar table."""

import numpy as np
import pandas as pd
import pytest

from signals import catalogue  # noqa: F401
from signals.framework import Context, check_prefix_invariance, specs
from signals.synthetic import make_raw

SNAP_SIGNALS = [s.id for s in specs() if s.grid == "snap" and s.kind == "signal"]


@pytest.fixture(scope="module")
def raw():
    return make_raw()


@pytest.fixture(scope="module")
def both(raw):
    return Context("NIFTY", raw), Context("NIFTY", raw, snaps="all")


@pytest.mark.parametrize("sid", SNAP_SIGNALS)
def test_fixed_times_are_a_read_of_the_every_bar_table(both, sid):
    fx, al = both
    a, b = fx.compute(sid), al.compute(sid)
    if a.empty:
        pytest.skip(f"{sid}: no rows on the synthetic data")
    keys = [c for c in ("date", "snap", "expiry") if c in a.columns]
    m = a.merge(b, on=keys, suffixes=("", "_all"))
    assert len(m) == len(a), f"{sid}: {len(a) - len(m)} fixed rows missing from the every-bar table"
    num = [c for c in a.columns if c not in keys and pd.api.types.is_numeric_dtype(a[c]) and f"{c}_all" in m.columns
           and not c.endswith("_s")]                                   # per-bar smoothing exists only at every bar
    for c in num:
        np.testing.assert_allclose(m[c].to_numpy(float), m[f"{c}_all"].to_numpy(float), rtol=1e-9, atol=1e-12, equal_nan=True,
                                   err_msg=f"{sid}.{c}")
    assert len(b) > len(a)


@pytest.mark.parametrize("sid", SNAP_SIGNALS)
def test_every_bar_signals_are_point_in_time(raw, sid):
    ts = pd.to_datetime(raw["opt_1m"]["ts"])
    last = ts.dt.date.max()
    for cut in (pd.Timestamp(last) + pd.Timedelta(hours=5, minutes=7), pd.Timestamp(last) + pd.Timedelta(hours=8, minutes=31)):
        ok, msg = check_prefix_invariance(sid, "NIFTY", raw, cut, snaps="all")
        assert ok, msg


def test_smoothed_skew_does_not_jump_when_a_stale_wing_prints(raw):
    """One wing strike missing for 20 minutes, then printing 3 vol points off: the raw skew jumps, the smoothed one moves a little."""
    al = Context("NIFTY", raw, snaps="all")
    s = al.compute("skew")
    day = s["date"].max()
    g = s[(s["date"] == day)].sort_values("known_at")
    g = g[g["expiry"] == g["expiry"].iloc[0]].reset_index(drop=True)
    from signals.options.surface import skew as skew_fn
    snaps = al.get("atm_snaps").copy()
    sel = (snaps["date"] == day) & (snaps["expiry"] == g["expiry"].iloc[0])
    idx = snaps[sel].sort_values("known_at").index
    snaps.loc[idx, "iv_m1"] = snaps.loc[idx, "iv_m1"].ffill().bfill()
    snaps.loc[idx, "iv_p1"] = snaps.loc[idx, "iv_p1"].ffill().bfill()
    snaps.loc[idx[100:120], "iv_m1"] = np.nan                     # stale: no fresh print
    snaps.loc[idx[120], "iv_m1"] += 0.03                          # the stale strike prints again, 3 vol points off
    ctx = Context("NIFTY", raw, snaps="all", cache={"atm_snaps": snaps})
    out = skew_fn(ctx, {"halflife_min": 5})
    o = out[(out["date"] == day) & (out["expiry"] == g["expiry"].iloc[0])].sort_values("known_at").reset_index(drop=True)
    raw_jump = abs(o["skew1"].iloc[120] - o["skew1"].iloc[99])
    smooth_jump = abs(o["skew1_s"].iloc[120] - o["skew1_s"].iloc[119])
    assert raw_jump > 0.025 and smooth_jump < 0.4 * raw_jump
    assert o["skew1_s"].iloc[100:120].notna().all()                # the gap is carried, not blank
