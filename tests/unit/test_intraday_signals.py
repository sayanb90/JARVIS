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
    num = [c for c in a.columns if c not in keys and pd.api.types.is_numeric_dtype(a[c]) and f"{c}_all" in m.columns]
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
