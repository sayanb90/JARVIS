"""Realised-volatility engine (SCRUM-174): estimators on GBM with a known volatility, point-in-time HAR and time-of-day profile."""

import numpy as np
import pandas as pd
import pytest

from analytics import realised as R


def gbm(n_days=320, sigma=0.15, on_share=0.2, seed=1, jump_day=None):
    rng = np.random.default_rng(seed)
    rows, px = [], 20000.0
    for i, d in enumerate(pd.bdate_range("2023-01-02", periods=n_days)):
        px *= np.exp(rng.normal(0, sigma * np.sqrt(on_share / 252)))
        r = rng.normal(0, sigma * np.sqrt((1 - on_share) / 252 / 375), 375)
        if jump_day is not None and i == jump_day:
            r[200] += 0.02
        c = px * np.exp(np.cumsum(r))
        o = np.r_[px, c[:-1]]
        rows.append(pd.DataFrame({"ts": d + pd.Timedelta(hours=3, minutes=45) + pd.to_timedelta(np.arange(375), unit="min"),
                                  "open": o, "high": np.maximum(o, c), "low": np.minimum(o, c), "close": c}))
        px = c[-1]
    return pd.concat(rows, ignore_index=True)


@pytest.fixture(scope="module")
def daily():
    return R.daily_measures(gbm())


def test_rv_and_total_variance(daily):
    assert R.annualise(daily["rv"].mean()) == pytest.approx(0.15 * np.sqrt(0.8), rel=0.03)
    assert R.annualise(R.total_variance(daily).mean()) == pytest.approx(0.15, rel=0.05)
    assert (daily["rs_up"] + daily["rs_down"]).sub(daily["rv"]).abs().max() < 1e-12


def test_range_estimators_and_yang_zhang(daily):
    yz = R.annualise(R.yang_zhang(daily, 20).dropna().mean())
    assert yz == pytest.approx(0.15, rel=0.1)
    assert R.annualise(daily["garman_klass"].mean()) == pytest.approx(0.15 * np.sqrt(0.8), rel=0.15)


def test_jump_flag():
    d = R.daily_measures(gbm(n_days=30, jump_day=10))
    assert bool(d.loc[10, "jump_flag"])
    assert d["jump_flag"].mean() < 0.15


def test_har_is_point_in_time():
    m = gbm(n_days=300)
    d = R.daily_measures(m)
    full = R.har_forecast(d, min_obs=200)
    cut = R.har_forecast(d.iloc[:260].reset_index(drop=True), min_obs=200)
    a, b = full["har_5"].iloc[:260].to_numpy(), cut["har_5"].to_numpy()
    assert np.allclose(a, b, equal_nan=True)
    assert np.isfinite(a[-1]) and R.annualise(a[-1]) == pytest.approx(0.15, rel=0.3)


def test_tod_profile_point_in_time_and_monotone():
    m = gbm(n_days=60)
    p = R.tod_profile(m, window=30, min_days=10)
    one = p[p["date"] == p["date"].max()].sort_values("minute")
    assert one["remaining"].iloc[0] == pytest.approx(1.0, abs=0.01)
    assert one["remaining"].iloc[-1] == pytest.approx(0.0, abs=1e-9)
    assert (np.diff(one["remaining"].to_numpy()) <= 1e-12).all()
    first_day = sorted(m["ts"].dt.date.unique())[10]
    p_cut = R.tod_profile(m[m["ts"].dt.date <= first_day], window=30, min_days=10)
    a = p[p["date"] == first_day]["remaining"].to_numpy()
    b = p_cut[p_cut["date"] == first_day]["remaining"].to_numpy()
    assert np.allclose(a, b)
