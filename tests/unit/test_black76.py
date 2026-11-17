"""Black-76 prices, Greeks and implied volatility (SCRUM-173)."""

import math

import numpy as np
import pytest

from analytics import black76 as b
from data_layer import greeks_calc


def test_ncdf_matches_math_erfc():
    x = np.linspace(-9, 9, 73)
    ref = np.array([0.5 * math.erfc(-v / math.sqrt(2)) for v in x])
    assert np.all(np.abs(b.ncdf(x) - ref) <= 2e-7 * ref + 1e-300)


def test_put_call_parity():
    F, K, T, s, r = 24000.0, np.array([23000, 24000, 25000.0]), 7 / 365, 0.14, 0.065
    c, p = b.price(True, F, K, T, s, r), b.price(False, F, K, T, s, r)
    assert np.allclose(c - p, np.exp(-r * T) * (F - K), atol=1e-8)


@pytest.mark.parametrize("days", [0.05, 0.5, 2, 7, 30])
@pytest.mark.parametrize("is_call", [True, False])
def test_iv_round_trip(days, is_call):
    F, T, r = 24000.0, days / 365, 0.065
    K = F * np.exp(np.linspace(-0.04, 0.04, 17))
    sig = 0.10 + 0.5 * np.abs(np.log(K / F))
    prem = b.price(is_call, F, K, T, sig, r)
    iv = b.implied_vol(is_call, prem, F, K, T, r)
    ok = prem > 1e-4 * F                      # a few rupees: below the chain's MIN_IV_PRICE the tick dominates anyway
    assert np.nanmax(np.abs(iv[ok] - sig[ok])) < 1e-4


def test_iv_nan_where_no_solution():
    F, T = 24000.0, 7 / 365
    assert np.isnan(b.implied_vol(True, 900.0, F, 23000.0, T))            # below intrinsic (1000)
    assert np.isnan(b.implied_vol(True, 0.0, F, 25000.0, T))
    assert np.isnan(b.implied_vol(True, 100.0, F, 24000.0, 0.0))
    assert np.isnan(b.implied_vol(True, 30000.0, F, 24000.0, T))         # above the forward


def test_greeks_against_greeks_calc():
    F, K, T, s = 24000.0, np.array([23500.0, 24000.0, 24500.0]), 5 / 365, 0.13
    g = b.greeks(True, F, K, T, s, 0.0)
    ref = greeks_calc.bs_greeks(np.array([True] * 3), np.full(3, F), K, np.full(3, T), np.full(3, 13.0), rate=0.0)
    assert np.allclose(g["delta"], ref["delta"], atol=1e-6)
    assert np.allclose(g["gamma"], ref["gamma"], rtol=1e-6)


def _implied_vol_reference(is_call, premium, F, K, T, r=0.0):
    """implied_vol as it was before 17 Nov 2026 (price() at every bisection step), with the same number of steps."""
    is_call, premium, F, K, T, r = b._arr(is_call, premium, F, K, T, r)
    df = np.exp(-r * T)
    intrinsic = np.where(is_call > 0.5, np.maximum(F - K, 0.0), np.maximum(K - F, 0.0)) * df
    upper = np.where(is_call > 0.5, F, K) * df
    ok = (T > 0) & (premium > 0) & (F > 0) & (K > 0) & np.isfinite(premium) & np.isfinite(F)
    ok &= premium - intrinsic > 1e-8 * np.maximum(F, K)
    ok &= premium < upper
    lo = np.full(premium.shape, b.VOL_LO)
    hi = np.full(premium.shape, b.VOL_HI)
    ok &= b.price(is_call, F, K, T, hi, r) >= premium
    for _ in range(b.BISECT_STEPS):
        mid = 0.5 * (lo + hi)
        above = b.price(is_call, F, K, T, mid, r) > premium
        hi = np.where(above, mid, hi)
        lo = np.where(above, lo, mid)
    sig = 0.5 * (lo + hi)
    for _ in range(2):
        v = b.greeks(is_call, F, K, T, sig, r)["vega"]
        diff = b.price(is_call, F, K, T, sig, r) - premium
        with np.errstate(divide="ignore", invalid="ignore"):
            step = np.where(v > 1e-12, diff / v, 0.0)
        sig = np.clip(sig - step, b.VOL_LO, b.VOL_HI)
    return np.where(ok, sig, np.nan)


def test_implied_vol_is_bit_for_bit_the_price_based_bisection():
    rng = np.random.default_rng(11)
    n = 600
    F = 24000.0 * np.exp(rng.normal(0, 0.003, n))
    K = 24000.0 * np.exp(rng.uniform(-0.08, 0.08, n))
    T = rng.choice([0.0, 0.2, 1, 3, 7, 30, 90], n) / 365
    is_call = rng.random(n) < 0.5
    sig = rng.uniform(0.05, 0.9, n)
    r = 0.065
    prem = b.price(is_call, F, K, T, sig, r) * rng.choice([1.0, 1.0, 1.0, 0.0, 1e-12], n)      # some with no time value
    got = b.implied_vol(is_call, prem, F, K, T, r)
    want = _implied_vol_reference(is_call, prem, F, K, T, r)
    assert np.array_equal(got, want, equal_nan=True) and np.isfinite(got).sum() > 200
