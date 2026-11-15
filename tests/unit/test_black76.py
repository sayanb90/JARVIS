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
