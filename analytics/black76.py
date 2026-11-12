"""
Black-76 on the forward, vectorised: prices, Greeks and implied volatility for European index options (SCRUM-173).

NSE index options are European and settle in cash on the index, so pricing on the forward F (from put-call parity or the future) removes
the need for a dividend yield or carry assumption:

  call = e^(-rT) [F N(d1) - K N(d2)]      put = e^(-rT) [K N(-d2) - F N(-d1)]      d1 = (ln(F/K) + s^2 T/2) / (s sqrt T),  d2 = d1 - s sqrt T

`implied_vol` inverts the price by bisection on [VOL_LO, VOL_HI] (monotone in s, so it always converges) followed by two Newton steps.
It returns NaN, never an error, when the price is below the discounted intrinsic value, above the upper bound, or when T or the price is
not positive.  Greeks are per unit of the underlying: delta = dV/dF * e^(rT) equals the spot delta N(d1) for a forward bought today,
gamma is d2V/dF2 per unit, vega per 1.0 of volatility (divide by 100 for a volatility point).
"""

from __future__ import annotations

import math

import numpy as np

VOL_LO, VOL_HI = 1e-4, 5.0
BISECT_STEPS = 64
_SQRT2 = math.sqrt(2.0)
_SQRT2PI = math.sqrt(2.0 * math.pi)


def _erfc(z: np.ndarray) -> np.ndarray:
    """Complementary error function, fractional error below 1.2e-7 everywhere (Numerical Recipes erfcc); numpy only, so no scipy."""
    a = np.abs(z)
    t = 1.0 / (1.0 + 0.5 * a)
    y = t * np.exp(-a * a - 1.26551223 + t * (1.00002368 + t * (0.37409196 + t * (0.09678418 + t * (-0.18628806 + t * (0.27886807
        + t * (-1.13520398 + t * (1.48851587 + t * (-0.82215223 + t * 0.17087277)))))))))
    return np.where(z >= 0, y, 2.0 - y)


def ncdf(x) -> np.ndarray:
    """Standard normal CDF."""
    x = np.asarray(x, dtype=float)
    return 0.5 * _erfc(-x / _SQRT2)


_ncdf = ncdf


def npdf(x):
    x = np.asarray(x, dtype=float)
    return np.exp(-0.5 * x * x) / _SQRT2PI


def _arr(*xs):
    return [np.asarray(x, dtype=float) for x in np.broadcast_arrays(*xs)]


def d1d2(F, K, T, sigma):
    F, K, T, sigma = _arr(F, K, T, sigma)
    with np.errstate(divide="ignore", invalid="ignore"):
        sd = sigma * np.sqrt(T)
        d1 = (np.log(F / K) + 0.5 * sd * sd) / sd
    return d1, d1 - sd


def price(is_call, F, K, T, sigma, r=0.0):
    """Black-76 price; is_call is a boolean array (or scalar)."""
    is_call, F, K, T, sigma, r = _arr(is_call, F, K, T, sigma, r)
    d1, d2 = d1d2(F, K, T, sigma)
    df = np.exp(-r * T)
    call = df * (F * _ncdf(d1) - K * _ncdf(d2))
    put = df * (K * _ncdf(-d2) - F * _ncdf(-d1))
    return np.where(is_call > 0.5, call, put)


def greeks(is_call, F, K, T, sigma, r=0.0) -> dict[str, np.ndarray]:
    is_call, F, K, T, sigma, r = _arr(is_call, F, K, T, sigma, r)
    d1, d2 = d1d2(F, K, T, sigma)
    df = np.exp(-r * T)
    sqt = np.sqrt(T)
    with np.errstate(divide="ignore", invalid="ignore"):
        gamma = df * npdf(d1) / (F * sigma * sqt)
    delta = np.where(is_call > 0.5, _ncdf(d1), _ncdf(d1) - 1.0)
    vega = df * F * npdf(d1) * sqt
    return {"delta": delta, "gamma": gamma, "vega": vega}


def implied_vol(is_call, premium, F, K, T, r=0.0) -> np.ndarray:
    """Implied volatility (fraction) for each row; NaN where no volatility in [VOL_LO, VOL_HI] reproduces the premium."""
    is_call, premium, F, K, T, r = _arr(is_call, premium, F, K, T, r)
    df = np.exp(-r * T)
    intrinsic = np.where(is_call > 0.5, np.maximum(F - K, 0.0), np.maximum(K - F, 0.0)) * df
    upper = np.where(is_call > 0.5, F, K) * df
    ok = (T > 0) & (premium > 0) & (F > 0) & (K > 0) & np.isfinite(premium) & np.isfinite(F)
    ok &= premium - intrinsic > 1e-8 * np.maximum(F, K)            # no time value left: the volatility is not identified
    ok &= premium < upper
    lo = np.full(premium.shape, VOL_LO)
    hi = np.full(premium.shape, VOL_HI)
    ok &= price(is_call, F, K, T, hi, r) >= premium
    for _ in range(BISECT_STEPS):
        mid = 0.5 * (lo + hi)
        above = price(is_call, F, K, T, mid, r) > premium
        hi = np.where(above, mid, hi)
        lo = np.where(above, lo, mid)
    sig = 0.5 * (lo + hi)
    for _ in range(2):                                   # polish
        v = greeks(is_call, F, K, T, sig, r)["vega"]
        diff = price(is_call, F, K, T, sig, r) - premium
        with np.errstate(divide="ignore", invalid="ignore"):
            step = np.where(v > 1e-12, diff / v, 0.0)
        sig = np.clip(sig - step, VOL_LO, VOL_HI)
    return np.where(ok, sig, np.nan)
