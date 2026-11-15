"""
Smile and ATM summaries of one chain snapshot, in numpy (SCRUM-201): the per-bucket versions of signals.options.core smile() and atm(),
with the same rules (tests/unit/test_engine_v2.py compares them on every bucket of a replayed day).

  smile   per expiry: forward, years to expiry, ATM IV (linear in k = ln(K / F), never extrapolated), the IVs at SMILE_POINTS, the strike
          nearest the forward and its straddle
  atm     the front expiry's smile row, the next expiry's ATM IV, and iv7 (7-day constant-maturity ATM IV, total-variance interpolation
          across expiries at least a day out, flat beyond them)
"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd

from signals.engine.state import Chain, OptionBook
from signals.options.core import CM_DAYS, MIN_T_IV7, SMILE_POINTS

SMILE_COLS = ["ts", "expiry", "fwd", "t", "atm_iv"] + [f"iv_{n}" for n in SMILE_POINTS] + ["atm_strike", "straddle"]
ATM_COLS = SMILE_COLS + ["next_expiry", "next_t", "next_atm_iv", "iv7", "iv7_how"]


def _interp(ks: np.ndarray, vs: np.ndarray, k: float) -> float:
    """Linear interpolation at k between the nearest source points at or below and at or above; NaN outside (interp_at)."""
    if not len(ks) or k != k:
        return math.nan
    lo = np.searchsorted(ks, k, side="right") - 1
    hi = np.searchsorted(ks, k, side="left")
    if lo < 0 or hi >= len(ks):
        return math.nan
    k_lo, k_hi, v_lo, v_hi = ks[lo], ks[hi], vs[lo], vs[hi]
    span = k_hi - k_lo
    w = (k - k_lo) / span if span > 0 else 0.0
    return float(v_lo + w * (v_hi - v_lo))


def smile_rows(book: OptionBook, ch: Chain) -> list[dict]:
    """One dict per expiry with a chain row in this bucket, ordered by t (as smile() sorts)."""
    if not len(ch):
        return []
    s = ch.slots
    cols = ch.cols
    exp = book.expiry[s]
    strike = book.strike[s]
    is_c = book.is_call[s]
    out = []
    for e in dict.fromkeys(exp):
        m = exp == e
        st, c_side = strike[m], is_c[m]
        iv, fwd, t, close = cols["iv"][m], cols["fwd"][m], cols["t"][m], cols["close"][m]
        ks = np.unique(st)                                            # sorted strikes of the outer join of calls and puts
        pos = np.searchsorted(ks, st)
        n = len(ks)
        def side(vals, call):
            a = np.full(n, np.nan)
            sel = c_side == call
            a[pos[sel]] = vals[sel]
            return a
        iv_c, iv_p = side(iv, True), side(iv, False)
        f_c, f_p = side(fwd, True), side(fwd, False)
        t_c, t_p = side(t, True), side(t, False)
        cl_c, cl_p = side(close, True), side(close, False)
        f_k = np.where(np.isnan(f_c), f_p, f_c)
        t_k = np.where(np.isnan(t_c), t_p, t_c)
        with np.errstate(divide="ignore", invalid="ignore"):
            k = np.log(ks / f_k)
        put_side = k < 0
        iv_otm = np.where(put_side, np.where(np.isnan(iv_p), iv_c, iv_p), np.where(np.isnan(iv_c), iv_p, iv_c))
        nn = ~np.isnan(f_k)
        fwd_first = f_k[nn][0] if nn.any() else math.nan
        tn = ~np.isnan(t_k)
        t_first = t_k[tn][0] if tn.any() else math.nan
        ok = ~np.isnan(iv_otm) & ~np.isnan(k)
        order = np.argsort(k[ok], kind="stable")
        src_k, src_v = k[ok][order], iv_otm[ok][order]
        row = {"ts": pd.Timestamp(ch.b), "expiry": e, "fwd": fwd_first, "t": t_first, "atm_iv": _interp(src_k, src_v, 0.0)}
        for name, mny in SMILE_POINTS.items():
            row[f"iv_{name}"] = _interp(src_k, src_v, float(np.log(mny)))
        dist = np.abs(ks - f_k)
        if (~np.isnan(dist)).any():
            j = int(np.nanargmin(dist))
            row["atm_strike"], row["straddle"] = float(ks[j]), float(cl_c[j] + cl_p[j])
        else:
            row["atm_strike"], row["straddle"] = math.nan, math.nan
        out.append(row)
    out.sort(key=lambda r: (r["t"] if r["t"] == r["t"] else math.inf))
    return out


def atm_row(rows: list[dict]) -> dict | None:
    """The atm() row of one bucket from its smile rows."""
    if not rows:
        return None
    front = dict(rows[0])
    if len(rows) > 1:
        front.update(next_expiry=rows[1]["expiry"], next_t=rows[1]["t"], next_atm_iv=rows[1]["atm_iv"])
    else:
        front.update(next_expiry=math.nan, next_t=math.nan, next_atm_iv=math.nan)
    t7 = CM_DAYS / 365.0
    cm = [(r["t"], r["atm_iv"]) for r in rows if r["t"] >= MIN_T_IV7 and r["atm_iv"] == r["atm_iv"]]
    cm.sort(key=lambda x: x[0])
    below = [x for x in cm if x[0] <= t7]
    above = [x for x in cm if x[0] >= t7]
    a = below[-1] if below else None
    b = above[0] if above else None
    iv7, how = math.nan, None
    if a and b:
        span = b[0] - a[0]
        if span > 0:
            wa, wb = a[1] ** 2 * a[0], b[1] ** 2 * b[0]
            w = wa + (wb - wa) * (t7 - a[0]) / span
            iv7, how = math.sqrt(max(w, 0.0) / t7), "interp"
        else:
            iv7, how = a[1], "exact"
    elif a:
        iv7, how = a[1], "flat"
    elif b:
        iv7, how = b[1], "flat"
    front.update(iv7=iv7, iv7_how=how)
    return front


def frames(rows: list[dict], atm: dict | None) -> tuple[pd.DataFrame, pd.DataFrame]:
    sm = pd.DataFrame(rows, columns=SMILE_COLS)
    at = pd.DataFrame([atm], columns=ATM_COLS) if atm else pd.DataFrame(columns=ATM_COLS)
    return sm, at
