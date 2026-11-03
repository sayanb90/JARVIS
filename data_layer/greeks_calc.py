"""
Option Greeks from implied volatility (Black-Scholes), for the Greeks the feed does not store (SCRUM-108).

Production stores IV, delta and gamma per tick (JARVIS_GREEKS=core).  Theta, vega and rho are functions of IV, the
underlying price, the strike, the time to expiry and a rate, so the viewer recomputes them here.

  python -m data_layer.greeks_calc calibrate --stream probe_data/stream_fo     # run on the VPS against a day that stored all six

`calibrate` compares what this module computes with the values Upstox delivered for near-the-money options, tries a few
interest rates and unit scalings (theta per day or per year, vega per volatility point or per 1.0, rho per 1% or per 1.0),
prints the median and 95th-percentile relative error of the best setting for each Greek, and exits non-zero when delta,
theta or vega are off by more than the tolerance (default 2%) or gamma by more than 0.00005 (Upstox rounds gamma to four decimals).  If it fails, keep JARVIS_GREEKS=all.

Conventions (what the webapp shows): IV in percent (the feed stores it as a fraction; `iv_to_percent`), theta per calendar day, vega per volatility point, rho per 1% of rate,
expiry at 15:30 IST, continuous compounding, no dividend.  The underlying is the spot implied by the nearest future,
spot = future * exp(-carry * time to the future's expiry), because a weekly option's moneyness is sensitive to the monthly
future's carry.  The rate, the carry and the IV unit come from JARVIS_GREEKS_RATE, JARVIS_GREEKS_CARRY and JARVIS_GREEKS_IV_SCALE (environment or .env) when set,
otherwise the defaults above; `calibrate` prints the values that fit Upstox's own numbers best.
"""

from __future__ import annotations

import argparse
import math
import os
import re
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import duckdb
import numpy as np
import pandas as pd

DEFAULT_RATE = 0.065                       # annual, continuously compounded; confirmed or replaced by `calibrate`
DEFAULT_CARRY = 0.065                      # spot = future price * exp(-carry * years to the future's expiry); `calibrate` fits it
DEFAULT_IV_SCALE = 1.0                     # stored IV * this = a fraction.  Upstox delivers a fraction (0.0195 to 2.63 on 2026-11-02),
                                           # so 1.0; a feed that delivered percent would need 0.01
EXPIRY_UTC = timedelta(hours=10)           # 15:30 IST
YEAR_S = 365.0 * 86400.0
MIN_T = 1e-6                               # years; keeps the formulas finite in the last seconds before expiry

_erf = np.frompyfunc(math.erf, 1, 1)
OPTION_RE = re.compile(r"^(?P<u>.+)_(?P<k>\d+(?:\.\d+)?)_(?P<t>CE|PE)_(?P<d>\d{2})_(?P<m>[A-Z]{3})_(?P<y>\d{2})$")
FUTURE_RE = re.compile(r"^(?P<u>.+)_FUT_(?P<d>\d{2})_(?P<m>[A-Z]{3})_(?P<y>\d{2})$")
COMPUTED = ("theta", "vega", "rho")
GAMMA_ABS_TOLERANCE = 5e-5                # median absolute gamma error that passes; Upstox rounds gamma to 0.0001


def _ncdf(x) -> np.ndarray:
    return 0.5 * (1.0 + np.asarray(_erf(np.asarray(x, dtype=float) / math.sqrt(2.0)), dtype=float))


def _npdf(x) -> np.ndarray:
    x = np.asarray(x, dtype=float)
    return np.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


def _setting(name: str, default: float) -> float:
    v = os.environ.get(name)
    if not v:
        try:
            from dotenv import dotenv_values
            from .lake import ENV_FILE
            v = dotenv_values(ENV_FILE).get(name)
        except Exception:                                                     # noqa: BLE001 -- settings are optional
            v = None
    try:
        return float(v) if v else default
    except ValueError:
        return default


def rate() -> float:
    return _setting("JARVIS_GREEKS_RATE", DEFAULT_RATE)


def iv_scale() -> float:
    return _setting("JARVIS_GREEKS_IV_SCALE", DEFAULT_IV_SCALE)


def iv_to_percent(iv, scale: Optional[float] = None):
    """The stored implied volatility as a percent (what the viewer shows and bs_greeks takes)."""
    return iv * (iv_scale() if scale is None else scale) * 100.0


def carry() -> float:
    return _setting("JARVIS_GREEKS_CARRY", DEFAULT_CARRY)


def spot_from_future(price, ts, fut_expiry: date, carry_rate: Optional[float] = None) -> np.ndarray:
    """The spot implied by a future's price: price * exp(-carry * years to the future's expiry)."""
    c = carry() if carry_rate is None else carry_rate
    return np.asarray(price, dtype=float) * np.exp(-c * years_to_expiry(ts, fut_expiry))


def parse_option(symbol: str) -> Optional[tuple[str, float, str, date]]:
    """NIFTY_25000_CE_03_NOV_26 -> ('NIFTY', 25000.0, 'CE', date(2026, 11, 3)); None for anything else."""
    m = OPTION_RE.match(symbol or "")
    if not m:
        return None
    try:
        exp = datetime.strptime(f"{m['d']} {m['m']} {m['y']}", "%d %b %y").date()
    except ValueError:
        return None
    return m["u"], float(m["k"]), m["t"], exp


def parse_future(symbol: str) -> Optional[tuple[str, date]]:
    m = FUTURE_RE.match(symbol or "")
    if not m:
        return None
    try:
        return m["u"], datetime.strptime(f"{m['d']} {m['m']} {m['y']}", "%d %b %y").date()
    except ValueError:
        return None


def years_to_expiry(ts, expiry: date) -> np.ndarray:
    """Years from each (naive UTC) timestamp to 15:30 IST on `expiry`, never below MIN_T."""
    end = pd.Timestamp(datetime(expiry.year, expiry.month, expiry.day) + EXPIRY_UTC)
    secs = (end - pd.to_datetime(pd.Series(ts)).reset_index(drop=True)).dt.total_seconds().to_numpy()
    return np.maximum(secs / YEAR_S, MIN_T)


def bs_greeks(is_call, under, strike, t, iv_pct, rate: float = DEFAULT_RATE, div: float = 0.0) -> dict[str, np.ndarray]:
    """
    Black-Scholes delta, gamma, theta, vega and rho.  Inputs broadcast: `is_call` bool, `under` and `strike` prices,
    `t` years to expiry, `iv_pct` implied volatility in percent.  Returns RAW units: theta per year, vega per 1.0 of
    volatility, rho per 1.0 of rate -- scale with `to_display`.
    """
    s, k, t, vol = (np.asarray(a, dtype=float) for a in (under, strike, t, np.asarray(iv_pct, dtype=float) / 100.0))
    call = np.asarray(is_call, dtype=bool)
    vol = np.where(vol > 1e-6, vol, np.nan)
    sq = np.sqrt(t)
    d1 = (np.log(s / k) + (rate - div + 0.5 * vol * vol) * t) / (vol * sq)
    d2 = d1 - vol * sq
    disc_q, disc_r = np.exp(-div * t), np.exp(-rate * t)
    pdf = _npdf(d1)
    delta = np.where(call, disc_q * _ncdf(d1), disc_q * (_ncdf(d1) - 1.0))
    gamma = disc_q * pdf / (s * vol * sq)
    vega = s * disc_q * pdf * sq
    common = -s * disc_q * pdf * vol / (2.0 * sq)
    theta = np.where(call,
                     common - rate * k * disc_r * _ncdf(d2) + div * s * disc_q * _ncdf(d1),
                     common + rate * k * disc_r * _ncdf(-d2) - div * s * disc_q * _ncdf(-d1))
    rho = np.where(call, k * t * disc_r * _ncdf(d2), -k * t * disc_r * _ncdf(-d2))
    return {"delta": delta, "gamma": gamma, "theta": theta, "vega": vega, "rho": rho}


# the unit each stored/displayed Greek is expressed in: raw value divided by this
DISPLAY = {"theta": 365.0, "vega": 100.0, "rho": 100.0, "delta": 1.0, "gamma": 1.0}


def to_display(raw: dict[str, np.ndarray]) -> dict[str, np.ndarray]:
    return {g: v / DISPLAY[g] for g, v in raw.items()}


def add_computed(df: pd.DataFrame, symbol: str, under: pd.Series, rate_: Optional[float] = None) -> pd.DataFrame:
    """
    Fill theta, vega and rho (and any other Greek that is entirely missing) in a Greeks series of one option.
    `df` has ts, iv, delta, gamma, theta, vega, rho; `under` is the spot aligned to df's rows (see spot_from_future).  Values
    Upstox delivered are never overwritten.  Returns a copy with a `computed` list attribute in df.attrs.
    """
    out = df.copy()
    meta = parse_option(symbol)
    computed: list[str] = []
    if meta is None or out.empty:
        out.attrs["computed"] = computed
        return out
    _, strike, kind, expiry = meta
    t = years_to_expiry(out["ts"], expiry)
    raw = bs_greeks(kind == "CE", under.to_numpy(dtype=float), strike, t, iv_to_percent(out["iv"].to_numpy(dtype=float)), rate() if rate_ is None else rate_)
    disp = to_display(raw)
    for g in ("delta", "gamma", "theta", "vega", "rho"):
        if g not in out.columns or out[g].isna().all():
            out[g] = disp[g]
            computed.append(g)
    out.attrs["computed"] = computed
    return out


# ---------------------------------------------------------------------------------------------- calibration
def _read_rows(stream: Path, max_rows: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    root = stream / "ticks" if (stream / "ticks").is_dir() else stream
    files = sorted(root.rglob("*.parquet"))
    if not files:
        raise SystemExit(f"no parquet files under {root}")
    src = f"read_parquet({[str(f) for f in files]!r}, union_by_name=true, hive_partitioning=false)"
    con = duckdb.connect()
    have = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {src}").fetchall()}
    need = {"ts", "symbol", "ltp", "iv", "delta", "gamma", "theta", "vega", "rho"}
    if not need <= have:
        raise SystemExit(f"these files lack {sorted(need - have)}: calibrate needs a day that stored all six Greeks "
                         "(JARVIS_GREEKS=all)")
    n = con.execute(f"SELECT count(*) FROM {src} WHERE iv IS NOT NULL AND theta IS NOT NULL").fetchone()[0]
    step = max(1, math.ceil(n / max_rows))
    opts = con.execute(f"""SELECT ts, symbol, ltp, iv, delta, gamma, theta, vega, rho FROM {src}
                           WHERE iv IS NOT NULL AND theta IS NOT NULL AND delta IS NOT NULL
                             AND hash(symbol, ts) % {step} = 0""").df()
    futs = con.execute(f"SELECT ts, symbol, ltp FROM {src} WHERE symbol LIKE '%\\_FUT\\_%' ESCAPE '\\' AND ltp IS NOT NULL").df()
    opts["ts"], futs["ts"] = pd.to_datetime(opts["ts"]), pd.to_datetime(futs["ts"])
    opts.attrs["total_rows"] = int(n)
    return opts, futs


def _prepare(opts: pd.DataFrame, futs: pd.DataFrame, near: float) -> pd.DataFrame:
    meta = opts["symbol"].map(parse_option)
    opts = opts[meta.notna()].copy()
    meta = meta[meta.notna()]
    opts["u"], opts["k"] = [m[0] for m in meta], [m[1] for m in meta]
    opts["call"], opts["exp"] = [m[2] == "CE" for m in meta], [m[3] for m in meta]
    fm = futs["symbol"].map(parse_future)
    futs = futs[fm.notna()].copy()
    futs["u"], futs["fexp"] = [m[0] for m in fm[fm.notna()]], [m[1] for m in fm[fm.notna()]]
    futs = futs.sort_values("ts")[["ts", "u", "ltp", "fexp"]].rename(columns={"ltp": "fut"})
    joined = pd.merge_asof(opts.sort_values("ts"), futs, on="ts", by="u", direction="backward").dropna(subset=["fut"])
    joined = joined[(joined["k"] / joined["fut"] - 1.0).abs() <= near].reset_index(drop=True)
    t, tf = np.empty(len(joined)), np.empty(len(joined))
    for col, out in (("exp", t), ("fexp", tf)):
        for e, idx in joined.groupby(col).indices.items():
            out[idx] = years_to_expiry(joined["ts"].iloc[idx], e)
    joined["t"], joined["tf"] = t, tf
    return joined[joined["t"] > 5.0 / 365.0 / 24.0 / 12.0].reset_index(drop=True)      # skip the last few minutes before expiry


def _errors(calc: np.ndarray, ref: np.ndarray) -> tuple[float, float, int]:
    ok = np.isfinite(calc) & np.isfinite(ref)
    if ok.sum() == 0:
        return float("inf"), float("inf"), 0
    calc, ref = calc[ok], ref[ok]
    big = np.abs(ref) > 0.05 * np.median(np.abs(ref))
    if big.sum() == 0:
        return float("inf"), float("inf"), 0
    rel = np.abs(calc[big] - ref[big]) / np.abs(ref[big])
    return float(np.median(rel)), float(np.percentile(rel, 95)), int(big.sum())


def calibrate(stream: Path, max_rows: int = 100_000, near: float = 0.05, tolerance: float = 0.02,
              rates: tuple = (0.0, 0.05, 0.065, 0.08), carries: tuple = (0.0, 0.02, 0.04, 0.05, 0.06, 0.065, 0.07, 0.08, 0.1)) -> dict:
    """Best (rate, carry, units) per Greek against Upstox's own values.  Returns a dict with `ok`, `rate`, `carry`, `greeks`, `rows`."""
    opts, futs = _read_rows(stream, max_rows)
    d = _prepare(opts, futs, near)
    if d.empty:
        raise SystemExit("no near-the-money option rows with a matching future price; check --stream and --near")
    call, k, t, iv_raw = d["call"].to_numpy(), d["k"].to_numpy(), d["t"].to_numpy(), d["iv"].to_numpy(dtype=float)
    fut, tf = d["fut"].to_numpy(), d["tf"].to_numpy()
    delta_ref, gamma_ref = d["delta"].to_numpy(), d["gamma"].to_numpy()

    def trial(scale: float, c: float, r: float):
        under = fut * np.exp(-c * tf)
        raw = bs_greeks(call, under, k, t, iv_to_percent(iv_raw, scale), r)
        return _errors(raw["delta"], delta_ref)[0] + _errors(raw["gamma"], gamma_ref)[0], raw

    scales = {s_: trial(s_, DEFAULT_CARRY, DEFAULT_RATE)[0] for s_ in (1.0, 0.01)}               # fraction or percent?
    iv_unit = min(scales, key=scales.get)
    best = None
    for c in carries:
        for r in rates:
            err, raw = trial(iv_unit, c, r)
            if best is None or err < best[0]:
                best = (err, r, c, raw)
    _, rate, carry_, raw = best
    greeks = {}
    for g in ("delta", "gamma", "theta", "vega", "rho"):
        choices = [(1.0, "as is")] if g in ("delta", "gamma") else [
            (DISPLAY[g], {"theta": "per day", "vega": "per vol point", "rho": "per 1%"}[g]),
            (1.0, "per year" if g == "theta" else "per 1.0")]
        scored = []
        for scale, label in choices:
            med, p95, n = _errors(raw[g] / scale, d[g].to_numpy())
            scored.append((med, p95, n, scale, label))
        med, p95, n, scale, label = min(scored, key=lambda r: r[0])
        greeks[g] = {"median_rel_error": med, "p95_rel_error": p95, "rows": n, "unit": label, "scale": scale}
    # Gamma is stored with four decimals (48 distinct values over 2.65 million rows on 2026-11-02), so a relative error on
    # small gammas measures the rounding, not the formula: judge it by absolute error, half of the 0.0001 step.
    gamma_ref_ok = np.isfinite(raw["gamma"]) & np.isfinite(gamma_ref)
    greeks["gamma"]["median_abs_error"] = float(np.median(np.abs(raw["gamma"][gamma_ref_ok] - gamma_ref[gamma_ref_ok]))) if gamma_ref_ok.any() else float("inf")
    judged = ("delta", "theta", "vega")
    ok = all(greeks[g]["median_rel_error"] <= tolerance for g in judged) and greeks["gamma"]["median_abs_error"] <= GAMMA_ABS_TOLERANCE
    stored = {g: {"distinct": int(pd.Series(d[g]).nunique()), "min": float(d[g].min()), "max": float(d[g].max())}
              for g in ("iv", "delta", "gamma", "theta", "vega", "rho")}
    return {"ok": ok, "rate": rate, "carry": carry_, "iv_scale": iv_unit, "tolerance": tolerance, "greeks": greeks, "rows": len(d),
            "total_rows": opts.attrs["total_rows"], "stored": stored}


def render(res: dict) -> str:
    lines = [f"Calibration on {res['rows']:,} near-the-money rows (sampled from {res['total_rows']:,}); best rate {res['rate']:.3f}, future carry {res['carry']:.3f}, "
             f"IV stored as {'a fraction' if res['iv_scale'] == 1.0 else 'a percent'}",
             f"{'greek':<7}{'median error':>14}{'95th pct':>11}{'rows':>9}  unit that matched"]
    for g, r in res["greeks"].items():
        extra = f"  (judged by absolute error: median {r['median_abs_error']:.6f}, limit {GAMMA_ABS_TOLERANCE:g})" if "median_abs_error" in r else ""
        lines.append(f"{g:<7}{r['median_rel_error'] * 100:>13.2f}%{r['p95_rel_error'] * 100:>10.2f}%{r['rows']:>9,}  {r['unit']}{extra}")
    lines.append("\nstored values in the sample (distinct values, min, max):")
    lines += [f"  {g:<6}{v['distinct']:>9,}  {v['min']:.6g}  {v['max']:.6g}" for g, v in res["stored"].items()]
    lines.append(f"\n{'PASS' if res['ok'] else 'FAIL'}: delta, theta and vega must each be within {res['tolerance'] * 100:.0f}% (median) and gamma within {GAMMA_ABS_TOLERANCE:g} (absolute)."
                 + (f"\nSet in .env: JARVIS_GREEKS_RATE={res['rate']:g}  JARVIS_GREEKS_CARRY={res['carry']:g}  JARVIS_GREEKS_IV_SCALE={res['iv_scale']:g}" if res["ok"]
                    else "  Keep JARVIS_GREEKS=all until this passes."))
    return "\n".join(lines)


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Compare computed option Greeks with the values Upstox delivered")
    sub = ap.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("calibrate", help="check theta, vega and rho (and delta, gamma) against a day that stored all six")
    c.add_argument("--stream", type=Path, required=True, help="directory holding ticks/ (for example probe_data/stream_fo) or the ticks folder itself")
    c.add_argument("--max-rows", type=int, default=100_000)
    c.add_argument("--near", type=float, default=0.05, help="only strikes within this fraction of the future price")
    c.add_argument("--tolerance", type=float, default=0.02)
    a = ap.parse_args(argv)
    res = calibrate(a.stream, a.max_rows, a.near, a.tolerance)
    print(render(res))
    return 0 if res["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
