"""
Option Greeks from implied volatility (Black-Scholes), for the Greeks the feed does not store (SCRUM-108).

Production stores IV, delta and gamma per tick (JARVIS_GREEKS=core).  Theta, vega and rho are functions of IV, the
underlying price, the strike, the time to expiry and a rate, so the viewer recomputes them here.

  python -m data_layer.greeks_calc calibrate --stream probe_data/stream_fo     # run on the VPS against a day that stored all six

`calibrate` compares what this module computes with the values Upstox delivered for near-the-money options, tries a few
interest rates and unit scalings (theta per day or per year, vega per volatility point or per 1.0, rho per 1% or per 1.0),
prints the median and 95th-percentile relative error of the best setting for each Greek, and exits non-zero when delta,
gamma, theta or vega are off by more than the tolerance (default 2%).  If it fails, keep JARVIS_GREEKS=all.

Conventions (what the webapp shows): IV in percent, theta per calendar day, vega per volatility point, rho per 1% of rate,
expiry at 15:30 IST, underlying = price of the nearest future, continuous compounding, no dividend.
"""

from __future__ import annotations

import argparse
import itertools
import math
import re
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import duckdb
import numpy as np
import pandas as pd

DEFAULT_RATE = 0.065                       # annual, continuously compounded; confirmed or replaced by `calibrate`
EXPIRY_UTC = timedelta(hours=10)           # 15:30 IST
YEAR_S = 365.0 * 86400.0
MIN_T = 1e-6                               # years; keeps the formulas finite in the last seconds before expiry

_erf = np.frompyfunc(math.erf, 1, 1)
OPTION_RE = re.compile(r"^(?P<u>.+)_(?P<k>\d+(?:\.\d+)?)_(?P<t>CE|PE)_(?P<d>\d{2})_(?P<m>[A-Z]{3})_(?P<y>\d{2})$")
FUTURE_RE = re.compile(r"^(?P<u>.+)_FUT_(?P<d>\d{2})_(?P<m>[A-Z]{3})_(?P<y>\d{2})$")
COMPUTED = ("theta", "vega", "rho")


def _ncdf(x) -> np.ndarray:
    return 0.5 * (1.0 + np.asarray(_erf(np.asarray(x, dtype=float) / math.sqrt(2.0)), dtype=float))


def _npdf(x) -> np.ndarray:
    x = np.asarray(x, dtype=float)
    return np.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


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


def add_computed(df: pd.DataFrame, symbol: str, under: pd.Series, rate: float = DEFAULT_RATE) -> pd.DataFrame:
    """
    Fill theta, vega and rho (and any other Greek that is entirely missing) in a Greeks series of one option.
    `df` has ts, iv, delta, gamma, theta, vega, rho; `under` is the underlying price aligned to df's rows.  Values
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
    raw = bs_greeks(kind == "CE", under.to_numpy(dtype=float), strike, t, out["iv"].to_numpy(dtype=float), rate)
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
    futs["u"] = [m[0] for m in fm[fm.notna()]]
    futs = futs.sort_values("ts")[["ts", "u", "ltp"]].rename(columns={"ltp": "under"})
    joined = pd.merge_asof(opts.sort_values("ts"), futs, on="ts", by="u", direction="backward").dropna(subset=["under"])
    joined = joined[(joined["k"] / joined["under"] - 1.0).abs() <= near]
    t = np.empty(len(joined))
    for exp, idx in joined.groupby("exp").indices.items():
        t[idx] = years_to_expiry(joined["ts"].iloc[idx], exp)
    joined["t"] = t
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


def calibrate(stream: Path, max_rows: int = 200_000, near: float = 0.05, tolerance: float = 0.02,
              rates: tuple = (0.0, 0.05, 0.06, 0.065, 0.07, 0.08)) -> dict:
    """Best (rate, units) per Greek against Upstox's own values.  Returns a dict with `ok`, `rate`, `greeks` and `rows`."""
    opts, futs = _read_rows(stream, max_rows)
    d = _prepare(opts, futs, near)
    if d.empty:
        raise SystemExit("no near-the-money option rows with a matching future price; check --stream and --near")
    best = None
    for rate in rates:
        raw = bs_greeks(d["call"].to_numpy(), d["under"].to_numpy(), d["k"].to_numpy(), d["t"].to_numpy(), d["iv"].to_numpy(), rate)
        err = _errors(raw["delta"], d["delta"].to_numpy())[0] + _errors(raw["gamma"], d["gamma"].to_numpy())[0]
        if best is None or err < best[0]:
            best = (err, rate, raw)
    _, rate, raw = best
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
    judged = ("delta", "gamma", "theta", "vega")
    ok = all(greeks[g]["median_rel_error"] <= tolerance for g in judged)
    return {"ok": ok, "rate": rate, "tolerance": tolerance, "greeks": greeks, "rows": len(d), "total_rows": opts.attrs["total_rows"]}


def render(res: dict) -> str:
    lines = [f"Calibration on {res['rows']:,} near-the-money rows (sampled from {res['total_rows']:,}); best rate {res['rate']:.3f}",
             f"{'greek':<7}{'median error':>14}{'95th pct':>11}{'rows':>9}  unit that matched"]
    for g, r in res["greeks"].items():
        lines.append(f"{g:<7}{r['median_rel_error'] * 100:>13.2f}%{r['p95_rel_error'] * 100:>10.2f}%{r['rows']:>9,}  {r['unit']}")
    lines.append(f"\n{'PASS' if res['ok'] else 'FAIL'}: delta, gamma, theta and vega must each be within {res['tolerance'] * 100:.0f}% (median)."
                 + ("" if res["ok"] else "  Keep JARVIS_GREEKS=all until this passes."))
    return "\n".join(lines)


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Compare computed option Greeks with the values Upstox delivered")
    sub = ap.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("calibrate", help="check theta, vega and rho (and delta, gamma) against a day that stored all six")
    c.add_argument("--stream", type=Path, required=True, help="directory holding ticks/ (for example probe_data/stream_fo) or the ticks folder itself")
    c.add_argument("--max-rows", type=int, default=200_000)
    c.add_argument("--near", type=float, default=0.05, help="only strikes within this fraction of the future price")
    c.add_argument("--tolerance", type=float, default=0.02)
    a = ap.parse_args(argv)
    res = calibrate(a.stream, a.max_rows, a.near, a.tolerance)
    print(render(res))
    return 0 if res["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
