"""
Synthetic 1-second demo data for the viewer: NOT market data.

  python -m webapp.demo --out lake_demo --days 3
  python -m webapp --lake lake_demo

Writes lake_demo/bars/interval=1s/date=YYYY-MM-DD/bars.parquet in the same schema the collector uses and a
`DEMO` marker file, which makes the viewer show a red "synthetic" banner.  The generator has the shapes you
would expect from a real day (U-shaped volume, volatility clustering, tick-rounded books, spreads that widen
when volatility rises, a book imbalance that weakly predicts the next move) so every chart can be exercised.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

SESSION = 22_500
TICK = 0.05
# symbol, start price, activity (share of seconds with an update), annual vol, typical book size, spread widening
STOCKS = [
    ("RELIANCE.NS", 2450.0, 0.80, 0.22, 900, 0.3), ("HDFCBANK.NS", 1650.0, 0.78, 0.20, 1200, 0.3),
    ("INFY.NS", 1520.0, 0.70, 0.24, 800, 0.4), ("TCS.NS", 3900.0, 0.55, 0.21, 300, 0.6),
    ("ICICIBANK.NS", 1180.0, 0.75, 0.23, 1500, 0.3), ("ITC.NS", 440.0, 0.65, 0.18, 4000, 0.4),
    ("WIPRO.NS", 480.0, 0.40, 0.26, 3000, 0.9), ("AXISBANK.NS", 1100.0, 0.60, 0.27, 1000, 0.6),
    ("MARUTI.NS", 12500.0, 0.25, 0.25, 60, 1.6), ("BAJFINANCE.NS", 7000.0, 0.35, 0.33, 150, 1.2),
]


def weekdays_ending(end: date, n: int) -> list[date]:
    out, d = [], end
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d -= timedelta(days=1)
    return out[::-1]


def simulate(symbol, price0, activity, ann_vol, size, widen, day: date, rng: np.random.Generator) -> pd.DataFrame:
    t = np.arange(SESSION)
    u = 1 + 2.4 * np.exp(-t / 1500) + 1.8 * np.exp(-(SESSION - t) / 1500)        # U-shaped intraday activity
    u = u / u.mean()
    # volatility regime: slow AR(1) in log space, scaled by the intraday shape
    z = np.zeros(SESSION); eps = rng.normal(0, 0.008, SESSION)
    for i in range(1, SESSION):
        z[i] = 0.9995 * z[i - 1] + eps[i]
    regime = np.exp(z - z.var() / 2)
    sig = ann_vol / np.sqrt(250 * SESSION) * regime * np.sqrt(u)
    imb = np.zeros(SESSION); e2 = rng.normal(0, 0.25, SESSION)
    for i in range(1, SESSION):
        imb[i] = 0.97 * imb[i - 1] + e2[i]
    imb = np.clip(imb, -2.5, 2.5)
    ret = rng.normal(0, 1, SESSION) * sig + 0.0 * sig
    ret[1:] += 0.12 * sig[1:] * np.tanh(imb[:-1])                                # imbalance weakly predicts the move
    mid = price0 * np.exp(np.cumsum(ret))

    active = rng.random(SESSION) < np.clip(activity * u, 0, 0.98)
    idx = np.nonzero(active)[0]
    n = len(idx)
    m = mid[idx]
    spread_ticks = 1 + rng.poisson(widen * regime[idx] * (1.0 + 1500.0 / np.maximum(size, 1) * 0.2), n)
    spread_ticks = np.minimum(spread_ticks, 12)
    bid = np.floor((m - spread_ticks * TICK / 2) / TICK) * TICK
    ask = bid + spread_ticks * TICK
    bid_qty = np.maximum(1, (size * np.exp(0.7 * imb[idx] + rng.normal(0, 0.45, n)))).astype(np.int64)
    ask_qty = np.maximum(1, (size * np.exp(-0.7 * imb[idx] + rng.normal(0, 0.45, n)))).astype(np.int64)

    buy = rng.random(n) < 0.5 + 0.1 * np.tanh(imb[idx])
    close = np.where(buy, ask, bid)
    prev = np.concatenate(([m[0]], close[:-1]))
    open_ = np.round(prev / TICK) * TICK
    wiggle = np.abs(rng.normal(0, 1, n)) * sig[idx] * m * 0.6
    high = np.maximum(open_, close) + np.round(wiggle / TICK) * TICK
    low = np.minimum(open_, close) - np.round(rng.permutation(wiggle) / TICK) * TICK
    n_ticks = 1 + rng.poisson(1.2 * u[idx] * (0.5 + activity), n)
    volume = np.maximum(1, (n_ticks * np.exp(rng.normal(np.log(size / 6), 0.8, n)))).astype(np.int64)
    vwap = (open_ + close + high + low) / 4
    ts = datetime(day.year, day.month, day.day, 3, 45) + pd.to_timedelta(idx, unit="s")
    return pd.DataFrame({
        "ts": pd.to_datetime(ts), "symbol": symbol, "market": "IN", "open": open_, "high": high, "low": low,
        "close": close, "volume": volume, "vwap": vwap, "n_ticks": n_ticks.astype("int64"),
        "bid": bid, "ask": ask, "bid_qty": bid_qty, "ask_qty": ask_qty, "src": "demo"})


def write_demo(out: Path, days: int = 3, end: date | None = None, seed: int = 11) -> list[date]:
    end = end or date(2026, 10, 23)
    ds = weekdays_ending(end, days)
    con = duckdb.connect()
    for di, d in enumerate(ds):
        frames = []
        for si, (sym, p0, act, vol, size, widen) in enumerate(STOCKS):
            rng = np.random.default_rng([seed, d.toordinal(), si])                # same date + seed -> same data, whatever `days` is
            drift = 1 + 0.004 * ((d.toordinal() % 5) - 2)                         # a different starting level per date
            frames.append(simulate(sym, p0 * drift, act, vol, size, widen, d, rng))
        df = pd.concat(frames).sort_values(["symbol", "ts"]).reset_index(drop=True)
        for c in ("n_ticks", "bid_qty", "ask_qty"):
            df[c] = df[c].astype("Int64")
        folder = out / "bars" / "interval=1s" / f"date={d}"
        folder.mkdir(parents=True, exist_ok=True)
        con.register("t", df)
        con.execute(f"COPY (SELECT * FROM t) TO '{folder / 'bars.parquet'}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        con.unregister("t")
    (out / "DEMO").write_text("Synthetic data generated by webapp.demo. Not market data.\n")
    return ds


def main() -> None:
    ap = argparse.ArgumentParser(description="Generate synthetic 1-second demo data (not market data)")
    ap.add_argument("--out", type=Path, default=Path("lake_demo"))
    ap.add_argument("--days", type=int, default=3)
    ap.add_argument("--end", type=date.fromisoformat, default=None)
    a = ap.parse_args()
    ds = write_demo(a.out, a.days, a.end)
    print(f"wrote {len(ds)} synthetic days ({ds[0]} to {ds[-1]}) for {len(STOCKS)} stocks to {a.out}")


if __name__ == "__main__":
    main()
