"""
Which cadence can each live signal afford?  `python -m signals feasibility [--contracts 2000] [--minutes 30]`

Replays a synthetic session (signals/synthetic.py, about 2,000 option contracts at a 10-point strike step) through engine v2 (default;
--engine runner for the first engine) at 1 s, 10 s and 1 min, with every live signal at that cadence, and reports per signal the mean time per cycle and the share of the
cadence it uses.  The shared builds (option chain, smile, tick chain) are reported once per cadence: every signal of a cadence group
shares them.  Synthetic ticks are sparser than a real session (about one per contract a minute), so the numbers are a floor; the
first live session (SCRUM-200) gives the real ones.
"""

from __future__ import annotations

import time
from typing import Optional

import pandas as pd

from signals import cadence as cadence_mod
from signals.live_runner import LiveRunner, live_signal_ids


def synthetic_day(contracts: int = 2000):
    from signals.synthetic import make_raw
    from tests.unit.test_live_runner import split_day, trim
    n_strikes = max(4, contracts // 8)                    # two expiries x calls and puts x (2 n + 1) strikes
    raw = make_raw(n_strikes=n_strikes, step=10, option_days=4, tick_days=1, index_days=120)
    hist, ticks, day = split_day(raw)
    return trim(hist), ticks, day


def measure(hist: dict, ticks: pd.DataFrame, day: pd.Timestamp, bucket: str, minutes: int, ids: Optional[list[str]] = None) -> pd.DataFrame:
    ids = [s for s in (ids or live_signal_ids())
           if cadence_mod.ORDER.index(cadence_mod.MIN_CADENCE.get(s, ("1s", ""))[0]) <= cadence_mod.ORDER.index(bucket)]
    r = LiveRunner("NIFTY", hist, bucket=bucket, ids=ids)
    t = ticks.copy()
    t["fts"] = pd.to_datetime(t["fts"])
    start = t["fts"].min().floor("min")
    t = t[t["fts"] < start + pd.Timedelta(minutes=minutes)]
    step = pd.Timedelta(bucket)
    wall = []
    first = True
    for _, g in t.groupby(t["fts"].dt.floor(step), sort=True):
        r.ingest(g)
        t0 = time.perf_counter()
        r.cycle()
        if first:                                          # the once-a-day start (history, providers) is not a cycle cost
            r.timings, r.cycles_run, first = {}, 0, False
            continue
        wall.append(time.perf_counter() - t0)
    n = max(1, r.cycles_run)
    cad = step.total_seconds()
    rows = [{"cadence": bucket, "item": k, "ms_per_cycle": 1000 * v / n, "share_of_cadence": v / n / cad} for k, v in r.timings.items()]
    df = pd.DataFrame(rows).sort_values("ms_per_cycle", ascending=False)
    total = pd.Series(wall)
    df.attrs.update(cycles=n, p50_ms=1000 * total.median(), p99_ms=1000 * total.quantile(0.99), cadence_s=cad)
    return df


def measure_v2(hist: dict, ticks: pd.DataFrame, day: pd.Timestamp, bucket: str, minutes: int, ids: Optional[list[str]] = None) -> pd.DataFrame:
    """Engine v2 (signals/engine/core.py): time per bucket close, split into the per-tick state (chain snapshots, smile, ATM), the
    shared inputs, the native operators and each adapter signal; plus the per-tick apply cost."""
    import numpy as np
    from signals.engine.core import OperatorEngine
    ids = [s for s in (ids or live_signal_ids())
           if cadence_mod.ORDER.index(cadence_mod.MIN_CADENCE.get(s, ("1s", ""))[0]) <= cadence_mod.ORDER.index(bucket)]
    t = ticks.copy()
    t["fts"] = pd.to_datetime(t["fts"])
    start = t["fts"].min().floor("min")
    t = t[t["fts"] < start + pd.Timedelta(minutes=minutes)].sort_values("fts", kind="stable")
    eng = OperatorEngine("NIFTY", hist, {s: bucket for s in ids}, day)
    wall = []
    grp = eng.groups[bucket]
    for _, g in t.groupby(t["fts"].dt.floor(bucket), sort=True):
        n0 = grp.buckets
        t0 = time.perf_counter()
        eng.on_ticks(g)
        if grp.buckets > n0:                               # per bucket closed (a sparse tick can close several at once)
            wall += [(time.perf_counter() - t0) / (grp.buckets - n0)] * (grp.buckets - n0)
    n = max(1, grp.buckets)
    cad = pd.Timedelta(bucket).total_seconds()
    rows = [{"cadence": bucket, "item": k, "ms_per_cycle": 1000 * v / n, "share_of_cadence": v / n / cad}
            for k, v in grp.timings.items() if k != "total"]
    df = pd.DataFrame(rows).sort_values("ms_per_cycle", ascending=False)
    w = np.array(wall[2:] or wall)
    df.attrs.update(cycles=n, p50_ms=1000 * float(np.median(w)), p99_ms=1000 * float(np.quantile(w, 0.99)), cadence_s=cad,
                    us_per_tick=1e6 * eng.stats["apply_s"] / max(1, eng.stats["ticks"]))
    return df


def report(contracts: int = 2000, minutes: int = 30, cadences=("1min", "10s", "1s"), engine: str = "operators") -> dict:
    hist, ticks, day = synthetic_day(contracts)
    out = {}
    for c in cadences:
        m = minutes if c != "1s" else min(minutes, 5)           # 1 s: 300 cycles is enough to measure
        out[c] = measure_v2(hist, ticks, day, c, m) if engine == "operators" else measure(hist, ticks, day, c, m)
    return out


def to_markdown(res: dict) -> str:
    lines = []
    for c, df in res.items():
        a = df.attrs
        extra = f"; {a['us_per_tick']:.1f} us per tick applied" if "us_per_tick" in a else ""
        lines.append(f"### Cadence {c}: per bucket p50 {a['p50_ms']:.0f} ms, p99 {a['p99_ms']:.0f} ms over {a['cycles']} buckets "
                     f"({100 * a['p50_ms'] / 1000 / a['cadence_s']:.0f} % of the cadence at p50){extra}\n")
        lines.append("| item | ms per cycle | share of cadence |\n| --- | --- | --- |")
        for _, r in df.iterrows():
            lines.append(f"| {r['item']} | {r['ms_per_cycle']:.1f} | {100 * r['share_of_cadence']:.1f} % |")
        lines.append("")
    return "\n".join(lines)
