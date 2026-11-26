"""
python -m backtester run --config backtester/examples/fut_noise_breakout.toml --from 2024-11-01 --to 2026-11-20 [--lake L] [--store S] [--out logs/backtests]
python -m backtester spreads [--underlying NIFTY] [--from 2026-11-03] [--to D]     half-spreads from the tick era's quotes, for `spread = "measured"`

Replays a strategy over history (SCRUM-217) and writes trades.csv, daily.csv, skipped.csv and summary.json to <out>/<name>-<from>-<to>/, then prints
the summary.  Reads the lake and the signal store, writes nothing else.  Heavy over long ranges: not between 03:40 and 10:05 UTC on a weekday.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="python -m backtester", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--config", required=True)
    r.add_argument("--from", dest="d0", type=date.fromisoformat, required=True)
    r.add_argument("--to", dest="d1", type=date.fromisoformat, required=True)
    r.add_argument("--lake", default=None)
    r.add_argument("--store", default=None)
    r.add_argument("--out", default="logs/backtests")
    r.add_argument("--force", action="store_true", help="run during the market session (03:40-10:05 UTC on a weekday)")
    sp = sub.add_parser("spreads", help="measure half-spreads from the tick era's quotes -> <store>/reports/spreads-<U>.parquet")
    sp.add_argument("--underlying", default="NIFTY")
    sp.add_argument("--from", dest="d0", type=date.fromisoformat, default=date(2026, 11, 3))
    sp.add_argument("--to", dest="d1", type=date.fromisoformat, default=None)
    sp.add_argument("--lake", default=None)
    sp.add_argument("--store", default=None)
    sp.add_argument("--force", action="store_true")
    a = ap.parse_args(argv)
    now = datetime.now(timezone.utc)
    if not a.force and now.weekday() < 5 and (3 * 60 + 40) <= now.hour * 60 + now.minute < (10 * 60 + 5):
        print("refused: heavy job during the market session (03:40-10:05 UTC); use --force to override", file=sys.stderr)
        return 3
    from backtester import report, spec
    from backtester.engine import Backtest
    from data_layer.lake import LAKE_DIR
    lake = Path(a.lake or os.getenv("JARVIS_LAKE") or LAKE_DIR)
    store = Path(a.store or os.getenv("JARVIS_SIGNAL_STORE") or "store")
    if a.cmd == "spreads":
        from backtester import spreads
        df = spreads.measure(lake, a.underlying, a.d0, a.d1 or now.date())
        out = spreads.table_path(store, a.underlying)
        out.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(out, index=False)
        top = df[df[["premium", "moneyness", "dte", "tod"]].isna().all(axis=1)] if len(df) else df
        print(top.to_string(index=False) if len(top) else "no quotes in the range")
        print(f"written: {out} ({len(df)} buckets)", file=sys.stderr)
        return 0
    st = spec.load(a.config)
    bt = Backtest(st, lake, store, a.d0, a.d1)
    res = bt.run(log=lambda d, n: print(f"{d}: {n} trades so far", file=sys.stderr, flush=True))
    folder = report.write(res, Path(a.out) / f"{st.name}-{a.d0}-{a.d1}")
    print(json.dumps(report.summary(res), indent=1, default=str))
    print(f"written: {folder}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
