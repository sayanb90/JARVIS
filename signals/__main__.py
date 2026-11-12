"""
Signal runner (SCRUM-146).

  python -m signals list [--family F]
  python -m signals run --lake LAKE --root STORE --underlying NIFTY --from 2022-11-01 --to 2026-11-12 [--ids a,b] [--family F]
                        [--no-chain-store] [--no-live]
  python -m signals validate-iv --lake LAKE --root STORE --underlying NIFTY --from 2026-11-03 --to 2026-11-10
  python -m signals report day-night --root STORE --underlying NIFTY
  python -m signals smoke [--root STORE]           # the whole catalogue on the synthetic market (no lake needed)

`run` works in two passes so four years of option minutes never sit in memory at once:
  pass 1, month by month: the option chain (with CHAIN_WARMUP days before the month for signals that look back over sessions), the
          per-minute ATM table, and every signal that reads the chain or the ticks; written to the store, the ATM table under derived/atm
  pass 2, the whole range at once: index, VIX, futures, participant and event data (small), the ATM table read back from the store, and
          the pass-1 outputs injected, for every other signal
Every run is appended to STORE/trials.jsonl.  LAKE may be the production lake or a research snapshot (python -m research.snapshot); the
runner only reads it.  STORE must be outside the lake.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

from signals import catalogue
from signals.framework import REGISTRY, Context, log_trial, read_output, specs, write_output

PASS1_INPUTS = {"chain", "ticks_fo", "fills"}
CHAIN_WARMUP = {"option_volume": 35, "spread_gate": 35, "oi_quadrant": 5, "day_night_returns": 5, "pin_test": 1, "volume_burst": 1}
DEFAULT_WARMUP = 3
INDEX_HISTORY_FROM = date(2022, 1, 3)


def is_pass1(sid: str) -> bool:
    return bool(set(REGISTRY[sid][0].inputs) & PASS1_INPUTS)


def months(d0: date, d1: date) -> list[tuple[date, date]]:
    out, a = [], date(d0.year, d0.month, 1)
    while a <= d1:
        b = date(a.year + (a.month // 12), a.month % 12 + 1, 1) - timedelta(days=1)
        out.append((max(a, d0), min(b, d1)))
        a = b + timedelta(days=1)
    return out


def _within(df: pd.DataFrame, key: str, a: date, b: date) -> pd.DataFrame:
    if df is None or not len(df):
        return df
    d = pd.to_datetime(df[key]).dt.date
    return df[(d >= a) & (d <= b)]


def _write_derived(root: Path, name: str, underlying: str, df: pd.DataFrame) -> int:
    if df is None or not len(df):
        return 0
    days = pd.to_datetime(df["ts"]).dt.date
    for d, part in df.groupby(days):
        folder = Path(root) / "derived" / name / f"underlying={underlying}" / f"date={d}"
        folder.mkdir(parents=True, exist_ok=True)
        tmp = folder / "part.parquet.tmp"
        part.to_parquet(tmp, index=False)
        tmp.replace(folder / "part.parquet")
    return len(df)


def _read_derived(root: Path, name: str, underlying: str, d0: date, d1: date) -> pd.DataFrame:
    base = Path(root) / "derived" / name / f"underlying={underlying}"
    files = [f for f in sorted(base.glob("date=*/part.parquet")) if d0 <= date.fromisoformat(f.parent.name[5:]) <= d1]
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True) if files else pd.DataFrame()


def run(lake: Path, root: Path, underlying: str, d0: date, d1: date, ids: Optional[list[str]] = None, family: Optional[str] = None,
        chain_store: bool = True, live: bool = True, provenance: Optional[dict] = None, log=print) -> dict:
    from signals.lakeio import load_raw
    lake, root = Path(lake).resolve(), Path(root).resolve()
    if root == lake or lake in root.parents:
        raise SystemExit("the store must be outside the lake")
    chosen = [s.id for s in specs(family=family) if (not ids or s.id in ids)]
    if not live:
        chosen = [s for s in chosen if not REGISTRY[s][0].live_only]
    p1 = [s for s in chosen if is_pass1(s)]
    p2 = [s for s in chosen if not is_pass1(s)]
    stats: dict[str, int] = {}
    warm = max([CHAIN_WARMUP.get(s, DEFAULT_WARMUP) for s in p1] + [DEFAULT_WARMUP])
    for a, b in months(d0, d1):
        raw = load_raw(lake, underlying, a - timedelta(days=warm + 10), b, option_from=a - timedelta(days=warm), ticks=live)
        ctx = Context(underlying, raw)
        if raw["opt_1m"] is not None and len(raw["opt_1m"]):
            if chain_store:
                _write_derived(root, "option_chain", underlying, _within(ctx.get("chain"), "ts", a, b))
            _write_derived(root, "atm", underlying, _within(ctx.get("atm"), "ts", a, b))
        for sid in p1:
            spec = REGISTRY[sid][0]
            key = "ts" if spec.grid == "1m" else "date"
            out = _within(ctx.compute(sid), key, a, b)
            n = write_output(root, spec, out)
            stats[sid] = stats.get(sid, 0) + n
        log(f"pass 1 {underlying} {a}..{b}: {sum(stats.values()):,} rows so far")
    raw = load_raw(lake, underlying, min(d0, INDEX_HISTORY_FROM), d1, option_from=d1 + timedelta(days=1), ticks=False)
    atm = _read_derived(root, "atm", underlying, d0, d1)
    injected = {s.id: read_output(root, s.id, underlying) for s in specs() if is_pass1(s.id)}
    ctx = Context(underlying, raw, cache={"atm": atm} if len(atm) else {"atm": pd.DataFrame()}, signals={k: v for k, v in injected.items() if len(v)})
    for sid in p2:
        spec = REGISTRY[sid][0]
        key = "ts" if spec.grid == "1m" else "date"
        out = _within(ctx.compute(sid), key, d0, d1)
        stats[sid] = write_output(root, spec, out)
    for sid in chosen:
        log_trial(root, REGISTRY[sid][0], underlying, d0, d1, stats.get(sid, 0), provenance)
    log(f"pass 2 {underlying}: {len(p2)} signals; total {sum(stats.values()):,} rows")
    return stats


def validate_iv(lake: Path, root: Path, underlying: str, d0: date, d1: date) -> dict:
    from signals.lakeio import load_raw
    from signals.options.validate import compare_iv, gate
    raw = load_raw(lake, underlying, d0 - timedelta(days=5), d1, option_from=d0)
    m = compare_iv(Context(underlying, raw))
    res = gate(m)
    res.update(underlying=underlying, first=str(d0), last=str(d1))
    out = Path(root) / "reports" / f"iv_gate_{underlying}_{d0}_{d1}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(res, indent=1, default=str))
    return res


def smoke(root: Optional[Path] = None) -> dict:
    """Every signal on the synthetic market; returns {id: rows}.  Writes to `root` when given."""
    from signals.synthetic import make_raw
    ctx = Context("NIFTY", make_raw())
    out = {}
    for s in specs():
        df = ctx.compute(s.id)
        out[s.id] = len(df)
        if root:
            write_output(root, s, df)
    return out


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m signals", description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    ls = sub.add_parser("list")
    ls.add_argument("--family")
    for name in ("run", "validate-iv"):
        r = sub.add_parser(name)
        r.add_argument("--lake", type=Path, required=True)
        r.add_argument("--root", type=Path, required=True)
        r.add_argument("--underlying", choices=("NIFTY", "BANKNIFTY"), required=True)
        r.add_argument("--from", dest="d0", type=date.fromisoformat, required=True)
        r.add_argument("--to", dest="d1", type=date.fromisoformat, required=True)
        if name == "run":
            r.add_argument("--ids", default="")
            r.add_argument("--family")
            r.add_argument("--no-chain-store", action="store_true")
            r.add_argument("--no-live", action="store_true")
    rp = sub.add_parser("report")
    rp.add_argument("what", choices=("day-night",))
    rp.add_argument("--root", type=Path, required=True)
    rp.add_argument("--underlying", required=True)
    sm = sub.add_parser("smoke")
    sm.add_argument("--root", type=Path)
    a = ap.parse_args(argv)
    if a.cmd == "list":
        for s in specs(family=a.family):
            flags = ",".join(x for x in (s.kind if s.kind != "signal" else "", "live" if s.live_only else "") if x)
            print(f"{s.family:12} {s.id:26} v{s.version} {s.grid:6} {flags:12} {','.join(s.tickets):22} {s.doc.split(': ', 1)[0]}")
        return 0
    if a.cmd == "run":
        prov = {}
        snap = a.lake / "manifest.json"
        if snap.exists():
            from research.access import provenance_of
            prov = provenance_of(a.lake)
        run(a.lake, a.root, a.underlying, a.d0, a.d1, [x for x in a.ids.split(",") if x] or None, a.family, not a.no_chain_store,
            not a.no_live, prov)
        return 0
    if a.cmd == "validate-iv":
        res = validate_iv(a.lake, a.root, a.underlying, a.d0, a.d1)
        print(json.dumps(res, indent=1, default=str))
        return 0 if res.get("pass") else 1
    if a.cmd == "report":
        from signals.options.holding import summarise_day_night
        print(summarise_day_night(read_output(a.root, "day_night_returns", a.underlying)).to_string(index=False))
        return 0
    if a.cmd == "smoke":
        res = smoke(a.root)
        for k, v in sorted(res.items()):
            print(f"{k:26} {v:8,}")
        return 0
    return 2


if __name__ == "__main__":
    sys.exit(main())
