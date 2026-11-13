"""
Signal runner (SCRUM-146).

  python -m signals list [--family F]
  python -m signals run --lake LAKE --root STORE --underlying NIFTY --from 2022-11-01 --to 2026-11-12 [--ids a,b] [--family F]
                        [--no-chain-store] [--no-live]
  python -m signals validate-iv --lake LAKE --root STORE --underlying NIFTY --from 2026-11-03 --to 2026-11-10
  python -m signals report day-night --root STORE --underlying NIFTY
  python -m signals smoke [--root STORE]           # the whole catalogue on the synthetic market (no lake needed)
  python -m signals nightly [--lake L] [--root STORE] [--date D]   # cron: every signal for the newest closed session
  python -m signals coverage [--root STORE]        # per signal: rows, days, missing days, days per era
  python -m signals health [--lake L] [--date D]   # inputs of every signal present for the day (exit 1 on a failure)

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
PASS2_LOOKBACK_DAYS = 400       # ATM history read back for pass 2, so trailing windows (z-scores, vol-of-vol) are full on a one-day run
UNDERLYINGS = ("NIFTY", "BANKNIFTY")


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
    atm = _read_derived(root, "atm", underlying, d0 - timedelta(days=PASS2_LOOKBACK_DAYS), d1)
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


def default_lake() -> Path:
    """The latest research snapshot when one exists (read-only, closed files only), else the production lake."""
    from data_layer.lake import LAKE_DIR
    try:
        from research.snapshot import DEFAULT_ROOT
        latest = DEFAULT_ROOT / "snapshots" / "latest"
        if (latest / "manifest.json").exists():
            return latest.resolve()
    except Exception:                                                       # noqa: BLE001
        pass
    return Path(LAKE_DIR)


def default_store() -> Path:
    import os
    if os.environ.get("JARVIS_SIGNAL_STORE"):
        return Path(os.environ["JARVIS_SIGNAL_STORE"])
    from research.snapshot import DEFAULT_ROOT
    return DEFAULT_ROOT / "store"


def nightly(lake: Path, root: Path, day: Optional[date] = None, underlyings=UNDERLYINGS, log=print) -> dict:
    """Compute every signal for the newest closed session (or `day`) for each underlying.  Idempotent: a rerun rewrites that day."""
    from signals.health import latest_day
    day = day or latest_day(lake)
    if day is None:
        raise SystemExit("no 1-minute bars in the lake")
    prov = {}
    if (Path(lake) / "manifest.json").exists():
        from research.access import provenance_of
        prov = provenance_of(Path(lake))
    out = {}
    for u in underlyings:
        st = run(lake, root, u, day, day, provenance=prov, log=log)
        out[u] = sum(st.values())
    return {"day": str(day), "rows": out}


def coverage(root: Path, underlyings=UNDERLYINGS) -> pd.DataFrame:
    """Per signal and underlying: rows, first and last day, days present, trading days missing between them (against the sessions
    of `expiry_last_hour`, which has one row per session), and days per era."""
    from signals.options.core import era_of
    rows = []
    for u in underlyings:
        sessions = read_output(root, "expiry_last_hour", u)
        cal = sorted(set(pd.to_datetime(sessions["date"]).dt.date)) if len(sessions) else []
        for s in specs():
            df = read_output(root, s.id, u)
            key = "ts" if s.grid == "1m" else "date"
            if not len(df):
                rows.append({"underlying": u, "signal": s.id, "family": s.family, "kind": s.kind, "live_only": s.live_only, "rows": 0})
                continue
            days = sorted(set(pd.to_datetime(df[key]).dt.date))
            span = [d for d in cal if days[0] <= d <= days[-1]]
            missing = [d for d in span if d not in set(days)]
            eras = pd.Series([era_of(d) for d in days]).value_counts().to_dict()
            rows.append({"underlying": u, "signal": s.id, "family": s.family, "kind": s.kind, "live_only": s.live_only, "rows": len(df),
                         "first": days[0], "last": days[-1], "days": len(days), "missing_days": len(missing),
                         "min_sample": s.min_sample, "below_min_sample": s.live_only and len(days) < s.min_sample,
                         **{f"days_{k}": v for k, v in eras.items()}})
    return pd.DataFrame(rows)


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
    nt = sub.add_parser("nightly", help="every signal for the newest closed session (cron)")
    nt.add_argument("--lake", type=Path, default=None, help="default: the latest research snapshot, else the lake")
    nt.add_argument("--root", type=Path, default=None, help="default: $JARVIS_SIGNAL_STORE or <research root>/store")
    nt.add_argument("--date", type=date.fromisoformat)
    cv = sub.add_parser("coverage")
    cv.add_argument("--root", type=Path, default=None)
    hl = sub.add_parser("health", add_help=False)
    if argv is None:
        argv = sys.argv[1:]
    if argv and argv[0] == "health":
        from signals.health import main as health_main
        return health_main(argv[1:])
    a = ap.parse_args(argv)
    if a.cmd == "nightly":
        res = nightly(a.lake or default_lake(), a.root or default_store(), a.date)
        print(json.dumps(res))
        return 0
    if a.cmd == "coverage":
        root = a.root or default_store()
        cov = coverage(root)
        out = Path(root) / "reports" / f"coverage_{date.today()}.csv"
        out.parent.mkdir(parents=True, exist_ok=True)
        cov.to_csv(out, index=False)
        print(cov.to_string(index=False))
        print(f"written {out}")
        return 0
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
