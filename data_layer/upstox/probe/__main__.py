"""
Command line for the Upstox probe.  See the package docstring; typical use on the VPS:

  python -m data_layer.upstox.probe check
  python -m data_layer.upstox.probe run                 # start before 08:50 IST (03:20 UTC); ends after the close
  python -m data_layer.upstox.probe report

Logs: logs/probe/probe-<date>.log (readable), resources-<date>.jsonl (CPU, memory, disk every 30 s), jobs-<date>.jsonl.
Data and reports: probe_data/ (reports in probe_data/reports/).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import date, datetime, time as dtime, timedelta
from pathlib import Path

from dotenv import load_dotenv

from .jobs import BLUE_CHIP_ISINS, Ctx
from .report import build_report, write_report
from .runner import Guard, ProbeRun, run_check, utcnow
from .store import ProbeStore
from .. import feed as feedmod
from ..instruments import segment_key_map
from ..rest import UpstoxRest
from ...lake import ENV_FILE
from ...sysmon import ResourceMonitor

log = logging.getLogger("probe")
STREAM_CAP = 2000                                               # full-mode keys per connection


def token() -> str:
    load_dotenv(ENV_FILE, override=True)
    return feedmod.current_token() or os.getenv("UPSTOX_ANALYTICS_TOKEN", "")


def setup_logging(logs: Path, day: date) -> None:
    logs.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    for h in list(root.handlers):
        root.removeHandler(h)
    for h in (logging.FileHandler(logs / f"probe-{day:%Y%m%d}.log"), logging.StreamHandler(sys.stdout)):
        h.setFormatter(fmt)
        root.addHandler(h)


def _safe_map(segment: str, limit: int | None) -> dict:
    try:
        km = segment_key_map(segment)
    except (SystemExit, Exception) as exc:                      # noqa: BLE001 -- a missing universe file must not stop the probe
        log.warning("no %s instrument list (%s); that stream is skipped", segment, exc)
        return {}
    return dict(list(km.items())[:limit]) if limit else km


def isin_sample(n: int) -> list[str]:
    try:
        from ...fmp_client import read_universe
        rows = [r.get("isin", "").strip() for r in read_universe("IN")]
    except Exception:                                           # noqa: BLE001
        rows = []
    out = list(BLUE_CHIP_ISINS) + [i for i in rows if i and i not in BLUE_CHIP_ISINS and len(i) == 12 and i.startswith("IN")]
    return out[:n]


def build_ctx(args, root: Path) -> Ctx:
    rest = UpstoxRest(token)
    ctx = Ctx(rest=rest, store=ProbeStore(root), fund_sample=args.fund_sample, news_stocks=args.news_stocks,
              eq_keys=_safe_map("eq", args.eq_limit), fo_keys=_safe_map("fo", args.fo_limit))
    ctx.isins = isin_sample(args.fund_sample)
    return ctx


def stream_plan(ctx: Ctx, args) -> tuple[dict, dict]:
    """Which instruments each probe connection subscribes, and how to scale the measured cost to the production plan."""
    a = {**ctx.eq_keys, **ctx.index_keys, **ctx.global_keys}
    if len(a) > STREAM_CAP:
        log.warning("equity stream has %d keys; trimmed to the %d-key cap", len(a), STREAM_CAP)
        a = dict(list(a.items())[:STREAM_CAP])
    fo = ctx.fo_keys
    if len(fo) > STREAM_CAP:
        log.warning("F&O stream has %d keys; trimmed to the %d-key cap (scaled up in the projection)", len(fo), STREAM_CAP)
        fo = dict(list(fo.items())[:STREAM_CAP])
    scale = {"equity": 500.0 / max(1, len(ctx.eq_keys)) if len(ctx.eq_keys) < 500 else 1.0,
             "fo": len(ctx.fo_keys) / max(1, len(fo)) if fo else 1.0}
    return {"stream_eq": a, "stream_fo": fo, "check": dict(list(a.items())[:20]) | dict(list(fo.items())[:5])}, scale


def parse_hhmm(s: str, day: date) -> datetime:
    h, m = map(int, s.split(":"))
    return datetime.combine(day, dtime(h, m))


def cmd_check(args) -> int:
    root, logs = Path(args.out), Path(args.logs)
    setup_logging(logs, utcnow().date())
    if not token():
        print("No Upstox token: set UPSTOX_ACCESS_TOKEN or UPSTOX_ANALYTICS_TOKEN in .env (see data_layer.upstox.auth).")
        return 2
    ctx = build_ctx(args, root)
    mon = ResourceMonitor(logs / f"resources-{utcnow():%Y%m%d}.jsonl", args.interval, [root, Path("lake"), Path(".")])
    from .jobs import job_instrument_master
    job_instrument_master(ctx, "check")
    keys, _ = stream_plan(ctx, args)
    path = run_check(ctx, root, logs, mon, keys, token(), args.stream_seconds)
    print(f"\nCapability report: {path}")
    return 0


def cmd_run(args) -> int:
    root, logs = Path(args.out), Path(args.logs)
    day = args.day or utcnow().date()
    while day.weekday() >= 5 and not args.now:
        day += timedelta(days=1)
    setup_logging(logs, day)
    if not token():
        print("No Upstox token: set UPSTOX_ACCESS_TOKEN or UPSTOX_ANALYTICS_TOKEN in .env (see data_layer.upstox.auth).")
        return 2
    start, end = parse_hhmm(args.start_utc, day), parse_hhmm(args.end_utc, day)
    if args.now and utcnow() > start:
        start = utcnow()
    ctx = build_ctx(args, root)
    mon = ResourceMonitor(logs / f"resources-{day:%Y%m%d}.jsonl", args.interval, [root, Path("lake"), Path(".")])
    guard = Guard(root, min_free_gb=args.min_free_gb, max_gb=args.max_gb)
    root.mkdir(parents=True, exist_ok=True)
    pr = ProbeRun(ctx, root, logs, token, start=start, end=end, guard=guard, monitor=mon, no_stream=args.no_stream,
                  meta={"window_utc": [str(start), str(end)], "eq_keys": len(ctx.eq_keys), "fo_keys": len(ctx.fo_keys),
                        "fund_sample": args.fund_sample, "max_gb": args.max_gb, "min_free_gb": args.min_free_gb})
    pr.run_job("instrument_master")                             # fills the index and global keys the streams need
    pr.stream_keys, pr.universe_scale = stream_plan(ctx, args)
    log.info("streams: equity+index+global %d keys, F&O %d keys; scale to plan %s", len(pr.stream_keys["stream_eq"]),
             len(pr.stream_keys["stream_fo"]), pr.universe_scale)
    path = pr.run()
    print(f"\nReport: {path}")
    return 0


def cmd_report(args) -> int:
    root = Path(args.out)
    state = root / "_state.json"
    if not state.exists():
        print(f"No {state}: run `probe run` or `probe check` first.")
        return 2
    s = json.loads(state.read_text())
    rep = build_report(root, mode=s["mode"], monitor=s["monitor"], rest=s["rest"], results=s["results"], streams=s["streams"],
                       universe_scale=s["universe_scale"], meta=s["meta"], partial=s.get("partial", False))
    print(f"Report: {write_report(root, rep, 'report-rebuilt')[1]}")
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m data_layer.upstox.probe", description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--out", default=os.getenv("JARVIS_PROBE_DIR", "probe_data"), help="directory for probe data and reports")
    ap.add_argument("--logs", default="logs/probe")
    ap.add_argument("--interval", type=float, default=30.0, help="seconds between resource samples")
    ap.add_argument("--eq-limit", type=int, default=500)
    ap.add_argument("--fo-limit", type=int, default=2000)
    ap.add_argument("--fund-sample", type=int, default=25, help="companies to sample for fundamentals")
    ap.add_argument("--news-stocks", type=int, default=500)
    sub = ap.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("check", help="one pass over every dataset plus a short stream test")
    c.add_argument("--stream-seconds", type=float, default=20.0)
    r = sub.add_parser("run", help="a full session: stream, scheduled REST jobs, resource log, report")
    r.add_argument("--day", type=date.fromisoformat, default=None)
    r.add_argument("--start-utc", default="03:20", help="stream and polls start (market opens 03:45 UTC)")
    r.add_argument("--end-utc", default="10:25", help="stream ends (closing auction ends 10:00 UTC; F&O closes 10:10)")
    r.add_argument("--now", action="store_true", help="start immediately even if the window has begun or it is a weekend")
    r.add_argument("--no-stream", action="store_true")
    r.add_argument("--max-gb", type=float, default=25.0, help="stop writing if the probe directory exceeds this")
    r.add_argument("--min-free-gb", type=float, default=15.0, help="stop writing if free disk falls below this")
    sub.add_parser("report", help="rebuild the report from what is on disk")
    a = ap.parse_args(argv)
    return {"check": cmd_check, "run": cmd_run, "report": cmd_report}[a.cmd](a)


if __name__ == "__main__":
    sys.exit(main())
