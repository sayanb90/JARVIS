"""
Signal history backfill job (SCRUM-214): every signal that 1-minute candles can produce, from 2022 to yesterday, resumable and safe to
leave on a schedule.

  python -m signals backfill plan                         # which signals can and cannot be backfilled, and why
  python -m signals backfill run [--underlying NIFTY,BANKNIFTY] [--from 2022-11-01] [--max-minutes 360]
  python -m signals backfill status                       # months done, failed and pending per underlying and layer

What it runs (both existing runners, `signals run` and `signals run --every-bucket`, one calendar month at a time):
  history   every signal that needs no ticks, at its own grid: 1-minute signals per minute, snapshot signals at 09:20 / 12:00 / 15:15,
            daily and expiry signals per session.  Signals that need the live feed (ticks, the 5-level book, vendor Greeks) are left out:
            they exist only from 2026-11-03 and the nightly job computes them.
  minute    the snapshot signals at every 1-minute bar from candles (layer intraday/source=candles/bucket=1m), up to 2026-11-02; from
            2026-11-03 the live engine's tick-based rows cover every bucket.
Months go oldest first on purpose: the daily signals (HAR forecast, 250-session percentiles, vol-of-vol) read the earlier months' output.

Safe to run unattended:
  * a gate first: our candle IV against Upstox's IV on the live sessions (`validate-iv`); an underlying that fails it is skipped and said so;
  * progress in <store>/backfill/state.json: a finished month is never redone, a failed one is retried, the newest (still open) month is
    redone each run; stop it any time and run it again;
  * one run at a time (a lock file), a time limit (--max-minutes), and it stops between months when the free memory is below
    --min-free-gb or when the market session starts (heavy jobs never run 03:40-10:05 UTC on weekdays);
  * exit code 0: finished or stopped on the time limit (run again); 1: a month failed; 3: refused to start (session hours, lock, memory).
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import resource
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Optional

import pandas as pd

START_DEFAULT = date(2022, 11, 1)            # the history from candles starts here (the IV gate and the chain are built for this era)
TICK_ERA = date(2026, 11, 3)                 # from here the live engine's tick-based rows exist
SESSION_UTC = (3 * 60 + 40, 10 * 60 + 5)     # minutes since midnight UTC: no heavy job in this window on weekdays
UNDERLYINGS = ("NIFTY", "BANKNIFTY")
GATE_VALID_DAYS = 14
MAX_CONSECUTIVE_FAILURES = 3
LAYERS = ("history", "minute")


# ---------------------------------------------------------------------------------------------- what can be backfilled
# live-only signals and the candle proxy that stands in for them in the history (None: there is none)
PROXY = {"signed_flow": "vol_demand_proxy", "opening_flow": "opening_flow_daily", "illiquidity": "amihud", "pin_full": "pin",
         "pcr_full": "pcr_near", "gex_full": "gex"}
NEEDS = {"ticks_fo": "tick prices, quotes and the 5-level book from the live feed (from 2026-11-03)",
         "participant": "NSE participant-wise OI files", "weights": "index constituent weights (history: today's snapshot only)",
         "events": "the scheduled-events file", "fills": "your own fills (paper trading)"}


def plan() -> pd.DataFrame:
    """One row per registered signal: can the history be built from 1-minute candles, how, and what stands in when it cannot."""
    import signals.catalogue  # noqa: F401 -- registers every signal
    from signals.framework import REGISTRY
    rows = []
    for sid, (s, _) in sorted(REGISTRY.items(), key=lambda kv: (kv[1][0].family, kv[0])):
        needs = [NEEDS[i] for i in s.inputs if i in NEEDS]
        if s.live_only:
            verdict, how = "no", "needs the live feed: " + "; ".join(needs or ["tick data"])
            if PROXY.get(sid):
                how += f" -> candle proxy: {PROXY[sid]}"
        elif s.kind == "label":
            verdict, how = "yes", "forward-looking outcome kept for research"
        elif s.grid == "1m":
            verdict, how = "yes", "per minute"
        elif s.grid == "snap":
            verdict, how = "yes", "at 09:20 / 12:00 / 15:15, and every minute from candles (minute layer)"
        else:
            verdict, how = "yes", f"per session ({s.grid})"
        partial = [n for n in needs if not s.live_only]
        if partial:
            verdict = "partly" if "weights" in " ".join(partial) else verdict
            how += "; also uses " + "; ".join(partial)
        rows.append({"signal": sid, "family": s.family, "grid": s.grid, "backfill": verdict, "how": how})
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------------------------- state
def state_path(root: Path) -> Path:
    return Path(root) / "backfill" / "state.json"


def load_state(root: Path) -> dict:
    try:
        return json.loads(state_path(root).read_text())
    except (OSError, ValueError):
        return {}


def save_state(root: Path, st: dict) -> None:
    p = state_path(root)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(st, indent=1, default=str))
    os.replace(tmp, p)


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def in_session_hours(t: datetime) -> bool:
    minutes = t.hour * 60 + t.minute
    return t.weekday() < 5 and SESSION_UTC[0] <= minutes < SESSION_UTC[1]


def free_gb() -> float:
    try:
        for line in Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("MemAvailable:"):
                return int(line.split()[1]) / 1024 / 1024
    except (OSError, ValueError, IndexError):
        pass
    return float("inf")                                                   # unknown: do not block


def _months(d0: date, d1: date) -> list[tuple[date, date]]:
    from signals.__main__ import months
    return months(d0, d1)


# ---------------------------------------------------------------------------------------------- the runners
def _run_history(lake: Path, root: Path, underlying: str, a: date, b: date) -> dict:
    from signals.__main__ import run
    return run(lake, root, underlying, a, b, live=False, provenance={"job": "backfill", "layer": "history"}, log=lambda *_: None)


def _run_minute(lake: Path, root: Path, underlying: str, a: date, b: date) -> dict:
    from signals.__main__ import run_every_bucket
    return run_every_bucket(lake, root, underlying, a, b, log=lambda *_: None)


def _gate(lake: Path, root: Path, underlying: str, d0: date, d1: date) -> dict:
    from signals.__main__ import validate_iv
    return validate_iv(lake, root, underlying, d0, d1)


@dataclass
class Result:
    code: int
    message: str
    chunks: int = 0
    rows: int = 0


def run_job(lake: Path, root: Path, underlyings=UNDERLYINGS, d0: date = START_DEFAULT, d1: Optional[date] = None, layers=LAYERS,
            max_minutes: float = 360, min_free_gb: float = 5.0, now: Callable[[], datetime] = _now, log: Callable = print,
            run_history: Callable = _run_history, run_minute: Callable = _run_minute, gate: Callable = _gate,
            memory: Callable = free_gb, session_check: bool = True, clock: Callable = time.monotonic) -> Result:
    lake, root = Path(lake).resolve(), Path(root).resolve()
    if root == lake or lake in root.parents:
        raise SystemExit("the store must be outside the lake")
    t0 = now()
    if session_check and in_session_hours(t0):
        return Result(3, "refused: the market session is running (heavy jobs never run 03:40-10:05 UTC on weekdays); try again after 10:05 UTC")
    yesterday = (d1 or (t0.date() - timedelta(days=1)))
    lockf = root / "backfill" / "lock"
    lockf.parent.mkdir(parents=True, exist_ok=True)
    fh = open(lockf, "w")
    try:
        fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        fh.close()
        return Result(3, "refused: another backfill run holds the lock")
    start = clock()
    st = load_state(root)
    chunks = rows = failures = 0
    stop = ""
    try:
        for u in underlyings:
            ust = st.setdefault(u, {})
            g = ust.get("gate") or {}
            fresh = g.get("pass") and (t0 - datetime.fromisoformat(g["at"])).days < GATE_VALID_DAYS if g.get("at") else False
            if not fresh:
                try:
                    res = gate(lake, root, u, TICK_ERA, yesterday)
                except Exception as exc:                                  # noqa: BLE001 -- recorded, the underlying is skipped
                    res = {"pass": False, "reason": f"gate failed to run: {exc!r}"[:200]}
                ust["gate"] = {"pass": bool(res.get("pass")), "at": t0.isoformat(timespec="seconds"),
                               "median_abs": res.get("median_abs"), "p95_abs": res.get("p95_abs"), "n": res.get("n"), "reason": res.get("reason")}
                save_state(root, st)
            if not ust["gate"]["pass"]:
                log(f"{u}: skipped, the IV gate failed ({ust['gate'].get('reason') or 'median ' + str(ust['gate'].get('median_abs'))})")
                continue
            for layer in layers:
                last = yesterday if layer == "history" else min(yesterday, TICK_ERA - timedelta(days=1))
                done = ust.setdefault(layer, {})
                for a, b in _months(d0, last):
                    key = f"{a:%Y-%m}"
                    prev = done.get(key) or {}
                    through = date.fromisoformat(prev["through"]) if prev.get("ok") and prev.get("through") else None
                    if through is not None and through >= b:                  # covered to the end of the month (or to yesterday): nothing to do
                        continue
                    first = a if through is None else through + timedelta(days=1)    # the open month grows by the days since the last run
                    if (clock() - start) / 60 >= max_minutes:
                        stop = f"time limit of {max_minutes:g} minutes reached"
                        break
                    if session_check and in_session_hours(now()):
                        stop = "the market session started"
                        break
                    if memory() < min_free_gb:
                        stop = f"free memory below {min_free_gb:g} GB"
                        break
                    c0 = clock()
                    try:
                        stats = (run_history if layer == "history" else run_minute)(lake, root, u, first, b)
                        n = int(sum(stats.values())) if stats else 0
                        old_sig = prev.get("signals", {}) if through is not None else {}
                        sig = {k: int(v) + int(old_sig.get(k, 0)) for k, v in (stats or {}).items()}
                        sig.update({k: v for k, v in old_sig.items() if k not in sig})
                        done[key] = {"ok": True, "through": b.isoformat(), "rows": n + (prev.get("rows", 0) if through is not None else 0),
                                     "secs": round(clock() - c0, 1), "at": now().isoformat(timespec="seconds"),
                                     "peak_rss_mb": round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024), "signals": sig}
                        chunks, rows, failures = chunks + 1, rows + n, 0
                        log(f"{u} {layer} {first}..{b}: {n:,} rows in {done[key]['secs']} s")
                    except Exception as exc:                              # noqa: BLE001 -- recorded and retried next run, from the start of the month
                        done[key] = {"ok": False, "error": f"{type(exc).__name__}: {exc}"[:300], "at": now().isoformat(timespec="seconds")}
                        failures += 1
                        log(f"{u} {layer} {first}..{b}: FAILED {done[key]['error']}")
                    save_state(root, st)
                    if failures >= MAX_CONSECUTIVE_FAILURES:
                        stop = f"{failures} months failed in a row"
                        break
                if stop:
                    break
            if stop:
                break
    finally:
        fcntl.flock(fh, fcntl.LOCK_UN)
        fh.close()
    bad = sum(1 for u in st.values() if isinstance(u, dict) for L in LAYERS for v in (u.get(L) or {}).values() if not v.get("ok"))
    msg = f"{chunks} month(s) done, {rows:,} rows" + (f"; stopped: {stop}" if stop else "; nothing left to do" if chunks == 0 else "; finished")
    return Result(1 if bad else 0, msg + (f"; {bad} month(s) failed, they will be retried" if bad else ""), chunks, rows)


def status_text(root: Path, d0: date = START_DEFAULT, today: Optional[date] = None) -> str:
    st = load_state(root)
    today = today or _now().date()
    out = [f"signal backfill, store {root}"]
    if not st:
        return "\n".join(out + ["nothing has run yet: python -m signals backfill run"])
    for u in UNDERLYINGS:
        ust = st.get(u)
        if not ust:
            continue
        g = ust.get("gate") or {}
        out.append(f"{u}: IV gate " + ("passed" if g.get("pass") else f"FAILED ({g.get('reason') or 'median ' + str(g.get('median_abs'))})") + f" on {str(g.get('at', ''))[:10]}")
        for layer in LAYERS:
            last = today - timedelta(days=1) if layer == "history" else min(today - timedelta(days=1), TICK_ERA - timedelta(days=1))
            want = [f"{a:%Y-%m}" for a, _ in _months(d0, last)]
            d = ust.get(layer) or {}
            ok = [k for k in want if d.get(k, {}).get("ok")]
            bad = [k for k, v in d.items() if not v.get("ok")]
            rows = sum(d[k].get("rows", 0) for k in ok)
            nxt = next((k for k in want if k not in ok), None)
            out.append(f"  {layer:<8} {len(ok)}/{len(want)} months, {rows:,} rows" + (f", next {nxt}" if nxt else ", complete")
                       + (f", FAILED: {', '.join(sorted(bad))}" if bad else ""))
    return "\n".join(out)


# ---------------------------------------------------------------------------------------------- command line
def main(argv: Optional[list[str]] = None) -> int:
    from signals.__main__ import default_lake, default_store
    ap = argparse.ArgumentParser(prog="python -m signals backfill", description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("plan")
    s = sub.add_parser("status")
    s.add_argument("--root", type=Path, default=None)
    r = sub.add_parser("run")
    r.add_argument("--lake", type=Path, default=None)
    r.add_argument("--root", type=Path, default=None)
    r.add_argument("--underlying", default=",".join(UNDERLYINGS))
    r.add_argument("--from", dest="d0", type=date.fromisoformat, default=START_DEFAULT)
    r.add_argument("--to", dest="d1", type=date.fromisoformat, default=None, help="default: yesterday")
    r.add_argument("--layers", default=",".join(LAYERS))
    r.add_argument("--max-minutes", type=float, default=360)
    r.add_argument("--min-free-gb", type=float, default=5.0)
    a = ap.parse_args(argv)
    if a.cmd == "plan":
        df = plan()
        with pd.option_context("display.width", 250, "display.max_colwidth", 120, "display.max_rows", 500):
            print(df.to_string(index=False))
        print("\n" + df["backfill"].value_counts().to_string())
        return 0
    root = a.root or default_store()
    if a.cmd == "status":
        print(status_text(root))
        return 0
    us = tuple(x.strip().upper() for x in a.underlying.split(",") if x.strip())
    ls = tuple(x.strip() for x in a.layers.split(",") if x.strip())
    if any(u not in UNDERLYINGS for u in us) or any(x not in LAYERS for x in ls):
        ap.error(f"underlying must be in {UNDERLYINGS}, layers in {LAYERS}")
    res = run_job(a.lake or default_lake(), root, us, a.d0, a.d1, ls, a.max_minutes, a.min_free_gb,
                  log=lambda m: print(f"{datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S} {m}", flush=True))
    print(f"{datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S} signal backfill: {res.message}")
    print(status_text(root))
    return res.code


if __name__ == "__main__":
    sys.exit(main())
