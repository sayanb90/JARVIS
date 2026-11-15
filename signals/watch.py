"""
A live view of the signal engines for a terminal (tmux) during the session: python -m signals watch [--every 5] [--once]

Reads the engines' heartbeat files (<store>/live/engine-<U>.heartbeat.json, rewritten every 250 ms) and the paper trader's last status
line (logs/paper-nifty.log), and prints one block every few seconds.  Flags:
  STALE      the heartbeat is more than 5 s old (engine stopped, stuck or crashed)
  BEHIND     the newest tick is more than 5 s behind the clock (feed slow, or the engine is falling behind)
  FALLBACK   a cadence group overran and moved to a coarser cadence
  ERRORS     tick batches or timer steps raised (logged in logs/signals-live-<u>.log; the session continued)
  LATE       more than 0.1 % of ticks arrived after their bucket had closed
Read only.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Optional

import pandas as pd

STALE_S = 5.0
BEHIND_S = 5.0


def flags(doc: dict, now: pd.Timestamp) -> dict:
    """Per underlying: the alarms above and the numbers behind them."""
    out = {}
    age = (now - pd.Timestamp(doc["at"])).total_seconds()
    for u, e in doc.get("engines", {}).items():
        f = []
        if age > STALE_S:
            f.append(f"STALE {age:.0f}s")
        if e.get("lag_s") is not None and e["lag_s"] > BEHIND_S:
            f.append(f"BEHIND {e['lag_s']:.1f}s")
        if e.get("fallbacks"):
            f.append(f"FALLBACK {e['fallbacks']}")
        if e.get("errors"):
            f.append(f"ERRORS {e['errors']}")
        if e.get("ticks") and e.get("late", 0) / e["ticks"] > 0.001:
            f.append(f"LATE {e['late']}/{e['ticks']}")
        out[u] = {"age_s": round(age, 1), "flags": f, "engine": e}
    return out


def render(store: Path, logs: Path, now: Optional[pd.Timestamp] = None) -> str:
    now = now or pd.Timestamp.utcnow().tz_localize(None)
    ist = now + pd.Timedelta(hours=5, minutes=30)
    lines = [f"JARVIS live  {now:%H:%M:%S} UTC  ({ist:%H:%M:%S} IST)"]
    files = sorted((Path(store) / "live").glob("engine-*.heartbeat.json"))
    if not files:
        lines.append(f"  no engine heartbeat in {Path(store) / 'live'} (engines not started, or JARVIS_SIGNAL_ENGINE=runner)")
    for f in files:
        try:
            doc = json.loads(f.read_text())
        except (OSError, ValueError):
            lines.append(f"  {f.name}: unreadable (being rewritten?)")
            continue
        for u, x in flags(doc, now).items():
            e = x["engine"]
            st = e.get("last_status") or {}
            groups = " ".join(f"{c}:{b}" for c, b in e.get("buckets", {}).items())
            slow = " ".join(f"{c} mean {g.get('mean_ms')} / max {g.get('max_ms')} ms" for c, g in (st.get("groups") or {}).items()
                            if g.get("signals"))
            state = "OK" if not x["flags"] else "  ".join(x["flags"])
            lines.append(f"  {u:<10} {state}")
            lines.append(f"    beat {x['age_s']}s ago  lag {e.get('lag_s')}s  ticks {e.get('ticks')}  late {e.get('late')}  "
                         f"rss {st.get('rss_mb', '?')} MB  buckets {groups}")
            if slow:
                lines.append(f"    last minute: {slow}")
    p = Path(logs) / "paper-nifty.log"
    if p.exists():
        last = [ln for ln in p.read_text(errors="replace").splitlines()[-200:] if "status paper" in ln]
        lines.append("  paper      " + (last[-1].split("status paper:", 1)[1].strip() if last else "no status line yet"))
    return "\n".join(lines)


def main(argv=None) -> int:
    import argparse
    from signals.__main__ import default_store
    ap = argparse.ArgumentParser(prog="python -m signals watch")
    ap.add_argument("--root", type=Path, default=None, help="signal store (default: the research store)")
    ap.add_argument("--logs", type=Path, default=Path("logs"))
    ap.add_argument("--every", type=float, default=5.0)
    ap.add_argument("--once", action="store_true")
    a = ap.parse_args(argv)
    store = a.root or default_store()
    while True:
        print(render(store, a.logs), flush=True)
        if a.once:
            return 0
        time.sleep(a.every)
        print()
