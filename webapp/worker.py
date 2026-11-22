"""
The viewer's heavy whole-day calls, run in a child process (SCRUM-208): the viewer starts `python -m webapp.worker ...` through webapp/heavy.py, reads the JSON
from its standard output, and the child exits, so the memory it used (800 to 900 MB for the Market overview and the F&O chain) goes back to the system.

  python -m webapp.worker overview --lake L --date D [--interval 1s]
  python -m webapp.worker chain    --lake L --date D --underlying NIFTY [--expiry 2026-11-24] [--interval 1s]

The answer of a finished day is also saved in the day cache (webapp/daycache.py) by the call itself, so the next request needs no child at all.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="python -m webapp.worker")
    ap.add_argument("action", choices=["overview", "chain"])
    ap.add_argument("--lake", required=True)
    ap.add_argument("--date", required=True)
    ap.add_argument("--interval", default="1s")
    ap.add_argument("--underlying", default=None)
    ap.add_argument("--expiry", default=None)
    a = ap.parse_args(argv)
    from fastapi.encoders import jsonable_encoder
    from data_layer.query import Lake
    from webapp.app import chain_payload, overview_payload
    lake = Path(a.lake)
    q = Lake(lake)
    if a.action == "overview":
        out = overview_payload(q, lake, a.date, a.interval)
    else:
        if not a.underlying:
            print("chain needs --underlying", file=sys.stderr)
            return 2
        out = chain_payload(q, lake, a.date, a.underlying, a.expiry or None, a.interval)
    sys.stdout.write(json.dumps(jsonable_encoder(out)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
