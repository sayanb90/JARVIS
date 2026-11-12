"""
Dated market events for India (SCRUM-176): RBI policy decisions, Union Budgets, election results and the rule changes that split a
backtest into eras (expiry days, lot sizes, SEBI measures).

  python -m data_layer.market_events check            # validate the file; list rows not yet checked against an official source
  python -m data_layer.market_events list [--kind rbi_mpc] [--from 2024-01-01]

The file is data/reference/market_events.csv, tracked in git, one row per event:
  date        YYYY-MM-DD (the trading day the event lands on; a weekend event keeps its own date)
  time_ist    HH:MM when known (RBI decisions 10:00, Budget speech 11:00), else empty
  kind        one of KINDS
  description what happened
  source      where the date was taken from (a URL, or "internal" for our own milestones)
  verified    true once the date was checked against an official or primary source; false means it came from a secondary source or memory

Uses: event-implied variance (signal A9), keeping event days out of "normal" volatility baselines, and tagging rule-change eras.
Expiry dates and lot sizes for a given contract still come from the contract's own metadata; these rows only mark when the rules changed.
"""

from __future__ import annotations

import argparse
import csv
import sys
from dataclasses import dataclass
from datetime import date, time
from pathlib import Path
from typing import Iterable, Optional

ROOT = Path(__file__).resolve().parents[1]
EVENTS_PATH = ROOT / "data" / "reference" / "market_events.csv"
KINDS = ("rbi_mpc", "budget", "election_result", "sebi_rule", "expiry_rule", "lot_size", "other")
FIELDS = ("date", "time_ist", "kind", "description", "source", "verified")


class EventFileError(ValueError):
    pass


@dataclass(frozen=True)
class Event:
    day: date
    time_ist: Optional[time]
    kind: str
    description: str
    source: str
    verified: bool


def parse_rows(rows: Iterable[dict], origin: str = "market_events.csv") -> list[Event]:
    out, seen = [], set()
    for n, r in enumerate(rows, start=2):
        where = f"{origin} line {n}"
        try:
            day = date.fromisoformat((r.get("date") or "").strip())
        except ValueError:
            raise EventFileError(f"{where}: bad date {r.get('date')!r}") from None
        t = (r.get("time_ist") or "").strip()
        try:
            tm = time.fromisoformat(t) if t else None
        except ValueError:
            raise EventFileError(f"{where}: bad time {t!r}") from None
        kind = (r.get("kind") or "").strip()
        if kind not in KINDS:
            raise EventFileError(f"{where}: unknown kind {kind!r} (one of {', '.join(KINDS)})")
        desc = (r.get("description") or "").strip()
        src = (r.get("source") or "").strip()
        if not desc:
            raise EventFileError(f"{where}: description is empty")
        if not (src.startswith("https://") or src == "internal"):
            raise EventFileError(f"{where}: source must be an https URL or 'internal'")
        v = (r.get("verified") or "").strip().lower()
        if v not in ("true", "false"):
            raise EventFileError(f"{where}: verified must be true or false")
        key = (day, kind, desc)
        if key in seen:
            raise EventFileError(f"{where}: duplicate event")
        seen.add(key)
        out.append(Event(day, tm, kind, desc, src, v == "true"))
    return sorted(out, key=lambda e: (e.day, e.time_ist or time(0), e.kind))


def load(path: Optional[Path] = None) -> list[Event]:
    p = Path(path or EVENTS_PATH)
    with p.open(newline="") as f:
        reader = csv.DictReader(f)
        if tuple(reader.fieldnames or ()) != FIELDS:
            raise EventFileError(f"{p.name}: header must be {','.join(FIELDS)}")
        return parse_rows(reader, p.name)


def on(day: date, kinds: Optional[Iterable[str]] = None, events: Optional[list[Event]] = None) -> list[Event]:
    """Events on `day` (optionally of the given kinds)."""
    ks = set(kinds) if kinds else None
    return [e for e in (events if events is not None else load()) if e.day == day and (ks is None or e.kind in ks)]


def days_of(kinds: Iterable[str], events: Optional[list[Event]] = None) -> set[date]:
    ks = set(kinds)
    return {e.day for e in (events if events is not None else load()) if e.kind in ks}


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m data_layer.market_events")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("check")
    ls = sub.add_parser("list")
    ls.add_argument("--kind", choices=KINDS)
    ls.add_argument("--from", dest="d_from", type=date.fromisoformat)
    a = ap.parse_args(argv)
    try:
        events = load()
    except EventFileError as exc:
        print(f"FAIL {exc}")
        return 1
    if a.cmd == "check":
        todo = [e for e in events if not e.verified]
        print(f"OK {len(events)} events, {len(events) - len(todo)} verified, {len(todo)} to verify")
        for e in todo:
            print(f"  {e.day} {e.kind:15} {e.description}")
        return 0
    for e in events:
        if (a.kind and e.kind != a.kind) or (a.d_from and e.day < a.d_from):
            continue
        print(f"{e.day} {e.time_ist.strftime('%H:%M') if e.time_ist else '     '} {e.kind:15} {'Y' if e.verified else '?'} {e.description}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
