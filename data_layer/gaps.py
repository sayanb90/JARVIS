"""
Missing days in the 1-minute bars, found from what is stored, so a nightly job can fill them without anyone noticing a gap.

  yahoo_plan(lake, "1m", markets, today)     for each market, the weekdays of the last 28 with no rows (what the nightly Yahoo job re-fetches)
  india_missing(lake, today, now)            the days NSE was open (its holiday calendar) with no India rows (what the nightly Upstox job fetches)
  record(lake, key, days, today)             after a fill attempt: a day still empty is counted; after GIVE_UP attempts it is taken to be a
                                             holiday this calendar does not know and is not asked for again

Why a count and not a list of known holidays: there are nine Yahoo markets with nine holiday calendars, and a day that Yahoo does not serve
is indistinguishable from a day the market was shut.  Two empty nights in a row is a reliable enough sign; Yahoo keeps only 30 days of
1-minute bars anyway, so a day given up on is out of reach a month later whatever we do.
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Optional

import duckdb

from . import market_calendar

log = logging.getLogger(__name__)

SPAN_DAYS = 28                      # the deepest Yahoo 1-minute history we ask for (see yf_client.deepest)
GIVE_UP = 2                         # attempts after which a weekday with no data is taken to be a holiday
INDIA_AFTER_CLOSE_UTC = (10, 20)    # NSE closes 10:00 UTC; the candle service has the day a little later


def _state_path(lake_dir: Path) -> Path:
    return lake_dir / "backfill" / "gap_attempts.json"


def _load(lake_dir: Path) -> dict:
    p = _state_path(lake_dir)
    try:
        return json.loads(p.read_text()) if p.exists() else {}
    except (OSError, ValueError):
        return {}


def weekdays(first: date, last: date) -> list[date]:
    out, d = [], first
    while d <= last:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


def days_with_rows(lake_dir: Path, interval: str, first: date, last: date) -> dict[date, set[str]]:
    """{day: markets that have rows that day} for the day folders of `interval` between `first` and `last`."""
    base = Path(lake_dir) / "bars" / f"interval={interval}"
    out: dict[date, set[str]] = {}
    if not base.is_dir():
        return out
    con = duckdb.connect()
    for folder in sorted(base.glob("date=*")):
        try:
            day = date.fromisoformat(folder.name.split("=", 1)[1])
        except ValueError:
            continue
        files = sorted(folder.glob("*.parquet"))
        if not files or not first <= day <= last:
            continue
        try:
            names = ", ".join("'" + str(f).replace("'", "''") + "'" for f in files)
            out[day] = {r[0] for r in con.execute(f"SELECT DISTINCT market FROM read_parquet([{names}], union_by_name=true)").fetchall() if r[0]}
        except duckdb.Error as exc:                                    # a file being written right now: treat the day as not seen
            log.warning("could not read %s: %s", folder, exc)
    con.close()
    return out


def _given_up(state: dict, key: str, day: date) -> bool:
    return int(state.get(key, {}).get(day.isoformat(), 0)) >= GIVE_UP


def yahoo_plan(lake_dir: Path, interval: str, markets: Iterable[str], today: date, span: int = SPAN_DAYS) -> dict[str, list[date]]:
    """{market: weekdays from `today - span` to yesterday with no rows}, oldest first; days given up on are left out."""
    first, last = today - timedelta(days=span), today - timedelta(days=1)
    have, state, plan = days_with_rows(Path(lake_dir), interval, first, last), _load(Path(lake_dir)), {}
    for m in markets:
        key = f"{interval}|{m}"
        plan[m] = [d for d in weekdays(first, last) if m not in have.get(d, set()) and not _given_up(state, key, d)]
    return plan


def days_back_for(missing: list[date], today: date, floor: int = 0) -> int:
    """How many days back a fetch must reach to cover the oldest of `missing` (one day of margin; never less than `floor`)."""
    return max((today - min(missing)).days + 1, floor) if missing else floor


def india_missing(lake_dir: Path, today: date, now: Optional[datetime] = None, span: int = 14) -> list[date]:
    """Days NSE was open in the last `span` days (today only once the candle service has it) with no India rows, oldest first."""
    now = now or datetime.now(timezone.utc)
    cal = market_calendar.load(Path(lake_dir))
    first, last = today - timedelta(days=span), today
    have, state = days_with_rows(Path(lake_dir), "1m", first, last), _load(Path(lake_dir))
    key, out = "1m|IN", []
    for d in (first + timedelta(days=i) for i in range((last - first).days + 1)):
        if d == today and (now.hour, now.minute) < INDIA_AFTER_CLOSE_UTC:
            continue
        if not market_calendar.status(d, "NSE", cal=cal, today=today).expects_data or "IN" in have.get(d, set()) or _given_up(state, key, d):
            continue
        out.append(d)
    return out


def record(lake_dir: Path, interval: str, market: str, days: Iterable[date]) -> list[date]:
    """After a fill attempt over `days`: count each day that is still empty.  Returns the days now given up on."""
    days = list(days)
    if not days:
        return []
    lake_dir = Path(lake_dir)
    have = days_with_rows(lake_dir, interval, min(days), max(days))
    state, key, gone = _load(lake_dir), f"{interval}|{market}", []
    bucket = state.setdefault(key, {})
    for d in days:
        if market in have.get(d, set()):
            bucket.pop(d.isoformat(), None)
            continue
        bucket[d.isoformat()] = int(bucket.get(d.isoformat(), 0)) + 1
        if bucket[d.isoformat()] >= GIVE_UP:
            gone.append(d)
    cutoff = (date.today() - timedelta(days=60)).isoformat()            # forget old entries: nothing can be fetched for them any more
    for k in state:
        state[k] = {d: n for d, n in state[k].items() if d >= cutoff}
    p = _state_path(lake_dir)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state, indent=1, sort_keys=True))
    tmp.replace(p)
    return gone
