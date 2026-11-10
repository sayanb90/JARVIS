"""
Market calendar (SCRUM-141): is a day one on which an exchange should have produced data?

  python -m data_layer.market_calendar refresh          # download Upstox GET /v2/market/holidays into lake/reference/market_calendar.json
  python -m data_layer.market_calendar status [DATE]    # open, closed (and why), special session, or unknown
  python -m data_layer.market_calendar show             # every holiday in the file

Why it exists: on 2026-11-10 (Diwali-Balipratipada) both feeds ran a full session on a closed market and produced about 1.5 % of a
normal day with a reconnect every two minutes, and health said "no FAIL".  Without a calendar a closed market and a broken feed look
the same, so every check that expects data asks this module first.

Rules, in order:
  * a day the file lists as TRADING_HOLIDAY for the exchange (NSE for stocks, NFO for futures and options) is closed;
  * a SPECIAL_TIMING day that lists the exchange as open is a special session (for example the Diwali Muhurat evening, which can fall on
    a Sunday), whatever the weekday;
  * a Saturday or Sunday is otherwise closed;
  * any other weekday is open IF the file is recent enough to be trusted; an old or missing file gives "unknown", which callers
    must treat as "data expected, but warn that the calendar cannot confirm it": unknown is never silently read as closed.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from . import lake as lake_mod

log = logging.getLogger(__name__)

HOLIDAYS_PATH = "/v2/market/holidays"
MAX_AGE_DAYS = 45                       # an older file no longer vouches for a weekday being open
EXCHANGE_OF_SEGMENT = {"eq": "NSE", "fo": "NFO"}


@dataclass(frozen=True)
class Day:
    day: date
    state: str                          # open, closed, special or unknown
    reason: str = ""                    # "weekend", "Diwali-Balipratipada", "no calendar file", ...

    @property
    def expects_data(self) -> bool:
        """Whether a check should expect a normal day of data: open, special sessions and unknown days (see the module note)."""
        return self.state != "closed"

    def __str__(self) -> str:
        return f"{self.day} {self.state}" + (f" ({self.reason})" if self.reason else "")


def calendar_path(lake_dir: Path | None = None) -> Path:
    return (lake_dir or lake_mod.LAKE_DIR) / "reference" / "market_calendar.json"


# ---------------------------------------------------------------------------------------------- parsing and storage
def parse_holidays(body) -> list[dict]:
    """The holidays of an Upstox /v2/market/holidays response, as {date, description, holiday_type, closed_exchanges, open_exchanges}.
    Entries without a readable date are skipped; every exchange name is upper-cased."""
    out: list[dict] = []
    items = body.get("data") if isinstance(body, dict) else body
    for h in items or []:
        if not isinstance(h, dict):
            continue
        raw = h.get("date") or h.get("holiday_date") or ""
        try:
            d = date.fromisoformat(str(raw)[:10])
        except ValueError:
            continue
        opened = []
        for o in h.get("open_exchanges") or []:
            name = o.get("exchange") if isinstance(o, dict) else o
            if name:
                opened.append(str(name).upper())
        out.append({"date": d.isoformat(), "description": str(h.get("description") or ""),
                    "holiday_type": str(h.get("holiday_type") or "").upper(),
                    "closed_exchanges": [str(x).upper() for x in h.get("closed_exchanges") or []],
                    "open_exchanges": opened,
                    "timings": h.get("open_exchanges") if isinstance(h.get("open_exchanges"), list) else []})
    return sorted(out, key=lambda x: x["date"])


def load(lake_dir: Path | None = None) -> Optional[dict]:
    """The stored calendar, or None when there is none or it cannot be read."""
    p = calendar_path(lake_dir)
    try:
        cal = json.loads(p.read_text())
        return cal if isinstance(cal.get("holidays"), list) else None
    except (OSError, ValueError, AttributeError):
        return None


def save(holidays: list[dict], lake_dir: Path | None = None, now: datetime | None = None) -> Path:
    p = calendar_path(lake_dir)
    p.parent.mkdir(parents=True, exist_ok=True)
    now = now or datetime.now(timezone.utc).replace(tzinfo=None)
    tmp = p.with_suffix(".json.tmp")
    tmp.write_text(json.dumps({"fetched_at": now.isoformat(timespec="seconds"), "source": "upstox " + HOLIDAYS_PATH,
                               "holidays": holidays}, indent=1))
    os.replace(tmp, p)
    return p


def refresh(rest, lake_dir: Path | None = None, now: datetime | None = None) -> dict:
    """Download the holiday list and store it.  A failed or empty answer keeps the file that is there (an empty list would turn every
    holiday into an ordinary open day).  Returns {"ok", "holidays", "message"}."""
    r = rest.get(HOLIDAYS_PATH, tag="market.holidays")
    if not r.ok:
        return {"ok": False, "holidays": 0, "message": f"Upstox answered HTTP {r.status} {r.error_code} {r.error_message}".strip()}
    hols = parse_holidays(r.data)
    if not hols:
        return {"ok": False, "holidays": 0, "message": "the response held no holidays: kept the existing calendar"}
    save(hols, lake_dir, now)
    return {"ok": True, "holidays": len(hols), "message": f"stored {len(hols)} holidays, {hols[0]['date']} to {hols[-1]['date']}"}


# ---------------------------------------------------------------------------------------------- the question
def status(day: date, exchange: str = "NSE", cal: Optional[dict] = None, today: Optional[date] = None) -> Day:
    """Is `day` a day on which `exchange` trades?  `cal` is the loaded calendar (None: none available)."""
    exchange = exchange.upper()
    today = today or datetime.now(timezone.utc).date()
    entries = [h for h in (cal or {}).get("holidays", []) if h.get("date") == day.isoformat()]
    for h in entries:
        if h.get("holiday_type") == "TRADING_HOLIDAY" and exchange in h.get("closed_exchanges", []):
            return Day(day, "closed", h.get("description") or "trading holiday")
    for h in entries:
        if h.get("holiday_type") == "SPECIAL_TIMING" and exchange in h.get("open_exchanges", []):
            return Day(day, "special", h.get("description") or "special timing")
    if day.weekday() >= 5:
        return Day(day, "closed", "weekend")
    if cal is None:
        return Day(day, "unknown", "no calendar file (run: python -m data_layer.market_calendar refresh)")
    try:
        fetched = datetime.fromisoformat(cal.get("fetched_at", "")).date()
    except ValueError:
        return Day(day, "unknown", "the calendar file has no fetch time")
    newest_listed = max((h["date"] for h in cal.get("holidays", [])), default="")
    if (today - fetched).days > MAX_AGE_DAYS and day > fetched:
        return Day(day, "unknown", f"the calendar is {(today - fetched).days} days old")
    if day.isoformat() > newest_listed and day > fetched + timedelta(days=MAX_AGE_DAYS):
        return Day(day, "unknown", "the day is beyond what the calendar lists")
    return Day(day, "open")


def previous_open_day(day: date, exchange: str = "NSE", cal: Optional[dict] = None) -> date:
    """The latest day before `day` that was not closed (unknown days count as open)."""
    d = day - timedelta(days=1)
    for _ in range(30):
        if status(d, exchange, cal).state != "closed":
            return d
        d -= timedelta(days=1)
    return day - timedelta(days=1)


def open_days_back(day: date, n: int, exchange: str = "NSE", cal: Optional[dict] = None) -> list[date]:
    """The `n` most recent days before `day` that were not closed, newest first (the baseline for "a normal day")."""
    out: list[date] = []
    d = day
    for _ in range(n * 4 + 10):
        d = previous_open_day(d, exchange, cal)
        out.append(d)
        if len(out) == n:
            break
    return out


# ---------------------------------------------------------------------------------------------- command line
def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Market calendar from Upstox holidays")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("refresh", help="download the holiday list")
    s = sub.add_parser("status", help="is a day open?")
    s.add_argument("date", nargs="?", default=None)
    s.add_argument("--exchange", default="NSE")
    sub.add_parser("show", help="list the stored holidays")
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if a.cmd == "refresh":
        from .upstox.feed import current_token
        from .upstox.rest import UpstoxRest
        if not current_token():
            print("no Upstox token: set UPSTOX_FEED_TOKEN or UPSTOX_ACCESS_TOKEN in .env")
            return 2
        res = refresh(UpstoxRest(current_token))
        print(f"{datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S} market calendar: {res['message']}")
        return 0 if res["ok"] else 1
    cal = load()
    if a.cmd == "show":
        for h in (cal or {}).get("holidays", []):
            print(f"{h['date']}  {h['holiday_type']:<17} {h['description']}  closed: {','.join(h['closed_exchanges']) or '-'}"
                  + (f"  open: {','.join(h['open_exchanges'])}" if h["open_exchanges"] else ""))
        if cal is None:
            print("no calendar file yet")
        return 0 if cal else 1
    d = date.fromisoformat(a.date) if a.date else datetime.now(timezone.utc).date()
    st = status(d, a.exchange, cal)
    print(st)
    return 0 if st.state != "unknown" else 1


if __name__ == "__main__":
    sys.exit(main())
