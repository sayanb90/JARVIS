"""
Participant-wise open interest and trading volume in NSE equity derivatives (SCRUM-175): how many contracts Clients, DIIs, FIIs and Pro
(proprietary) traders hold long and short, by instrument (index and stock futures, index and stock calls and puts), at each day's close.

  python -m data_layer.participant_oi daily [--days 7]                    # the last N weekdays not yet stored (cron, after NSE publishes)
  python -m data_layer.participant_oi backfill --from 2012-01-01 [--to D] [--sleep 1.0] [--limit N]
  python -m data_layer.participant_oi status

Why: it is the only public record of who is net long or short index options in India, so it gives dealer-gamma signals a sign instead of
an assumption (options build plan, signals B5 and B11).  Index options are reported for all indices together, not per underlying.

Source: NSE's public archive, one small CSV per kind per day (about 1 KB):
  https://nsearchives.nseindia.com/content/nsccl/fao_participant_oi_DDMMYYYY.csv
  https://nsearchives.nseindia.com/content/nsccl/fao_participant_vol_DDMMYYYY.csv
A day the market was shut answers 404.  The files go back to January 2012.

Storage (vendor-neutral names): lake/positioning/participant_oi.parquet and lake/positioning/participant_volume.parquet, one row per
(date, participant) with the 14 contract counts (null where an early file says NA), upserted and replaced atomically.  Each file stays under 1 MB for the full history.
Days that answered 404 are kept in lake/positioning/participant_missing.json so a backfill does not ask for them again; a recent day
that is missing is retried by the nightly job until it is MISSING_FINAL_DAYS old (NSE sometimes publishes late).

Checks on every file: the four participants add up to the TOTAL row, and total long equals total short for each instrument (within a few
contracts: NSE's own totals are sometimes off by one).  A file that
fails is not stored and is reported.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import logging
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterable, Optional

import pandas as pd

from . import lake as lake_mod

log = logging.getLogger(__name__)

BASE_URL = "https://nsearchives.nseindia.com/content/nsccl/"
KINDS = {"oi": "fao_participant_oi", "volume": "fao_participant_vol"}
TABLE = {"oi": "participant_oi.parquet", "volume": "participant_volume.parquet"}
FIRST_DAY = date(2012, 1, 2)
PARTICIPANTS = ("Client", "DII", "FII", "Pro")
COLUMNS = ("future_index_long", "future_index_short", "future_stock_long", "future_stock_short",
           "option_index_call_long", "option_index_put_long", "option_index_call_short", "option_index_put_short",
           "option_stock_call_long", "option_stock_put_long", "option_stock_call_short", "option_stock_put_short",
           "total_long", "total_short")
PAIRS = (("future_index_long", "future_index_short"), ("future_stock_long", "future_stock_short"),
         ("option_index_call_long", "option_index_call_short"), ("option_index_put_long", "option_index_put_short"),
         ("option_stock_call_long", "option_stock_call_short"), ("option_stock_put_long", "option_stock_put_short"),
         ("total_long", "total_short"))
USER_AGENT = "jarvis-data/1.0 (daily research archive)"
MISSING_FINAL_DAYS = 10            # a 404 older than this is taken to be a holiday and not asked for again
MAX_CONSECUTIVE_ERRORS = 5         # stop a run when NSE keeps refusing (rate limit, outage)


def _close(a: int, b: int) -> bool:
    """NSE's own totals are sometimes off by a contract or two (seen on 2026-11-11): allow 10 contracts or 0.001 %."""
    return abs(a - b) <= max(10, 1e-5 * max(abs(a), abs(b)))


class ParseError(ValueError):
    pass


def url_for(kind: str, day: date) -> str:
    return f"{BASE_URL}{KINDS[kind]}_{day:%d%m%Y}.csv"


def positioning_dir(lake_dir: Optional[Path] = None) -> Path:
    return Path(lake_dir or lake_mod.LAKE_DIR) / "positioning"


def table_path(kind: str, lake_dir: Optional[Path] = None) -> Path:
    return positioning_dir(lake_dir) / TABLE[kind]


def missing_path(lake_dir: Optional[Path] = None) -> Path:
    return positioning_dir(lake_dir) / "participant_missing.json"


# ---------------------------------------------------------------------------------------------- parsing
_TITLE_DATE = re.compile(r"as on\s+([A-Za-z]+\s+\d{1,2},\s*\d{4})", re.I)


def _title_date(title: str) -> Optional[date]:
    m = _TITLE_DATE.search(title)
    if not m:
        return None
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%B %d,%Y", "%b %d,%Y"):
        try:
            return datetime.strptime(m.group(1).strip(), fmt).date()
        except ValueError:
            continue
    return None


def parse(text: str, kind: str, day: date) -> pd.DataFrame:
    """One NSE participant file -> rows for the four participants (date, participant, the 14 counts).  Raises ParseError when the file is
    not what it should be (wrong date in the title, missing participant, totals that do not add up)."""
    rows = [r for r in csv.reader(io.StringIO(text.lstrip("﻿"))) if any(c.strip() for c in r)]
    if len(rows) < 3:
        raise ParseError("too few lines")
    title = text.lstrip("\ufeff").splitlines()[0].replace('"', "")      # recent files double the quotes, so the CSV cell is cut at the comma
    want = "open interest" if kind == "oi" else "trading volume"
    if want not in title.lower():
        raise ParseError(f"title does not describe {want}: {title[:80]!r}")
    seen_day = _title_date(title)
    if seen_day and seen_day != day:
        raise ParseError(f"file is for {seen_day}, asked for {day}")
    header = [h.strip().lower().replace("_", " ") for h in rows[1]]
    if not header or header[0] != "client type" or len(header) < 1 + len(COLUMNS):
        raise ParseError(f"unexpected header: {rows[1][:3]}")
    values: dict[str, list[Optional[int]]] = {}
    for r in rows[2:]:
        name = r[0].strip()
        try:
            values[name.upper()] = [None if c.strip().upper() in ("", "NA", "-", "N/A") else int(float(c.strip())) for c in r[1:1 + len(COLUMNS)]]
        except ValueError as exc:
            raise ParseError(f"bad number in row {name!r}: {exc}") from None
    out = []
    for p in PARTICIPANTS:
        if p.upper() not in values:
            raise ParseError(f"participant {p} missing")
        out.append(dict(date=day, participant=p, **dict(zip(COLUMNS, values[p.upper()]))))
    df = pd.DataFrame(out)
    for c in COLUMNS:
        df[c] = df[c].astype("Int64")                   # early files say NA where a participant had no position reported
    if "TOTAL" in values:
        total = dict(zip(COLUMNS, values["TOTAL"]))
        for c in COLUMNS:
            if total[c] is not None and not _close(int(df[c].sum()), total[c]):
                raise ParseError(f"{c}: participants add to {int(df[c].sum())}, TOTAL says {total[c]}")
    for lo, sh in PAIRS:
        if not _close(int(df[lo].sum()), int(df[sh].sum())):
            raise ParseError(f"{lo} {int(df[lo].sum())} != {sh} {int(df[sh].sum())}")
    return df


# ---------------------------------------------------------------------------------------------- fetching
Fetch = Callable[[str], tuple[int, str]]          # url -> (HTTP status, body text)


def http_fetch(url: str, timeout: float = 20.0) -> tuple[int, str]:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "text/csv,*/*"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        return e.code, ""
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        log.warning("fetch %s: %s", url, e)
        return 0, ""


# ---------------------------------------------------------------------------------------------- storage
def load(kind: str, lake_dir: Optional[Path] = None) -> pd.DataFrame:
    p = table_path(kind, lake_dir)
    if not p.exists():
        return pd.DataFrame(columns=["date", "participant", *COLUMNS, "fetched_at"])
    df = pd.read_parquet(p)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    return df


def save(kind: str, new: pd.DataFrame, lake_dir: Optional[Path] = None) -> int:
    """Upsert `new` on (date, participant); returns the number of rows in the table."""
    if new.empty:
        return len(load(kind, lake_dir))
    old = load(kind, lake_dir)
    both = pd.concat([old, new], ignore_index=True) if len(old) else new.copy()
    both = both.drop_duplicates(["date", "participant"], keep="last").sort_values(["date", "participant"]).reset_index(drop=True)
    for c in COLUMNS:
        both[c] = both[c].astype("Int64")
    p = table_path(kind, lake_dir)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".parquet.tmp")
    both.to_parquet(tmp, index=False)
    tmp.replace(p)
    return len(both)


def load_missing(lake_dir: Optional[Path] = None) -> dict[str, dict]:
    p = missing_path(lake_dir)
    try:
        return json.loads(p.read_text()) if p.exists() else {}
    except (OSError, ValueError):
        return {}


def save_missing(state: dict, lake_dir: Optional[Path] = None) -> None:
    p = missing_path(lake_dir)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state, indent=1, sort_keys=True))
    tmp.replace(p)


def stored_days(kind: str, lake_dir: Optional[Path] = None) -> set[date]:
    return set(load(kind, lake_dir)["date"])


# ---------------------------------------------------------------------------------------------- runs
def weekdays(first: date, last: date) -> list[date]:
    out, d = [], first
    while d <= last:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


def run(days: Iterable[date], lake_dir: Optional[Path] = None, fetch: Fetch = http_fetch, sleep_s: float = 1.0,
        today: Optional[date] = None, limit: Optional[int] = None, sleeper: Callable[[float], None] = time.sleep) -> dict:
    """Fetch both kinds for each of `days` not already stored and not known to be a holiday.  Stores in batches, so a stopped run keeps
    what it got.  Returns counts."""
    today = today or datetime.now(timezone.utc).date()
    missing = load_missing(lake_dir)
    have = {k: stored_days(k, lake_dir) for k in KINDS}
    stats = dict(stored=0, holiday=0, errors=0, bad=0, skipped=0, calls=0)
    batch: dict[str, list[pd.DataFrame]] = {k: [] for k in KINDS}
    consecutive = 0

    def flush() -> None:
        for k in KINDS:
            if batch[k]:
                save(k, pd.concat(batch[k], ignore_index=True), lake_dir)
                batch[k] = []
        save_missing(missing, lake_dir)

    todo = [d for d in sorted(set(days)) if d >= FIRST_DAY]
    for d in todo:
        key = d.isoformat()
        if all(d in have[k] for k in KINDS):
            stats["skipped"] += 1
            continue
        m = missing.get(key)
        if m and m.get("final"):
            stats["skipped"] += 1
            continue
        if limit is not None and stats["calls"] >= limit:
            break
        for k in KINDS:
            if d in have[k]:
                continue
            if stats["calls"]:
                sleeper(sleep_s)
            status, body = fetch(url_for(k, d))
            stats["calls"] += 1
            if status == 404:
                missing[key] = {"final": (today - d).days > MISSING_FINAL_DAYS, "seen": today.isoformat()}
                stats["holiday"] += 1
                consecutive = 0
                break                                   # a 404 for one kind means the market was shut: do not ask for the other
            if status != 200 or not body:
                stats["errors"] += 1
                consecutive += 1
                log.warning("%s %s: HTTP %s", k, d, status)
                if consecutive >= MAX_CONSECUTIVE_ERRORS:
                    flush()
                    stats["stopped"] = f"{consecutive} errors in a row (last HTTP {status})"
                    return stats
                continue
            consecutive = 0
            try:
                batch[k].append(parse(body, k, d))
                have[k].add(d)
                missing.pop(key, None)
                stats["stored"] += 1
            except ParseError as exc:
                stats["bad"] += 1
                log.warning("%s %s: %s", k, d, exc)
        if sum(len(v) for v in batch.values()) >= 100:
            flush()
    flush()
    return stats


def status(lake_dir: Optional[Path] = None) -> dict:
    out = {}
    for k in KINDS:
        p = table_path(k, lake_dir)
        df = load(k, lake_dir)
        out[k] = dict(days=int(df["date"].nunique()), first=str(min(df["date"])) if len(df) else None,
                      last=str(max(df["date"])) if len(df) else None, bytes=p.stat().st_size if p.exists() else 0)
    miss = load_missing(lake_dir)
    out["holidays_known"] = sum(1 for v in miss.values() if v.get("final"))
    out["missing_recent"] = sorted(k for k, v in miss.items() if not v.get("final"))
    return out


def main(argv: Optional[list[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(prog="python -m data_layer.participant_oi", description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    dl = sub.add_parser("daily", help="the last N weekdays not yet stored")
    dl.add_argument("--days", type=int, default=7)
    bf = sub.add_parser("backfill", help="every weekday in a range not yet stored")
    bf.add_argument("--from", dest="d_from", type=date.fromisoformat, default=FIRST_DAY)
    bf.add_argument("--to", dest="d_to", type=date.fromisoformat, default=None)
    bf.add_argument("--limit", type=int, default=None, help="stop after this many requests")
    for p in (dl, bf):
        p.add_argument("--sleep", type=float, default=1.0, help="seconds between requests (default 1)")
    sub.add_parser("status")
    a = ap.parse_args(argv)
    today = datetime.now(timezone.utc).date()
    if a.cmd == "status":
        print(json.dumps(status(), indent=1))
        return 0
    if a.cmd == "daily":
        days = weekdays(today - timedelta(days=a.days), today)
        st = run(days, sleep_s=a.sleep, today=today)
    else:
        st = run(weekdays(a.d_from, a.d_to or today), sleep_s=a.sleep, today=today, limit=a.limit)
    print(f"participant {a.cmd}: {st['stored']} files stored, {st['holiday']} holidays, {st['skipped']} days already done, "
          f"{st['errors']} errors, {st['bad']} bad files, {st['calls']} requests" + (f"; stopped: {st['stopped']}" if "stopped" in st else ""))
    return 1 if ("stopped" in st or st["bad"]) else 0


if __name__ == "__main__":
    sys.exit(main())
