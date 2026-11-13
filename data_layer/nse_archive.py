"""
Shared plumbing for the collectors of public daily files (NSE's archive, the RBI home page): a polite fetcher with an honest User-Agent,
the weekday loop that remembers which days answered 404 (holidays), atomic writes, and the stop rules.  Each collector (eod_fo,
fii_derivatives, nse_daily_files, index_weights, rates) keeps its own parser, table and CLI; this module holds only what they share.

Rules every collector follows (copied from participant_oi.py, SCRUM-175):
  * one request at a time, about one a second (`Polite`), User-Agent "jarvis-data/1.0 (daily research archive)"; no browser headers, no
    cookies.  A source that refuses that (403, a login, or an HTML page in place of the file) is not worked around: the collector stops and
    offers `import FILE` for a file a person downloaded.
  * a 404 means the market was shut that day.  It is remembered in `<table>_missing.json` so a backfill does not ask again; a recent one is
    retried until it is MISSING_FINAL_DAYS old (NSE sometimes publishes late).  A file that fails its checks is remembered as `bad` (never
    final, so the next run tries again) and reported.
  * five errors in a row (rate limit, outage, refusal) stop the run; what was fetched so far is kept.
  * every table file is written to a temp name and renamed, so a reader never sees half a file.
"""

from __future__ import annotations

import io
import json
import logging
import time
import urllib.error
import urllib.request
import zipfile
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

import pandas as pd

log = logging.getLogger(__name__)

USER_AGENT = "jarvis-data/1.0 (daily research archive)"
MISSING_FINAL_DAYS = 10            # a 404 older than this is taken to be a holiday and not asked for again
MAX_CONSECUTIVE_ERRORS = 5         # stop a run when the source keeps failing (rate limit, outage, refusal)
HTML_INSTEAD = -1                  # pseudo status: the server answered 200 with a web page where a data file was asked for (a refusal page)

Fetch = Callable[[str], tuple[int, bytes]]          # url -> (HTTP status, body); 0 = no answer (network error)


class ParseError(ValueError):
    """The file is not what it should be (wrong date, missing columns, totals that do not add up)."""


def http_fetch(url: str, timeout: float = 30.0) -> tuple[int, bytes]:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "*/*"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read()
            ctype = (r.headers.get("Content-Type") or "").lower()
            if "text/html" in ctype and not url.lower().split("?")[0].endswith((".html", ".htm", ".aspx", "/")):
                return HTML_INSTEAD, body
            return r.status, body
    except urllib.error.HTTPError as e:
        return e.code, b""
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        log.warning("fetch %s: %s", url, e)
        return 0, b""


class Polite:
    """Wraps a fetch function: waits `sleep_s` between two requests and counts them."""

    def __init__(self, fetch: Fetch = http_fetch, sleep_s: float = 1.0, sleeper: Callable[[float], None] = time.sleep):
        self.fetch, self.sleep_s, self.sleeper, self.calls = fetch, sleep_s, sleeper, 0

    def __call__(self, url: str) -> tuple[int, bytes]:
        if self.calls and self.sleep_s > 0:
            self.sleeper(self.sleep_s)
        self.calls += 1
        return self.fetch(url)


def unzip_one(body: bytes, suffix: str = ".csv") -> bytes:
    """The first member of a zip archive whose name ends with `suffix`."""
    try:
        with zipfile.ZipFile(io.BytesIO(body)) as z:
            names = [n for n in z.namelist() if n.lower().endswith(suffix)]
            if not names:
                raise ParseError(f"no {suffix} file in the archive ({z.namelist()[:3]})")
            return z.read(names[0])
    except zipfile.BadZipFile as exc:
        raise ParseError(f"not a zip archive: {exc}") from None


def weekdays(first: date, last: date) -> list[date]:
    out, d = [], first
    while d <= last:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


# ---------------------------------------------------------------------------------------------- atomic files
def write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    df.to_parquet(tmp, index=False)
    tmp.replace(path)


def write_json(obj: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(obj, indent=1, sort_keys=True, default=str))
    tmp.replace(path)


def read_parquet(path: Path, columns: Iterable[str] = ()) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=list(columns))
    return pd.read_parquet(path)


def upsert_parquet(path: Path, new: pd.DataFrame, keys: list[str], order: Optional[list[str]] = None) -> int:
    """Merge `new` into the table at `path` on `keys` (new rows win), sorted by `order`, written atomically.  Returns the row count."""
    old = read_parquet(path)
    if len(old) and len(new):
        both = pd.concat([old, new], ignore_index=True)
    else:
        both = new.copy() if len(new) else old
    both = both.drop_duplicates(keys, keep="last").sort_values(order or keys).reset_index(drop=True)
    write_parquet(both, path)
    return len(both)


# ---------------------------------------------------------------------------------------------- remembered days
class DayState:
    """Days that answered 404 (holidays), failed their checks (bad), or were fetched and held no rows (empty: a day with no ban, no short
    sale), kept in one JSON file next to the table.  Days with rows come from the table itself (`stored`)."""

    def __init__(self, path: Path, stored: Iterable[date] = ()):
        self.path = Path(path)
        raw = {}
        try:
            raw = json.loads(self.path.read_text()) if self.path.exists() else {}
        except (OSError, ValueError):
            raw = {}
        self.missing: dict[str, dict] = dict(raw.get("missing", {}))
        self.empty: set[str] = set(raw.get("empty", []))
        self.stored: set[str] = {d.isoformat() for d in stored}

    def done(self, d: date) -> bool:
        k = d.isoformat()
        return k in self.stored or k in self.empty

    def skip(self, d: date) -> bool:
        return self.done(d) or bool(self.missing.get(d.isoformat(), {}).get("final"))

    def mark_done(self, d: date, rows: int = 1) -> None:
        k = d.isoformat()
        self.missing.pop(k, None)
        (self.stored if rows else self.empty).add(k)

    def mark_missing(self, d: date, today: date) -> None:
        self.missing[d.isoformat()] = {"final": (today - d).days > MISSING_FINAL_DAYS, "seen": today.isoformat(), "reason": "404"}

    def mark_bad(self, d: date, today: date, why: str) -> None:
        self.missing[d.isoformat()] = {"final": False, "seen": today.isoformat(), "reason": f"bad: {why}"[:300]}

    def save(self) -> None:
        write_json({"missing": self.missing, "empty": sorted(self.empty)}, self.path)

    def summary(self) -> dict:
        return {"holidays_known": sum(1 for v in self.missing.values() if v.get("final")),
                "missing_recent": sorted(k for k, v in self.missing.items() if not v.get("final") and v.get("reason", "404") == "404"),
                "bad": {k: v["reason"] for k, v in sorted(self.missing.items()) if str(v.get("reason", "")).startswith("bad")},
                "empty_days": len(self.empty)}


@dataclass
class Outcome:
    """What one day's attempt gave: ok (data = rows to store), missing (404), bad (failed checks), error (no usable answer)."""
    kind: str
    data: Any = None
    note: str = ""


def run_days(days: Iterable[date], state: DayState, attempt: Callable[[date], Outcome], store: Callable[[list[tuple[date, Any]]], None],
             fetcher: Polite, today: date, limit: Optional[int] = None, first_day: Optional[date] = None, flush_every: int = 20,
             label: str = "") -> dict:
    """For each day not done and not a known holiday: `attempt(day)`, then `store([(day, data), ...])` in batches.  The state is saved
    after each stored batch, so a stopped run keeps what it got and the next one carries on.  Returns counts."""
    stats = dict(stored=0, holiday=0, errors=0, bad=0, skipped=0, calls=0)
    pending: list[tuple[date, Any]] = []
    consecutive = 0

    def flush() -> None:
        if pending:
            store(list(pending))
            pending.clear()
        state.save()

    for d in sorted(set(days)):
        if (first_day and d < first_day) or d > today:
            continue
        if state.skip(d):
            stats["skipped"] += 1
            continue
        if limit is not None and fetcher.calls >= limit:
            break
        out = attempt(d)
        if out.kind == "ok":
            consecutive = 0
            n = len(out.data) if hasattr(out.data, "__len__") else 1
            if n:
                pending.append((d, out.data))
            state.mark_done(d, n)
            stats["stored"] += 1
        elif out.kind == "missing":
            consecutive = 0
            state.mark_missing(d, today)
            stats["holiday"] += 1
        elif out.kind == "bad":
            consecutive = 0
            state.mark_bad(d, today, out.note)
            stats["bad"] += 1
            log.warning("%s %s: %s", label, d, out.note)
        else:
            stats["errors"] += 1
            consecutive += 1
            log.warning("%s %s: %s", label, d, out.note or "error")
            if consecutive >= MAX_CONSECUTIVE_ERRORS:
                stats["stopped"] = f"{consecutive} errors in a row ({out.note})"
                break
        if len(pending) >= flush_every:
            flush()
    flush()
    stats["calls"] = fetcher.calls
    return stats


def summary_line(name: str, cmd: str, st: dict) -> str:
    return (f"{name} {cmd}: {st['stored']} days stored, {st['holiday']} holidays, {st['skipped']} days already done, {st['errors']} errors, "
            f"{st['bad']} bad files, {st['calls']} requests" + (f"; stopped: {st['stopped']}" if "stopped" in st else ""))


def classify(status: int, what: str) -> Optional[Outcome]:
    """The Outcome for a status that is not a usable 200, or None when the body should be parsed."""
    if status == 200:
        return None
    if status == 404:
        return Outcome("missing")
    if status == HTML_INSTEAD:
        return Outcome("error", note=f"{what}: the server sent a web page instead of the file (refused)")
    return Outcome("error", note=f"{what}: HTTP {status}")
