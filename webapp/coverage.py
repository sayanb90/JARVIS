"""
What data the lake holds, and what the historical run has done: the numbers behind the viewer's Data tab.

Everything here is read-only and cheap: it lists partition folders, reads a few small files and, for the daily candles and the per-market
ranges, one narrow DuckDB query with a memory cap.  It never opens a tick file.  `coverage()` is the fast part (cached for a minute by the
app); `market_ranges()` reads the bars tables and is fetched separately so the page does not wait for it.

Per dataset the page needs: the dates that have data, the first and last, how many trading days in between have nothing (a gap), and a
state for each of the last RECENT_DAYS days:
  ok       the day has data
  open     the day's folder still holds uncompacted segments (the feed is writing it, or it was cut off)
  missing  an open market day with no data after the dataset began
  closed   weekend or exchange holiday (India: the Upstox calendar in lake/reference; other markets: weekends only)
  before   the dataset did not exist yet
  pending  today, and the data has not arrived yet
  none     the dataset is not a daily series (fundamentals: only its last fetch is meaningful)
"""

from __future__ import annotations

import json
import os
import shutil
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import duckdb

RECENT_DAYS = 60
STAGES = ("universe", "daily", "1m", "fx", "fundamentals", "trial-spot", "trial-expired", "spot", "expired")     # deploy/historical-run.sh
DB_MEMORY = "1GB"


@dataclass(frozen=True)
class Spec:
    id: str
    label: str
    group: str
    source: str
    kind: str                       # "live": recorded from the day capture began, cannot be backfilled; "history": backfillable bars
    cal: Optional[str]              # exchange calendar for closed days ("NSE", "NFO"), None: weekends only
    note: str
    daily: bool = True              # False: not a daily series
    short: str = ""                 # the short row name on the charts


SPECS = (
    Spec("ticks_eq", "Ticks: stocks and indices", "India, recorded live", "Upstox feed", "live", "NSE",
         "every feed update: price, volume, best bid and ask with sizes, trade size, auction fields, three timestamps", short="Ticks · stocks"),
    Spec("ticks_fo", "Ticks: NIFTY and BANKNIFTY futures and options", "India, recorded live", "Upstox feed", "live", "NFO",
         "the same, plus open interest, IV, delta and gamma on options", short="Ticks · F&O"),
    Spec("events", "Session events", "India, recorded live", "Upstox feed", "live", "NSE", "segment status, pre-open and closing-auction phases", short="Session events"),
    Spec("bars_1m", "1-minute bars", "Bars", "Upstox candles (India), Yahoo (other markets)", "history", None,
         "open, high, low, close, volume, and open interest for derivatives; no bid, ask or size", short="1-minute bars"),
    Spec("ohlc", "Daily candles", "Bars", "Yahoo", "history", None, "open, high, low, close, volume, adjusted by Yahoo for splits and dividends", short="Daily candles"),
    Spec("fx", "Daily FX rates", "Reference", "Yahoo", "history", None, "currency against the dollar, to compare statements with prices", short="FX rates"),
    Spec("fund_yahoo", "Fundamentals (Yahoo)", "Reference", "Yahoo", "history", None, "annual statements, profile, dividends and splits; four years per company", daily=False),
    Spec("fund_upstox", "Fundamentals (Upstox)", "Reference", "Upstox", "history", None, "statements, ratios, shareholding and corporate actions, first-seen dates", daily=False),
)


# ---------------------------------------------------------------------------------------------- dates per dataset
def _dated_folders(base: Path) -> tuple[set[str], set[str]]:
    """(dates with files, dates still holding uncompacted segments) of a `date=YYYY-MM-DD` partition folder."""
    days, open_days = set(), set()
    if not base.is_dir():
        return days, open_days
    for f in base.glob("date=*"):
        files = [p for p in f.iterdir() if p.suffix == ".parquet"] if f.is_dir() else []
        if not files:
            continue
        d = f.name[5:]
        days.add(d)
        if any(p.name.startswith("seg-") for p in files):
            open_days.add(d)
    return days, open_days


def _query_dates(con, sql: str) -> set[str]:
    try:
        return {str(r[0])[:10] for r in con.execute(sql).fetchall() if r[0] is not None}
    except Exception:                                              # noqa: BLE001 -- a file that cannot be read is a dataset with no dates
        return set()


def _con() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{DB_MEMORY}'")
    con.execute("SET threads=2")
    return con


def dataset_dates(lake: Path) -> dict[str, tuple[set[str], set[str]]]:
    out: dict[str, tuple[set[str], set[str]]] = {}
    legacy = _dated_folders(lake / "ticks")                         # the older layout: one folder for both segments
    for seg, ds in (("eq", "ticks_eq"), ("fo", "ticks_fo")):
        days, open_days = _dated_folders(lake / "ticks" / f"segment={seg}")
        out[ds] = (days | legacy[0], open_days | legacy[1])
    out["events"] = _dated_folders(lake / "events")
    out["bars_1m"] = _dated_folders(lake / "bars" / "interval=1m")
    con = None
    if any((lake / "ohlc").glob("year=*/ohlc.parquet")):
        con = _con()
        out["ohlc"] = (_query_dates(con, f"SELECT DISTINCT CAST(date AS DATE) FROM read_parquet('{lake}/ohlc/year=*/ohlc.parquet', hive_partitioning=false)"), set())
    fx = lake / "fx" / "rates.parquet"
    if fx.is_file():
        out["fx"] = (_query_dates(con or _con(), f"SELECT DISTINCT CAST(date AS DATE) FROM read_parquet('{fx}')"), set())
    for ds, folder in (("fund_yahoo", "yf_fundamentals"), ("fund_upstox", "upstox_fundamentals")):
        log = lake / folder / "fetch_log.parquet"
        if log.is_file():
            out[ds] = (_query_dates(con or _con(), f"SELECT DISTINCT CAST(CAST(fetched_at AS TIMESTAMP) AS DATE) FROM read_parquet('{log}')"), set())
    return out


# ---------------------------------------------------------------------------------------------- states
def _calendar(lake: Path) -> Optional[dict]:
    from data_layer import market_calendar
    try:
        return market_calendar.load(lake)
    except Exception:                                              # noqa: BLE001
        return None


def day_state(d: date, spec: Spec, present: set[str], open_days: set[str], first: Optional[str], today: date, cal: Optional[dict]) -> str:
    iso = d.isoformat()
    if iso in present:
        return "open" if iso in open_days and d < today else "ok"
    if not spec.daily:
        return "none"
    if spec.cal:
        from data_layer import market_calendar
        if market_calendar.status(d, spec.cal, cal, today).state == "closed":
            return "closed"
    elif d.weekday() >= 5:
        return "closed"
    if first is None or iso < first:
        return "before"
    if d >= today:
        return "pending"
    return "missing"


def _expected_missing(spec: Spec, present: set[str], first: str, today: date, cal: Optional[dict]) -> int:
    """Open days from the first day with data up to yesterday that have no data (a dataset that stopped growing counts its days since)."""
    n, d, end = 0, date.fromisoformat(first), today - timedelta(days=1)
    while d <= end:
        if d.isoformat() not in present and day_state(d, spec, present, set(), first, today, cal) == "missing":
            n += 1
        d += timedelta(days=1)
    return n


def build_datasets(lake: Path, today: Optional[date] = None) -> list[dict]:
    today = today or datetime.now(timezone.utc).date()
    cal = _calendar(lake)
    found = dataset_dates(lake)
    specs = list(SPECS)
    known = {"1m"}
    for p in sorted((lake / "bars").glob("interval=*")) if (lake / "bars").is_dir() else []:
        iv = p.name.split("=", 1)[1]
        if iv not in known and _dated_folders(p)[0]:
            specs.append(Spec(f"bars_{iv}", f"{iv} bars (older collection)", "Bars", "earlier feed or Yahoo", "history", None, "kept as stored", short=f"{iv} bars"))
            found[f"bars_{iv}"] = _dated_folders(p)
    out = []
    for spec in specs:
        days, open_days = found.get(spec.id, (set(), set()))
        first, last = (min(days), max(days)) if days else (None, None)
        recent = []
        if spec.daily and days:
            for k in range(RECENT_DAYS - 1, -1, -1):
                d = today - timedelta(days=k)
                recent.append({"d": d.isoformat(), "s": day_state(d, spec, days, open_days, first, today, cal)})
        out.append({"id": spec.id, "label": spec.label, "group": spec.group, "source": spec.source, "kind": spec.kind, "note": spec.note,
                    "short": spec.short or spec.label, "daily": spec.daily, "first": first, "last": last, "days": len(days), "open_days": len(open_days),
                    "missing": _expected_missing(spec, days, first, today, cal) if (spec.daily and days) else 0,
                    "recent_missing": sum(1 for r in recent if r["s"] == "missing"), "recent": recent})
    return out


# ---------------------------------------------------------------------------------------------- historical run, backfill, snapshot
def historical_run(logs_dir: Path) -> dict:
    """Stage statuses from the historical-run script's state file (last line per stage wins)."""
    p = logs_dir / "historical-run.state"
    last: dict[str, tuple[str, str]] = {}
    if p.is_file():
        for line in p.read_text().splitlines():
            parts = line.split("\t")
            if len(parts) >= 3:
                last[parts[0]] = (parts[1], parts[2])
    return {"present": p.is_file(), "stages": [{"stage": s, "status": last.get(s, ("pending", ""))[0], "date": last.get(s, ("", ""))[1]} for s in STAGES]}


def backfill_state(lake: Path) -> dict:
    p = lake / "backfill" / "state.json"
    if not p.is_file():
        return {"present": False, "kinds": [], "last": None, "errors": []}
    try:
        tasks = json.loads(p.read_text())
    except (OSError, ValueError):
        return {"present": False, "kinds": [], "last": None, "errors": []}
    kinds: dict[str, dict] = {}
    errors = []
    for task, v in tasks.items():
        k = kinds.setdefault(task.split("|", 1)[0], {"kind": task.split("|", 1)[0], "ok": 0, "empty": 0, "error": 0, "rows": 0})
        st = v.get("status", "error")
        k[st if st in k else "error"] += 1
        k["rows"] += int(v.get("rows") or 0)
        if st == "error" and len(errors) < 5:
            errors.append({"task": task, "msg": v.get("msg", ""), "at": v.get("at", "")})
    return {"present": True, "kinds": sorted(kinds.values(), key=lambda k: k["kind"]), "last": max((v.get("at", "") for v in tasks.values()), default=None), "errors": errors}


def snapshot_info() -> dict:
    try:
        from research import snapshot as sn
    except Exception:                                              # noqa: BLE001
        return {"present": False}
    root = Path(os.getenv("JARVIS_RESEARCH_ROOT") or sn.DEFAULT_ROOT)
    if not (root / "snapshots").is_dir():
        return {"present": False, "root": str(root)}
    ok, line = sn.status(root)
    try:
        m = sn._load_manifest(sn.resolve(root))
    except Exception:                                              # noqa: BLE001
        return {"present": True, "ok": False, "line": line, "root": str(root)}
    ticks = [c["last"] for k, c in m["coverage"].items() if k.startswith("ticks") and c["last"]]
    return {"present": True, "ok": ok, "line": line, "root": str(root), "id": m["snapshot_id"], "created": m["created_at"], "files": len(m["files"]),
            "bytes": m["bytes"], "skipped": len(m["skipped"]), "warnings": len(m["warnings"]), "newest_tick_day": max(ticks) if ticks else None,
            "count": len(sn.list_snapshots(root))}


# ---------------------------------------------------------------------------------------------- storage
def _dir_bytes(path: Path) -> int:
    total = 0
    try:
        with os.scandir(path) as it:
            for e in it:
                try:
                    total += _dir_bytes(Path(e.path)) if e.is_dir(follow_symlinks=False) else e.stat(follow_symlinks=False).st_size
                except OSError:
                    continue
    except OSError:
        pass
    return total


def storage(lake: Path) -> dict:
    rows = []
    if lake.is_dir():
        for top in sorted(p for p in lake.iterdir() if p.is_dir() and not p.name.startswith(".")):
            if top.name in ("ticks", "bars"):
                for sub in sorted(p for p in top.iterdir() if p.is_dir() and "=" in p.name and not p.name.startswith("date=")):
                    rows.append({"name": f"{top.name}/{sub.name}", "bytes": _dir_bytes(sub)})
                rest = sum(_dir_bytes(p) for p in top.iterdir() if p.is_dir() and p.name.startswith("date="))
                if rest:
                    rows.append({"name": f"{top.name}/older layout", "bytes": rest})
            else:
                rows.append({"name": top.name, "bytes": _dir_bytes(top)})
    tick_days = []
    for seg in ("eq", "fo"):
        base = lake / "ticks" / f"segment={seg}"
        for f in sorted(base.glob("date=*")) if base.is_dir() else []:
            tick_days.append((f.name[5:], _dir_bytes(f)))
    per_day: dict[str, int] = {}
    for d, b in tick_days:
        per_day[d] = per_day.get(d, 0) + b
    recent = [b for _, b in sorted(per_day.items())[-5:] if b > 0]
    avg = sum(recent) / len(recent) if recent else None
    try:
        free = shutil.disk_usage(lake if lake.exists() else lake.parent).free
    except OSError:
        free = None
    return {"rows": sorted(rows, key=lambda r: -r["bytes"]), "total": sum(r["bytes"] for r in rows), "free": free, "tick_day_avg": avg,
            "tick_days_counted": len(recent), "runway_days": (free / avg) if (free and avg) else None}


# ---------------------------------------------------------------------------------------------- per-market ranges (the slow part)
def market_ranges(lake: Path) -> dict:
    out: dict = {"ohlc": [], "bars_1m": [], "error": None}
    try:
        con = _con()
        if any((lake / "ohlc").glob("year=*/ohlc.parquet")):
            for m, a, b, n in con.execute(f"SELECT market, min(date), max(date), count(DISTINCT symbol) FROM read_parquet('{lake}/ohlc/year=*/ohlc.parquet', "
                                          f"hive_partitioning=false) GROUP BY 1 ORDER BY 1").fetchall():
                out["ohlc"].append({"market": m, "first": str(a)[:10], "last": str(b)[:10], "symbols": int(n)})
        days = sorted(p for p in (lake / "bars" / "interval=1m").glob("date=*") if any(p.glob("*.parquet"))) if (lake / "bars" / "interval=1m").is_dir() else []
        if days:
            pat = f"{lake}/bars/interval=1m/date=*/*.parquet"
            rng = {m: (a, b, n) for m, a, b, n in con.execute(f"SELECT market, min(ts), max(ts), count(*) FROM read_parquet('{pat}', union_by_name=true) GROUP BY 1").fetchall()}
            latest: dict[str, tuple[int, str]] = {}                  # market -> (symbols, day) on that market's own last day: newest folders first
            for folder in reversed(days[-15:]):
                for m, n in con.execute(f"SELECT market, count(DISTINCT symbol) FROM read_parquet('{folder}/*.parquet', union_by_name=true) GROUP BY 1").fetchall():
                    latest.setdefault(m, (int(n), folder.name[5:]))
                if set(rng) <= set(latest):
                    break
            for m in sorted(rng):
                a, b, n = rng[m]
                sym, day = latest.get(m, (0, str(b)[:10]))
                out["bars_1m"].append({"market": m, "first": str(a)[:10], "last": str(b)[:10], "bars": int(n), "symbols_latest": sym, "latest_day": day})
    except Exception as exc:                                       # noqa: BLE001 -- the page shows the reason instead of failing
        out["error"] = str(exc).splitlines()[0][:200]
    return out


# ---------------------------------------------------------------------------------------------- the whole payload
def coverage(lake: Path, today: Optional[date] = None) -> dict:
    lake = Path(lake)
    today = today or datetime.now(timezone.utc).date()
    datasets = build_datasets(lake, today)
    live = [d for d in datasets if d["kind"] == "live" and d["days"]]
    return {"generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), "today": today.isoformat(), "recent_days": RECENT_DAYS,
            "datasets": datasets, "history_run": historical_run(lake.parent / "logs"), "backfill": backfill_state(lake), "snapshot": snapshot_info(),
            "storage": storage(lake), "gaps_recent": sum(d["recent_missing"] for d in datasets),
            "live_sessions": max((d["days"] for d in live), default=0), "demo": (lake / "DEMO").exists()}
