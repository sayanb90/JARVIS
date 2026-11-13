"""
FII derivative statistics (SCRUM-185): what foreign institutional investors bought and sold in NSE's derivatives each day, and what they
held at the close -- contracts and value in crores for index futures, index options, stock futures and stock options, and (from 2023:
not in the 02-Jan-2023 file, there by 01-Jun-2023) the same split by index (NIFTY, BANKNIFTY, FINNIFTY, MIDCPNIFTY, NIFTYNXT50 ...).

  python -m data_layer.fii_derivatives daily [--days 10]                   # the last N weekdays not yet stored (cron, after NSE publishes)
  python -m data_layer.fii_derivatives backfill --from 2014-12-01 [--to D] [--sleep 1.0] [--limit N]
  python -m data_layer.fii_derivatives import FILE --date YYYY-MM-DD      # a file downloaded by hand
  python -m data_layer.fii_derivatives status

Why: participant OI (participant_oi.py) gives FII positions in contracts and long/short; this file adds the rupee value of the day's buying
and selling and of the open interest, which is how FII flow is usually quoted.

Source: one small Excel 97 (.xls) file per day on NSE's public archive (6 to 30 KB):
  https://nsearchives.nseindia.com/content/fo/fii_stats_DD-Mon-YYYY.xls        (e.g. fii_stats_12-Nov-2026.xls)
(archives.nseindia.com serves the same files.)  Measured 2026-11-13: the first file is 01-Dec-2014; every day tried before it (2005 to
28-Nov-2014) answers 404.  A day the market was shut answers 404 and is remembered (lake/positioning/fii_derivatives_missing.json).
Reading .xls needs the `xlrd` package (added to requirements.txt; openpyxl reads only .xlsx).

Storage: lake/positioning/fii_derivatives.parquet, one row per (date, instrument, underlying): instrument is index_futures,
index_options, stock_futures or stock_options; underlying is ALL for the instrument's total and the index name for the per-index rows.
Columns buy_contracts, buy_amount_cr, sell_contracts, sell_amount_cr, oi_contracts, oi_amount_cr (crores of rupees: options bought and sold
are valued at strike x quantity, open interest at the underlying's close, futures at the traded or settlement price -- NSE's notes).
About 30 rows a day, a few hundred KB for the whole history.

Checks on every file: the title's date is the requested day, the four instruments are there, and the per-index rows add up to the
instrument's total (contracts within a few, crores within rounding).  A file that fails is not stored and is reported.
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import re
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

from . import lake as lake_mod
from .nse_archive import (DayState, Outcome, ParseError, Polite, classify, http_fetch, read_parquet, run_days, summary_line,
                          upsert_parquet, weekdays)

log = logging.getLogger(__name__)

URL = "https://nsearchives.nseindia.com/content/fo/fii_stats_{d:%d}-{mon}-{d:%Y}.xls"
FIRST_DAY = date(2014, 12, 1)
INSTRUMENTS = {"INDEX FUTURES": "index_futures", "INDEX OPTIONS": "index_options", "STOCK FUTURES": "stock_futures",
               "STOCK OPTIONS": "stock_options"}
VALUES = ["buy_contracts", "buy_amount_cr", "sell_contracts", "sell_amount_cr", "oi_contracts", "oi_amount_cr"]
COLUMNS = ["date", "instrument", "underlying", *VALUES]
KEY = ["date", "instrument", "underlying"]


def url_for(day: date) -> str:
    return URL.format(d=day, mon=day.strftime("%b"))


def table_path(lake_dir: Optional[Path] = None) -> Path:
    return Path(lake_dir or lake_mod.LAKE_DIR) / "positioning" / "fii_derivatives.parquet"


def state_path(lake_dir: Optional[Path] = None) -> Path:
    return table_path(lake_dir).with_name("fii_derivatives_missing.json")


# ---------------------------------------------------------------------------------------------- parsing
_TITLE = re.compile(r"STATISTICS\s+FOR\s+(\d{1,2}-[A-Za-z]{3}-\d{4})", re.I)


def _read_sheet(body: bytes) -> pd.DataFrame:
    try:
        return pd.read_excel(io.BytesIO(body), header=None, engine="xlrd")
    except ImportError:
        raise
    except Exception as exc:                                   # noqa: BLE001 -- xlrd raises its own types for a file that is not .xls
        raise ParseError(f"not a readable .xls file: {exc}") from None


def parse(body: bytes, day: date) -> pd.DataFrame:
    sheet = _read_sheet(body)
    cells = sheet.astype(object).where(sheet.notna(), None)
    title = " ".join(str(v) for v in cells.iloc[:3, 0] if v)
    m = _TITLE.search(title)
    if not m:
        raise ParseError(f"no 'FII DERIVATIVES STATISTICS FOR <date>' title: {title[:80]!r}")
    seen = datetime.strptime(m.group(1), "%d-%b-%Y").date()
    if seen != day:
        raise ParseError(f"file is for {seen}, asked for {day}")
    rows, current = [], None
    for r in cells.itertuples(index=False):
        label = str(r[0] or "").strip().upper()
        if not label or label.startswith("NOTES"):
            if label.startswith("NOTES"):
                break
            continue
        if not label.endswith(("FUTURES", "OPTIONS")):
            continue
        try:
            vals = [float(str(v).replace(",", "")) for v in r[1:7]]
        except (TypeError, ValueError):
            raise ParseError(f"bad number in row {label!r}: {list(r[1:7])}") from None
        if label in INSTRUMENTS:
            current, underlying = INSTRUMENTS[label], "ALL"
        elif current in ("index_futures", "index_options") and label.endswith("FUTURES" if current == "index_futures" else "OPTIONS"):
            underlying = label.rsplit(" ", 1)[0].strip()
        else:
            raise ParseError(f"row {label!r} is not under an index instrument")
        rows.append(dict(date=day, instrument=current, underlying=underlying, **dict(zip(VALUES, vals))))
    df = pd.DataFrame(rows, columns=COLUMNS)
    have = set(df.loc[df["underlying"] == "ALL", "instrument"])
    if have != set(INSTRUMENTS.values()):
        raise ParseError(f"instruments missing: {sorted(set(INSTRUMENTS.values()) - have)}")
    if df.duplicated(KEY).any():
        raise ParseError("a row appears twice")
    for inst, g in df.groupby("instrument"):
        parts = g[g["underlying"] != "ALL"]
        if parts.empty:
            continue
        total = g[g["underlying"] == "ALL"].iloc[0]
        for c in VALUES:
            tol = (3 if c.endswith("contracts") else 0.02 * len(parts) + 0.05)
            if abs(parts[c].sum() - total[c]) > tol:
                raise ParseError(f"{inst} {c}: indices add to {parts[c].sum():.2f}, total says {total[c]:.2f}")
    for c in VALUES:
        if c.endswith("contracts"):
            df[c] = df[c].round().astype("Int64")
    return df


# ---------------------------------------------------------------------------------------------- storage and runs
def load(lake_dir: Optional[Path] = None) -> pd.DataFrame:
    df = read_parquet(table_path(lake_dir), COLUMNS)
    if len(df):
        df["date"] = pd.to_datetime(df["date"]).dt.date
    return df


def save(new: pd.DataFrame, lake_dir: Optional[Path] = None) -> int:
    return upsert_parquet(table_path(lake_dir), new, KEY, ["date", "instrument", "underlying"])


def run(days, lake_dir: Optional[Path] = None, fetch=http_fetch, sleep_s: float = 1.0, today: Optional[date] = None,
        limit: Optional[int] = None, sleeper=None) -> dict:
    today = today or datetime.now(timezone.utc).date()
    polite = Polite(fetch, sleep_s, sleeper) if sleeper else Polite(fetch, sleep_s)
    state = DayState(state_path(lake_dir), set(load(lake_dir)["date"]))

    def attempt(d: date) -> Outcome:
        status_, body = polite(url_for(d))
        bad = classify(status_, "fii_stats")
        if bad:
            return bad
        try:
            return Outcome("ok", parse(body, d))
        except ParseError as exc:
            return Outcome("bad", note=str(exc))

    def store(batch) -> None:
        save(pd.concat([df for _, df in batch], ignore_index=True), lake_dir)

    return run_days(days, state, attempt, store, polite, today, limit=limit, first_day=FIRST_DAY, flush_every=50, label="fii_derivatives")


def import_file(path: Path, day: date, lake_dir: Optional[Path] = None) -> int:
    df = parse(Path(path).read_bytes(), day)
    save(df, lake_dir)
    return len(df)


def status(lake_dir: Optional[Path] = None) -> dict:
    df, p = load(lake_dir), table_path(lake_dir)
    days = df["date"].nunique() if len(df) else 0
    size = p.stat().st_size if p.exists() else 0
    out = dict(days=int(days), first=str(min(df["date"])) if len(df) else None, last=str(max(df["date"])) if len(df) else None,
               rows=len(df), bytes=size, bytes_per_day=int(size / days) if days else 0,
               days_with_index_split=int(df.loc[df["underlying"] != "ALL", "date"].nunique()) if len(df) else 0)
    out.update(DayState(state_path(lake_dir), set(df["date"]) if len(df) else ()).summary())
    return out


def main(argv: Optional[list[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(prog="python -m data_layer.fii_derivatives", description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    dl = sub.add_parser("daily", help="the last N weekdays not yet stored")
    dl.add_argument("--days", type=int, default=10)
    bf = sub.add_parser("backfill", help="every weekday in a range not yet stored")
    bf.add_argument("--from", dest="d_from", type=date.fromisoformat, default=FIRST_DAY)
    bf.add_argument("--to", dest="d_to", type=date.fromisoformat, default=None)
    bf.add_argument("--limit", type=int, default=None, help="stop after this many requests")
    for p in (dl, bf):
        p.add_argument("--sleep", type=float, default=1.0, help="seconds between requests (default 1)")
    im = sub.add_parser("import", help="store a file downloaded by hand")
    im.add_argument("file", type=Path)
    im.add_argument("--date", type=date.fromisoformat, required=True)
    sub.add_parser("status")
    a = ap.parse_args(argv)
    today = datetime.now(timezone.utc).date()
    if a.cmd == "status":
        print(json.dumps(status(), indent=1))
        return 0
    if a.cmd == "import":
        print(f"fii_derivatives import: {import_file(a.file, a.date)} rows stored for {a.date}")
        return 0
    if a.cmd == "daily":
        st = run(weekdays(today - timedelta(days=a.days), today), sleep_s=a.sleep, today=today)
    else:
        st = run(weekdays(a.d_from, a.d_to or today), sleep_s=a.sleep, today=today, limit=a.limit)
    print(summary_line("fii_derivatives", a.cmd, st))
    return 1 if ("stopped" in st or st["bad"]) else 0


if __name__ == "__main__":
    sys.exit(main())
