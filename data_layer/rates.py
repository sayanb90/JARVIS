"""
Money-market rates (SCRUM-186, collector only): the RBI policy repo rate and its corridor, the call money range, and the 91/182/364-day
T-bill cut-off yields, from the RBI home page each day; overnight MIBOR from a file downloaded by hand from FBIL.  `rate_on` gives the
option-chain code a risk-free rate for a day.

  python -m data_layer.rates daily                      # one request to rbi.org.in; stores today's values (cron)
  python -m data_layer.rates import FILE [--series mibor_overnight] [--source FBIL]   # a CSV/XLSX of dates and rates saved by hand
  python -m data_layer.rates status

  from data_layer.rates import rate_on
  r = rate_on(lake_dir, date(2026, 11, 12))           # a fraction (5.45 % -> 0.0545), or the default 0.065 when nothing is stored

Sources (checked 2026-11-13, User-Agent "jarvis-data/1.0 (daily research archive)", no cookies):
  RBI       https://www.rbi.org.in/   the "Current Rates" panel of the public home page (static HTML): Policy Repo Rate, Standing Deposit
            Facility Rate, Marginal Standing Facility Rate, Bank Rate, Fixed Reverse Repo Rate; Call Rates (low - high, "as on" the
            previous session); 91/182/364 day T-bills ("cut-off at the last auction").  One page, about 150 KB.  It holds today's
            values only, so history builds forward from the first run (RBI's DBIE database has the history; not wired here).
  FBIL      overnight MIBOR is published by FBIL at https://www.fbil.org.in/#/home, a JavaScript application: the page itself is an empty
            shell (1.5 KB) and the rates arrive from the site's internal JSON services, which are not a published download and are not
            called from here.  So MIBOR is not fetched automatically: save FBIL's MIBOR table (the site offers a download) or any
            CSV/XLSX with a date column and a rate column, and load it with `import FILE`.  Until then `rate_on` falls back to its
            default (or pass series='repo' to use the policy rate, which is fetched daily).

Storage: lake/reference/rates.parquet, one row per (date, series): value_pct (percent), source (RBI, FBIL, ...), fetched_at.
  date is the day the value applies: today for the policy rates (the rate in force) and for the T-bill cut-offs (the latest auction's,
  RBI does not print the auction date), the printed "as on" day for call rates.
  series: repo, sdf, msf, bank_rate, reverse_repo, call_low, call_high, tbill_91d, tbill_182d, tbill_364d, mibor_overnight.

Checks: every expected label is on the page with a percentage between 0 and 20, the corridor holds (SDF <= repo <= MSF), call low <=
call high, the "as on" date is not in the future and at most 10 days old.  A page that fails is not stored and is reported.
"""

from __future__ import annotations

import argparse
import html as html_mod
import json
import logging
import re
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

from . import lake as lake_mod
from .nse_archive import ParseError, http_fetch, read_parquet, upsert_parquet

log = logging.getLogger(__name__)

RBI_URL = "https://www.rbi.org.in/"
COLUMNS = ["date", "series", "value_pct", "source", "fetched_at"]
RBI_LABELS = {"Policy Repo Rate": "repo", "Standing Deposit Facility Rate": "sdf", "Marginal Standing Facility Rate": "msf",
              "Bank Rate": "bank_rate", "Fixed Reverse Repo Rate": "reverse_repo", "91 day T-bills": "tbill_91d",
              "182 day T-bills": "tbill_182d", "364 day T-bills": "tbill_364d"}
DEFAULT_RATE = 0.065


def table_path(lake_dir: Optional[Path] = None) -> Path:
    return Path(lake_dir or lake_mod.LAKE_DIR) / "reference" / "rates.parquet"


# ---------------------------------------------------------------------------------------------- RBI home page
def _cells(page: str) -> dict[str, str]:
    """{row label: cell text} for every <th>label</th><td>value</td> pair on the page, comments and tags removed."""
    page = re.sub(r"<!--.*?-->", "", page, flags=re.S)
    out = {}
    for th, td in re.findall(r"<th[^>]*>(.*?)</th>\s*<td[^>]*>(.*?)</td>", page, flags=re.S | re.I):
        label = re.sub(r"\s+", " ", html_mod.unescape(re.sub(r"<[^>]+>", " ", th))).strip()
        out.setdefault(label, re.sub(r"\s+", " ", html_mod.unescape(re.sub(r"<[^>]+>", " ", td))).strip())
    return out


_PCT = re.compile(r"(\d{1,2}(?:\.\d+)?)\s*%")
_AS_ON = re.compile(r"as on\s+([A-Z][a-z]+ \d{1,2}, \d{4})")


def parse_rbi(page: str, today: date) -> pd.DataFrame:
    cells = _cells(page)
    rows = []
    for label, series in RBI_LABELS.items():
        if label not in cells:
            raise ParseError(f"{label!r} is not on the page")
        m = _PCT.search(cells[label])
        if not m:
            raise ParseError(f"{label!r}: no percentage in {cells[label]!r}")
        rows.append((today, series, float(m.group(1))))
    call = cells.get("Call Rates")
    if call is None:
        raise ParseError("'Call Rates' is not on the page")
    pcts = [float(x) for x in _PCT.findall(call)]
    if len(pcts) != 2:
        raise ParseError(f"call rates: expected 'low% - high%', got {call!r}")
    text = re.sub(r"<!--.*?-->", "", page, flags=re.S)
    i = text.find("Call Rates")
    m = _AS_ON.search(re.sub(r"<[^>]+>", " ", text[i:i + 2000]))
    if not m:
        raise ParseError("no 'as on' date under the call rates")
    call_day = datetime.strptime(m.group(1), "%B %d, %Y").date()
    if call_day > today or (today - call_day).days > 10:
        raise ParseError(f"call rates are as on {call_day}, today is {today}")
    rows += [(call_day, "call_low", pcts[0]), (call_day, "call_high", pcts[1])]
    df = pd.DataFrame(rows, columns=["date", "series", "value_pct"])
    v = dict(zip(df["series"], df["value_pct"]))
    if not all(0 < x < 20 for x in v.values()):
        raise ParseError(f"implausible rates {v}")
    if not v["sdf"] <= v["repo"] <= v["msf"] or v["call_low"] > v["call_high"]:
        raise ParseError(f"the corridor does not hold: {v}")
    df["source"] = "RBI"
    return df


# ---------------------------------------------------------------------------------------------- files saved by hand (FBIL MIBOR ...)
def parse_rate_file(path: Path, series: str, source: str) -> pd.DataFrame:
    """A CSV or Excel table with a date column and a rate column (the first column whose name says rate/mibor/value/%)."""
    p = Path(path)
    raw = pd.read_excel(p) if p.suffix.lower() in (".xls", ".xlsx") else pd.read_csv(p)
    cols = {c: str(c).strip().lower() for c in raw.columns}
    dcol = next((c for c, n in cols.items() if "date" in n), None)
    rcol = next((c for c, n in cols.items() if c != dcol and any(k in n for k in ("mibor", "rate", "value", "%"))), None)
    if dcol is None or rcol is None:
        raise ParseError(f"need a date column and a rate column, have {list(raw.columns)}")
    dates = pd.to_datetime(raw[dcol].astype(str).str.strip(), dayfirst=True, errors="coerce", format="mixed")
    vals = pd.to_numeric(raw[rcol].astype(str).str.replace("%", "").str.strip(), errors="coerce")
    df = pd.DataFrame({"date": dates.dt.date, "series": series, "value_pct": vals, "source": source}).dropna(subset=["date", "value_pct"])
    if df.empty:
        raise ParseError("no rows with a date and a rate")
    if not df["value_pct"].between(0, 20).all():
        raise ParseError("a rate outside 0-20 %: is the column in percent?")
    if df["date"].duplicated().any():
        raise ParseError("a date appears twice")
    return df.reset_index(drop=True)


# ---------------------------------------------------------------------------------------------- storage and use
def load(lake_dir: Optional[Path] = None) -> pd.DataFrame:
    df = read_parquet(table_path(lake_dir), COLUMNS)
    if len(df):
        df["date"] = pd.to_datetime(df["date"]).dt.date
    return df


def save(df: pd.DataFrame, lake_dir: Optional[Path] = None) -> int:
    df = df.copy()
    df["fetched_at"] = datetime.now(timezone.utc).replace(microsecond=0)
    return upsert_parquet(table_path(lake_dir), df[COLUMNS], ["date", "series"], ["series", "date"])


def rate_on(lake_dir: Optional[Path], day: date, series: str = "mibor_overnight", default: float = DEFAULT_RATE) -> float:
    """The `series` rate in force on `day` (the last value on or before it), as a fraction (5.45 % -> 0.0545); `default` when none is stored."""
    df = load(lake_dir)
    if df.empty:
        return default
    s = df[(df["series"] == series) & (df["date"] <= day)]
    if s.empty:
        return default
    return float(s.sort_values("date")["value_pct"].iloc[-1]) / 100.0


def daily(lake_dir: Optional[Path] = None, fetch=http_fetch, today: Optional[date] = None) -> dict:
    today = today or datetime.now(timezone.utc).date()
    status_, body = fetch(RBI_URL)
    if status_ != 200:
        return {"error": f"RBI home page: HTTP {status_}"}
    try:
        df = parse_rbi(body.decode("utf-8", "replace"), today)
    except ParseError as exc:
        return {"error": f"RBI home page: {exc}"}
    n = save(df, lake_dir)
    return {"stored": len(df), "rows": n, "values": dict(zip(df["series"], df["value_pct"]))}


def status(lake_dir: Optional[Path] = None) -> dict:
    df, p = load(lake_dir), table_path(lake_dir)
    out = {"rows": len(df), "bytes": p.stat().st_size if p.exists() else 0}
    for s, g in (df.groupby("series") if len(df) else []):
        last = g.sort_values("date").iloc[-1]
        out[s] = dict(days=int(g["date"].nunique()), first=str(g["date"].min()), last=str(last["date"]), value_pct=float(last["value_pct"]),
                      source=last["source"])
    return out


def main(argv: Optional[list[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(prog="python -m data_layer.rates", description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("daily", help="today's RBI rates")
    im = sub.add_parser("import", help="a CSV/XLSX of dates and rates saved by hand")
    im.add_argument("file", type=Path)
    im.add_argument("--series", default="mibor_overnight")
    im.add_argument("--source", default="FBIL")
    sub.add_parser("status")
    a = ap.parse_args(argv)
    if a.cmd == "status":
        print(json.dumps(status(), indent=1))
        return 0
    if a.cmd == "import":
        df = parse_rate_file(a.file, a.series, a.source)
        save(df)
        print(f"rates import: {len(df)} {a.series} values from {df['date'].min()} to {df['date'].max()}")
        return 0
    res = daily()
    print(json.dumps(res, indent=1, default=str))
    return 1 if "error" in res else 0


if __name__ == "__main__":
    sys.exit(main())
