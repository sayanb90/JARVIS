"""
NSE F&O end-of-day bhavcopy (SCRUM-184): one row per futures and options contract per trading day -- open, high, low, close, settlement
price, open interest and its change, contracts traded and turnover -- for every index and stock contract NSE lists, from 2015.

  python -m data_layer.eod_fo daily [--days 10]                       # the last N weekdays not yet stored (cron, after NSE publishes)
  python -m data_layer.eod_fo backfill --from 2015-01-01 [--to D] [--sleep 1.0] [--limit N]
  python -m data_layer.eod_fo import FILE --date YYYY-MM-DD           # a bhavcopy downloaded by hand (zip or csv, either format)
  python -m data_layer.eod_fo status

Why: the live feed records only the watchlist's contracts and only from 2026; the bhavcopy is the full history of every strike and
expiry, so option-chain research (skew, OI walls, expiry studies) has years of closes instead of weeks.

Source: NSE's public archive, one zip per day (no cookies, no login):
  new (UDiFF) format, from 2024-01-01:   https://nsearchives.nseindia.com/content/fo/BhavCopy_NSE_FO_0_0_0_YYYYMMDD_F_0000.csv.zip
  old format, to 2024-07-05:             https://nsearchives.nseindia.com/content/historical/DERIVATIVES/YYYY/MON/foDDMONYYYYbhav.csv.zip
Measured 2026-11-13: the old format answers from 2001 to 2024-07-05 and 404 from 2024-07-08; the new one answers from 2024-01-01 and 404
for every day of 2023 we tried.  Both exist for 2024-01-01 .. 2024-07-05 and agree row for row (contracts, turnover, OI checked on
2024-07-05), so the new format is used from 2024-01-01 and the old one before; a 404 for one in the overlap tries the other.  A day the
market was shut answers 404 and is remembered (lake/eod_fo/eod_fo_missing.json).  About 0.3 MB (2015) to 1.4 MB (2026) a day to download.

Normalized schema (both formats map to it; `source_format` says which one a row came from):
  date, instrument_type (FUTIDX, OPTIDX, FUTSTK, OPTSTK, FUTIVX in old files), symbol, expiry, actual_expiry, strike (null for futures),
  option_type (CE, PE; XX for futures), open, high, low, close, last_price, prev_close, settle, underlying_price (null in the old format),
  oi, oi_change (in units of the underlying -- shares or index units -- as NSE reports both formats; oi / lot_size is contracts),
  volume (contracts traded), turnover (rupees; the old file's lakhs x 1e5; options at strike + premium, as NSE computes it),
  trades and lot_size and instrument_id (new format only, else null), source_format ("udiff" or "legacy").

Storage: one Parquet file per trading day, lake/eod_fo/year=YYYY/part-YYYY-MM-DD.parquet (about 1-2 MB for a 2026 day), never
rewritten once stored, so a daily run writes one small file and a reader picks days by name:
  SELECT * FROM read_parquet('lake/eod_fo/year=*/*.parquet', hive_partitioning=false) WHERE date = DATE '2026-11-12'
`read(lake_dir, first, last)` does the same from Python.

Checks on every file: the expected columns are there, every row carries the requested trade date, there is at least one NIFTY index
future, no contract appears twice, and volume and OI are never negative.  A file that fails is not stored and is reported.
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

from . import lake as lake_mod
from .nse_archive import (DayState, Outcome, ParseError, Polite, classify, http_fetch, run_days, summary_line, unzip_one, weekdays,
                          write_parquet)

log = logging.getLogger(__name__)

NEW_URL = "https://nsearchives.nseindia.com/content/fo/BhavCopy_NSE_FO_0_0_0_{d:%Y%m%d}_F_0000.csv.zip"
OLD_URL = "https://nsearchives.nseindia.com/content/historical/DERIVATIVES/{y}/{mon}/fo{dd}{mon}{y}bhav.csv.zip"
NEW_FIRST = date(2024, 1, 1)          # first day the new (UDiFF) file exists
OLD_LAST = date(2024, 7, 5)           # last day the old file exists
FIRST_DAY = date(2015, 1, 1)          # where the backfill starts by default (the old archive goes back to 2001)

COLUMNS = ["date", "instrument_type", "symbol", "expiry", "actual_expiry", "strike", "option_type", "open", "high", "low", "close",
           "last_price", "prev_close", "settle", "underlying_price", "oi", "oi_change", "volume", "turnover", "trades", "lot_size",
           "instrument_id", "source_format"]
KEY = ["instrument_type", "symbol", "expiry", "strike", "option_type"]
NEW_TYPES = {"IDF": "FUTIDX", "IDO": "OPTIDX", "STF": "FUTSTK", "STO": "OPTSTK"}
NEW_REQUIRED = ("TradDt", "FinInstrmTp", "TckrSymb", "XpryDt", "StrkPric", "OptnTp", "OpnPric", "HghPric", "LwPric", "ClsPric",
                "SttlmPric", "OpnIntrst", "ChngInOpnIntrst", "TtlTradgVol", "TtlTrfVal")
OLD_REQUIRED = ("INSTRUMENT", "SYMBOL", "EXPIRY_DT", "STRIKE_PR", "OPTION_TYP", "OPEN", "HIGH", "LOW", "CLOSE", "SETTLE_PR", "CONTRACTS",
                "VAL_INLAKH", "OPEN_INT", "CHG_IN_OI", "TIMESTAMP")
FLOATS = ("strike", "open", "high", "low", "close", "last_price", "prev_close", "settle", "underlying_price", "turnover")
INTS = ("oi", "oi_change", "volume", "trades", "lot_size", "instrument_id")


def new_url(day: date) -> str:
    return NEW_URL.format(d=day)


def old_url(day: date) -> str:
    mon = day.strftime("%b").upper()
    return OLD_URL.format(y=day.year, mon=mon, dd=f"{day:%d}")


def urls_for(day: date) -> list[tuple[str, str]]:
    """[(format, url)] to try, preferred first."""
    if day >= NEW_FIRST:
        return [("udiff", new_url(day))] + ([("legacy", old_url(day))] if day <= OLD_LAST else [])
    return [("legacy", old_url(day))]


def base_dir(lake_dir: Optional[Path] = None) -> Path:
    return Path(lake_dir or lake_mod.LAKE_DIR) / "eod_fo"


def day_path(day: date, lake_dir: Optional[Path] = None) -> Path:
    return base_dir(lake_dir) / f"year={day.year}" / f"part-{day.isoformat()}.parquet"


def state_path(lake_dir: Optional[Path] = None) -> Path:
    return base_dir(lake_dir) / "eod_fo_missing.json"


# ---------------------------------------------------------------------------------------------- parsing
def _num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s.astype(str).str.strip().replace({"": None, "-": None, "nan": None}), errors="coerce")


def _finish(df: pd.DataFrame, day: date) -> pd.DataFrame:
    for c in FLOATS:
        df[c] = _num(df[c]).astype("float64")
    for c in INTS:
        df[c] = _num(df[c]).round().astype("Int64")
    df.loc[df["instrument_type"].str.startswith("FUT"), "strike"] = float("nan")
    df["date"] = day
    df = df[COLUMNS].reset_index(drop=True)
    if not ((df["instrument_type"] == "FUTIDX") & (df["symbol"] == "NIFTY")).any():
        raise ParseError("no NIFTY index future in the file")
    dup = int(df.duplicated(KEY).sum())
    if dup:
        raise ParseError(f"{dup} contracts appear more than once")
    for c in ("volume", "oi"):
        if (df[c].fillna(0) < 0).any():
            raise ParseError(f"negative {c}")
    return df


def parse_new(text: str, day: date) -> pd.DataFrame:
    raw = pd.read_csv(io.StringIO(text.lstrip("﻿")), dtype=str, keep_default_na=False)
    raw.columns = [c.strip() for c in raw.columns]
    miss = [c for c in NEW_REQUIRED if c not in raw.columns]
    if miss:
        raise ParseError(f"missing columns {miss}")
    if raw.empty:
        raise ParseError("no rows")
    dates = set(raw["TradDt"].str.strip())
    if dates != {day.isoformat()}:
        raise ParseError(f"file is for {sorted(dates)[:3]}, asked for {day}")
    tp = raw["FinInstrmTp"].str.strip()
    unknown = set(tp) - set(NEW_TYPES)
    if unknown:
        log.warning("eod_fo %s: unknown instrument types kept as they are: %s", day, sorted(unknown))
    get = lambda c: raw[c] if c in raw.columns else pd.Series([None] * len(raw))        # noqa: E731
    opt = raw["OptnTp"].str.strip().replace("", "XX")
    df = pd.DataFrame({
        "instrument_type": tp.map(lambda t: NEW_TYPES.get(t, t)), "symbol": raw["TckrSymb"].str.strip(),
        "expiry": pd.to_datetime(raw["XpryDt"].str.strip(), format="%Y-%m-%d").dt.date,
        "actual_expiry": pd.to_datetime(get("FininstrmActlXpryDt").astype(str).str.strip(), format="%Y-%m-%d", errors="coerce").dt.date,
        "strike": raw["StrkPric"], "option_type": opt, "open": raw["OpnPric"], "high": raw["HghPric"], "low": raw["LwPric"],
        "close": raw["ClsPric"], "last_price": get("LastPric"), "prev_close": get("PrvsClsgPric"), "settle": raw["SttlmPric"],
        "underlying_price": get("UndrlygPric"), "oi": raw["OpnIntrst"], "oi_change": raw["ChngInOpnIntrst"], "volume": raw["TtlTradgVol"],
        "turnover": raw["TtlTrfVal"], "trades": get("TtlNbOfTxsExctd"), "lot_size": get("NewBrdLotQty"), "instrument_id": get("FinInstrmId"),
        "source_format": "udiff"})
    return _finish(df, day)


def parse_old(text: str, day: date) -> pd.DataFrame:
    raw = pd.read_csv(io.StringIO(text.lstrip("﻿")), dtype=str, keep_default_na=False)
    raw.columns = [c.strip() for c in raw.columns]
    raw = raw[[c for c in raw.columns if c and not c.startswith("Unnamed")]]     # the old files end every line with a comma
    miss = [c for c in OLD_REQUIRED if c not in raw.columns]
    if miss:
        raise ParseError(f"missing columns {miss}")
    if raw.empty:
        raise ParseError("no rows")
    stamps = {datetime.strptime(s.strip(), "%d-%b-%Y").date() for s in set(raw["TIMESTAMP"])}
    if stamps != {day}:
        raise ParseError(f"file is for {sorted(stamps)[:3]}, asked for {day}")
    n = len(raw)
    none = pd.Series([None] * n)
    df = pd.DataFrame({
        "instrument_type": raw["INSTRUMENT"].str.strip(), "symbol": raw["SYMBOL"].str.strip(),
        "expiry": pd.to_datetime(raw["EXPIRY_DT"].str.strip(), format="%d-%b-%Y").dt.date, "actual_expiry": none,
        "strike": raw["STRIKE_PR"], "option_type": raw["OPTION_TYP"].str.strip().replace("", "XX"), "open": raw["OPEN"], "high": raw["HIGH"],
        "low": raw["LOW"], "close": raw["CLOSE"], "last_price": none, "prev_close": none, "settle": raw["SETTLE_PR"],
        "underlying_price": none, "oi": raw["OPEN_INT"], "oi_change": raw["CHG_IN_OI"], "volume": raw["CONTRACTS"],
        "turnover": _num(raw["VAL_INLAKH"]) * 1e5, "trades": none, "lot_size": none, "instrument_id": none, "source_format": "legacy"})
    df["actual_expiry"] = df["expiry"]
    return _finish(df, day)


def parse(body: bytes, day: date, fmt: Optional[str] = None) -> pd.DataFrame:
    """A bhavcopy (zip or csv bytes, either format) -> normalized rows.  `fmt` None: told apart by the header."""
    if body[:2] == b"PK":
        body = unzip_one(body, ".csv")
    text = body.decode("utf-8", "replace")
    head = text.lstrip("﻿").split("\n", 1)[0]
    if fmt is None:
        fmt = "udiff" if "TradDt" in head else "legacy" if "INSTRUMENT" in head else ""
    if fmt == "udiff":
        return parse_new(text, day)
    if fmt == "legacy":
        return parse_old(text, day)
    raise ParseError(f"unrecognised header: {head[:80]!r}")


# ---------------------------------------------------------------------------------------------- storage
def stored_days(lake_dir: Optional[Path] = None) -> list[date]:
    out = []
    for p in base_dir(lake_dir).glob("year=*/part-*.parquet"):
        try:
            out.append(date.fromisoformat(p.stem[len("part-"):]))
        except ValueError:
            continue
    return sorted(out)


def save_day(df: pd.DataFrame, day: date, lake_dir: Optional[Path] = None) -> Path:
    p = day_path(day, lake_dir)
    write_parquet(df, p)
    return p


def read(lake_dir: Optional[Path] = None, first: Optional[date] = None, last: Optional[date] = None) -> pd.DataFrame:
    """Every stored contract-day from `first` to `last` (inclusive)."""
    days = [d for d in stored_days(lake_dir) if (first is None or d >= first) and (last is None or d <= last)]
    if not days:
        return pd.DataFrame(columns=COLUMNS)
    return pd.concat([pd.read_parquet(day_path(d, lake_dir)) for d in days], ignore_index=True)


# ---------------------------------------------------------------------------------------------- runs
def run(days, lake_dir: Optional[Path] = None, fetch=http_fetch, sleep_s: float = 1.0, today: Optional[date] = None,
        limit: Optional[int] = None, sleeper=None) -> dict:
    today = today or datetime.now(timezone.utc).date()
    polite = Polite(fetch, sleep_s, sleeper) if sleeper else Polite(fetch, sleep_s)
    state = DayState(state_path(lake_dir), stored_days(lake_dir))

    def attempt(d: date) -> Outcome:
        last = None
        for fmt, url in urls_for(d):
            status, body = polite(url)
            last = classify(status, f"{fmt} file")
            if last is None:
                try:
                    return Outcome("ok", parse(body, d, fmt))
                except ParseError as exc:
                    return Outcome("bad", note=f"{fmt}: {exc}")
            if last.kind != "missing":
                return last
        return last

    def store(batch) -> None:
        for d, df in batch:
            save_day(df, d, lake_dir)

    return run_days(days, state, attempt, store, polite, today, limit=limit, first_day=date(2000, 1, 1), flush_every=1, label="eod_fo")


def import_file(path: Path, day: date, lake_dir: Optional[Path] = None) -> int:
    df = parse(Path(path).read_bytes(), day)
    save_day(df, day, lake_dir)
    st = DayState(state_path(lake_dir), stored_days(lake_dir))
    st.mark_done(day)
    st.save()
    return len(df)


def status(lake_dir: Optional[Path] = None) -> dict:
    days = stored_days(lake_dir)
    files = list(base_dir(lake_dir).glob("year=*/part-*.parquet"))
    size = sum(p.stat().st_size for p in files)
    by_year = {}
    for d in days:
        by_year[d.year] = by_year.get(d.year, 0) + 1
    out = dict(days=len(days), first=str(days[0]) if days else None, last=str(days[-1]) if days else None, bytes=size,
               bytes_per_day=int(size / len(days)) if days else 0, days_by_year=by_year)
    out.update(DayState(state_path(lake_dir), days).summary())
    return out


def main(argv: Optional[list[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(prog="python -m data_layer.eod_fo", description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    dl = sub.add_parser("daily", help="the last N weekdays not yet stored")
    dl.add_argument("--days", type=int, default=10)
    bf = sub.add_parser("backfill", help="every weekday in a range not yet stored")
    bf.add_argument("--from", dest="d_from", type=date.fromisoformat, default=FIRST_DAY)
    bf.add_argument("--to", dest="d_to", type=date.fromisoformat, default=None)
    bf.add_argument("--limit", type=int, default=None, help="stop after this many requests")
    for p in (dl, bf):
        p.add_argument("--sleep", type=float, default=1.0, help="seconds between requests (default 1)")
    im = sub.add_parser("import", help="store a bhavcopy downloaded by hand")
    im.add_argument("file", type=Path)
    im.add_argument("--date", type=date.fromisoformat, required=True)
    sub.add_parser("status")
    a = ap.parse_args(argv)
    today = datetime.now(timezone.utc).date()
    if a.cmd == "status":
        print(json.dumps(status(), indent=1))
        return 0
    if a.cmd == "import":
        print(f"eod_fo import: {import_file(a.file, a.date)} contracts stored for {a.date}")
        return 0
    if a.cmd == "daily":
        st = run(weekdays(today - timedelta(days=a.days), today), sleep_s=a.sleep, today=today)
    else:
        st = run(weekdays(a.d_from, a.d_to or today), sleep_s=a.sleep, today=today, limit=a.limit)
    print(summary_line("eod_fo", a.cmd, st))
    return 1 if ("stopped" in st or st["bad"]) else 0


if __name__ == "__main__":
    sys.exit(main())
