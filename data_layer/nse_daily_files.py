"""
NSE daily market files (SCRUM-187): security-wise delivery, the F&O ban list, combined open interest against the market-wide position
limit (MWPL), short selling, and bulk and block deals -- one parser per file, one shared fetch / backfill loop.

  python -m data_layer.nse_daily_files daily [--days 10] [--only delivery,ban_list,...]   # the last N weekdays not yet stored, plus today's deals
  python -m data_layer.nse_daily_files backfill --from 2015-01-01 [--to D] [--only ...] [--sleep 1.0] [--limit N]
  python -m data_layer.nse_daily_files import NAME FILE [--date YYYY-MM-DD]                # a file downloaded by hand (bulk/block history)
  python -m data_layer.nse_daily_files status

Why: delivery percentage separates investment buying from intraday churn; a stock in the ban list (OI above 95 % of MWPL) can only reduce
positions, which moves its options; MWPL use is the early warning for it; short selling and bulk/block deals show large or bearish flow.

Sources (each URL checked with one real download on 2026-11-13; no cookies, User-Agent "jarvis-data/1.0 (daily research archive)"):
  delivery       https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_DDMMYYYY.csv           ~400 KB, from 2019-10-01
                 (2019-09-30 is the first file that answers, but it holds 27-Jun-2019's data; 2019-09-27 and earlier answer 404).
                 A holiday does not answer 404 here: it serves the previous session's file (2026-10-02 gave 2026-10-01), so a file
                 holding an earlier day is treated as a 404 (OlderFile), retried while recent and then taken to be a holiday.
  ban_list       https://nsearchives.nseindia.com/archives/fo/sec_ban/fo_secban_DDMMYYYY.csv               ~60 B, back to 2010 at least
  combined_oi    https://nsearchives.nseindia.com/archives/nsccl/mwpl/combineoi_DDMMYYYY.zip              ~20 KB, back to 2011 at least
                 (the /content/nsccl/combineoi_DDMMYYYY.zip path answers 404)
  short_selling  https://nsearchives.nseindia.com/archives/equities/shortSelling/shortselling_DDMMYYYY.csv  ~10 KB, back to 2013 at least.
                 The file is the day's disclosure: its trades are from one or more earlier sessions (the 12-Nov-2026 file reports
                 11-Nov trades, the 11-Nov file 09-Nov), so rows carry both the trade date and the report date.
  bulk_deals     https://nsearchives.nseindia.com/content/equities/bulk.csv     the LAST session only (no date in the name)
  block_deals    https://nsearchives.nseindia.com/content/equities/block.csv    the LAST session only
Bulk and block deal history is not in the public archive: dated names (archives/equities/bulk/bulk_DDMMYYYY.csv) answer 404, and the
website's JSON API refuses automated requests -- https://www.nseindia.com/api/historical/bulk-deals?from=10-11-2026&to=12-11-2026
answered HTTP 200 with an HTML page "Service Temporarily Unavailable. This site is not accessible in your region at the moment" instead
of data (2026-11-13).  That is not worked around: the nightly job stores the last session's file each day (history builds forward), and
a CSV downloaded by hand from the website's bulk/block deals page loads with `import bulk_deals FILE` (same columns).

Storage (vendor-neutral names), lake/market_files/<name>.parquet, upserted and replaced atomically:
  delivery       date, symbol, series, prev_close, open, high, low, last, close, avg_price, volume (shares), turnover (rupees),
                 trades, delivery_qty, delivery_pct (null where NSE prints '-', e.g. series BE)        about 200 KB a day
  ban_list       date, symbol                       (a day with no ban is remembered as an empty day, not a row)
  combined_oi    date, isin, name, symbol, mwpl, oi, future_equivalent_oi (from about 2020; null before), limit_next_day (null when
                 'No Fresh Positions'), no_fresh_positions
  short_selling  date (trade date), report_date, symbol, name, quantity
  bulk_deals / block_deals   date, symbol, name, client, side (BUY/SELL), quantity, price, remarks
Days that answered 404, and bad files, are kept in lake/market_files/<name>_missing.json (see nse_archive.py).

Checks: the expected columns, the date inside the file equals the requested day (delivery DATE1, ban title, combined OI Date; for short
selling every trade date is on or before the report date and at most 31 days before it; for bulk/block one date for all rows, a weekday
in the last 10 days), no duplicate keys, delivery_qty <= volume, OI and quantities never negative.
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import re
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Optional

import pandas as pd

from . import lake as lake_mod
from .nse_archive import (DayState, OlderFile, Outcome, ParseError, Polite, classify, http_fetch, read_parquet, run_days, summary_line, unzip_one,
                          upsert_parquet, weekdays)

log = logging.getLogger(__name__)

ARCH = "https://nsearchives.nseindia.com"


def _text(body: bytes) -> str:
    return body.decode("utf-8-sig", "replace").replace("\r\n", "\n")


def _csv(text: str) -> pd.DataFrame:
    df = pd.read_csv(io.StringIO(text), dtype=str, keep_default_na=False, skipinitialspace=True)
    df.columns = [c.strip() for c in df.columns]
    return df.apply(lambda s: s.str.strip())


def _num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s.replace({"": None, "-": None, "NA": None}), errors="coerce")


def _need(df: pd.DataFrame, cols) -> None:
    miss = [c for c in cols if c not in df.columns]
    if miss:
        raise ParseError(f"missing columns {miss} (have {list(df.columns)[:8]})")


def _date(s: str) -> date:
    s = s.strip()
    for fmt in ("%d-%b-%Y", "%d-%B-%Y", "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    raise ParseError(f"unreadable date {s!r}")


def _one_day(values: pd.Series, day: date, what: str) -> None:
    seen = {_date(v) for v in set(values)}
    if len(seen) == 1 and min(seen) < day:
        raise OlderFile(f"{what}: the file holds {min(seen)}'s data, asked for {day} (a holiday, or not published yet)")
    if seen != {day}:
        raise ParseError(f"{what}: file is for {sorted(seen)[:3]}, asked for {day}")


def _no_dupes(df: pd.DataFrame, key: list[str]) -> pd.DataFrame:
    dup = int(df.duplicated(key).sum())
    if dup:
        raise ParseError(f"{dup} rows appear twice on {key}")
    return df


# ---------------------------------------------------------------------------------------------- parsers (one per file)
def parse_delivery(body: bytes, day: date) -> pd.DataFrame:
    raw = _csv(_text(body))
    _need(raw, ["SYMBOL", "SERIES", "DATE1", "PREV_CLOSE", "OPEN_PRICE", "HIGH_PRICE", "LOW_PRICE", "LAST_PRICE", "CLOSE_PRICE", "AVG_PRICE",
                "TTL_TRD_QNTY", "TURNOVER_LACS", "NO_OF_TRADES", "DELIV_QTY", "DELIV_PER"])
    if raw.empty:
        raise ParseError("no rows")
    _one_day(raw["DATE1"], day, "DATE1")
    df = pd.DataFrame({"date": day, "symbol": raw["SYMBOL"], "series": raw["SERIES"], "prev_close": _num(raw["PREV_CLOSE"]),
                       "open": _num(raw["OPEN_PRICE"]), "high": _num(raw["HIGH_PRICE"]), "low": _num(raw["LOW_PRICE"]),
                       "last": _num(raw["LAST_PRICE"]), "close": _num(raw["CLOSE_PRICE"]), "avg_price": _num(raw["AVG_PRICE"]),
                       "volume": _num(raw["TTL_TRD_QNTY"]).astype("Int64"), "turnover": _num(raw["TURNOVER_LACS"]) * 1e5,
                       "trades": _num(raw["NO_OF_TRADES"]).astype("Int64"), "delivery_qty": _num(raw["DELIV_QTY"]).astype("Int64"),
                       "delivery_pct": _num(raw["DELIV_PER"])})
    over = df["delivery_qty"].notna() & (df["delivery_qty"] > df["volume"])
    if over.any():
        raise ParseError(f"delivery above volume for {list(df.loc[over, 'symbol'])[:3]}")
    if (df["volume"].fillna(0) < 0).any():
        raise ParseError("negative volume")
    return _no_dupes(df, ["symbol", "series"])


_BAN_TITLE = re.compile(r"Trade Date\s+(\d{1,2}-[A-Za-z]+-\d{4})", re.I)


def parse_ban_list(body: bytes, day: date) -> pd.DataFrame:
    lines = [l.strip() for l in _text(body).split("\n") if l.strip()]
    if not lines:
        raise ParseError("empty file")
    m = _BAN_TITLE.search(lines[0])
    if not m or "ban" not in lines[0].lower():
        raise ParseError(f"no 'Securities in Ban For Trade Date' title: {lines[0][:80]!r}")
    seen = _date(m.group(1))
    if seen != day:
        raise ParseError(f"file is for {seen}, asked for {day}")
    syms = []
    for l in lines[1:]:
        parts = [p.strip() for p in l.split(",")]
        if len(parts) < 2 or not parts[0].isdigit() or not parts[1]:
            raise ParseError(f"unexpected line {l[:60]!r}")
        syms.append(parts[1])
    if "NIL" in lines[0].upper().split(":")[-1] and syms:
        raise ParseError("title says NIL but symbols follow")
    return _no_dupes(pd.DataFrame({"date": [day] * len(syms), "symbol": syms}, columns=["date", "symbol"]), ["symbol"])


def parse_combined_oi(body: bytes, day: date) -> pd.DataFrame:
    if body[:2] == b"PK":
        body = unzip_one(body, ".csv")
    raw = _csv(_text(body))
    _need(raw, ["Date", "ISIN", "Scrip Name", "NSE Symbol", "MWPL", "Open Interest", "Limit for Next Day"])
    if raw.empty:
        raise ParseError("no rows")
    _one_day(raw["Date"], day, "Date")
    lim = raw["Limit for Next Day"]
    nfp = lim.str.lower().str.contains("no fresh")
    df = pd.DataFrame({"date": day, "isin": raw["ISIN"], "name": raw["Scrip Name"], "symbol": raw["NSE Symbol"],
                       "mwpl": _num(raw["MWPL"]).astype("Int64"), "oi": _num(raw["Open Interest"]).astype("Int64"),
                       "future_equivalent_oi": _num(raw["Future Equivalent Open Interest"]) if "Future Equivalent Open Interest" in raw.columns
                       else float("nan"), "limit_next_day": _num(lim.where(~nfp, "")).astype("Int64"), "no_fresh_positions": nfp})
    if (df["oi"].fillna(0) < 0).any() or (df["mwpl"].fillna(1) <= 0).any():
        raise ParseError("negative OI or non-positive MWPL")
    return _no_dupes(df, ["symbol"])


def parse_short_selling(body: bytes, day: date) -> pd.DataFrame:
    text = _text(body).strip()
    cols = ["date", "report_date", "symbol", "name", "quantity"]
    if not text or text.lower().startswith("no records"):
        return pd.DataFrame(columns=cols)
    raw = _csv(text)
    sym = next((c for c in ("Symbol Name", "Security Symbol", "Symbol") if c in raw.columns), None)
    if sym is None:
        raise ParseError(f"no symbol column (have {list(raw.columns)})")
    _need(raw, ["Security Name", "Trade Date", "Quantity"])
    if raw.empty:
        return pd.DataFrame(columns=cols)
    trade = raw["Trade Date"].map(_date)
    if any(t > day or (day - t).days > 31 for t in trade):
        raise ParseError(f"trade dates {sorted(set(trade))[:3]} not in the month before the report date {day}")
    df = pd.DataFrame({"date": trade, "report_date": day, "symbol": raw[sym], "name": raw["Security Name"],
                       "quantity": _num(raw["Quantity"].str.replace(",", "")).astype("Int64")})
    if (df["quantity"].fillna(0) < 0).any():
        raise ParseError("negative quantity")
    return df.groupby(["date", "report_date", "symbol", "name"], as_index=False)["quantity"].sum()   # a symbol listed twice for one session


def _deal_col(df: pd.DataFrame, *names: str) -> str:
    norm = {re.sub(r"[^a-z]", "", c.lower()): c for c in df.columns}
    for n in names:
        if n in norm:
            return norm[n]
    raise ParseError(f"no column like {names[0]!r} (have {list(df.columns)})")


def parse_deals(body: bytes, day: Optional[date] = None, recent_from: Optional[date] = None) -> pd.DataFrame:
    """bulk.csv / block.csv, or a history CSV from the website (same columns, several dates).  `day` given: every row must carry it."""
    cols = ["date", "symbol", "name", "client", "side", "quantity", "price", "remarks"]
    raw = _csv(_text(body))
    if raw.empty:
        return pd.DataFrame(columns=cols)
    c = {k: _deal_col(raw, *v) for k, v in dict(date=("date",), symbol=("symbol",), name=("securityname",), client=("clientname",),
                                                    side=("buysell",), quantity=("quantitytraded",),
                                                    price=("tradepricewghtavgprice", "tradeprice")).items()}
    rem = next((col for col in raw.columns if re.sub(r"[^a-z]", "", col.lower()) == "remarks"), None)
    df = pd.DataFrame({"date": raw[c["date"]].map(_date), "symbol": raw[c["symbol"]], "name": raw[c["name"]], "client": raw[c["client"]],
                       "side": raw[c["side"]].str.upper(), "quantity": _num(raw[c["quantity"]].str.replace(",", "")).astype("Int64"),
                       "price": _num(raw[c["price"]].str.replace(",", "")), "remarks": raw[rem].replace("-", "") if rem else ""})
    if day is not None:
        _one_day(raw[c["date"]], day, "Date")
    if recent_from is not None:
        days = set(df["date"])
        if len(days) != 1 or min(days) < recent_from or min(days).weekday() >= 5:
            raise ParseError(f"the last-session file holds {sorted(days)[:3]}, not one recent weekday")
    if not set(df["side"]) <= {"BUY", "SELL"}:
        raise ParseError(f"unexpected Buy/Sell values {sorted(set(df['side']) - {'BUY', 'SELL'})}")
    if (df["quantity"].fillna(0) < 0).any():
        raise ParseError("negative quantity")
    return df[cols]


# ---------------------------------------------------------------------------------------------- the files
@dataclass(frozen=True)
class File:
    name: str
    url: Callable[[date], str]               # day -> URL (for the 'current' files the day is ignored)
    parse: Callable[[bytes, date], pd.DataFrame]
    key: list[str]
    first_day: date                          # where the archive starts (what a backfill asks for at the earliest)
    dated: bool = True                       # False: one fixed URL holding the last session (bulk/block deals)
    order: list[str] = field(default_factory=list)


FILES: dict[str, File] = {f.name: f for f in (
    File("delivery", lambda d: f"{ARCH}/products/content/sec_bhavdata_full_{d:%d%m%Y}.csv", parse_delivery, ["date", "symbol", "series"],
         date(2019, 10, 1)),
    File("ban_list", lambda d: f"{ARCH}/archives/fo/sec_ban/fo_secban_{d:%d%m%Y}.csv", parse_ban_list, ["date", "symbol"], date(2010, 1, 4)),
    File("combined_oi", lambda d: f"{ARCH}/archives/nsccl/mwpl/combineoi_{d:%d%m%Y}.zip", parse_combined_oi, ["date", "symbol"],
         date(2011, 1, 3)),
    File("short_selling", lambda d: f"{ARCH}/archives/equities/shortSelling/shortselling_{d:%d%m%Y}.csv", parse_short_selling,
         ["date", "report_date", "symbol"], date(2013, 1, 1), order=["report_date", "date", "symbol"]),
    File("bulk_deals", lambda d: f"{ARCH}/content/equities/bulk.csv", lambda b, d: parse_deals(b, d),
         ["date", "symbol", "client", "side", "quantity", "price"], date(2026, 11, 1), dated=False),
    File("block_deals", lambda d: f"{ARCH}/content/equities/block.csv", lambda b, d: parse_deals(b, d),
         ["date", "symbol", "client", "side", "quantity", "price"], date(2026, 11, 1), dated=False),
)}
DEFAULT_FROM = date(2015, 1, 1)


def base_dir(lake_dir: Optional[Path] = None) -> Path:
    return Path(lake_dir or lake_mod.LAKE_DIR) / "market_files"


def table_path(name: str, lake_dir: Optional[Path] = None) -> Path:
    return base_dir(lake_dir) / f"{name}.parquet"


def state_path(name: str, lake_dir: Optional[Path] = None) -> Path:
    return base_dir(lake_dir) / f"{name}_missing.json"


def load(name: str, lake_dir: Optional[Path] = None) -> pd.DataFrame:
    df = read_parquet(table_path(name, lake_dir))
    for c in ("date", "report_date"):
        if c in df.columns and len(df):
            df[c] = pd.to_datetime(df[c]).dt.date
    return df


def save(name: str, new: pd.DataFrame, lake_dir: Optional[Path] = None) -> int:
    f = FILES[name]
    return upsert_parquet(table_path(name, lake_dir), new, f.key, f.order or f.key)


def stored_days(name: str, lake_dir: Optional[Path] = None) -> set[date]:
    df = load(name, lake_dir)
    col = "report_date" if name == "short_selling" else "date"          # short selling is fetched by report date
    return set(df[col]) if len(df) else set()


# ---------------------------------------------------------------------------------------------- runs
def run(name: str, days, lake_dir: Optional[Path] = None, fetch=http_fetch, sleep_s: float = 1.0, today: Optional[date] = None,
        limit: Optional[int] = None, sleeper=None, polite: Optional[Polite] = None) -> dict:
    f = FILES[name]
    today = today or datetime.now(timezone.utc).date()
    polite = polite or (Polite(fetch, sleep_s, sleeper) if sleeper else Polite(fetch, sleep_s))
    calls0 = polite.calls
    if not f.dated:
        return run_current(name, lake_dir, polite, today)
    state = DayState(state_path(name, lake_dir), stored_days(name, lake_dir))

    def attempt(d: date) -> Outcome:
        status_, body = polite(f.url(d))
        bad = classify(status_, name)
        if bad:
            return bad
        try:
            return Outcome("ok", f.parse(body, d))
        except OlderFile:
            return Outcome("missing")
        except ParseError as exc:
            return Outcome("bad", note=str(exc))

    def store(batch) -> None:
        save(name, pd.concat([df for _, df in batch], ignore_index=True), lake_dir)

    st = run_days(days, state, attempt, store, polite, today, limit=limit, first_day=f.first_day, flush_every=100, label=name)
    st["calls"] = polite.calls - calls0
    return st


def run_current(name: str, lake_dir: Optional[Path], polite: Polite, today: date) -> dict:
    """Bulk and block deals: the one file NSE keeps for the last session.  Stored under the date it carries; an empty file (no deals that
    session) stores nothing."""
    f = FILES[name]
    st = dict(stored=0, holiday=0, errors=0, bad=0, skipped=0, calls=1)
    status_, body = polite(f.url(today))
    bad = classify(status_, name)
    if bad:
        st["errors" if bad.kind == "error" else "holiday"] += 1
        if bad.kind == "error":
            log.warning("%s: %s", name, bad.note)
        return st
    try:
        df = parse_deals(body, recent_from=today - timedelta(days=10))
    except ParseError as exc:
        log.warning("%s: %s", name, exc)
        st["bad"] = 1
        return st
    if len(df):
        save(name, df, lake_dir)
        st["stored"] = 1
    else:
        st["skipped"] = 1
    return st


def import_file(name: str, path: Path, day: Optional[date] = None, lake_dir: Optional[Path] = None) -> int:
    f = FILES[name]
    body = Path(path).read_bytes()
    if f.dated:
        if day is None:
            raise SystemExit(f"import {name}: --date is needed")
        df = f.parse(body, day)
    else:
        df = parse_deals(body, day)                                         # a history download: any number of dates
    save(name, df, lake_dir)
    if f.dated:
        st = DayState(state_path(name, lake_dir), stored_days(name, lake_dir))
        st.mark_done(day, len(df))
        st.save()
    return len(df)


def status(lake_dir: Optional[Path] = None) -> dict:
    out = {}
    for name in FILES:
        df, p = load(name, lake_dir), table_path(name, lake_dir)
        col = "report_date" if name == "short_selling" else "date"
        st = DayState(state_path(name, lake_dir), stored_days(name, lake_dir))
        days = len((set(df[col]) if len(df) else set()) | {date.fromisoformat(d) for d in st.empty})
        size = p.stat().st_size if p.exists() else 0
        out[name] = dict(days=days, rows=len(df), first=str(min(df[col])) if len(df) else None, last=str(max(df[col])) if len(df) else None,
                         bytes=size, bytes_per_day=int(size / days) if days else 0, **st.summary())
    return out


def main(argv: Optional[list[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(prog="python -m data_layer.nse_daily_files", description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    dl = sub.add_parser("daily", help="the last N weekdays not yet stored, and the last session's bulk and block deals")
    dl.add_argument("--days", type=int, default=10)
    bf = sub.add_parser("backfill", help="every weekday in a range not yet stored (dated files only)")
    bf.add_argument("--from", dest="d_from", type=date.fromisoformat, default=DEFAULT_FROM)
    bf.add_argument("--to", dest="d_to", type=date.fromisoformat, default=None)
    bf.add_argument("--limit", type=int, default=None, help="stop after this many requests (all files together)")
    for p in (dl, bf):
        p.add_argument("--sleep", type=float, default=1.0, help="seconds between requests (default 1)")
        p.add_argument("--only", default="", help="comma-separated file names (default: all): " + ", ".join(FILES))
    im = sub.add_parser("import", help="store a file downloaded by hand")
    im.add_argument("name", choices=list(FILES))
    im.add_argument("file", type=Path)
    im.add_argument("--date", type=date.fromisoformat, default=None)
    sub.add_parser("status")
    a = ap.parse_args(argv)
    today = datetime.now(timezone.utc).date()
    if a.cmd == "status":
        print(json.dumps(status(), indent=1))
        return 0
    if a.cmd == "import":
        print(f"nse_daily_files import {a.name}: {import_file(a.name, a.file, a.date)} rows stored")
        return 0
    names = [n.strip() for n in a.only.split(",") if n.strip()] or list(FILES)
    unknown = set(names) - set(FILES)
    if unknown:
        ap.error(f"unknown file names {sorted(unknown)}")
    polite, rc = Polite(http_fetch, a.sleep), 0
    for name in names:
        if a.cmd == "backfill" and not FILES[name].dated:
            print(f"{name}: no dated archive (only the last session's file); history loads with `import {name} FILE`")
            continue
        days = weekdays(today - timedelta(days=a.days), today) if a.cmd == "daily" else weekdays(a.d_from, a.d_to or today)
        lim = None if a.cmd == "daily" else a.limit                      # the fetcher is shared, so the limit counts every file's requests
        st = run(name, days, today=today, limit=lim, polite=polite)
        print(summary_line(name, a.cmd, st))
        rc |= 1 if ("stopped" in st or st["bad"] or st["errors"]) else 0
    return rc


if __name__ == "__main__":
    sys.exit(main())
