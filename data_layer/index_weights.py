"""
Index constituents and weights for the NSE indices with listed options (SCRUM-183): NIFTY 50, NIFTY BANK, NIFTY FINANCIAL SERVICES,
NIFTY NEXT 50 and NIFTY MIDCAP SELECT -- who is in each, and how much each member weighs -- kept as dated snapshots.

  python -m data_layer.index_weights refresh [--index NIFTY50,...]     # fetch, store a new snapshot where something changed (cron, daily)
  python -m data_layer.index_weights import-factsheet FILE --index NIFTYBANK   # a factsheet PDF downloaded by hand
  python -m data_layer.index_weights status

Why: index options are priced off a handful of heavyweights; dispersion, earnings-day and "HDFC moves BANKNIFTY" signals need the members
and their weights, and the history of when the member list changed.

Sources (checked 2026-11-13, no cookies; User-Agent "jarvis-data/1.0 (daily research archive)"):
  members   https://nsearchives.nseindia.com/content/indices/ind_<code>list.csv          (Company Name, Industry, Symbol, Series, ISIN Code)
            codes: nifty50, niftybank, niftyfinance (the financial services index; ind_niftyfinancialserviceslist.csv is 404),
            niftynext50, niftymidcapselect_
  weights   NSE Indices' monthly factsheet PDF, page 1, "Top constituents by weightage" -- the ten largest members only, as of the
            month's last trading day:  https://www.niftyindices.com/Factsheet/ind_nifty50.pdf, ind_nifty_bank.pdf,
            ind_nifty_financial_services.pdf.  The Next 50 and Midcap Select names we tried (ind_niftynext50.pdf,
            ind_nifty_next_50.pdf, ind_niftymidcapselect.pdf, ind_nifty_midcap_select.pdf) answer 200 with an HTML page, not a PDF.
            Full weights for every member are published only through niftyindices.com's web pages and live JSON (the blob host
            iislliveblob.niftyindices.com was not reachable from here); not used.  So:
  approx    for every member: weight = its share of the members' total market cap (shares outstanding from the Yahoo profile table,
            lake/yf_fundamentals/profile.parquet, times the last close in lake/ohlc on or before the day; the profile's market_cap when
            there is no close).  Free-float factors and NSE's capping are NOT applied, so promoter-heavy names come out too heavy:
            compare with the factsheet's top ten.  Members with neither price nor shares get a null weight.

Storage: lake/reference/index_weights.parquet, one row per (date, index, source, symbol):
  date          the day this snapshot was first seen (first_seen); a later snapshot is stored only when something changed
  index         NIFTY50, NIFTYBANK, NIFTYFIN, NIFTYNEXT50, NIFTYMIDSELECT (the codes the F&O tables use for the same indices:
                NIFTY, BANKNIFTY, FINNIFTY, NIFTYNXT50, MIDCPNIFTY, in `fo_symbol`)
  source        members (the member list; weight null), factsheet_top10 (official weight), approx_mcap (see above)
  symbol        the repo's universe key (Yahoo form, HDFCBANK.NS, as in data/universe.csv); nse_symbol HDFCBANK; isin; company
  weight_pct    percent of the index; as_of: the source's own date (factsheet month end, price date, fetch date)
"Changed" means: a different member list, a factsheet with another date or other weights, or an approximate weight that moved by more
than APPROX_TOLERANCE_PP percentage points (prices move every day; a new row for every tick of the market would bury the real changes).

Checks: the member file has the expected columns, 10 to 60 EQ members with distinct symbols and ISINs (the documented member count for
NIFTY 50 is 50, for NIFTY BANK 12 to 14); factsheet weights are between 0 and 40 % and the ten add up to under 100 %, every name maps to
a member; approximate weights add up to 100 %.
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import re
import sys
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

from . import lake as lake_mod
from .nse_archive import ParseError, Polite, http_fetch, read_parquet, write_parquet

log = logging.getLogger(__name__)

MEMBERS_URL = "https://nsearchives.nseindia.com/content/indices/ind_{code}list.csv"
FACTSHEET_URL = "https://www.niftyindices.com/Factsheet/{name}.pdf"
APPROX_TOLERANCE_PP = 0.25
COLUMNS = ["date", "index", "source", "symbol", "nse_symbol", "isin", "company", "weight_pct", "as_of", "fo_symbol"]


@dataclass(frozen=True)
class Index:
    key: str
    members_code: str
    factsheet: Optional[str]
    fo_symbol: str
    size: tuple[int, int]


INDICES = {i.key: i for i in (
    Index("NIFTY50", "nifty50", "ind_nifty50", "NIFTY", (50, 50)),
    Index("NIFTYBANK", "niftybank", "ind_nifty_bank", "BANKNIFTY", (10, 15)),
    Index("NIFTYFIN", "niftyfinance", "ind_nifty_financial_services", "FINNIFTY", (15, 25)),
    Index("NIFTYNEXT50", "niftynext50", None, "NIFTYNXT50", (50, 50)),
    Index("NIFTYMIDSELECT", "niftymidcapselect_", None, "MIDCPNIFTY", (20, 30)),
)}


def table_path(lake_dir: Optional[Path] = None) -> Path:
    return Path(lake_dir or lake_mod.LAKE_DIR) / "reference" / "index_weights.parquet"


def universe_symbol(nse_symbol: str) -> str:
    return f"{nse_symbol}.NS"


# ---------------------------------------------------------------------------------------------- parsing
def parse_members(body: bytes, ix: Index) -> pd.DataFrame:
    try:
        raw = pd.read_csv(io.BytesIO(body), dtype=str, keep_default_na=False)
    except Exception as exc:                                   # noqa: BLE001
        raise ParseError(f"not a CSV: {exc}") from None
    raw.columns = [c.strip() for c in raw.columns]
    need = ["Company Name", "Symbol", "Series", "ISIN Code"]
    miss = [c for c in need if c not in raw.columns]
    if miss:
        raise ParseError(f"missing columns {miss}")
    raw = raw.apply(lambda s: s.str.strip())
    raw = raw[raw["Series"] == "EQ"]
    lo, hi = ix.size
    if not lo <= len(raw) <= hi:
        raise ParseError(f"{ix.key}: {len(raw)} members, expected {lo} to {hi}")
    if raw["Symbol"].duplicated().any() or raw["ISIN Code"].duplicated().any():
        raise ParseError("a symbol or ISIN appears twice")
    if not raw["ISIN Code"].str.fullmatch(r"IN[A-Z0-9]{10}").all():
        raise ParseError("an ISIN is not a 12-character Indian ISIN")
    return pd.DataFrame({"nse_symbol": raw["Symbol"], "isin": raw["ISIN Code"], "company": raw["Company Name"]}).reset_index(drop=True)


_MONTH_DATE = re.compile(r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),\s*(\d{4})")
_ROW = re.compile(r"^(.+?)\s+(\d{1,2}\.\d{2})\s*$")


def factsheet_text(body: bytes) -> str:
    if not body.startswith(b"%PDF"):
        raise ParseError("not a PDF (the site sent a web page)")
    from pypdf import PdfReader
    return PdfReader(io.BytesIO(body)).pages[0].extract_text()


def parse_factsheet(text: str) -> tuple[date, list[tuple[str, float]]]:
    """Page-1 text -> (as-of date, [(company name, weight %)] for the top constituents)."""
    m = _MONTH_DATE.search(text)
    if not m:
        raise ParseError("no as-of date on the factsheet")
    as_of = datetime.strptime(" ".join(m.groups()), "%B %d %Y").date()
    i = text.find("Top constituents by weightage")
    if i < 0:
        raise ParseError("no 'Top constituents by weightage' table")
    rows = []
    for line in text[i:].splitlines()[1:]:
        line = line.strip()
        if not line or line.startswith("Company"):
            continue
        r = _ROW.match(line)
        if not r:
            break
        rows.append((r.group(1).strip(), float(r.group(2))))
    if not 5 <= len(rows) <= 10:
        raise ParseError(f"{len(rows)} constituents read from the factsheet")
    if any(not 0 < w <= 40 for _, w in rows) or sum(w for _, w in rows) >= 100:
        raise ParseError(f"implausible weights {rows}")
    return as_of, rows


def _norm(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", name.lower().replace("’", "'").replace("limited", "ltd"))


def factsheet_rows(as_of: date, rows: list[tuple[str, float]], members: pd.DataFrame) -> pd.DataFrame:
    by_name = {_norm(c): r for c, r in zip(members["company"], members.itertuples(index=False))}
    out = []
    for name, w in rows:
        m = by_name.get(_norm(name))
        if m is None:
            raise ParseError(f"factsheet name {name!r} is not in the member list")
        out.append(dict(nse_symbol=m.nse_symbol, isin=m.isin, company=m.company, weight_pct=w, as_of=as_of))
    return pd.DataFrame(out)


# ---------------------------------------------------------------------------------------------- approximate weights
def market_caps(lake_dir: Path, symbols: list[str], day: date) -> tuple[dict[str, float], Optional[date]]:
    """{universe symbol: market cap} from Yahoo shares x the last close on or before `day` (else the profile's market cap)."""
    prof = read_parquet(Path(lake_dir) / "yf_fundamentals" / "profile.parquet")
    if prof.empty:
        return {}, None
    prof = prof[prof["symbol"].isin(symbols)].drop_duplicates("symbol", keep="last").set_index("symbol")
    closes, price_day = {}, None
    files = sorted((Path(lake_dir) / "ohlc").glob("year=*/ohlc.parquet"))[-2:]           # this year and last are enough for a last close
    if files:
        import duckdb
        names = ", ".join("'" + str(f).replace("'", "''") + "'" for f in files)
        q = (f"SELECT symbol, arg_max(close, date) AS close, max(date) AS d FROM read_parquet([{names}], union_by_name=true) "
             f"WHERE date <= ? AND close IS NOT NULL AND symbol IN (SELECT unnest(?)) GROUP BY symbol")
        con = duckdb.connect()
        for sym, close, d in con.execute(q, [day, list(symbols)]).fetchall():
            closes[sym] = float(close)
            price_day = max(price_day, d) if price_day else d
        con.close()
    caps = {}
    for sym in symbols:
        if sym not in prof.index:
            continue
        shares, mcap = prof.at[sym, "shares_outstanding"], prof.at[sym, "market_cap"]
        if sym in closes and pd.notna(shares) and shares > 0:
            caps[sym] = closes[sym] * float(shares)
        elif pd.notna(mcap) and mcap > 0:
            caps[sym] = float(mcap)
    return caps, (price_day.date() if hasattr(price_day, "date") else price_day)


def approx_rows(members: pd.DataFrame, lake_dir: Path, day: date) -> pd.DataFrame:
    syms = [universe_symbol(s) for s in members["nse_symbol"]]
    caps, price_day = market_caps(lake_dir, syms, day)
    if not caps:
        return pd.DataFrame()
    total = sum(caps.values())
    df = members.copy()
    df["weight_pct"] = [100.0 * caps[s] / total if s in caps else float("nan") for s in syms]
    df["as_of"] = price_day or day
    return df


# ---------------------------------------------------------------------------------------------- snapshots
def load(lake_dir: Optional[Path] = None) -> pd.DataFrame:
    df = read_parquet(table_path(lake_dir), COLUMNS)
    for c in ("date", "as_of"):
        if len(df):
            df[c] = pd.to_datetime(df[c]).dt.date
    return df


def latest(lake_dir: Optional[Path] = None, index: str = "NIFTY50", source: str = "approx_mcap", on: Optional[date] = None) -> pd.DataFrame:
    """The snapshot of (index, source) in force on `on` (default: the newest)."""
    df = load(lake_dir)
    df = df[(df["index"] == index) & (df["source"] == source)]
    if on is not None:
        df = df[df["date"] <= on]
    if df.empty:
        return df
    return df[df["date"] == df["date"].max()].reset_index(drop=True)


def _changed(old: pd.DataFrame, new: pd.DataFrame, source: str) -> bool:
    if old.empty:
        return True
    if set(old["nse_symbol"]) != set(new["nse_symbol"]):
        return True
    if source == "members":
        return False
    if source == "factsheet_top10":
        return old["as_of"].iloc[0] != new["as_of"].iloc[0] or \
            dict(zip(old["nse_symbol"], old["weight_pct"].round(2))) != dict(zip(new["nse_symbol"], new["weight_pct"].round(2)))
    a = dict(zip(old["nse_symbol"], old["weight_pct"]))
    return any(abs((a.get(s) if pd.notna(a.get(s)) else -99) - (w if pd.notna(w) else -99)) > APPROX_TOLERANCE_PP
               for s, w in zip(new["nse_symbol"], new["weight_pct"]))


def store_snapshot(ix: Index, source: str, snap: pd.DataFrame, day: date, lake_dir: Optional[Path] = None) -> bool:
    """Append `snap` as (day, index, source) when it differs from the snapshot in force; returns whether it was stored."""
    if snap.empty:
        return False
    old = latest(lake_dir, ix.key, source)
    if not _changed(old, snap, source):
        return False
    rows = snap.assign(date=day, index=ix.key, source=source, fo_symbol=ix.fo_symbol)
    rows["symbol"] = rows["nse_symbol"].map(universe_symbol)
    if "weight_pct" not in rows:
        rows["weight_pct"] = float("nan")
    if "as_of" not in rows:
        rows["as_of"] = day
    rows = rows[COLUMNS]
    df = load(lake_dir)
    df = df[~((df["date"] == day) & (df["index"] == ix.key) & (df["source"] == source))]
    both = pd.concat([df, rows], ignore_index=True) if len(df) else rows
    both["weight_pct"] = both["weight_pct"].astype("float64")
    write_parquet(both.sort_values(["index", "source", "date", "symbol"]).reset_index(drop=True), table_path(lake_dir))
    return True


def refresh(lake_dir: Optional[Path] = None, fetch=http_fetch, sleep_s: float = 1.0, today: Optional[date] = None,
            indices: Optional[list[str]] = None, sleeper=None) -> dict:
    today = today or datetime.now(timezone.utc).date()
    lake_dir = Path(lake_dir or lake_mod.LAKE_DIR)
    polite = Polite(fetch, sleep_s, sleeper) if sleeper else Polite(fetch, sleep_s)
    out: dict[str, dict] = {}
    for key in indices or list(INDICES):
        ix, res = INDICES[key], {}
        out[key] = res
        status_, body = polite(MEMBERS_URL.format(code=ix.members_code))
        if status_ != 200:
            res["error"] = f"members: HTTP {status_}"
            continue
        try:
            members = parse_members(body, ix)
        except ParseError as exc:
            res["error"] = f"members: {exc}"
            continue
        res["members"] = len(members)
        res["members_stored"] = store_snapshot(ix, "members", members.assign(as_of=today), today, lake_dir)
        if ix.factsheet:
            st, pdf = polite(FACTSHEET_URL.format(name=ix.factsheet))
            try:
                if st != 200:
                    raise ParseError(f"HTTP {st}")
                as_of, rows = parse_factsheet(factsheet_text(pdf))
                res["factsheet_as_of"] = str(as_of)
                res["factsheet_stored"] = store_snapshot(ix, "factsheet_top10", factsheet_rows(as_of, rows, members), today, lake_dir)
            except ParseError as exc:
                res["factsheet_error"] = str(exc)
        approx = approx_rows(members, lake_dir, today)
        if approx.empty:
            res["approx"] = "no prices or shares in the lake (run the Yahoo daily OHLC and fundamentals jobs)"
        else:
            total = approx["weight_pct"].sum()
            if abs(total - 100) > 0.01:
                res["approx_error"] = f"approximate weights add to {total}"
            else:
                res["approx_covered"] = int(approx["weight_pct"].notna().sum())
                res["approx_stored"] = store_snapshot(ix, "approx_mcap", approx, today, lake_dir)
    return out


def import_factsheet(path: Path, key: str, lake_dir: Optional[Path] = None, fetch=http_fetch, today: Optional[date] = None) -> int:
    """A factsheet PDF saved by hand; the member list is the stored one (or fetched once when none is stored)."""
    today = today or datetime.now(timezone.utc).date()
    ix = INDICES[key]
    as_of, rows = parse_factsheet(factsheet_text(Path(path).read_bytes()))
    members = latest(lake_dir, key, "members")
    if members.empty:
        status_, body = fetch(MEMBERS_URL.format(code=ix.members_code))
        if status_ != 200:
            raise SystemExit(f"no stored member list for {key} and the fetch answered HTTP {status_}")
        members = parse_members(body, ix)
    store_snapshot(ix, "factsheet_top10", factsheet_rows(as_of, rows, members), today, lake_dir)
    return len(rows)


def status(lake_dir: Optional[Path] = None) -> dict:
    df, p = load(lake_dir), table_path(lake_dir)
    out = {"rows": len(df), "bytes": p.stat().st_size if p.exists() else 0,
           "snapshot_days": int(df["date"].nunique()) if len(df) else 0}
    for (ix, src), g in (df.groupby(["index", "source"]) if len(df) else []):
        last = g[g["date"] == g["date"].max()]
        out[f"{ix}/{src}"] = dict(snapshots=int(g["date"].nunique()), first=str(g["date"].min()), last=str(g["date"].max()),
                                  members=len(last), as_of=str(last["as_of"].iloc[0]))
    return out


def main(argv: Optional[list[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(prog="python -m data_layer.index_weights", description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    rf = sub.add_parser("refresh", help="fetch member lists and factsheets, store snapshots that changed")
    rf.add_argument("--index", default="", help="comma-separated: " + ", ".join(INDICES))
    rf.add_argument("--sleep", type=float, default=1.0)
    im = sub.add_parser("import-factsheet", help="store a factsheet PDF saved by hand")
    im.add_argument("file", type=Path)
    im.add_argument("--index", required=True, choices=list(INDICES))
    sub.add_parser("status")
    a = ap.parse_args(argv)
    if a.cmd == "status":
        print(json.dumps(status(), indent=1))
        return 0
    if a.cmd == "import-factsheet":
        print(f"index_weights: {import_factsheet(a.file, a.index)} factsheet weights stored for {a.index}")
        return 0
    res = refresh(sleep_s=a.sleep, indices=[s.strip() for s in a.index.split(",") if s.strip()] or None)
    print(json.dumps(res, indent=1, default=str))
    return 1 if any("error" in r for r in res.values()) else 0


if __name__ == "__main__":
    sys.exit(main())
