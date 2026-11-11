"""
Build the stock universe for the Yahoo collectors from index constituents (US, UK, Europe, Japan, Korea, Taiwan, Singapore, China, Hong Kong).

  python -m data_layer.index_universe fetch  [--only sp500,hsi]    # download each index's list from the web into data/index_sources/<key>.csv
  python -m data_layer.index_universe import KEY FILE.csv         # a downloaded member list -> data/index_sources/<key>.csv
  python -m data_layer.index_universe build  [--validate]          # source files -> data/universe_global.csv (+ data/index_sources/_rejects.csv)
  python -m data_layer.index_universe status

Indices: sp500 (US), ftse100 (UK), stoxx600 (EU; the London-listed members join UK), nikkei225 (JP), kospi200 (KR), taiwan50 (TW),
sti30 (SG), csi300 (CN), hsi (HK).  India stays in data/universe.csv (NSE, with ISINs for the Upstox feed).

How it works, and where it can go wrong:
  * `fetch` reads public pages (mostly Wikipedia tables, the STOXX 600 from the iShares fund's holdings file).  The page layouts are
    not under our control, so every index has an expected member count: a list outside it is rejected, never half-written.
  * An index whose page cannot be read is supplied as a file instead: data/index_sources/<key>.csv with the columns
    code, name [, exchange, country, sector, yahoo].  A `yahoo` value wins over the symbol worked out from `code`.
  * `build` turns codes into Yahoo symbols (7203 -> 7203.T, 005930 -> 005930.KS, 600519 -> 600519.SS, 700 -> 0700.HK ...), drops
    duplicates across indices, and with --validate asks Yahoo for five days of daily bars: symbols with no data are rejected and listed.
  * The result is data/universe_global.csv (git-ignored).  universe_files.read_universe uses it for every market but India.
"""

from __future__ import annotations

import argparse
import csv
import io
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Callable, Optional

from . import universe_files
from .lake import MARKETS, ROOT

UA = {"User-Agent": "Mozilla/5.0 (compatible; JARVIS-research/1.0)"}
HEADER = ["symbol", "name", "market", "exchange", "isin", "cusip", "ric", "bbg_code", "sector", "currency"]
CURRENCY = {"US": "USD", "UK": "GBp", "JP": "JPY", "KR": "KRW", "TW": "TWD", "SG": "SGD", "CN": "CNY", "HK": "HKD"}   # EU is mixed: left blank
SOURCE_COLUMNS = ["code", "name", "exchange", "country", "sector", "yahoo"]


# ---------------------------------------------------------------------------------------------- code -> Yahoo symbol
def _digits(c: str) -> str:
    return re.sub(r"\D", "", c)


def y_us(code, ex=""):
    c = code.strip().upper().replace(".", "-")
    return c or None


def y_uk(code, ex=""):
    c = code.strip().upper().rstrip(".")
    if not c:
        return None
    return c if c.endswith(".L") else c.replace(".", "-") + ".L"


def y_jp(code, ex=""):
    c = code.strip().upper().replace(".T", "")
    return f"{c}.T" if re.fullmatch(r"\d{3}[0-9A-Z]", c) else None


def y_kr(code, ex=""):
    d = _digits(code)
    return f"{d.zfill(6)}.KS" if d and len(d) <= 6 else None


def y_tw(code, ex=""):
    d = _digits(code)
    return f"{d}.TW" if len(d) == 4 else None


def y_sg(code, ex=""):
    c = re.sub(r"^.*:\s*", "", code.strip()).upper().replace(".SI", "")
    return f"{c}.SI" if re.fullmatch(r"[0-9A-Z]{3,4}", c) else None


def y_cn(code, ex=""):
    d = _digits(code)
    if len(d) != 6:
        return None
    return f"{d}.SS" if d[0] in "69" else f"{d}.SZ" if d[0] in "023" else None


def y_hk(code, ex=""):
    d = _digits(code)
    return f"{int(d):04d}.HK" if d and int(d) > 0 else None


# exchange (and, for the Nordic exchange, country) -> Yahoo suffix: first match wins
EU_SUFFIX = [("xetra", ".DE"), ("deutsche", ".DE"), ("frankfurt", ".F"), ("paris", ".PA"), ("amsterdam", ".AS"), ("borsa italiana", ".MI"),
             ("milan", ".MI"), ("six swiss", ".SW"), ("swiss", ".SW"), ("madrid", ".MC"), ("brussels", ".BR"), ("lisbon", ".LS"),
             ("wiener", ".VI"), ("vienna", ".VI"), ("irish", ".IR"), ("dublin", ".IR"), ("oslo", ".OL"), ("london", ".L"),
             ("warsaw", ".WA"), ("athens", ".AT"), ("prague", ".PR"),
             ("sweden", ".ST"), ("stockholm", ".ST"), ("denmark", ".CO"), ("copenhagen", ".CO"), ("finland", ".HE"), ("helsinki", ".HE"),
             ("norway", ".OL")]


def y_eu(code, ex=""):
    c = code.strip().upper().replace(" ", "-").replace("/", "-")
    if not c:
        return None
    if re.search(r"\.[A-Z]{1,3}$", c):                    # already a Yahoo-style symbol
        return c
    low = ex.lower()
    for key, suffix in EU_SUFFIX:
        if key in low:
            return c.replace(".", "-") + suffix
    return None


# ---------------------------------------------------------------------------------------------- the indices
@dataclass
class Index:
    key: str
    market: str
    title: str
    expect: tuple[int, int]                               # members the list must have (a page that parses to another count is rejected)
    to_yahoo: Callable
    url: str = ""
    code_cols: tuple[str, ...] = ("symbol", "ticker", "code")
    name_cols: tuple[str, ...] = ("security", "company", "name", "constituent")
    sector_cols: tuple[str, ...] = ("sector", "industry")
    code_clean: Optional[Callable[[str], str]] = None
    kind: str = "wiki"                                    # wiki: tables in an HTML page; ishares: a holdings CSV


WIKI = "https://en.wikipedia.org/wiki/"
INDICES: list[Index] = [
    Index("sp500", "US", "S&P 500", (480, 520), y_us, WIKI + "List_of_S%26P_500_companies", ("symbol", "ticker"), ("security", "company"),
          ("gics sector", "sector")),
    Index("ftse100", "UK", "FTSE 100", (95, 105), y_uk, WIKI + "FTSE_100_Index", ("ticker", "epic", "symbol"), ("company", "name"),
          ("industry", "sector")),
    Index("stoxx600", "EU", "STOXX Europe 600", (500, 620), y_eu,
          "https://www.ishares.com/uk/individual/en/products/251931/ishares-stoxx-europe-600-ucits-etf-de-fund/1506575576011.ajax"
          "?fileType=csv&fileName=EXSA_holdings&dataType=fund", kind="ishares"),
    # found on the VPS run of 2026-11-10: the official Nikkei page lists the 225 in 34 sector tables (Code, Company Name); the generic
    # KOSPI page only lists ten "major components", the dedicated KOSPI 200 page lists all 200 (Company, Symbol, GICS Sector)
    Index("nikkei225", "JP", "Nikkei 225", (215, 235), y_jp, "https://indexes.nikkei.co.jp/en/nkave/index/component?idx=nk225",
          ("code",), ("company name", "company", "name")),
    Index("kospi200", "KR", "KOSPI 200", (190, 210), y_kr, WIKI + "KOSPI_200", ("symbol", "code", "ticker"), ("company", "name"),
          ("gics sector", "sector")),
    Index("taiwan50", "TW", "FTSE TWSE Taiwan 50", (48, 52), y_tw, WIKI + "FTSE_TWSE_Taiwan_50_Index", ("ticker", "code", "symbol"),
          ("company", "name")),
    Index("sti30", "SG", "Straits Times Index", (28, 32), y_sg, WIKI + "Straits_Times_Index", ("ticker", "code", "symbol"),
          ("company", "name")),
    Index("csi300", "CN", "CSI 300", (290, 310), y_cn, WIKI + "CSI_300_Index", ("code", "ticker", "symbol"), ("company", "name")),
    Index("hsi", "HK", "Hang Seng Index", (70, 100), y_hk, WIKI + "Hang_Seng_Index", ("ticker", "code", "stock code", "symbol"),
          ("company", "name", "constituent")),
]
BY_KEY = {i.key: i for i in INDICES}


def sources_dir(root: Path | None = None) -> Path:
    return (root or ROOT) / "data" / "index_sources"


# ---------------------------------------------------------------------------------------------- reading web pages
class _Tables(HTMLParser):
    """Every table of an HTML page as a list of rows of cell texts (citations in <sup> are dropped)."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.tables: list[list[list[str]]] = []
        self._stack: list[list[list[str]]] = []
        self._cell: list[str] | None = None
        self._sup = 0

    def handle_starttag(self, tag, attrs):
        if tag == "table":
            self._stack.append([])
        elif tag == "tr" and self._stack:
            self._stack[-1].append([])
        elif tag in ("td", "th") and self._stack and self._stack[-1]:
            self._cell = []
        elif tag == "sup":
            self._sup += 1
        elif tag == "br" and self._cell is not None:
            self._cell.append(" ")

    def handle_endtag(self, tag):
        if tag in ("td", "th") and self._cell is not None and self._stack and self._stack[-1]:
            self._stack[-1][-1].append(clean("".join(self._cell)))
            self._cell = None
        elif tag == "table" and self._stack:
            t = [r for r in self._stack.pop() if r]
            if t:
                self.tables.append(t)
        elif tag == "sup" and self._sup:
            self._sup -= 1

    def handle_data(self, data):
        if self._cell is not None and not self._sup:
            self._cell.append(data)


def clean(text: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"\[[^\]]*\]", "", text.replace("\xa0", " "))).strip()


def parse_tables(html: str) -> list[list[list[str]]]:
    p = _Tables()
    p.feed(html)
    return p.tables


def _col(header: list[str], candidates: tuple[str, ...]) -> int | None:
    low = [h.lower() for h in header]
    for cand in candidates:                               # earlier candidates are preferred
        for i, h in enumerate(low):
            if cand in h:
                return i
    return None


def members_from_tables(ix: Index, tables: list[list[list[str]]]) -> list[dict]:
    """The constituents a page lists: the one table holding a plausible number of members, else all matching tables together."""
    found: list[list[dict]] = []
    for t in tables:
        header = t[0]
        ci = _col(header, ix.code_cols)
        if ci is None:
            continue
        ni, si = _col(header, ix.name_cols), _col(header, ix.sector_cols)
        rows = []
        for r in t[1:]:
            if len(r) <= ci or not r[ci]:
                continue
            rows.append({"code": r[ci], "name": r[ni] if ni is not None and ni < len(r) else "", "exchange": "", "country": "",
                         "sector": r[si] if si is not None and si < len(r) else "", "yahoo": ""})
        if rows:
            found.append(rows)
    lo, hi = ix.expect
    for rows in sorted(found, key=len, reverse=True):
        if lo <= len({r["code"] for r in rows}) <= hi:
            return rows
    merged = {r["code"]: r for rows in found for r in rows}
    return list(merged.values())


def members_from_ishares(text: str) -> list[dict]:
    """Equity rows of an iShares holdings CSV (a few lines of fund details come first, then the Ticker, Name, ... table)."""
    lines = text.lstrip("﻿").splitlines()
    start = next((i for i, l in enumerate(lines) if l.lower().startswith(("ticker,", '"ticker"'))), None)
    if start is None:
        return []
    out = []
    for rec in csv.DictReader(io.StringIO("\n".join(lines[start:]))):
        rec = {(k or "").strip().lower(): (v or "").strip() for k, v in rec.items()}
        if rec.get("asset class", "Equity").lower() != "equity" or not rec.get("ticker") or rec["ticker"] == "-":
            continue
        out.append({"code": rec["ticker"], "name": rec.get("name", ""), "exchange": rec.get("exchange", ""), "country": rec.get("location", ""),
                    "sector": rec.get("sector", ""), "yahoo": ""})
    return out


def members_from_csv(text: str) -> list[dict]:
    """Members from any CSV that has a code column (ticker, symbol, code, ...) and a name column: an iShares holdings file (whose
    fund-details lines come first), a file saved from an index provider's page, or one written by hand.  Rows that are cash,
    derivatives or have no code are skipped."""
    lines = text.lstrip("\ufeff").splitlines()
    code_names = ("ticker", "symbol", "code", "stock code", "constituent code")
    start = next((i for i, l in enumerate(lines) if any(c in [x.strip().strip('"').lower() for x in l.split(",")] for c in code_names)), None)
    if start is None:
        return []
    out = []
    for rec in csv.DictReader(io.StringIO("\n".join(lines[start:]))):
        rec = {(k or "").strip().lower(): (v or "").strip() for k, v in rec.items()}
        code = next((rec[c] for c in code_names if rec.get(c)), "")
        if not code or code == "-" or rec.get("asset class", "equity").lower() not in ("equity", ""):
            continue
        name = next((rec[c] for c in ("name", "company name", "company", "security", "constituent") if rec.get(c)), "")
        out.append({"code": code, "name": name, "exchange": rec.get("exchange", ""), "country": rec.get("location") or rec.get("country", ""),
                    "sector": rec.get("sector", ""), "yahoo": rec.get("yahoo", "")})
    return out


def import_file(ix: Index, path: Path, root: Path | None = None) -> tuple[bool, str]:
    """Turn a downloaded member list into data/index_sources/<key>.csv.  The member count must be in the index's expected range."""
    try:
        text = Path(path).read_text(encoding="utf-8-sig", errors="replace")
    except OSError as exc:
        return False, f"cannot read {path}: {exc}"
    members = members_from_csv(text)
    n = len({m["code"] for m in members})
    if not ix.expect[0] <= n <= ix.expect[1]:
        return False, f"{n} members found in the file, expected {ix.expect[0]} to {ix.expect[1]}: not imported"
    return True, f"{n} members -> {write_source(ix, members, root)}"


def download(url: str) -> str:
    import requests
    r = requests.get(url, headers=UA, timeout=60)
    r.raise_for_status()
    return r.text


def fetch_index(ix: Index, getter: Callable[[str], str] = download) -> tuple[list[dict], str]:
    """(members, note).  Members are empty, with the reason in the note, when the page could not be read or the count is off."""
    if not ix.url:
        return [], "no web source: supply data/index_sources/%s.csv" % ix.key
    try:
        text = getter(ix.url)
    except Exception as exc:                              # noqa: BLE001 -- one unreachable page must not stop the others
        return [], f"download failed: {type(exc).__name__}: {str(exc)[:100]}"
    members = members_from_ishares(text) if ix.kind == "ishares" else members_from_tables(ix, parse_tables(text))
    n = len({m["code"] for m in members})
    if not ix.expect[0] <= n <= ix.expect[1]:
        return [], f"parsed {n} members, expected {ix.expect[0]} to {ix.expect[1]}: not used (supply the file instead)"
    return members, f"{n} members"


def write_source(ix: Index, members: list[dict], root: Path | None = None) -> Path:
    d = sources_dir(root)
    d.mkdir(parents=True, exist_ok=True)
    path = d / f"{ix.key}.csv"
    seen, rows = set(), []
    for m in members:
        if m["code"] not in seen:
            seen.add(m["code"])
            rows.append(m)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, SOURCE_COLUMNS)
        w.writeheader()
        w.writerows({k: m.get(k, "") for k in SOURCE_COLUMNS} for m in rows)
    return path


def read_source(ix: Index, root: Path | None = None) -> list[dict]:
    path = sources_dir(root) / f"{ix.key}.csv"
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8-sig") as f:
        return [{k.strip().lower(): (v or "").strip() for k, v in rec.items() if k} for rec in csv.DictReader(f)]


# ---------------------------------------------------------------------------------------------- build
@dataclass
class Built:
    rows: list[dict] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    rejects: list[tuple[str, str, str]] = field(default_factory=list)       # (symbol or code, index, reason)
    problems: int = 0


def assemble(root: Path | None = None, only: list[str] | None = None) -> Built:
    out, seen = Built(), set()
    for ix in INDICES:
        if only and ix.key not in only:
            continue
        members = read_source(ix, root)
        n = len({m.get("code", "") for m in members})
        if not members:
            out.notes.append(f"{ix.key:<10} no source file (run fetch, or supply data/index_sources/{ix.key}.csv)")
            out.problems += 1
            continue
        if not ix.expect[0] <= n <= ix.expect[1]:
            out.notes.append(f"{ix.key:<10} {n} members, expected {ix.expect[0]} to {ix.expect[1]}: not used")
            out.problems += 1
            continue
        added = dup = 0
        for m in members:
            sym = (m.get("yahoo") or "").strip() or ix.to_yahoo(m.get("code", ""), f"{m.get('exchange', '')} {m.get('country', '')}")
            if not sym:
                out.rejects.append((m.get("code", ""), ix.key, "no Yahoo symbol could be worked out (give a `yahoo` column value)"))
                continue
            if sym in seen:
                dup += 1
                continue
            seen.add(sym)
            market = "UK" if sym.endswith(".L") else ix.market           # London listings of the STOXX 600 join the FTSE 100's market
            out.rows.append({"symbol": sym, "name": m.get("name", ""), "market": market, "exchange": m.get("exchange", ""), "isin": "",
                             "cusip": "", "ric": "", "bbg_code": "", "sector": m.get("sector", ""), "currency": CURRENCY.get(market, "")})
            added += 1
        out.notes.append(f"{ix.key:<10} {n} members -> {added} symbols" + (f", {dup} already listed under another index" if dup else ""))
    order = {m: i for i, m in enumerate(MARKETS)}
    out.rows.sort(key=lambda r: (order.get(r["market"], 99), r["symbol"]))
    return out


def validate(built: Built, batch: Callable | None = None) -> tuple[int, int]:
    """Ask Yahoo for five daily bars per symbol; a symbol with none is moved to the rejects.  A chunk with no answer at all (Yahoo down
    or blocking us) proves nothing, so its symbols stay.  Returns (rejected, unchecked)."""
    if batch is None:
        from .yf_client import CHUNK_SIZE, _batch_download
        size = CHUNK_SIZE
        batch = lambda chunk: _batch_download(chunk, period="5d")          # noqa: E731
    else:
        size = 100
    keep, rejected, unchecked = [], 0, 0
    syms = [r["symbol"] for r in built.rows]
    bad: set[str] = set()
    for i in range(0, len(syms), size):
        chunk = syms[i:i + size]
        try:
            got = batch(chunk)
        except Exception:                                  # noqa: BLE001
            got = {}
        if not got:
            unchecked += len(chunk)
            continue
        bad |= set(chunk) - set(got)
    for r in built.rows:
        if r["symbol"] in bad:
            built.rejects.append((r["symbol"], r["market"], "Yahoo has no daily bars for this symbol"))
            rejected += 1
        else:
            keep.append(r)
    built.rows = keep
    return rejected, unchecked


def write_universe(built: Built, root: Path | None = None) -> Path:
    path = (root or ROOT) / "data" / "universe_global.csv"
    tmp = path.with_suffix(".csv.tmp")
    with tmp.open("w", newline="", encoding="utf-8") as f:
        f.write(f"# Built {datetime.utcnow():%Y-%m-%d %H:%M} UTC by python -m data_layer.index_universe build.  Git-ignored.\n")
        w = csv.DictWriter(f, HEADER)
        w.writeheader()
        w.writerows(built.rows)
    tmp.replace(path)
    if built.rejects:
        d = sources_dir(root)
        d.mkdir(parents=True, exist_ok=True)
        with (d / "_rejects.csv").open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["symbol_or_code", "index_or_market", "reason"])
            w.writerows(built.rejects)
    return path


def counts(rows: list[dict]) -> dict[str, int]:
    out: dict[str, int] = {}
    for r in rows:
        out[r["market"]] = out.get(r["market"], 0) + 1
    return out


# ---------------------------------------------------------------------------------------------- command line
def main(argv: list[str] | None = None, root: Path | None = None, getter: Callable[[str], str] = download) -> int:
    ap = argparse.ArgumentParser(description="Build the index-constituent universe for the Yahoo collectors")
    sub = ap.add_subparsers(dest="cmd", required=True)
    f = sub.add_parser("fetch", help="download each index's member list into data/index_sources/")
    f.add_argument("--only", default=None, help="comma-separated index keys: " + ", ".join(BY_KEY))
    i = sub.add_parser("import", help="turn a downloaded member list (CSV) into data/index_sources/<key>.csv")
    i.add_argument("key", help="index key: " + ", ".join(BY_KEY))
    i.add_argument("file", type=Path, help="a CSV with a ticker, symbol or code column and a name column (iShares holdings files work)")
    b = sub.add_parser("build", help="source files -> data/universe_global.csv")
    b.add_argument("--only", default=None)
    b.add_argument("--validate", action="store_true", help="drop symbols Yahoo has no daily bars for")
    b.add_argument("--dry-run", action="store_true", help="report only, write nothing")
    sub.add_parser("status", help="source files and what the universe holds")
    a = ap.parse_args(argv)
    root = root or ROOT
    only = [k.strip() for k in a.only.split(",")] if getattr(a, "only", None) else None
    bad = [k for k in only or [] if k not in BY_KEY]
    if bad:
        print("unknown index key(s): " + ", ".join(bad) + "; choose from " + ", ".join(BY_KEY))
        return 2

    if a.cmd == "fetch":
        failed = 0
        for ix in INDICES:
            if only and ix.key not in only:
                continue
            members, note = fetch_index(ix, getter)
            if members:
                print(f"{ix.key:<10} OK     {note} -> {write_source(ix, members, root)}")
            else:
                failed += 1
                print(f"{ix.key:<10} FAILED {note}")
        return 1 if failed else 0

    if a.cmd == "import":
        if a.key not in BY_KEY:
            print("unknown index key; choose from " + ", ".join(BY_KEY))
            return 2
        ok, msg = import_file(BY_KEY[a.key], a.file, root)
        print(("OK     " if ok else "FAILED ") + msg)
        return 0 if ok else 1

    if a.cmd == "status":
        for ix in INDICES:
            p = sources_dir(root) / f"{ix.key}.csv"
            n = len(read_source(ix, root))
            age = f"{(datetime.now() - datetime.fromtimestamp(p.stat().st_mtime)).days} days old" if p.exists() else "missing"
            ok = "ok" if ix.expect[0] <= n <= ix.expect[1] else "OUT OF RANGE" if p.exists() else ""
            print(f"{ix.key:<10} {ix.market}  {n:>4} members (expect {ix.expect[0]}-{ix.expect[1]})  {age}  {ok}")
        g = universe_files._global_rows() if root == ROOT else []
        print("universe_global.csv:", counts(g) or "not built")
        return 0

    built = assemble(root, only)
    rejected = unchecked = 0
    if a.validate:
        rejected, unchecked = validate(built)
    print("\n".join(built.notes))
    print("symbols per market:", counts(built.rows), f"(total {len(built.rows)})")
    if a.validate:
        print(f"validated against Yahoo: {rejected} rejected, {unchecked} not checked (Yahoo gave no answer for the chunk)")
    if built.rejects:
        print(f"{len(built.rejects)} symbols rejected; the list is in data/index_sources/_rejects.csv")
    if a.dry_run:
        return 1 if built.problems else 0
    if not built.rows:
        print("nothing to write")
        return 1
    print("written:", write_universe(built, root))
    return 1 if built.problems else 0


if __name__ == "__main__":
    sys.exit(main())
