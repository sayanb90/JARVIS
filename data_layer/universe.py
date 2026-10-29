"""
Build and check data/universe.csv.

  python -m data_layer.universe import-nse ind_nifty500list.csv [--replace-india]
  python -m data_layer.universe check

`import-nse` reads an NSE index constituents CSV (the files published for the Nifty indices have the columns
Company Name, Industry, Symbol, Series, ISIN Code) and adds each stock as `<SYMBOL>.NS` with its ISIN, which the live
feed needs.  Column names are matched loosely (anything containing "symbol", "isin", "company", "industry").
Existing rows for other markets are kept.  Existing India rows are kept and updated unless --replace-india is given.
Rows with an invalid ISIN are reported and skipped.  The ISIN is the only thing the Upstox instrument key needs.
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
from pathlib import Path

from . import fmp_client

ISIN_RE = re.compile(r"^IN[A-Z0-9]{9}[0-9]$")
HEADER = ["symbol", "name", "market", "exchange", "isin", "cusip", "ric", "bbg_code", "sector", "currency"]


def _col(fields: list[str], *needles: str) -> str | None:
    for f in fields:
        low = f.strip().lower()
        if any(n in low for n in needles):
            return f
    return None


def parse_nse_csv(path: Path) -> tuple[list[dict], list[str]]:
    """Return (rows ready for universe.csv, problems)."""
    rows, problems, seen = [], [], set()
    with path.open(newline="", encoding="utf-8-sig") as f:
        rd = csv.DictReader(f)
        fields = rd.fieldnames or []
        c_sym, c_isin = _col(fields, "symbol"), _col(fields, "isin")
        c_name, c_ind = _col(fields, "company", "name"), _col(fields, "industry", "sector")
        if not c_sym or not c_isin:
            raise SystemExit(f"{path}: need a Symbol column and an ISIN column, found {fields}")
        for i, rec in enumerate(rd, start=2):
            sym, isin = rec[c_sym].strip().upper(), rec[c_isin].strip().upper()
            if not sym:
                continue
            if not ISIN_RE.match(isin):
                problems.append(f"line {i}: {sym} has an invalid ISIN {isin!r}")
                continue
            if sym in seen:
                problems.append(f"line {i}: duplicate symbol {sym}")
                continue
            seen.add(sym)
            rows.append({"symbol": f"{sym}.NS", "name": (rec.get(c_name) or sym).strip(), "market": "IN", "exchange": "NSE",
                         "isin": isin, "cusip": "", "ric": "", "bbg_code": "", "sector": (rec.get(c_ind) or "").strip(),
                         "currency": "INR"})
    return rows, problems


def _read_all(path: Path) -> tuple[list[str], list[dict], list[str]]:
    comments, rows = [], []
    if path.exists():
        lines = path.read_text(encoding="utf-8").splitlines()
        comments = [ln for ln in lines if ln.startswith("#")]
        body = [ln for ln in lines if not ln.startswith("#") and ln.strip()]
        rows = [dict(r) for r in csv.DictReader(body)]
    return comments, rows, HEADER


def write_universe(path: Path, comments: list[str], rows: list[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        for c in comments:
            f.write(c + "\n")
        w = csv.DictWriter(f, fieldnames=HEADER, extrasaction="ignore", lineterminator="\n")
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in HEADER})


def import_nse(csv_path: Path, universe_path: Path | None = None, replace_india: bool = False) -> dict:
    universe_path = universe_path or fmp_client.UNIVERSE_CSV
    new, problems = parse_nse_csv(csv_path)
    comments, existing, _ = _read_all(universe_path)
    keep = [r for r in existing if r.get("market") != "IN"]
    old_in = {r["symbol"]: r for r in existing if r.get("market") == "IN"}
    merged = {r["symbol"]: r for r in new}
    if not replace_india:
        for sym, r in old_in.items():
            merged.setdefault(sym, r)
    out = keep + [merged[s] for s in sorted(merged)]
    write_universe(universe_path, comments, out)
    return {"imported": len(new), "india_total": len(merged), "other_markets": len(keep), "problems": problems,
            "added": len(set(merged) - set(old_in)), "kept_old": len(set(old_in) - {r["symbol"] for r in new}) if not replace_india else 0}


def check(universe_path: Path | None = None) -> list[str]:
    """Problems found in universe.csv (empty = fine)."""
    _, rows, _ = _read_all(universe_path or fmp_client.UNIVERSE_CSV)
    problems, seen = [], set()
    for r in rows:
        s = r.get("symbol", "")
        if s in seen:
            problems.append(f"duplicate symbol {s}")
        seen.add(s)
        if r.get("market") not in ("IN", "US", "UK", "EU"):
            problems.append(f"{s}: unknown market {r.get('market')!r}")
        if r.get("market") == "IN":
            if not s.endswith(".NS"):
                problems.append(f"{s}: India symbols must end in .NS")
            if not ISIN_RE.match(r.get("isin", "")):
                problems.append(f"{s}: missing or invalid ISIN (the live feed cannot subscribe it)")
    if not rows:
        problems.append("universe.csv has no rows")
    return problems


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    imp = sub.add_parser("import-nse")
    imp.add_argument("csv", type=Path)
    imp.add_argument("--replace-india", action="store_true")
    sub.add_parser("check")
    a = ap.parse_args(argv)
    if a.cmd == "import-nse":
        s = import_nse(a.csv, replace_india=a.replace_india)
        print(f"imported {s['imported']} NSE stocks ({s['added']} new); India total {s['india_total']}; "
              f"other markets untouched ({s['other_markets']} rows)")
        for p in s["problems"][:15]:
            print("  skipped:", p)
        return 0
    probs = check()
    print("\n".join(probs) if probs else "universe.csv OK")
    return 1 if probs else 0


if __name__ == "__main__":
    sys.exit(main())
