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
import os
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
    universe_path = universe_path or fmp_client.active_universe_csv()
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
    _, rows, _ = _read_all(universe_path or fmp_client.active_universe_csv())
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


# ---------------------------------------------------------------------------
# Ticker lists (admin upload page): a CSV of plain NSE tickers -> universe rows with ISINs and Upstox keys
# ---------------------------------------------------------------------------
MAX_TICKERS = 5000
_HEADER_WORDS = {"SYMBOL", "SYMBOLS", "TICKER", "TICKERS", "NAME", "INSTRUMENT", "STOCK"}
TICKER_RE = re.compile(r"^[A-Z0-9][A-Z0-9&\-]{0,24}$")


def normalise_ticker(raw: str) -> str:
    """'nse:reliance', ' RELIANCE.NS ', 'RELIANCE-EQ' -> 'RELIANCE'.  '' for blanks and header words."""
    t = raw.strip().strip('"').strip("'").upper()
    for pre in ("NSE:", "NSE_EQ|"):
        if t.startswith(pre):
            t = t[len(pre):]
    for suf in (".NS", "-EQ"):
        if t.endswith(suf):
            t = t[: -len(suf)]
    return "" if t in _HEADER_WORDS else t


def parse_ticker_text(text: str) -> list[str]:
    """First column of each line of a CSV (or one ticker per line), normalised and de-duplicated in order."""
    out, seen = [], set()
    for line in text.splitlines():
        cell = line.split(",")[0].split(";")[0].split("\t")[0]
        t = normalise_ticker(cell)
        if t and t not in seen:
            seen.add(t)
            out.append(t)
    return out


def map_tickers(tickers: list[str], eq_table, current: list[dict] | None = None) -> dict:
    """
    Map NSE tickers to universe rows using the equity instrument table (`fo_instruments import` stores it).
    Tickers already in the current universe with an ISIN map even without the table.  Returns
    {"rows": [...universe rows...], "unknown": [{"ticker", "suggestions"}], "invalid": [...]}.
    """
    import difflib
    current = current or []
    by_sym = {} if eq_table is None or len(eq_table) == 0 else {r.symbol: r for r in eq_table.itertuples()}
    have = {r["symbol"][:-3]: r for r in current if r.get("market") == "IN" and r.get("symbol", "").endswith(".NS")}
    rows, unknown, invalid = [], [], []
    for t in tickers[:MAX_TICKERS]:
        if not TICKER_RE.match(t):
            invalid.append(t)
            continue
        e = by_sym.get(t)
        if e is not None and ISIN_RE.match(e.isin):
            rows.append({"symbol": f"{t}.NS", "name": e.name or t, "market": "IN", "exchange": "NSE", "isin": e.isin,
                         "cusip": "", "ric": "", "bbg_code": "", "sector": (have.get(t) or {}).get("sector", ""),
                         "currency": "INR"})
        elif t in have and ISIN_RE.match(have[t].get("isin", "")):
            rows.append(dict(have[t]))
        else:
            unknown.append({"ticker": t, "suggestions": difflib.get_close_matches(t, list(by_sym), n=3, cutoff=0.75)})
    return {"rows": rows, "unknown": unknown, "invalid": invalid, "truncated": max(0, len(tickers) - MAX_TICKERS)}


def plan_india(rows: list[dict], current: list[dict], mode: str = "replace") -> dict:
    """The India list after an upload: `replace` swaps it for `rows`, `add` appends new symbols to it."""
    old = [r for r in current if r.get("market") == "IN"]
    old_syms = {r["symbol"] for r in old}
    new = {r["symbol"]: r for r in rows}
    if mode == "add":
        merged = {r["symbol"]: r for r in old}
        merged.update({k: v for k, v in new.items() if k not in merged})
    elif mode == "replace":
        merged = new
    else:
        raise ValueError("mode must be replace or add")
    return {"india": [merged[s] for s in sorted(merged)], "added": sorted(set(merged) - old_syms),
            "removed": sorted(old_syms - set(merged)), "kept": sorted(old_syms & set(merged))}


def write_active_universe(india: list[dict], history_dir: Path | None = None, now=None) -> dict:
    """
    Write data/universe.local.csv (the file the collectors read first) with the given India rows and every other
    market's rows unchanged, after saving the file in force to data/universe_history/.  Returns where it went.
    """
    from datetime import datetime
    src = fmp_client.active_universe_csv()
    local = fmp_client.UNIVERSE_CSV.with_name("universe.local.csv")
    history_dir = history_dir or local.parent / "universe_history"
    comments, existing, _ = _read_all(src)
    backup = None
    if src.exists():
        history_dir.mkdir(parents=True, exist_ok=True)
        stamp = (now or datetime.now()).strftime("%Y%m%d-%H%M%S")
        backup = history_dir / f"universe-{stamp}.csv"
        backup.write_bytes(src.read_bytes())
    out = [r for r in existing if r.get("market") != "IN"] + india
    tmp = local.with_name(local.name + ".tmp")
    write_universe(tmp, comments, out)
    os.replace(tmp, local)
    return {"written": str(local), "backup": str(backup) if backup else None, "india": len(india), "total": len(out)}


def history(history_dir: Path | None = None) -> list[dict]:
    d = history_dir or fmp_client.UNIVERSE_CSV.parent / "universe_history"
    files = sorted(d.glob("universe-*.csv"), reverse=True) if d.exists() else []
    return [{"name": f.name, "india": sum(1 for r in _read_all(f)[1] if r.get("market") == "IN")} for f in files]


def restore(name: str, history_dir: Path | None = None) -> dict:
    """Make a saved version the active universe again (the current one is saved first, so this is undoable)."""
    d = history_dir or fmp_client.UNIVERSE_CSV.parent / "universe_history"
    if not re.fullmatch(r"universe-\d{8}-\d{6}\.csv", name) or not (d / name).is_file():
        raise ValueError("no such saved version")
    india = [r for r in _read_all(d / name)[1] if r.get("market") == "IN"]
    return write_active_universe(india, d)


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
