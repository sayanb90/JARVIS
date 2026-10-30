"""
NSE futures and options: instrument master, contract selection, key maps.

  python -m data_layer.upstox.fo_instruments import --file NSE.json.gz     # or --download
  python -m data_layer.upstox.fo_instruments peek --underlying NIFTY       # eyeball real records (field names!)
  python -m data_layer.upstox.fo_instruments list --underlying NIFTY --type FUT
  python -m data_layer.upstox.fo_instruments select --underlying NIFTY BANKNIFTY RELIANCE \\
        --futures 2 --option-expiries 1 --strikes 10 [--spot NIFTY=24500]
  python -m data_layer.upstox.fo_instruments select --watchlist data/fo_watchlist.csv   # tracked subscription list

Flow: `import` parses Upstox's NSE instrument file into lake/instruments/fo.parquet (one row per contract,
first_seen / last_seen kept so expired contracts stay resolvable for old bars).  `select` picks the contracts
to collect (nearest futures expiries, option strikes around the money) and writes data/fo_universe.csv, which
`feed --segment fo` and `candles --segment fo` read.  Pass `--underlying` directly for a one-off pull, or
`--watchlist` (default data/fo_watchlist.csv, checked into git) to drive it from a per-underlying subscription
list -- each row can have its own futures/option-expiry/strike window, and `strikes` may be "all".

Lake symbol for a contract is Upstox's trading symbol with spaces turned into underscores, for example
NIFTY_25000_CE_28_NOV_24 or RELIANCE_FUT_28_NOV_24.  It is unique per contract and stays valid after expiry.

Assumed record shape (from community examples of the NSE instrument file; the docs site is not reachable from the
build sandbox, so run `peek` once and compare): segment "NSE_FO", instrument_type FUT / CE / PE, instrument_key
"NSE_FO|<token>", trading_symbol, underlying_symbol, underlying_key, expiry as epoch milliseconds, strike_price,
lot_size, tick_size, weekly.  Missing optional fields are tolerated; missing keys or expiries drop the row and are
counted in the log.
"""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

from .. import lake as lake_mod

log = logging.getLogger(__name__)

MASTER_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
SEGMENT = "NSE_FO"
FO_TYPES = ("FUT", "CE", "PE")
IST = timezone(timedelta(hours=5, minutes=30))
COLUMNS = ["instrument_key", "symbol", "underlying", "underlying_key", "instrument_type", "expiry", "strike",
           "lot_size", "tick_size", "weekly", "exchange_token", "freeze_qty"]
UNIVERSE_COLUMNS = ["symbol", "instrument_key", "underlying", "instrument_type", "expiry", "strike", "lot_size"]


def ist_today() -> date:
    return datetime.now(IST).date()


# ---------------------------------------------------------------------------
# Parsing the instrument file
# ---------------------------------------------------------------------------
def expiry_date(raw) -> date | None:
    """Epoch milliseconds (or an ISO string) -> the exchange date.  Midnight IST is 18:30 UTC the day before."""
    if raw in (None, "", 0):
        return None
    if isinstance(raw, (int, float, np.integer, np.floating)):
        return datetime.fromtimestamp(float(raw) / 1000, tz=timezone.utc).astimezone(IST).date()
    try:
        return pd.Timestamp(raw).date()
    except (ValueError, TypeError):
        return None


def contract_symbol(rec: dict, underlying: str, itype: str, expiry: date, strike: float | None) -> str:
    ts = (rec.get("trading_symbol") or "").strip()
    if ts:
        return "_".join(ts.split())
    tail = f"{strike:g}_{itype}" if itype != "FUT" else "FUT"
    return f"{underlying}_{tail}_{expiry:%d_%b_%y}".upper()


def parse_master(records: Iterable[dict]) -> pd.DataFrame:
    """NSE instrument records -> one row per futures/options contract (see COLUMNS)."""
    rows, dropped = [], 0
    for r in records:
        if r.get("segment") != SEGMENT or r.get("instrument_type") not in FO_TYPES:
            continue
        key, exp = r.get("instrument_key"), expiry_date(r.get("expiry"))
        under = (r.get("underlying_symbol") or r.get("asset_symbol") or r.get("name") or "").strip()
        if not key or exp is None or not under:
            dropped += 1
            continue
        itype = r["instrument_type"]
        try:
            strike = float(r["strike_price"]) if itype != "FUT" and r.get("strike_price") not in (None, "") else np.nan
        except (TypeError, ValueError):
            strike = np.nan
        if itype != "FUT" and np.isnan(strike):
            dropped += 1
            continue
        rows.append({
            "instrument_key": key, "symbol": contract_symbol(r, under, itype, exp, None if np.isnan(strike) else strike),
            "underlying": under, "underlying_key": r.get("underlying_key") or "", "instrument_type": itype,
            "expiry": exp, "strike": strike, "lot_size": int(r.get("lot_size") or r.get("minimum_lot") or 0),
            "tick_size": float(r.get("tick_size") or 0), "weekly": bool(r.get("weekly", False)),
            "exchange_token": str(r.get("exchange_token") or ""), "freeze_qty": int(r.get("freeze_quantity") or 0)})
    if dropped:
        log.warning("%d derivative records dropped (missing key, expiry, underlying or strike)", dropped)
    df = pd.DataFrame(rows, columns=COLUMNS)
    dup = df[df.duplicated("symbol", keep=False)]
    if len(dup):
        raise ValueError(f"{len(dup)} contracts share a lake symbol, for example {dup.symbol.iloc[0]!r}; "
                         "the trading_symbol field is not unique as assumed")
    return df


def read_records(path: Path | str) -> list[dict]:
    p = Path(path)
    raw = p.read_bytes()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    data = json.loads(raw)
    if not isinstance(data, list):
        raise ValueError(f"{p} is not a JSON list of instrument records")
    return data


def download(dest: Path, url: str = MASTER_URL, get=None) -> Path:
    """Fetch the NSE instrument file.  Some automated clients are refused, in which case download it in a browser."""
    import requests
    get = get or requests.get
    r = get(url, timeout=120, headers={"User-Agent": "Mozilla/5.0 (jarvis-research)"})
    if r.status_code != 200:
        raise RuntimeError(f"instrument file download failed (HTTP {r.status_code}). Download {url} in a browser, "
                           "copy it to the server and run import --file <path>")
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(r.content)
    return dest


# ---------------------------------------------------------------------------
# The lake table
# ---------------------------------------------------------------------------
def table_path(lake_dir: Path | None = None) -> Path:
    return (lake_dir or lake_mod.LAKE_DIR) / "instruments" / "fo.parquet"


def load_table(lake_dir: Path | None = None) -> pd.DataFrame:
    p = table_path(lake_dir)
    if not p.exists():
        return pd.DataFrame(columns=COLUMNS + ["first_seen", "last_seen"])
    return pd.read_parquet(p)


def save_table(df: pd.DataFrame, asof: date, lake_dir: Path | None = None) -> int:
    """
    Merge today's contracts into the stored table.  Contracts that have left the file (expired) are kept with
    their old last_seen; a contract that is still listed keeps its first_seen.  Returns the stored row count.
    """
    path = table_path(lake_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    new = df.copy()
    new["last_seen"] = asof
    if path.exists():
        old = pd.read_parquet(path)
        first = old.set_index("instrument_key")["first_seen"]
        new["first_seen"] = new["instrument_key"].map(first).fillna(asof)
        out = pd.concat([old[~old["instrument_key"].isin(new["instrument_key"])], new], ignore_index=True)
    else:
        new["first_seen"] = asof
        out = new
    out = out.sort_values(["underlying", "expiry", "instrument_type", "strike", "symbol"]).reset_index(drop=True)
    tmp = path.with_name(path.name + ".tmp")
    out.to_parquet(tmp, compression="zstd", index=False)
    os.replace(tmp, path)
    return len(out)


# ---------------------------------------------------------------------------
# Equities from the same file (NSE_EQ records): lets a plain list of tickers be mapped to Upstox keys
# ---------------------------------------------------------------------------
EQ_SEGMENT = "NSE_EQ"
EQ_COLUMNS = ["symbol", "name", "isin", "instrument_key", "instrument_type", "exchange_token", "tick_size", "lot_size"]


def parse_equities(records: Iterable[dict]) -> pd.DataFrame:
    """
    NSE cash-equity records -> one row per trading symbol (as on the exchange, e.g. RELIANCE).  The ISIN comes from
    the `isin` field or, failing that, from the key itself (`NSE_EQ|<ISIN>`).  When one symbol appears in several
    series, the regular `EQ` series wins.
    """
    rows = []
    for r in records:
        if r.get("segment") != EQ_SEGMENT:
            continue
        key, sym = r.get("instrument_key") or "", (r.get("trading_symbol") or "").strip().upper()
        if not key or not sym:
            continue
        isin = (r.get("isin") or key.split("|", 1)[-1]).strip().upper()
        rows.append({"symbol": sym, "name": (r.get("name") or "").strip(), "isin": isin, "instrument_key": key,
                     "instrument_type": (r.get("instrument_type") or "").strip().upper(),
                     "exchange_token": str(r.get("exchange_token") or ""), "tick_size": float(r.get("tick_size") or 0),
                     "lot_size": int(r.get("lot_size") or 1)})
    df = pd.DataFrame(rows, columns=EQ_COLUMNS)
    if df.empty:
        return df
    df["_rank"] = (df["instrument_type"] != "EQ").astype(int)
    return (df.sort_values(["symbol", "_rank"]).drop_duplicates("symbol").drop(columns="_rank")
              .sort_values("symbol").reset_index(drop=True))


def eq_table_path(lake_dir: Path | None = None) -> Path:
    return (lake_dir or lake_mod.LAKE_DIR) / "instruments" / "eq.parquet"


def save_eq_table(df: pd.DataFrame, lake_dir: Path | None = None) -> int:
    """Replace the equity table (a snapshot: listings and delistings come and go with the daily file)."""
    path = eq_table_path(lake_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    df.to_parquet(tmp, compression="zstd", index=False)
    os.replace(tmp, path)
    return len(df)


def load_eq_table(lake_dir: Path | None = None) -> pd.DataFrame:
    p = eq_table_path(lake_dir)
    return pd.read_parquet(p) if p.exists() else pd.DataFrame(columns=EQ_COLUMNS)


def resolve(symbol: str, lake_dir: Path | None = None) -> dict | None:
    """Contract record for a lake symbol (expired contracts included), or None."""
    t = load_table(lake_dir)
    hit = t[t["symbol"] == symbol]
    return None if hit.empty else hit.iloc[0].to_dict()


# ---------------------------------------------------------------------------
# Choosing what to collect
# ---------------------------------------------------------------------------
def select_contracts(table: pd.DataFrame, underlyings: list[str], day: date, futures: int = 2,
                     option_expiries: int = 1, strikes: int = 10, spot: dict[str, float] | None = None,
                     types: tuple[str, ...] = FO_TYPES) -> tuple[pd.DataFrame, dict]:
    """
    Nearest `futures` futures expiries and, for the nearest `option_expiries` option expiries, the strikes
    `strikes` steps either side of the at-the-money strike, for each underlying.  Expired contracts are ignored.
    The at-the-money level comes from `spot[underlying]`; without it the middle of the strike ladder is used
    (reported in the returned notes), which is only a rough proxy.
    Returns (contracts, notes) where notes lists underlyings that were not found and ones centred without a spot.
    """
    spot = spot or {}
    live = table[table["expiry"] >= day]
    picked, notes = [], {"missing": [], "no_spot": []}
    for u in underlyings:
        tu = live[live["underlying"] == u]
        if tu.empty:
            notes["missing"].append(u)
            continue
        if "FUT" in types:
            fut = tu[tu["instrument_type"] == "FUT"]
            for e in sorted(fut["expiry"].unique())[:futures]:
                picked.append(fut[fut["expiry"] == e])
        opt = tu[tu["instrument_type"].isin([t for t in types if t != "FUT"])]
        for e in sorted(opt["expiry"].unique())[:option_expiries]:
            chain = opt[opt["expiry"] == e]
            ladder = np.array(sorted(chain["strike"].unique()))
            if u in spot:
                centre = int(np.abs(ladder - spot[u]).argmin())
            else:
                centre = len(ladder) // 2
                if u not in notes["no_spot"]:
                    notes["no_spot"].append(u)
            lo, hi = max(0, centre - strikes), min(len(ladder), centre + strikes + 1)
            picked.append(chain[chain["strike"].isin(ladder[lo:hi])])
    if not picked:
        return pd.DataFrame(columns=UNIVERSE_COLUMNS), notes
    out = pd.concat(picked, ignore_index=True).drop_duplicates("instrument_key")
    out = out.sort_values(["underlying", "instrument_type", "expiry", "strike"]).reset_index(drop=True)
    return out[UNIVERSE_COLUMNS], notes


WATCHLIST_COLUMNS = ["underlying", "futures", "option_expiries", "strikes", "no_options"]


def watchlist_path() -> Path:
    return lake_mod.DATA_DIR / "fo_watchlist.csv"


def read_watchlist(path: Path | None = None) -> list[dict]:
    """The tracked subscription list for the derivatives pipeline (data/fo_watchlist.csv, checked into git
    unlike the generated fo_universe.csv): one row per underlying with its own expiry/strike window, so
    different underlyings can have different windows in one `select` run.  `strikes` may be an integer
    (steps either side of the at-the-money strike) or "all" for the whole chain."""
    p = path or watchlist_path()
    df = pd.read_csv(p)
    rows = []
    for _, r in df.iterrows():
        strikes_raw = str(r.get("strikes", 10)).strip().lower()
        strikes = 10**6 if strikes_raw == "all" else int(strikes_raw)
        rows.append({
            "underlying": str(r["underlying"]).strip(),
            "futures": int(r["futures"]) if pd.notna(r.get("futures")) else 2,
            "option_expiries": int(r["option_expiries"]) if pd.notna(r.get("option_expiries")) else 1,
            "strikes": strikes,
            "no_options": bool(r["no_options"]) if pd.notna(r.get("no_options")) else False,
        })
    return rows


def select_from_watchlist(table: pd.DataFrame, rows: list[dict], day: date,
                          spot: dict[str, float] | None = None) -> tuple[pd.DataFrame, dict]:
    """Run `select_contracts` once per watchlist row (each may have its own futures/option-expiry/strike
    window) and combine the results into one contract table, same shape as `select_contracts` returns."""
    spot = spot or {}
    picked, notes = [], {"missing": [], "no_spot": []}
    for row in rows:
        types = ("FUT",) if row["no_options"] else FO_TYPES
        sel, n = select_contracts(table, [row["underlying"]], day, row["futures"], row["option_expiries"],
                                  row["strikes"], spot, types)
        if not sel.empty:
            picked.append(sel)
        notes["missing"] += n["missing"]
        notes["no_spot"] += n["no_spot"]
    if not picked:
        return pd.DataFrame(columns=UNIVERSE_COLUMNS), notes
    out = pd.concat(picked, ignore_index=True).drop_duplicates("instrument_key")
    out = out.sort_values(["underlying", "instrument_type", "expiry", "strike"]).reset_index(drop=True)
    return out, notes


def universe_path() -> Path:
    return lake_mod.DATA_DIR / "fo_universe.csv"


def write_universe(sel: pd.DataFrame, path: Path | None = None) -> Path:
    p = path or universe_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    sel.to_csv(p, index=False)
    return p


def read_universe(path: Path | None = None, day: date | None = None) -> pd.DataFrame:
    """The selected contracts; rows that expired before `day` are dropped (default: today in India)."""
    p = path or universe_path()
    if not p.exists():
        raise SystemExit(f"{p} not found: run fo_instruments import, then select")
    df = pd.read_csv(p, parse_dates=["expiry"])
    df["expiry"] = df["expiry"].dt.date
    day = day or ist_today()
    live = df[df["expiry"] >= day]
    if len(live) < len(df):
        log.warning("%d selected contracts have expired and are skipped (re-run select)", len(df) - len(live))
    return live.reset_index(drop=True)


def fo_key_map(path: Path | None = None, day: date | None = None) -> dict[str, str]:
    df = read_universe(path, day)
    return dict(zip(df["instrument_key"], df["symbol"]))


def spot_from_lake(sel_table: pd.DataFrame, underlyings: list[str], day: date,
                   lake_dir: Path | None = None) -> dict[str, float]:
    """Last traded close of each underlying's nearest future in the lake's bars, if any were collected."""
    from ..query import Lake
    lake = Lake(lake_dir)
    out: dict[str, float] = {}
    for u in underlyings:
        fut = sel_table[(sel_table["underlying"] == u) & (sel_table["instrument_type"] == "FUT")
                        & (sel_table["expiry"] >= day)].sort_values("expiry")
        for sym in fut["symbol"].head(1):
            for view in [t for t in lake.tables() if t.startswith("bars_")]:
                df = lake.sql(f"SELECT close FROM {view} WHERE symbol = ? ORDER BY ts DESC LIMIT 1", [sym])
                if len(df):
                    out[u] = float(df["close"].iloc[0])
                    break
    return out


# ---------------------------------------------------------------------------
def _parse_spot(items: list[str] | None) -> dict[str, float]:
    out = {}
    for it in items or []:
        name, _, val = it.partition("=")
        out[name.strip()] = float(val)
    return out


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description="NSE futures and options instrument master and contract selection")
    sub = ap.add_subparsers(dest="cmd", required=True)
    pi = sub.add_parser("import", help="parse the NSE instrument file into lake/instruments/fo.parquet")
    pi.add_argument("--file", type=Path)
    pi.add_argument("--download", action="store_true", help=f"fetch {MASTER_URL}")
    pi.add_argument("--asof", type=date.fromisoformat, default=None)
    pp = sub.add_parser("peek", help="print a few raw derivative records to confirm the field names")
    pp.add_argument("--file", type=Path, required=True)
    pp.add_argument("--underlying")
    pp.add_argument("-n", type=int, default=3)
    pl = sub.add_parser("list", help="list stored contracts")
    pl.add_argument("--underlying")
    pl.add_argument("--type", choices=FO_TYPES)
    pl.add_argument("--day", type=date.fromisoformat, default=None, help="only contracts alive on this day")
    pl.add_argument("-n", type=int, default=40)
    ps = sub.add_parser("select", help="choose contracts to collect and write data/fo_universe.csv")
    ps.add_argument("--underlying", nargs="+", default=None,
                    help="omit to drive selection from --watchlist instead")
    ps.add_argument("--watchlist", type=Path, default=None,
                    help="CSV of underlying,futures,option_expiries,strikes[,no_options] -- strikes may be "
                         "'all'; used when --underlying is omitted (default data/fo_watchlist.csv)")
    ps.add_argument("--futures", type=int, default=2, help="nearest N futures expiries")
    ps.add_argument("--option-expiries", type=int, default=1, help="nearest N option expiries")
    ps.add_argument("--strikes", type=int, default=10, help="strikes each side of at-the-money")
    ps.add_argument("--no-options", action="store_true")
    ps.add_argument("--spot", nargs="*", metavar="UNDERLYING=LEVEL", help="at-the-money level per underlying")
    ps.add_argument("--day", type=date.fromisoformat, default=None)
    ps.add_argument("--out", type=Path, default=None)
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if a.cmd == "import":
        path = a.file
        if a.download:
            path = download(lake_mod.DATA_DIR / "NSE.json.gz")
        if not path:
            raise SystemExit("give --file <NSE.json[.gz]> or --download")
        records = read_records(path)
        eq = parse_equities(records)
        if not eq.empty:
            save_eq_table(eq)
            print(f"stored {len(eq)} NSE equities (ticker -> Upstox key) in {eq_table_path()}")
        df = parse_master(records)
        if df.empty:
            raise SystemExit("no NSE_FO futures/options records found: wrong file, or the field names differ. "
                             "Run `peek` and compare with the module docstring")
        n = save_table(df, a.asof or ist_today())
        by = df.groupby("instrument_type").size().to_dict()
        print(f"imported {len(df)} contracts ({by}), {df.underlying.nunique()} underlyings; table now holds {n}")
    elif a.cmd == "peek":
        shown = 0
        for r in read_records(a.file):
            if r.get("segment") == SEGMENT and (not a.underlying or r.get("underlying_symbol") == a.underlying):
                print(json.dumps(r, indent=1, sort_keys=True))
                shown += 1
                if shown >= a.n:
                    break
        if not shown:
            print("no NSE_FO records matched; segments present:",
                  sorted({str(r.get("segment")) for r in read_records(a.file)})[:20])
    elif a.cmd == "list":
        t = load_table()
        if a.underlying:
            t = t[t["underlying"] == a.underlying]
        if a.type:
            t = t[t["instrument_type"] == a.type]
        if a.day:
            t = t[t["expiry"] >= a.day]
        print(t[["symbol", "instrument_key", "expiry", "strike", "lot_size"]].head(a.n).to_string(index=False))
        print(f"({len(t)} contracts)")
    else:
        day = a.day or ist_today()
        t = load_table()
        if t.empty:
            raise SystemExit("no instrument table: run import first")
        if a.underlying:
            spot = {**spot_from_lake(t, a.underlying, day), **_parse_spot(a.spot)}
            sel, notes = select_contracts(t, a.underlying, day, a.futures, a.option_expiries, a.strikes, spot,
                                          ("FUT",) if a.no_options else FO_TYPES)
        else:
            wl_path = a.watchlist or watchlist_path()
            if not wl_path.exists():
                raise SystemExit(f"give --underlying ... or --watchlist <csv> (default {wl_path} not found)")
            rows = read_watchlist(wl_path)
            underlyings = [r["underlying"] for r in rows]
            spot = {**spot_from_lake(t, underlyings, day), **_parse_spot(a.spot)}
            sel, notes = select_from_watchlist(t, rows, day, spot)
        if sel.empty:
            raise SystemExit(f"nothing selected; not found: {notes['missing']}")
        p = write_universe(sel, a.out)
        print(f"selected {len(sel)} contracts -> {p}\n{sel.groupby(['underlying', 'instrument_type']).size().to_string()}")
        if notes["missing"]:
            print("NOT FOUND:", notes["missing"])
        if notes["no_spot"]:
            print("no spot given for", notes["no_spot"], "- strikes are centred on the middle of the ladder; "
                  "pass --spot NAME=LEVEL for a proper at-the-money window")


if __name__ == "__main__":
    main()
