"""
Daily FX rates (SCRUM-160): so earnings, cash flow and debt in one currency can be set against a price in another.

  python -m data_layer.fx refresh [--period 5y]      # Yahoo daily closes of the FX pairs -> lake/fx/rates.parquet
  python -m data_layer.fx show [CCY]                 # newest rate per currency, or the recent history of one
  python -m data_layer.fx convert 1000 CNY HKD [--date 2026-11-10]

Why: the pillars divide market cap (in the price's currency) by earnings or free cash flow (in the statements' currency).  They
differ: London prices are in pence while statements are in pounds or dollars (Shell reports in USD), and Hong Kong-listed Chinese
companies price in HKD and report in RMB.  Dividing one by the other without a conversion gives a wrong P/E.

Storage: lake/fx/rates.parquet, one row per (date, currency): usd_per_unit = how many US dollars one unit of the currency buys, the
Yahoo close of "<CCY>USD=X" (or the inverse of "USD<CCY>=X" when the first pair has no data).  Upserted on (date, currency) and replaced
atomically, so re-running a refresh is harmless.

Rules for the conversion:
  * pence (GBp, GBX) are 1/100 of a pound, and the same for the other minor units in MINOR_UNITS;
  * a day without a rate (weekend, holiday) uses the latest rate before it, up to MAX_STALE_DAYS old;
  * a currency with no usable rate gives None: never 1.0, never a guess.
"""

from __future__ import annotations

import argparse
import bisect
import logging
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Callable, Optional

import duckdb
import pandas as pd

from . import lake as lake_mod

log = logging.getLogger(__name__)

# the currencies of the universe's listings plus the usual reporting currencies; more are added with refresh(currencies=...)
CURRENCIES = ("EUR", "GBP", "JPY", "KRW", "TWD", "SGD", "CNY", "HKD", "INR", "CHF", "SEK", "DKK", "NOK", "PLN", "CZK", "HUF", "AUD", "CAD")
REQUIRED = ("EUR", "GBP", "JPY", "KRW", "TWD", "SGD", "CNY", "HKD", "INR")      # a refresh without one of these is a failure
MINOR_UNITS = {"GBP": ("GBp", "GBX"), "ZAR": ("ZAc", "ZAC"), "ILS": ("ILA", "ILa")}
MAX_STALE_DAYS = 7


def rates_path(lake_dir: Path | None = None) -> Path:
    return (lake_dir or lake_mod.LAKE_DIR) / "fx" / "rates.parquet"


def split_currency(code: str) -> tuple[str, float]:
    """("GBp") -> ("GBP", 0.01): the major currency and how many of it one unit of `code` is."""
    c = (code or "").strip()
    for major, minors in MINOR_UNITS.items():
        if c in minors:
            return major, 0.01
    return c.upper(), 1.0


# ---------------------------------------------------------------------------------------------- the lookup
@dataclass
class Rates:
    """Rates by currency, for fast "latest on or before" lookups."""
    _by: dict[str, tuple[list[date], list[float]]] = field(default_factory=dict)

    @classmethod
    def from_frame(cls, df: pd.DataFrame) -> "Rates":
        out: dict[str, tuple[list[date], list[float]]] = {}
        for ccy, g in df.sort_values("date").groupby("currency"):
            out[str(ccy)] = ([pd.Timestamp(d).date() for d in g["date"]], [float(v) for v in g["usd_per_unit"]])
        return cls(out)

    def currencies(self) -> list[str]:
        return sorted(self._by)

    def newest(self) -> Optional[date]:
        return max((d[-1] for d, _ in self._by.values()), default=None)

    def usd_per_unit(self, currency: str, day: date, max_stale_days: int = MAX_STALE_DAYS) -> Optional[float]:
        """US dollars per unit of `currency` on `day` (the latest earlier rate when there is none that day), else None."""
        major, scale = split_currency(currency)
        if major == "USD":
            return scale
        if major not in self._by:
            return None
        dates, vals = self._by[major]
        i = bisect.bisect_right(dates, day) - 1
        if i < 0 or (day - dates[i]).days > max_stale_days:
            return None
        return vals[i] * scale

    def convert(self, amount: Optional[float], from_ccy: str, to_ccy: str, day: date, max_stale_days: int = MAX_STALE_DAYS) -> Optional[float]:
        """`amount` of `from_ccy` expressed in `to_ccy` on `day`; None when either rate is missing."""
        if amount is None or pd.isna(amount):
            return None
        if split_currency(from_ccy) == split_currency(to_ccy):
            return float(amount)
        a, b = self.usd_per_unit(from_ccy, day, max_stale_days), self.usd_per_unit(to_ccy, day, max_stale_days)
        if a is None or b is None or b == 0:
            return None
        return float(amount) * a / b


def load_rates(lake_dir: Path | None = None) -> Optional[Rates]:
    p = rates_path(lake_dir)
    if not p.exists():
        return None
    try:
        return Rates.from_frame(duckdb.connect().execute(f"SELECT date, currency, usd_per_unit FROM read_parquet('{p}')").df())
    except Exception as exc:                                           # noqa: BLE001
        log.warning("cannot read %s: %s", p, exc)
        return None


# ---------------------------------------------------------------------------------------------- fetching
def _closes(df: pd.DataFrame) -> pd.Series:
    close = df["Close"] if "Close" in df.columns else df.iloc[:, 0]
    return close.dropna()[lambda s: s > 0]


def fetch_rates(currencies, period: str = "5y", download: Optional[Callable] = None) -> tuple[pd.DataFrame, list[str]]:
    """(rows, missing): daily usd_per_unit per currency from Yahoo, and the currencies for which neither pair had data."""
    if download is None:
        from .yf_client import _batch_download
        download = lambda tickers, **kw: _batch_download(tickers, period=period, **kw)      # noqa: E731
    wanted = sorted({split_currency(c)[0] for c in currencies} - {"USD"})
    direct = download([f"{c}USD=X" for c in wanted])
    frames, missing = [], []
    for c in wanted:
        s = _closes(direct[f"{c}USD=X"]) if f"{c}USD=X" in direct else pd.Series(dtype=float)
        if s.empty:
            missing.append(c)
            continue
        frames.append(pd.DataFrame({"date": [pd.Timestamp(d).date() for d in s.index], "currency": c, "usd_per_unit": s.to_numpy()}))
    if missing:                                                        # the other way round: "USD<CCY>=X" is CCY per dollar
        inverse = download([f"USD{c}=X" for c in missing])
        still = []
        for c in missing:
            s = _closes(inverse[f"USD{c}=X"]) if f"USD{c}=X" in inverse else pd.Series(dtype=float)
            if s.empty:
                still.append(c)
                continue
            frames.append(pd.DataFrame({"date": [pd.Timestamp(d).date() for d in s.index], "currency": c, "usd_per_unit": 1.0 / s.to_numpy()}))
        missing = still
    rows = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=["date", "currency", "usd_per_unit"])
    return rows, missing


def refresh(lake_dir: Path | None = None, currencies=None, period: Optional[str] = None, download: Optional[Callable] = None,
            now: Optional[datetime] = None) -> dict:
    """Fetch and store the rates.  The first run (no file) takes five years; later runs the last ten days, which overlap the stored
    ones.  A failed or empty answer keeps the file.  Returns {"ok", "rows", "currencies", "missing", "newest", "message"}."""
    path = rates_path(lake_dir)
    currencies = tuple(currencies) if currencies else CURRENCIES
    period = period or ("10d" if path.exists() else "5y")
    try:
        rows, missing = fetch_rates(currencies, period, download)
    except Exception as exc:                                           # noqa: BLE001 -- Yahoo problems are data here
        return {"ok": False, "rows": 0, "currencies": 0, "missing": [], "newest": None, "message": f"download failed: {type(exc).__name__}: {str(exc)[:100]}"}
    if rows.empty:
        return {"ok": False, "rows": 0, "currencies": 0, "missing": missing, "newest": None, "message": "Yahoo returned no FX rates: kept the existing file"}
    rows["source"] = "yahoo"
    rows["fetched_at"] = (now or datetime.now(timezone.utc).replace(tzinfo=None)).isoformat(timespec="seconds")
    con = duckdb.connect()
    con.register("new_rates", rows)
    n = lake_mod._upsert(con, path, "SELECT * FROM new_rates", ["date", "currency"], ["currency", "date"])
    required_missing = sorted(set(missing) & set(REQUIRED))
    newest = rows["date"].max()
    msg = f"stored {len(rows)} rates for {rows['currency'].nunique()} currencies (file now {n} rows), newest {newest}"
    if missing:
        msg += f"; no data for {', '.join(missing)}"
    return {"ok": not required_missing, "rows": len(rows), "currencies": int(rows["currency"].nunique()), "missing": missing,
            "newest": newest, "message": msg}


# ---------------------------------------------------------------------------------------------- command line
def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Daily FX rates from Yahoo")
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("refresh")
    r.add_argument("--period", default=None, help="Yahoo period (default 5y the first time, then 10d)")
    r.add_argument("--currencies", default=None, help="comma-separated, in addition to the standard list")
    s = sub.add_parser("show")
    s.add_argument("currency", nargs="?", default=None)
    c = sub.add_parser("convert")
    c.add_argument("amount", type=float)
    c.add_argument("from_ccy")
    c.add_argument("to_ccy")
    c.add_argument("--date", default=None)
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if a.cmd == "refresh":
        extra = [x.strip() for x in a.currencies.split(",")] if a.currencies else []
        res = refresh(currencies=list(CURRENCIES) + extra, period=a.period)
        print(f"{datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S} fx: {res['message']}")
        return 0 if res["ok"] else 1
    rates = load_rates()
    if rates is None:
        print("no FX rates yet: run python -m data_layer.fx refresh")
        return 1
    if a.cmd == "show":
        if a.currency:
            major, _ = split_currency(a.currency)
            dates, vals = rates._by.get(major, ([], []))
            for d, v in list(zip(dates, vals))[-10:]:
                print(f"{d}  1 {major} = {v:.6f} USD")
            return 0 if dates else 1
        for ccy in rates.currencies():
            d, v = rates._by[ccy][0][-1], rates._by[ccy][1][-1]
            print(f"{ccy}  {d}  1 {ccy} = {v:.6f} USD")
        return 0
    day = date.fromisoformat(a.date) if a.date else datetime.now(timezone.utc).date()
    out = rates.convert(a.amount, a.from_ccy, a.to_ccy, day)
    print("no rate available" if out is None else f"{a.amount:g} {a.from_ccy} = {out:,.4f} {a.to_ccy} on {day}")
    return 0 if out is not None else 1


if __name__ == "__main__":
    sys.exit(main())
