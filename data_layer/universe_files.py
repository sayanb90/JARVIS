"""
The universe files: which symbols the collectors work on.

  data/universe.csv          tracked: the India rows (NSE, with ISINs for the Upstox feed) and a few placeholder rows of other markets
  data/universe.local.csv    git-ignored, written by the admin upload page; when present it replaces universe.csv, so a `git pull` on the
                             server never conflicts with it
  data/universe_global.csv   git-ignored, built by `python -m data_layer.index_universe build`: the index constituents of every market but India

`read_universe(market)` gives India from the active file and every other market from the global file when it covers that market, else from
the active file's placeholder rows.
"""

from __future__ import annotations

import csv
import logging
from pathlib import Path

from .lake import ROOT

log = logging.getLogger(__name__)

DATA_DIR = ROOT / "data"
UNIVERSE_CSV = DATA_DIR / "universe.csv"

# the built-in India list used only when no universe file exists at all
NIFTY_50: list[str] = [
    "ADANIENT.NS",  "ADANIPORTS.NS", "APOLLOHOSP.NS", "ASIANPAINT.NS",
    "AXISBANK.NS",  "BAJAJ-AUTO.NS", "BAJFINANCE.NS", "BAJAJFINSV.NS",
    "BPCL.NS",      "BHARTIARTL.NS", "BRITANNIA.NS",  "CIPLA.NS",
    "COALINDIA.NS", "DIVISLAB.NS",   "DRREDDY.NS",    "EICHERMOT.NS",
    "GRASIM.NS",    "HCLTECH.NS",    "HDFCBANK.NS",   "HDFCLIFE.NS",
    "HEROMOTOCO.NS","HINDALCO.NS",   "HINDUNILVR.NS", "ICICIBANK.NS",
    "INDUSINDBK.NS","INFY.NS",       "ITC.NS",        "JSWSTEEL.NS",
    "KOTAKBANK.NS", "LT.NS",         "M&M.NS",        "MARUTI.NS",
    "NESTLEIND.NS", "NTPC.NS",       "ONGC.NS",       "POWERGRID.NS",
    "RELIANCE.NS",  "SBILIFE.NS",    "SBIN.NS",       "SHRIRAMFIN.NS",
    "SUNPHARMA.NS", "TATACONSUM.NS", "TATAMOTORS.NS", "TATASTEEL.NS",
    "TCS.NS",       "TECHM.NS",      "TITAN.NS",      "ULTRACEMCO.NS",
    "WIPRO.NS",     "ZOMATO.NS",
]


def active_universe_csv() -> Path:
    """The universe file in force: data/universe.local.csv when present, else the tracked data/universe.csv."""
    local = UNIVERSE_CSV.with_name("universe.local.csv")
    return local if local.exists() else UNIVERSE_CSV


def global_universe_csv() -> Path:
    return UNIVERSE_CSV.with_name("universe_global.csv")


def _global_rows() -> list[dict]:
    """The index constituents built by `python -m data_layer.index_universe build`: every market but India."""
    path = global_universe_csv()
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as f:
        return [{k: (v or "").strip() for k, v in rec.items()} for rec in csv.DictReader(r for r in f if not r.startswith("#"))
                if (rec.get("symbol") or "").strip()]


def read_universe(market: str | None = None) -> list[dict]:
    """
    Read the universe, optionally filtered by market.  India comes from the active universe file (see active_universe_csv); the
    other markets come from data/universe_global.csv when it exists (index constituents), else from the active file's few rows.
    Returns list of dicts with keys: symbol, name, market, exchange, isin, ...
    Falls back to NIFTY_50 if there is no universe file.
    """
    glob = _global_rows()
    if not glob or market == "IN":
        return _read_active_universe(market)
    have = {r.get("market") for r in glob}                    # a market the index build did not cover keeps the active file's rows
    if market is not None:
        return [r for r in glob if r.get("market") == market] if market in have else _read_active_universe(market)
    return [r for r in _read_active_universe(None) if r.get("market") not in have] + glob


def _read_active_universe(market: str | None) -> list[dict]:
    path = active_universe_csv()
    if not path.exists():
        log.warning("universe.csv not found — falling back to the built-in India list")
        rows = []
        if market in (None, "IN"):
            rows += [{"symbol": s, "market": "IN"} for s in NIFTY_50]
        return rows

    rows = []
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(row for row in f if not row.startswith("#"))
        for rec in reader:
            if market is None or rec.get("market", "").strip() == market:
                rows.append({k: v.strip() for k, v in rec.items()})
    return rows
