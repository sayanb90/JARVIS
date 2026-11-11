"""Shared builders for tests (no fixtures here, just plain functions)."""
from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

UNI_HEADER = ["symbol", "name", "market", "exchange", "isin", "cusip", "ric", "bbg_code", "sector", "currency"]
OHLC_H = ["date", "symbol", "market", "open", "high", "low", "close", "volume"]
BAR_H = ["ts", "symbol", "market", "open", "high", "low", "close", "volume"]


def write_csv(path: Path, header: list[str], rows: list[list]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        cw = csv.writer(f)
        cw.writerow(header)
        cw.writerows(rows)
    return path


def universe_rows(spec: dict[str, list[str]]) -> list[list]:
    """{'IN': ['INFY.NS'], 'US': ['AAPL']} -> universe.csv rows (ISIN only meaningful for IN)."""
    out = []
    for market, syms in spec.items():
        for i, s in enumerate(syms):
            isin = f"INE{abs(hash(s)) % 10**6:06d}A01{i:03d}"[:12] if market == "IN" else ""
            out.append([s, s + " Ltd", market, "NSE" if market == "IN" else "X", isin, "", "", "", "", ""])
    return out


def bday_frame(symbols: list[str], periods: int = 1300, end: pd.Timestamp | None = None,
               seed: int = 1) -> dict[str, pd.DataFrame]:
    """Yahoo-shaped {ticker: DataFrame(Open..Volume, DatetimeIndex)} on business days."""
    end = end if end is not None else pd.Timestamp.today().normalize() - pd.Timedelta(days=1)
    idx = pd.bdate_range(end=end, periods=periods)
    out = {}
    for k, t in enumerate(symbols):
        rng = np.random.default_rng(seed + k)
        c = np.abs(100 + np.cumsum(rng.normal(0, 1, len(idx)))) + 10
        out[t] = pd.DataFrame({"Open": c, "High": c + 1, "Low": c - 1, "Close": c,
                               "Volume": np.full(len(idx), 1_000_000)}, index=idx)
    return out


def ms(h: int, m: int, s: int, day: tuple[int, int, int] = (2026, 10, 23)) -> int:
    """Epoch milliseconds for a UTC wall-clock time."""
    return int((datetime(*day, h, m, s) - datetime(1970, 1, 1)).total_seconds() * 1000)


def seed_scoring(world, symbol="GOOD.NS", price=9.0, **o):
    """
    Load one ticker into the lake that scores 8/8 at price 9.0, then apply overrides that each break
    exactly one pillar.  Returns the Lake.  Overrides: avg_ni, avg_fcf, roic, ltd, shares=(newest, oldest),
    revenue=(newest, oldest), net_income=(newest, oldest), fcf=(newest, oldest), pm_shares, single_year.
    """
    avg_ni, avg_fcf = o.get("avg_ni", 45), o.get("avg_fcf", 45)
    roic, ltd = o.get("roic", 0.15), o.get("ltd", 50)
    shares = o.get("shares", (90, 100))
    pm_shares = o.get("pm_shares", shares[0])          # pillar_metrics share count (drives market cap)
    rev, ni, fcf = o.get("revenue", (200, 100)), o.get("net_income", (60, 30)), o.get("fcf", (70, 20))
    f = world.data / "fundamentals"
    write_csv(f / "income_statement_in.csv", ["date", "symbol", "market", "revenue", "net_income", "ebit"],
              [["2025.12.31", symbol, "IN", rev[0], ni[0], 1]] +
              ([] if o.get("single_year") else [["2021.12.31", symbol, "IN", rev[1], ni[1], 1]]))
    write_csv(f / "balance_sheet_in.csv", ["date", "symbol", "market", "total_assets", "total_liabilities",
                                          "long_term_debt", "total_equity"], [["2025.12.31", symbol, "IN", 1000, 400, ltd, 600]])
    write_csv(f / "cash_flow_in.csv", ["date", "symbol", "market", "free_cash_flow"],
              [["2025.12.31", symbol, "IN", fcf[0]], ["2021.12.31", symbol, "IN", fcf[1]]])
    write_csv(f / "ratios_in.csv", ["date", "symbol", "market", "roic", "shares_outstanding"],
              [["2025.12.31", symbol, "IN", roic, shares[0]], ["2021.12.31", symbol, "IN", roic, shares[1]]])
    write_csv(f / "pillar_metrics_in.csv",
              ["symbol", "market", "avg_net_income_5y", "avg_fcf_5y", "latest_revenue", "latest_ebit",
               "latest_total_assets", "latest_total_liabilities", "latest_long_term_debt",
               "latest_total_equity", "latest_roic", "latest_shares_outstanding"],
              [[symbol, "IN", avg_ni, avg_fcf, rev[0], 1, 1000, 400, ltd, 600, roic, pm_shares]])
    write_csv(world.data / "ohlc" / "ohlc_2026_in.csv", OHLC_H,
              [["2026.10.20", symbol, "IN", price, price, price, price, 1]])
    load_fundamentals(world.lake, ("IN",))
    world.lake.load_ohlc(("IN",))
    from data_layer.query import Lake
    return Lake()


def tick_folder(lake, day, segment: str = "eq") -> Path:
    """Where the raw ticks of a segment ("eq" stocks, "fo" futures and options) of a day live (SCRUM-133)."""
    return Path(lake) / "ticks" / f"segment={segment}" / f"date={day}"


def tick_file(lake, day, segment: str = "eq") -> Path:
    return tick_folder(lake, day, segment) / "ticks.parquet"


# -- legacy fundamentals CSVs -> lake tables (the loader itself was retired with FMP; scoring tests still seed through it) --------------
_STATEMENTS = {
    "income_statement": (["symbol", "date"], {"date": "DATE", "symbol": "VARCHAR", "market": "VARCHAR",
                                                "revenue": "DOUBLE", "net_income": "DOUBLE", "ebit": "DOUBLE"}),
    "balance_sheet": (["symbol", "date"], {"date": "DATE", "symbol": "VARCHAR", "market": "VARCHAR", "total_assets": "DOUBLE",
                                             "total_liabilities": "DOUBLE", "long_term_debt": "DOUBLE", "total_equity": "DOUBLE"}),
    "cash_flow": (["symbol", "date"], {"date": "DATE", "symbol": "VARCHAR", "market": "VARCHAR", "free_cash_flow": "DOUBLE"}),
    "ratios": (["symbol", "date"], {"date": "DATE", "symbol": "VARCHAR", "market": "VARCHAR", "roic": "DOUBLE",
                                      "shares_outstanding": "DOUBLE"}),
    "pillar_metrics": (["symbol"], {"symbol": "VARCHAR", "market": "VARCHAR", "avg_net_income_5y": "DOUBLE", "avg_fcf_5y": "DOUBLE",
                                      "latest_revenue": "DOUBLE", "latest_ebit": "DOUBLE", "latest_total_assets": "DOUBLE",
                                      "latest_total_liabilities": "DOUBLE", "latest_long_term_debt": "DOUBLE",
                                      "latest_total_equity": "DOUBLE", "latest_roic": "DOUBLE",
                                      "latest_shares_outstanding": "DOUBLE"}),
}


def load_fundamentals(lake, markets=("IN",)) -> None:
    """Load data/fundamentals/<table>_<market>.csv files into lake/fundamentals/<table>.parquet (test seeding only)."""
    import duckdb
    con = duckdb.connect()
    for name, (keys, types) in _STATEMENTS.items():
        for csv_path in lake._csv_paths(f"fundamentals/{name}_{{m}}.csv", markets):
            lake._upsert(con, lake.LAKE_DIR / "fundamentals" / f"{name}.parquet", lake._read_csv_sql(csv_path, types), keys, keys)
