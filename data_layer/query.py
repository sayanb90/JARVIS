"""
JARVIS query layer — DuckDB over the Parquet lake.

  from data_layer.query import Lake
  lake = Lake()
  lake.latest_prices("IN")
  lake.ohlc("INFY.NS", "2024-01-01", "2024-12-31")
  lake.score_all("IN").head(10)

Every method returns a pandas DataFrame (or a scalar).  Tables are exposed as
views, so ad-hoc SQL works too:  lake.sql("SELECT ... FROM ohlc ...").
Missing tables (e.g. no bars collected yet) simply do not exist as views.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from .lake import LAKE_DIR

PE_MAX, ROIC_MIN, LT_DEBT_FCF_MAX, PFCF_MAX = 22.5, 0.09, 5.0, 20.0


class Lake:
    def __init__(self, lake_dir: Path | str | None = None) -> None:
        self.dir = Path(lake_dir) if lake_dir else LAKE_DIR
        self.con = duckdb.connect()
        for name in ("income_statement", "balance_sheet", "cash_flow", "ratios",
                     "pillar_metrics", "fund_meta"):
            f = self.dir / "fundamentals" / f"{name}.parquet"
            if f.exists():
                self.con.execute(f"CREATE VIEW {name} AS SELECT * FROM read_parquet('{f}')")
        if any((self.dir / "ohlc").glob("year=*/ohlc.parquet")):
            self.con.execute(
                f"CREATE VIEW ohlc AS SELECT * FROM "
                f"read_parquet('{self.dir}/ohlc/year=*/ohlc.parquet', hive_partitioning=false)")
        for iv in sorted(p.name.split("=", 1)[1] for p in (self.dir / "bars").glob("interval=*")):
            self.con.execute(
                f"CREATE VIEW bars_{iv} AS SELECT * FROM "
                f"read_parquet('{self.dir}/bars/interval={iv}/date=*/*.parquet', union_by_name=true)")

    # -- generic ------------------------------------------------------------
    def sql(self, query: str, params: list | None = None) -> pd.DataFrame:
        return self.con.execute(query, params or []).df()

    def tables(self) -> list[str]:
        return [r[0] for r in self.con.execute("SHOW TABLES").fetchall()]

    # -- universe -----------------------------------------------------------
    def symbols(self, market: str | None = None) -> list[str]:
        q = "SELECT symbol FROM pillar_metrics" + (" WHERE market = ?" if market else "") + " ORDER BY symbol"
        return [r[0] for r in self.con.execute(q, [market] if market else []).fetchall()]

    def markets(self) -> list[str]:
        return [r[0] for r in self.con.execute(
            "SELECT DISTINCT market FROM pillar_metrics ORDER BY 1").fetchall()]

    # -- prices -------------------------------------------------------------
    def latest_prices(self, market: str | None = None) -> pd.DataFrame:
        """Most recent close per symbol: symbol, market, date, close."""
        where = "WHERE market = ?" if market else ""
        return self.sql(
            f"SELECT symbol, market, date, close FROM ("
            f"SELECT *, row_number() OVER (PARTITION BY symbol ORDER BY date DESC) rn "
            f"FROM ohlc {where}) WHERE rn = 1 ORDER BY symbol",
            [market] if market else [])

    def latest_price(self, symbol: str) -> float | None:
        r = self.con.execute(
            "SELECT close FROM ohlc WHERE symbol = ? ORDER BY date DESC LIMIT 1", [symbol]).fetchone()
        return r[0] if r else None

    def ohlc(self, symbol: str, start: str | None = None, end: str | None = None) -> pd.DataFrame:
        return self.sql(
            "SELECT * FROM ohlc WHERE symbol = ? AND date >= COALESCE(CAST(? AS DATE), DATE '1900-01-01') "
            "AND date <= COALESCE(CAST(? AS DATE), DATE '2999-12-31') ORDER BY date",
            [symbol, start, end])

    def ohlc_market(self, market: str, start: str, end: str) -> pd.DataFrame:
        return self.sql("SELECT * FROM ohlc WHERE market = ? AND date BETWEEN CAST(? AS DATE) "
                        "AND CAST(? AS DATE) ORDER BY symbol, date", [market, start, end])

    def bars(self, symbol: str, start: str, end: str, interval: str = "5m") -> pd.DataFrame:
        """Intraday bars between two UTC timestamps/dates (inclusive of start, exclusive of end)."""
        return self.sql(f"SELECT * FROM bars_{interval} WHERE symbol = ? AND ts >= CAST(? AS TIMESTAMP) "
                        f"AND ts < CAST(? AS TIMESTAMP) ORDER BY ts", [symbol, start, end])

    # -- fundamentals -------------------------------------------------------
    def fundamentals(self, symbol: str) -> dict[str, pd.DataFrame]:
        return {t: self.sql(f"SELECT * FROM {t} WHERE symbol = ? ORDER BY date DESC", [symbol])
                for t in ("income_statement", "balance_sheet", "cash_flow", "ratios")}

    def pillar_metrics(self, market: str | None = None) -> pd.DataFrame:
        return self.sql("SELECT * FROM pillar_metrics" + (" WHERE market = ?" if market else "")
                        + " ORDER BY symbol", [market] if market else [])

    # -- scoring ------------------------------------------------------------
    def score_ticker(self, symbol: str, price: float) -> int:
        """
        8-pillar score, 0-8.  "Growth"/"decrease" pillars compare the newest
        annual row with the oldest one available (ordered by date, explicitly).
        """
        m = self.sql("SELECT * FROM pillar_metrics WHERE symbol = ?", [symbol])
        if m.empty or price is None:
            return 0
        m = m.iloc[0]

        def num(col):
            v = m[col]
            return None if pd.isna(v) else float(v)

        def grew(table: str, col: str, decreasing: bool = False) -> bool:
            s = self.sql(f"SELECT {col} v FROM {table} WHERE symbol = ? AND {col} IS NOT NULL "
                         f"ORDER BY date", [symbol])["v"]
            if len(s) < 2:
                return False
            return bool(s.iloc[-1] < s.iloc[0]) if decreasing else bool(s.iloc[-1] > s.iloc[0])

        shares, ni, fcf = num("latest_shares_outstanding"), num("avg_net_income_5y"), num("avg_fcf_5y")
        ltd, roic = num("latest_long_term_debt"), num("latest_roic")
        mkt_cap = price * shares if shares else None

        pillars = [
            bool(mkt_cap and ni and ni > 0 and mkt_cap / ni < PE_MAX),                 # 1 avg P/E
            bool(roic is not None and roic > ROIC_MIN),                                  # 2 ROIC
            grew("income_statement", "revenue"),                                         # 3 revenue growth
            grew("income_statement", "net_income"),                                      # 4 net income growth
            grew("ratios", "shares_outstanding", decreasing=True),                       # 5 shares falling
            bool(fcf and fcf > 0 and ltd is not None and ltd / fcf < LT_DEBT_FCF_MAX),   # 6 LT debt / FCF
            grew("cash_flow", "free_cash_flow"),                                         # 7 FCF growth
            bool(mkt_cap and fcf and fcf > 0 and mkt_cap / fcf < PFCF_MAX),              # 8 avg P/FCF
        ]
        return sum(pillars)

    def score_all(self, market: str | None = None) -> pd.DataFrame:
        """symbol, market, price, score — sorted by score desc (symbols with no price score 0)."""
        pm = self.pillar_metrics(market)[["symbol", "market"]]
        px = self.latest_prices(market)[["symbol", "close"]].rename(columns={"close": "price"})
        df = pm.merge(px, on="symbol", how="left")
        df["score"] = [self.score_ticker(s, p) if pd.notna(p) else 0
                       for s, p in zip(df["symbol"], df["price"])]
        return df.sort_values(["score", "symbol"], ascending=[False, True]).reset_index(drop=True)

    def top_by_score(self, market: str | None, n: int = 10) -> pd.DataFrame:
        return self.score_all(market).head(n)
